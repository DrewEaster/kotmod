package io.kotmod.scheduling.dbscheduler

import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.reaction.GiveUp
import io.kotmod.reaction.ReactionResult
import io.kotmod.reaction.ReactionTimeoutException
import io.kotmod.reaction.Retry
import io.kotmod.support.OrderShipped
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * End-to-end failure scenarios on Postgres and db-scheduler. A "fix deployed later" is simulated with a flag the test
 * flips once it has seen the failure parked, so the parked state is observed deterministically before it is resolved.
 */
class EventPolicyFailuresIntegrationTest : IntegrationTest() {
    private fun Seen.forOrder(orderId: String) = handled.filter { it is Confirm && it.orderId == orderId }

    private fun reactorOffset() = PostgresOffsetManager(jdbc).getPosition("reactor").globalOffset

    @Test
    fun `a broken mapping is parked in its own event policy while the others run, and after the fix it runs exactly once`() =
        runBlocking<Unit> {
            val fixed = AtomicBoolean(false)
            val failures = AtomicInteger()
            val emails = OrderWork("emails")
            val audits =
                OrderWork("audits", mapping = { _, m ->
                    if (m.aggregateId.value == "o-1" && !fixed.get()) {
                        failures.incrementAndGet()
                        error("fix not deployed yet")
                    }
                    trigger(Confirm(m.aggregateId.value, m.sequence))
                })

            runningReactor(dataSource, jdbc, listOf(emails, audits)) { _, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-2", 1)
                eventually { emails.seen.handled.size == 2 && audits.seen.forOrder("o-2").isNotEmpty() && reactorOffset() == 2L }
                eventually { failures.get() >= 2 } // the reader's attempt, then the parked mapping's own retry
                assertNotNull(operations.parked("audits", "e-1"), "the failure is parked in audits")
                assertNull(operations.parked("emails", "e-1"), "and only in audits")
                assertTrue(audits.seen.forOrder("o-1").isEmpty())

                fixed.set(true)
                eventually(15.seconds) { audits.seen.forOrder("o-1").isNotEmpty() }
                eventually { operations.parked("audits", "e-1") == null }
                delay(500)
            }

            assertEquals(setOf<Work>(Confirm("o-1", 1), Confirm("o-2", 1)), emails.seen.handled.toSet())
            assertEquals(2, emails.seen.handled.size)
            assertEquals(listOf<Work>(Confirm("o-1", 1)), audits.seen.forOrder("o-1"))
            assertEquals(listOf("audits/e-1/0"), audits.seen.contexts.map { it.reactionId }.filter { it.contains("e-1") })
        }

    @Test
    fun `with ordering, a parked mapping holds back only its aggregate, and after the fix that aggregate runs in sequence order`() =
        runBlocking<Unit> {
            val fixed = AtomicBoolean(false)
            val failures = AtomicInteger()
            val projection =
                OrderWork("projection", ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    if (m.aggregateId.value == "o-1" && m.sequence == 1L && !fixed.get()) {
                        failures.incrementAndGet()
                        error("fix not deployed yet")
                    }
                    trigger(Confirm(m.aggregateId.value, m.sequence))
                })

            runningReactor(dataSource, jdbc, listOf(projection)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-1", 2, OrderShipped("book"))
                jdbc.appendOrderEvent("e-3", "o-2", 1)
                eventually { projection.seen.forOrder("o-2").isNotEmpty() }
                // e-1 is parked and e-2's trigger is queued behind it, still failing.
                eventually { failures.get() >= 2 && operations.parked("projection", "e-1") != null }
                eventually { "projection/e-2/0" in jdbc.reactionRowIds("projection") }
                delay(1_000)
                assertTrue(projection.seen.forOrder("o-1").isEmpty(), "o-1 waits behind its parked event")
                // An empty list is accepted on purpose: the front may be picked (running) as we sample, and picked rows aren't listed.
                assertTrue(
                    scheduler.lineInstances("projection", "Order/o-1").all { it == "line/Order/o-1/projection/e-1/mapping" },
                    "only o-1's front, its parked mapping, is scheduled",
                )

                fixed.set(true)
                eventually(20.seconds) { projection.seen.forOrder("o-1").size == 2 }
            }

            assertEquals(listOf<Work>(Confirm("o-1", 1), Confirm("o-1", 2)), projection.seen.forOrder("o-1"))
            assertEquals(Confirm("o-2", 1), projection.seen.handled.first())
        }

    @Test
    fun `a mapping that always throws stays parked and keeps retrying, and is never dropped`() =
        runBlocking<Unit> {
            val attempts = AtomicInteger()
            val broken =
                OrderWork("broken", mapping = { _, _ ->
                    attempts.incrementAndGet()
                    error("always broken")
                })
            val emails = OrderWork("emails")

            runningReactor(dataSource, jdbc, listOf(broken, emails)) { _, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                eventually(15.seconds) { (operations.parked("broken", "e-1")?.attempts ?: 0) >= 2 }
                assertTrue(attempts.get() >= 3, "the reader's attempt and at least two retries of the parked mapping")
                assertNotNull(operations.parked("broken", "e-1"), "still parked: never dropped")
                assertEquals(
                    listOf(EventId("e-1") to EventReactionId("broken/e-1/mapping")),
                    operations.parkedMappings("broken").map { it.eventId to it.reactionId },
                )
                assertTrue(operations.parkedMappings("broken").single().attempts >= 2)
                assertTrue(operations.parkedMappings("emails").isEmpty())
                assertEquals(1, emails.seen.handled.size)
            }

            assertTrue(broken.seen.contexts.isEmpty())
        }

    @Test
    fun `replaying the log while a mapping is parked parks it only once, and after the fix the work runs once`() =
        runBlocking<Unit> {
            val fixed = AtomicBoolean(false)
            val failures = AtomicInteger()
            val audits =
                OrderWork("audits", mapping = { _, m ->
                    if (!fixed.get()) {
                        failures.incrementAndGet()
                        error("fix not deployed yet")
                    }
                    trigger(Confirm(m.aggregateId.value, m.sequence))
                })
            val offsets = PostgresOffsetManager(jdbc)

            runningReactor(dataSource, jdbc, listOf(audits)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                eventually { operations.parked("audits", "e-1") != null && reactorOffset() == 1L }
                val before = failures.get()
                offsets.savePosition("reactor", EventLogPosition.START) // as if the reader crashed before saving its position
                eventually { reactorOffset() == 1L && failures.get() > before } // the reader really read e-1 again
                assertEquals(listOf("audits/e-1/mapping"), jdbc.reactionRowIds("audits"), "parked once")
                assertTrue(scheduler.instanceIds("audits").count { it.endsWith("audits/e-1/mapping") } <= 1, "scheduled once")

                fixed.set(true)
                eventually(15.seconds) { audits.seen.handled.isNotEmpty() && scheduler.instanceIds("audits").isEmpty() }
                delay(500)
            }

            assertEquals(listOf<Work>(Confirm("o-1", 1)), audits.seen.handled.toList())
            assertEquals(listOf("audits/e-1/0"), audits.seen.contexts.map { it.reactionId })
        }

    @Test
    fun `a failing handle retries only that reaction, and with ordering only its aggregate waits, in its event policy only`() =
        runBlocking<Unit> {
            // o-1's first reaction keeps failing until o-2 has been handled and o-1's second is queued behind it, so
            // the order the projection sees is deterministic.
            val failing = AtomicBoolean(true)
            val projection =
                OrderWork("projection", ordering = ReactionOrdering.PerAggregate(), work = { w, _ ->
                    if (w == Confirm("o-1", 1) && failing.get()) error("projection down")
                })
            val emails = OrderWork("emails")

            runningReactor(dataSource, jdbc, listOf(projection, emails)) { scheduler, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-1", 2, OrderShipped("book"))
                jdbc.appendOrderEvent("e-3", "o-2", 1)
                eventually { projection.seen.handled.contains(Confirm("o-2", 1)) && projection.seen.failures.isNotEmpty() }
                eventually { "projection/e-2/0" in jdbc.reactionRowIds("projection") }
                assertEquals(listOf<Work>(Confirm("o-2", 1)), projection.seen.handled.toList(), "o-1's second event waits behind the failing first")
                // An empty list is accepted on purpose: the front may be picked (running) as we sample, and picked rows aren't listed.
                assertTrue(
                    scheduler.lineInstances("projection", "Order/o-1").all { it == "line/Order/o-1/projection/e-1/0" },
                    "only o-1's front, the failing first, is scheduled",
                )

                failing.set(false)
                eventually(15.seconds) { projection.seen.handled.size == 3 && emails.seen.handled.size == 3 }
                delay(500)
            }

            assertEquals(Confirm("o-2", 1), projection.seen.handled.first())
            assertEquals(listOf<Work>(Confirm("o-1", 1), Confirm("o-1", 2)), projection.seen.forOrder("o-1"))
            val attempts = projection.seen.contexts.filter { it.reactionId == "projection/e-1/0" }.map { it.attempt }
            assertEquals((0 until attempts.size).toList(), attempts, "each retry counted once")
            assertEquals(attempts.size - 1, projection.seen.failures.size, "every attempt but the last failed")
            assertEquals(1, projection.seen.contexts.count { it.reactionId == "projection/e-2/0" }, "the later reaction ran once, not retried")
            assertEquals(listOf(0, 0, 0), emails.seen.contexts.map { it.attempt }, "the other event policy never retried")
        }

    @Test
    fun `an ordered event policy that produces a delayed trigger parks the event rather than stalling the reader`() =
        runBlocking<Unit> {
            val projection =
                OrderWork("projection", ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    trigger(Remind(m.aggregateId.value), notBefore = Clock.System.now() + 1.hours)
                })
            val emails = OrderWork("emails")

            runningReactor(dataSource, jdbc, listOf(projection, emails)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-2", 1)
                eventually { emails.seen.handled.size == 2 && reactorOffset() == 2L }
                eventually { operations.parked("projection", "e-1") != null && operations.parked("projection", "e-2") != null }
                assertTrue(scheduler.instanceIds("projection").all { it.endsWith("/mapping") }, "no delayed trigger was queued")
                assertTrue(jdbc.reactionRowIds("projection").all { it.endsWith("/mapping") }, "no delayed trigger was queued")
            }

            assertTrue(projection.seen.contexts.isEmpty())
        }

    @Test
    fun `GiveUp ends the reaction and onCompletion hears why, a timeout is a failure, and success completes`() =
        runBlocking<Unit> {
            val charges =
                OrderWork("charges", work = { _, _ -> error("card declined") }, decide = { _, attempt, _ -> if (attempt < 1) Retry(100.milliseconds) else GiveUp })
            val slow = OrderWork("slow", timeout = 200.milliseconds, work = { _, _ -> delay(5.seconds) }, decide = { _, _, _ -> GiveUp })
            val emails = OrderWork("emails")

            runningReactor(dataSource, jdbc, listOf(charges, slow, emails)) { scheduler, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                eventually(10.seconds) { listOf(charges, slow, emails).all { it.seen.completions.isNotEmpty() } }
                eventually { listOf("charges", "slow", "emails").all { scheduler.instanceIds(it).isEmpty() } }
                delay(500)
            }

            assertEquals(listOf(0, 1), charges.seen.contexts.map { it.attempt })
            assertTrue(charges.seen.handled.isEmpty())
            assertEquals("card declined", assertIs<ReactionResult.GaveUp>(charges.seen.completions.single().second).error.message)
            assertEquals(1, slow.seen.contexts.size)
            assertIs<ReactionTimeoutException>(slow.seen.failures.single())
            assertIs<ReactionTimeoutException>(assertIs<ReactionResult.GaveUp>(slow.seen.completions.single().second).error)
            assertEquals(listOf<Pair<Work, ReactionResult>>(Confirm("o-1", 1) to ReactionResult.Completed), emails.seen.completions.toList())
        }

    @Test
    fun `an ordered event policy that gives up with BlockAggregate holds its aggregate until an operator retries it`() =
        runBlocking<Unit> {
            val failing = AtomicBoolean(true)
            val projection =
                OrderWork(
                    "projection",
                    ordering = ReactionOrdering.PerAggregate(onGiveUp = OnGiveUp.BlockAggregate),
                    work = { w, _ -> if (w == Confirm("o-1", 1) && failing.get()) error("bad row") },
                    decide = { _, _, _ -> GiveUp },
                )

            runningReactor(dataSource, jdbc, listOf(projection)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-1", 2, OrderShipped("book"))
                jdbc.appendOrderEvent("e-3", "o-2", 1)
                eventually { operations.blockedReactions("projection").isNotEmpty() }
                eventually { projection.seen.handled.contains(Confirm("o-2", 1)) } // another aggregate still flows
                eventually { "projection/e-2/0" in jdbc.reactionRowIds("projection") }
                delay(1_000)
                assertEquals(listOf<Work>(Confirm("o-2", 1)), projection.seen.handled.toList(), "o-1's second event waits behind the blocked first")
                assertEquals(emptyList(), scheduler.lineInstances("projection", "Order/o-1"), "a blocked line has nothing scheduled")
                assertEquals(listOf(EventReactionId("projection/e-1/0")), operations.blockedReactions("projection").map { it.reactionId })
                assertIs<ReactionResult.GaveUp>(projection.seen.completions.single { it.first == Confirm("o-1", 1) }.second)

                failing.set(false)
                operations.retryBlocked("projection", EventReactionId("projection/e-1/0"))
                eventually(10.seconds) { projection.seen.handled.size == 3 }
                assertTrue(operations.blockedReactions("projection").isEmpty())
            }

            assertEquals(listOf<Work>(Confirm("o-1", 1), Confirm("o-1", 2)), projection.seen.forOrder("o-1"))
        }

    @Test
    fun `an operator can skip a blocked reaction so its aggregate moves on`() =
        runBlocking<Unit> {
            val projection =
                OrderWork(
                    "projection",
                    ordering = ReactionOrdering.PerAggregate(onGiveUp = OnGiveUp.BlockAggregate),
                    work = { w, _ -> if (w == Confirm("o-1", 1)) error("bad row") },
                    decide = { _, _, _ -> GiveUp },
                )

            runningReactor(dataSource, jdbc, listOf(projection)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-1", 2, OrderShipped("book"))
                eventually { operations.blockedReactions("projection").isNotEmpty() }
                eventually { "projection/e-2/0" in jdbc.reactionRowIds("projection") }
                delay(1_000) // e-2's reaction is queued, but held back
                assertTrue(projection.seen.handled.isEmpty(), "o-1's second event waits behind the blocked first")
                assertEquals(emptyList(), scheduler.instanceIds("projection"), "a blocked line has nothing scheduled")

                operations.skipBlocked("projection", EventReactionId("projection/e-1/0"))
                eventually(10.seconds) { projection.seen.handled.isNotEmpty() && scheduler.instanceIds("projection").isEmpty() }
                delay(500)
            }

            assertEquals(listOf<Work>(Confirm("o-1", 2)), projection.seen.handled.toList())
            assertEquals(1, projection.seen.contexts.count { it.reactionId == "projection/e-1/0" }, "the skipped reaction never ran again")
        }

    @Test
    fun `an operator can skip a parked mapping that will never succeed, so its aggregate moves on`() =
        runBlocking<Unit> {
            val projection =
                OrderWork("projection", ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    if (m.eventId.value == "e-1") error("this event can never be mapped")
                    trigger(Confirm(m.aggregateId.value, m.sequence))
                })

            runningReactor(dataSource, jdbc, listOf(projection)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-1", 2, OrderShipped("book"))
                eventually { "projection/e-2/0" in jdbc.reactionRowIds("projection") }
                // Wait for the parked mapping's third attempt, so the next one is 4s away.
                eventually(15.seconds) { (operations.parkedMappings("projection").singleOrNull()?.attempts ?: 0) >= 3 }
                assertTrue(projection.seen.handled.isEmpty(), "o-1's second event waits behind the parked first")
                // An empty list is accepted on purpose: the front may be picked (running) as we sample, and picked rows aren't listed.
                assertTrue(
                    scheduler.lineInstances("projection", "Order/o-1").all { it == "line/Order/o-1/projection/e-1/mapping" },
                    "only o-1's front, its parked mapping, is scheduled",
                )
                assertEquals(EventReactionId("projection/e-1/mapping"), operations.parkedMappings("projection").single().reactionId)

                // Refused while the third attempt is still running, so tried again as an operator would.
                retryWhileRunning { operations.skipParked("projection", EventId("e-1")) }
                eventually(10.seconds) { projection.seen.handled.isNotEmpty() && scheduler.instanceIds("projection").isEmpty() }
                delay(500)
            }

            assertEquals(listOf<Work>(Confirm("o-1", 2)), projection.seen.handled.toList())
        }
}

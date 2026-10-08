package io.kotmod.scheduling.dbscheduler

import io.kotmod.EventLogPosition
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.reaction.FailureDecision
import io.kotmod.reaction.GiveUp
import io.kotmod.reaction.ReactionContext
import io.kotmod.reaction.Retry
import io.kotmod.support.OrderShipped
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Ordered event policies end to end: kotmod's lines in `ddd_reaction_row`, run by db-scheduler. */
class OrderedReactionsIntegrationTest : IntegrationTest() {
    /** "start:<name>" / "end:<name>" per attempt, where a [Confirm]'s name is its order id and sequence (e.g. `a1`). */
    private class Log {
        val events = CopyOnWriteArrayList<String>()

        fun starts() = events.filter { it.startsWith("start:") }

        fun ends() = events.count { it.startsWith("end:") }
    }

    private fun Work.label() =
        when (this) {
            is Confirm -> "$orderId$sequence"
            is Remind -> "remind-$orderId"
            is Flag -> "flag-$customerId"
        }

    /**
     * An ordered event policy on test orders that logs each attempt: [succeeds] decides it, and a failure is retried after
     * 100ms, or given up on with [giveUp] (under [onGiveUp]).
     */
    private fun ordered(
        log: Log,
        name: String = "ordered",
        onGiveUp: OnGiveUp = OnGiveUp.ContinueWithNext,
        giveUp: Boolean = false,
        succeeds: suspend (Work, ReactionContext) -> Boolean = { _, _ ->
            delay(50)
            true
        },
    ): OrderWork {
        val decide: (Work, Int, Throwable) -> FailureDecision = { _, _, _ -> if (giveUp) GiveUp else Retry(100.milliseconds) }
        return OrderWork(
            name,
            ordering = ReactionOrdering.PerAggregate(onGiveUp),
            work = { work, context ->
                log.events += "start:${work.label()}"
                val ok =
                    try {
                        succeeds(work, context)
                    } finally {
                        log.events += "end:${work.label()}"
                    }
                if (!ok) error("${work.label()} failed")
            },
            decide = decide,
        )
    }

    private fun neverOverlap(log: Log): Boolean {
        var running = 0
        for (e in log.events) {
            running += if (e.startsWith("start:")) 1 else -1
            if (running > 1) return false
        }
        return true
    }

    @Test
    fun `reactions for one aggregate run one at a time in sequence order`() =
        runBlocking {
            val log = Log()
            val policy = ordered(log)

            runningReactor(dataSource, jdbc, listOf(policy)) { _, _ ->
                (1..8L).forEach { jdbc.appendOrderEvent("e-$it", "a", it) }
                eventually(20.seconds) { log.ends() == 8 }
            }

            assertTrue(neverOverlap(log), log.events.toString())
            assertEquals((1..8).map { "start:a$it" }, log.starts())
        }

    @Test
    fun `different aggregates run in parallel`() =
        runBlocking {
            val log = Log()
            val policy =
                ordered(log) { _, _ ->
                    delay(500)
                    true
                }

            runningReactor(dataSource, jdbc, listOf(policy)) { _, _ ->
                jdbc.appendOrderEvent("e-a", "a", 1)
                jdbc.appendOrderEvent("e-b", "b", 1)
                eventually(10.seconds) { log.ends() == 2 }
            }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `a retrying head holds back later reactions`() =
        runBlocking {
            val log = Log()
            val policy = ordered(log) { work, context -> !(work == Confirm("a", 1) && context.attempt < 2) }

            runningReactor(dataSource, jdbc, listOf(policy)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "a", 1)
                jdbc.appendOrderEvent("e-2", "a", 2, OrderShipped("book"))
                eventually(15.seconds) { log.events.contains("end:a2") }
            }

            assertEquals(listOf("start:a1", "start:a1", "start:a1", "start:a2"), log.starts())
        }

    @Test
    fun `ContinueWithNext moves on after a give-up`() =
        runBlocking {
            val log = Log()
            val policy = ordered(log, onGiveUp = OnGiveUp.ContinueWithNext, giveUp = true) { work, _ -> work != Confirm("a", 1) }

            runningReactor(dataSource, jdbc, listOf(policy)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "a", 1)
                jdbc.appendOrderEvent("e-2", "a", 2, OrderShipped("book"))
                eventually(10.seconds) { log.events.contains("end:a2") }
            }

            assertEquals(listOf("start:a1", "start:a2"), log.starts())
        }

    @Test
    fun `BlockAggregate holds the aggregate until retried, with nothing scheduled meanwhile`() =
        runBlocking {
            val log = Log()
            val failures = AtomicInteger()
            val policy =
                ordered(log, onGiveUp = OnGiveUp.BlockAggregate, giveUp = true) { work, _ ->
                    !(work == Confirm("a", 1) && failures.getAndIncrement() == 0)
                }

            runningReactor(dataSource, jdbc, listOf(policy)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "a", 1)
                jdbc.appendOrderEvent("e-2", "a", 2, OrderShipped("book"))
                eventually { operations.blockedReactions("ordered").isNotEmpty() }
                eventually { "ordered/e-2/0" in jdbc.reactionRowIds("ordered") }
                delay(1000)
                assertTrue("start:a2" !in log.events, "a2 must wait behind the blocked a1")
                assertEquals(emptyList(), scheduler.instanceIds("ordered"), "a blocked line has nothing scheduled")
                assertEquals(listOf(EventReactionId("ordered/e-1/0")), operations.blockedReactions("ordered").map { it.reactionId })

                operations.retryBlocked("ordered", EventReactionId("ordered/e-1/0"))
                eventually(10.seconds) { log.events.contains("end:a2") }
            }
            assertEquals(listOf("start:a1", "start:a1", "start:a2"), log.starts())
        }

    @Test
    fun `skipBlocked releases the aggregate without running the blocked reaction again`() =
        runBlocking {
            val log = Log()
            val policy = ordered(log, onGiveUp = OnGiveUp.BlockAggregate, giveUp = true) { work, _ -> work != Confirm("a", 1) }

            runningReactor(dataSource, jdbc, listOf(policy)) { _, operations ->
                jdbc.appendOrderEvent("e-1", "a", 1)
                jdbc.appendOrderEvent("e-2", "a", 2, OrderShipped("book"))
                eventually { operations.blockedReactions("ordered").isNotEmpty() }
                eventually { "ordered/e-2/0" in jdbc.reactionRowIds("ordered") }
                operations.skipBlocked("ordered", EventReactionId("ordered/e-1/0"))
                eventually(10.seconds) { log.events.contains("end:a2") }
            }
            assertEquals(1, log.events.count { it == "start:a1" })
        }

    @Test
    fun `a reaction queued again while it runs is absorbed`() =
        runBlocking {
            val log = Log()
            val release = AtomicBoolean(false)
            val mapped = AtomicInteger()
            val policy =
                OrderWork(
                    "ordered",
                    ordering = ReactionOrdering.PerAggregate(),
                    mapping = { _, m ->
                        mapped.incrementAndGet()
                        trigger(Confirm(m.aggregateId.value, m.sequence))
                    },
                    work = { work, _ ->
                        log.events += "start:${work.label()}"
                        while (!release.get()) delay(20)
                        log.events += "end:${work.label()}"
                    },
                )
            val offsets = PostgresOffsetManager(jdbc)

            runningReactor(dataSource, jdbc, listOf(policy)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "a", 1)
                eventually { log.events.contains("start:a1") && offsets.getPosition("reactor").globalOffset == 1L }
                offsets.savePosition("reactor", EventLogPosition.START) // replay, while the reaction runs
                eventually { mapped.get() >= 2 && offsets.getPosition("reactor").globalOffset == 1L }
                release.set(true)
                eventually { log.events.contains("end:a1") }
                delay(500)
            }
            assertEquals(1, log.events.count { it == "start:a1" })
        }

    @Test
    fun `aggregate ids with separators never share ordering`() =
        runBlocking {
            val log = Log()
            val policy =
                ordered(log) { _, _ ->
                    delay(400)
                    true
                }

            runningReactor(dataSource, jdbc, listOf(policy)) { _, _ ->
                jdbc.appendOrderEvent("e-x", "a", 2)
                jdbc.appendOrderEvent("e-y", "a/b#0000000000000000001", 1)
                eventually(10.seconds) { log.ends() == 2 }
            }

            // Different aggregates: both start before either ends.
            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `two event policies handling the same aggregate do not wait on each other`() =
        runBlocking {
            val log = Log()
            val slow: suspend (Work, ReactionContext) -> Boolean = { _, _ ->
                delay(500)
                true
            }
            val first = ordered(log, name = "first", succeeds = slow)
            val second = ordered(log, name = "second", succeeds = slow)

            runningReactor(dataSource, jdbc, listOf(first, second)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "a", 1)
                eventually(10.seconds) { log.ends() == 2 }
            }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `ordered work leaves at most one waiting scheduled_tasks row per line`() =
        runBlocking {
            val log = Log()
            val offsets = PostgresOffsetManager(jdbc)
            // The first reaction waits until every event is queued, so no publish can race a finishing front and
            // schedule a stale task for it (which would only run to schedule the real front).
            val policy =
                ordered(log) { work, _ ->
                    if (work == Confirm("a", 1)) eventually { offsets.getPosition("reactor").globalOffset == 10L }
                    delay(50)
                    true
                }
            val waitingRows = CopyOnWriteArrayList<Int>()

            runningReactor(dataSource, jdbc, listOf(policy)) { scheduler, _ ->
                (1..10L).forEach { jdbc.appendOrderEvent("e-$it", "a", it) }
                // Sample while the line drains: a row db-scheduler hasn't picked is the line's front, waiting to run. (The
                // running front's own picked row is removed just after it schedules its successor.)
                withTimeout(20.seconds) {
                    while (log.ends() < 10) {
                        waitingRows += scheduler.lineInstances("ordered", "Order/a").size
                        delay(10)
                    }
                }
            }

            assertEquals((1..10).map { "start:a$it" }, log.starts())
            assertTrue(waitingRows.isNotEmpty() && waitingRows.all { it <= 1 }, "waiting rows per sample: $waitingRows")
        }

    @Test
    fun `a parked mapping, once fixed, runs before the aggregate's later work`() =
        runBlocking {
            val log = Log()
            val fixed = AtomicBoolean(false)
            val failures = AtomicInteger()
            val policy =
                OrderWork(
                    "ordered",
                    ordering = ReactionOrdering.PerAggregate(),
                    mapping = { _, m ->
                        if (m.sequence == 1L && !fixed.get()) {
                            failures.incrementAndGet()
                            error("fix not deployed yet")
                        }
                        trigger(Confirm(m.aggregateId.value, m.sequence))
                        // The fixed mapping triggers two reactions; both take the parked mapping's place in line.
                        if (m.sequence == 1L) trigger(Remind(m.aggregateId.value))
                    },
                    work = { work, _ -> log.events += "start:${work.label()}" },
                )

            runningReactor(dataSource, jdbc, listOf(policy)) { scheduler, operations ->
                jdbc.appendOrderEvent("e-1", "a", 1)
                jdbc.appendOrderEvent("e-2", "a", 2, OrderShipped("book"))
                jdbc.appendOrderEvent("e-3", "a", 3, OrderShipped("book"))
                eventually { failures.get() >= 2 && listOf("ordered/e-1/mapping", "ordered/e-2/0", "ordered/e-3/0") == jdbc.reactionRowIds("ordered") }
                delay(500)
                assertTrue(log.events.isEmpty(), "the aggregate's later work waits behind its parked mapping")
                // An empty list is accepted on purpose: the front may be picked (running) as we sample, and picked rows aren\'t listed.
                assertTrue(
                    scheduler.lineInstances("ordered", "Order/a").all { it == "line/Order/a/ordered/e-1/mapping" },
                    "only the front, the parked mapping, is scheduled",
                )
                assertEquals(1, operations.parkedMappings("ordered").size)

                fixed.set(true)
                eventually(15.seconds) { log.events.size == 4 }
                eventually { jdbc.reactionRowIds("ordered").isEmpty() }
            }

            assertEquals(listOf("start:a1", "start:remind-a", "start:a2", "start:a3"), log.events.toList())
        }
}

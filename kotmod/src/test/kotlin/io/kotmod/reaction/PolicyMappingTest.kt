package io.kotmod.reaction

import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionChannel
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.event.reaction.ReactionQueues
import io.kotmod.process.ManualQueues
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped
import io.kotmod.support.persistedEvent
import kotlinx.coroutines.runBlocking
import java.io.IOException
import java.util.concurrent.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class PolicyMappingTest {
    private var now = Instant.parse("2026-10-06T10:00:00Z")
    private val log = InMemoryLog()
    private val queues = ManualQueues(enforceOrdering = true)

    private fun runtime(policy: EventPolicy<Notice>) = PolicyRuntime(policy, queues, log::readEvent, clock = { now }).also { it.start() }

    /** Appends [event] to the log and routes it to this event policy, as the reactor does. */
    private suspend fun PolicyRuntime<Notice>.read(event: PersistedEvent) = routeLocal(log.add(event))

    private suspend fun deliverAll(channel: String) {
        repeat(6) { queues.deliver(channel) }
    }

    private fun ids(channel: String) = queues.pending(channel).map { it.id.value }

    @Test
    fun `each trigger gets a deterministic id, and an ordered event policy stamps it with its aggregate, sequence and position`() =
        runBlocking {
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value))
                    trigger(Confirm("${m.aggregateId.value}-again"))
                })

            runtime(policy).read(orderEvent(OrderPlaced("book"), eventId = "e-1", orderId = "o-1", sequence = 3))

            val pending = queues.pending("confirmations")
            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-1/1"), pending.map { it.id.value })
            assertEquals(
                listOf(DispatchOrdering("Order/o-1", 3, 0, OnGiveUp.ContinueWithNext), DispatchOrdering("Order/o-1", 3, 1, OnGiveUp.ContinueWithNext)),
                pending.map { it.ordering },
            )
            assertEquals(listOf<Notice>(Confirm("o-1"), Confirm("o-1-again")), pending.map { it.notice() })
        }

    @Test
    fun `an unordered event policy queues delayed triggers with their notBefore and no stamp`() =
        runBlocking {
            val policy = RecordingPolicy(mapping = { _, m -> trigger(Confirm(m.aggregateId.value), notBefore = now + 1.hours) })

            runtime(policy).read(orderEvent(OrderPlaced("book")))

            val queued = queues.pending("confirmations").single()
            assertEquals(now + 1.hours, queued.notBefore)
            assertNull(queued.ordering)
        }

    @Test
    fun `events of aggregate types the event policy doesn't listen to are skipped`() =
        runBlocking {
            val policy = RecordingPolicy(mapping = { _, _ -> error("must not be called") })

            runtime(policy).read(paymentEvent("c-1"))

            assertTrue(queues.published.isEmpty())
        }

    @Test
    fun `a block that throws after triggering parks the event in its own queue and queues nothing else`() =
        runBlocking {
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value))
                    error("broken mapping")
                })

            runtime(policy).read(orderEvent(OrderPlaced("book")))

            val parked = queues.pending("confirmations").single()
            assertEquals("confirmations/e-1/mapping", parked.id.value)
            assertEquals(ParkedMapping("e-1", "Order", "o-1", "aggregate kind Order"), parked.trigger)
            assertNull(parked.ordering)
        }

    @Test
    fun `an event that can't be deserialized is parked`() =
        runBlocking {
            runtime(RecordingPolicy()).read(persistedEvent(globalOffset = 1, eventType = "OrderRefunded"))

            assertEquals(listOf("confirmations/e-1/mapping"), ids("confirmations"))
        }

    @Test
    fun `an ordered event policy parks an event whose block produces a delayed trigger, stamped with the event's position`() =
        runBlocking {
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value), notBefore = now + 1.hours)
                })

            runtime(policy).read(orderEvent(OrderPlaced("book"), sequence = 2))

            val parked = queues.pending("confirmations").single()
            assertEquals("confirmations/e-1/mapping", parked.id.value)
            assertEquals(DispatchOrdering("Order/o-1", 2, 0, OnGiveUp.ContinueWithNext), parked.ordering)
            assertNull(parked.notBefore)
        }

    @Test
    fun `once the block is fixed, a parked mapping queues the event's triggers with their normal ids, exactly once`() =
        runBlocking {
            var brokenFor = 2
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    if (brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm(m.aggregateId.value))
                })
            runtime(policy).read(orderEvent(OrderPlaced("book")))

            val outcomes = queues.deliver("confirmations") + queues.deliver("confirmations")

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Finished(gaveUp = false)), outcomes)
            assertEquals(listOf("confirmations/e-1/0"), ids("confirmations"))
            queues.deliver("confirmations")
            assertEquals(listOf<Notice>(Confirm("o-1")), policy.handled.map { it.first })
            assertEquals(1, queues.published.count { it.id.value == "confirmations/e-1/0" })
        }

    @Test
    fun `with ordering, an aggregate's later events wait behind its still-failing parked mapping and run in order after the fix`() =
        runBlocking {
            var brokenFor = 2
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    val parkedEvent = m.aggregateId.value == "o-1" && m.sequence == 1L
                    if (parkedEvent && brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm("${m.aggregateId.value}#${m.sequence}"))
                    if (parkedEvent) trigger(Confirm("o-1#1-again"))
                })
            val runtime = runtime(policy)
            runtime.read(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1", sequence = 1))
            runtime.read(orderEvent(OrderShipped("a"), eventId = "e-2", orderId = "o-1", sequence = 2))
            runtime.read(orderEvent(OrderPlaced("b"), eventId = "e-3", orderId = "o-2", sequence = 1))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Finished(gaveUp = false)), queues.deliver("confirmations"))

            assertEquals(listOf<Notice>(Confirm("o-2#1")), policy.handled.map { it.first })
            assertEquals(listOf("confirmations/e-1/mapping", "confirmations/e-2/0"), ids("confirmations"))
            deliverAll("confirmations")
            assertEquals(
                listOf<Notice>(Confirm("o-2#1"), Confirm("o-1#1"), Confirm("o-1#1-again"), Confirm("o-1#2")),
                policy.handled.map { it.first },
            )
        }

    @Test
    fun `routing an event again after its mapping succeeded queues no new work`() =
        runBlocking {
            val runtime = runtime(RecordingPolicy())
            val event = log.add(orderEvent(OrderPlaced("book")))

            runtime.routeLocal(event)
            runtime.routeLocal(event)

            assertEquals(listOf("confirmations/e-1/0"), ids("confirmations"))
        }

    @Test
    fun `routing a parked event again queues no new work`() =
        runBlocking {
            val runtime = runtime(RecordingPolicy(mapping = { _, _ -> error("broken") }))
            val event = log.add(orderEvent(OrderPlaced("book")))

            runtime.routeLocal(event)
            runtime.routeLocal(event)

            assertEquals(listOf("confirmations/e-1/mapping"), ids("confirmations"))
        }

    @Test
    fun `if queueing fails part-way, the failure reaches the reader and routing the event again queues every trigger exactly once`() =
        runBlocking {
            val flaky = FailingQueues(queues, failOnPublish = 2)
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value))
                    trigger(Confirm("${m.aggregateId.value}-again"))
                })
            val runtime = PolicyRuntime(policy, flaky, log::readEvent, clock = { now }).also { it.start() }
            val event = log.add(orderEvent(OrderPlaced("book")))

            assertFailsWith<IOException> { runtime.routeLocal(event) }
            assertEquals(listOf("confirmations/e-1/0"), ids("confirmations"))
            runtime.routeLocal(event)
            deliverAll("confirmations")

            assertEquals(listOf<Notice>(Confirm("o-1"), Confirm("o-1-again")), policy.handled.map { it.first })
            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-1/1"), policy.handled.map { it.second.reactionId })
        }

    @Test
    fun `a block that throws a CancellationException while the reader is running is parked`() =
        runBlocking {
            val policy = RecordingPolicy(mapping = { _, _ -> throw CancellationException("the app's, not the reader's") })

            runtime(policy).read(orderEvent(OrderPlaced("book")))

            assertEquals(listOf("confirmations/e-1/mapping"), ids("confirmations"))
        }

    @Test
    fun `a parked mapping whose block throws a CancellationException keeps retrying`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> throw CancellationException("the app's") })).read(orderEvent(OrderPlaced("book")))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("confirmations"))
            assertEquals(listOf("confirmations/e-1/mapping"), ids("confirmations"))
        }

    @Test
    fun `a mapping that always throws stays parked, retrying with growing backoff, and is never dropped`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> error("always broken") })).read(orderEvent(OrderPlaced("book")))

            val outcomes = (1..5).flatMap { queues.deliver("confirmations") }

            assertEquals(listOf(1, 2, 4, 8, 16).map { ReactionOutcome.Retry(it.seconds) }, outcomes)
            assertEquals(listOf("confirmations/e-1/mapping"), ids("confirmations"))
            assertEquals(5, queues.retries("confirmations", EventReactionId("confirmations/e-1/mapping")))
        }

    @Test
    fun `a parked mapping redelivered after it succeeded adds no duplicate work while its triggers are pending`() =
        runBlocking {
            var brokenFor = 1
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    if (brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm(m.aggregateId.value))
                }).apply { failWith = { _, context -> if (context.attempt == 0) RuntimeException("first try fails") else null } }
            runtime(policy).read(orderEvent(OrderPlaced("book")))
            val parked = queues.pending("confirmations").single()

            queues.deliver("confirmations") // the parked mapping succeeds and queues confirmations/e-1/0
            queues.redeliver(parked)
            deliverAll("confirmations")

            assertEquals(2, queues.published.count { it.id.value == "confirmations/e-1/0" })
            assertEquals(
                listOf(ReactionContext("confirmations/e-1/0", 0), ReactionContext("confirmations/e-1/0", 1)),
                policy.handled.map { it.second },
            )
        }

    @Test
    fun `a delayed trigger from a parked mapping that succeeds later waits until notBefore, then runs`() =
        runBlocking {
            var brokenFor = 1
            val remindAt = now + 1.hours
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    if (brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm(m.aggregateId.value), notBefore = remindAt)
                })
            runtime(policy).read(orderEvent(OrderPlaced("book")))

            queues.deliver("confirmations")
            assertEquals(remindAt, queues.pending("confirmations").single().notBefore)
            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Wait(1.hours)), queues.deliver("confirmations"))
            now = remindAt
            queues.deliver("confirmations")

            assertEquals(listOf<Notice>(Confirm("o-1")), policy.handled.map { it.first })
        }

    @Test
    fun `a parked mapping whose event policy no longer listens to the event's aggregate type finishes without triggers`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> error("broken") })).read(orderEvent(OrderPlaced("book")))
            runtime(RecordingPolicy(kind = null)) // the redeployed event policy no longer listens to orders

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = false)), queues.deliver("confirmations"))
            assertTrue(queues.pending("confirmations").isEmpty())
        }

    @Test
    fun `a parked mapping whose event is missing from the log keeps retrying`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> error("broken") })).routeLocal(orderEvent(OrderPlaced("book")))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("confirmations"))
            assertEquals(listOf("confirmations/e-1/mapping"), ids("confirmations"))
        }
}

/** Wraps [inner], throwing on its [failOnPublish]th publish (counting across channels). */
private class FailingQueues(
    private val inner: ReactionQueues,
    private var failOnPublish: Int,
) : ReactionQueues {
    override fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ReactionChannel<T> {
        val channel = inner.channel(name, triggerSerializer, ordered)
        val sink =
            object : EventReactionTriggerSink<T> {
                override val supportsOrdering = channel.sink.supportsOrdering

                override suspend fun publish(
                    id: EventReactionId,
                    trigger: T,
                    ordering: DispatchOrdering?,
                    notBefore: Instant?,
                ) {
                    if (--failOnPublish == 0) throw IOException("queue unavailable")
                    channel.sink.publish(id, trigger, ordering, notBefore)
                }
            }
        return ReactionChannel(sink, channel.source)
    }
}

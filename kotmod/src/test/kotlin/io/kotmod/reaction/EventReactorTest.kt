package io.kotmod.reaction

import io.kotmod.EventLogPosition
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.ReactionChannel
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionQueues
import io.kotmod.process.ManualQueues
import io.kotmod.process.ProcessEventSerialization
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped
import io.kotmod.support.persistedEvent
import io.kotmod.support.testOrderKind
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

class EventReactorTest {
    private val log = InMemoryLog()
    private val queues = ManualQueues(enforceOrdering = true)
    private var position = EventLogPosition.START
    private val saves = mutableListOf<EventLogPosition>()
    private var leader = true
    private var positionReads = 0
    private var failSaves = 0

    private fun reactor(
        vararg policies: EventPolicy<Notice>,
        queues: ReactionQueues = this.queues,
    ) = EventReactor(
        queues = queues,
        polling = log,
        readEvent = log::readEvent,
        getPosition = {
            positionReads++
            position
        },
        savePosition = {
            if (failSaves > 0) {
                failSaves--
                error("crashed before saving the position")
            }
            saves += it
            position = it
        },
        isLeader = { leader },
        name = "reactor",
        pollInterval = 50.milliseconds,
        batchSize = 100,
        clock = { Instant.parse("2026-10-06T10:00:00Z") },
    ).also { reactor -> policies.forEach { reactor.register(it) } }

    /** Every trigger appends `<orderId>#<sequence>`. */
    private fun everyEvent(
        name: String,
        ordering: ReactionOrdering = ReactionOrdering.Unordered,
    ) = RecordingPolicy(name = name, ordering = ordering, mapping = { _, m -> trigger(Confirm("${m.aggregateId.value}#${m.sequence}")) })

    /**
     * A queue factory whose sinks fail once when [failNext] is set (or, with [failNextOn], only the named channel's sink),
     * as a queue that is down would.
     */
    private class FlakyQueues(
        private val delegate: ManualQueues,
    ) : ReactionQueues {
        var failNext = false
        var failNextOn: String? = null

        override fun <T : EventReactionTrigger> channel(
            name: String,
            triggerSerializer: EventReactionTriggerSerializer<T>,
            ordered: Boolean,
        ): ReactionChannel<T> {
            val channel = delegate.channel(name, triggerSerializer, ordered)
            val sink =
                object : EventReactionTriggerSink<T> {
                    override val supportsOrdering = channel.sink.supportsOrdering

                    override suspend fun publish(
                        id: EventReactionId,
                        trigger: T,
                        ordering: DispatchOrdering?,
                        notBefore: Instant?,
                    ) {
                        if (failNext) {
                            failNext = false
                            error("queue unavailable")
                        }
                        if (failNextOn == name) {
                            failNextOn = null
                            error("queue $name unavailable")
                        }
                        channel.sink.publish(id, trigger, ordering, notBefore)
                    }
                }
            return ReactionChannel(sink, channel.source)
        }
    }

    @Test
    fun `several event policies each get their own triggers for one event in their own queues, and one listening to other types gets nothing`() =
        runBlocking {
            val reactor =
                reactor(
                    RecordingPolicy(name = "confirmations"),
                    everyEvent("audits"),
                    RecordingPolicy(name = "invoices", kind = testOrderKind("Invoice"), mapping = { _, _ -> error("must not be called") }),
                )
            log.add(orderEvent(OrderPlaced("book")))

            reactor.tickForTest()

            assertEquals(listOf("confirmations/e-1/0"), queues.pending("confirmations").map { it.id.value })
            assertEquals(listOf<Notice>(Confirm("o-1#1")), queues.pending("audits").map { it.notice() })
            assertTrue(queues.pending("invoices").isEmpty())
            assertEquals(log.events.single().position, position)
        }

    @Test
    fun `when one event policy's mapping throws, the others still get their triggers, only that event policy parks the event, and the reader moves on`() =
        runBlocking {
            val reactor = reactor(RecordingPolicy(name = "confirmations"), RecordingPolicy(name = "broken", mapping = { _, _ -> error("broken mapping") }))
            log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1"))
            log.add(orderEvent(OrderPlaced("b"), eventId = "e-2", orderId = "o-2"))

            reactor.tickForTest()

            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-2/0"), queues.pending("confirmations").map { it.id.value })
            assertEquals(listOf("broken/e-1/mapping", "broken/e-2/mapping"), queues.pending("broken").map { it.id.value })
            assertEquals(log.events.last().position, position)
        }

    @Test
    fun `a crash between queueing triggers and saving the position loses nothing and duplicates nothing`() =
        runBlocking {
            val policy = RecordingPolicy()
            val reactor = reactor(policy).also { it.startPoliciesForTest() }
            log.add(orderEvent(OrderPlaced("book")))
            failSaves = 1

            assertFailsWith<IllegalStateException> { reactor.tickForTest() }
            assertEquals(EventLogPosition.START, position)
            reactor.tickForTest()
            queues.deliver("confirmations")

            assertEquals(2, queues.published.count { it.id.value == "confirmations/e-1/0" })
            assertEquals(listOf(ReactionContext("confirmations/e-1/0", 0)), policy.handled.map { it.second })
            assertEquals(log.events.single().position, position)
        }

    @Test
    fun `replaying from an earlier position queues the same ids, which the queue absorbs while pending`() =
        runBlocking {
            val policy = RecordingPolicy()
            val reactor = reactor(policy).also { it.startPoliciesForTest() }
            log.add(orderEvent(OrderPlaced("book")))

            reactor.tickForTest()
            position = EventLogPosition.START
            reactor.tickForTest()
            queues.deliver("confirmations")

            assertEquals(listOf(ReactionContext("confirmations/e-1/0", 0)), policy.handled.map { it.second })
        }

    @Test
    fun `an ordered and an unordered event policy on the same aggregate have their own queues and don't block each other`() =
        runBlocking {
            val projection = everyEvent("projection", ReactionOrdering.PerAggregate()).apply { failWith = { _, _ -> RuntimeException("projection down") } }
            val emails = everyEvent("emails")
            val reactor = reactor(projection, emails).also { it.startPoliciesForTest() }
            log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", sequence = 1))
            log.add(orderEvent(OrderShipped("a"), eventId = "e-2", sequence = 2))

            reactor.tickForTest()
            queues.deliver("projection")
            queues.deliver("emails")

            assertEquals(listOf("projection" to true, "emails" to false), queues.channels)
            assertEquals(listOf("Order/o-1", "Order/o-1"), queues.pending("projection").map { it.ordering?.key })
            assertEquals(listOf<Notice>(Confirm("o-1#1")), projection.handled.map { it.first })
            assertEquals(listOf<Notice>(Confirm("o-1#1"), Confirm("o-1#2")), emails.handled.map { it.first })
        }

    @Test
    fun `two event policies with the same name are refused`() {
        val reactor = reactor(RecordingPolicy())

        val error = assertFailsWith<IllegalArgumentException> { reactor.register(RecordingPolicy()) }
        assertEquals("Reactor reactor already has an event policy named confirmations", error.message)
    }

    @Test
    fun `registering after start is refused`() =
        runBlocking<Unit> {
            leader = false
            val reactor = reactor()
            reactor.start()
            try {
                assertFailsWith<IllegalStateException> { reactor.register(RecordingPolicy()) }
            } finally {
                reactor.stop()
            }
        }

    @Test
    fun `an event policy can be registered with only one reactor`() {
        val policy = RecordingPolicy()
        reactor(policy)

        assertFailsWith<IllegalStateException> { reactor(policy) }
    }

    @Test
    fun `an event policy added to a running context sees only events from then on`() =
        runBlocking {
            reactor(RecordingPolicy()).run {
                log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1"))
                tickForTest()
            }
            val restarted = reactor(RecordingPolicy(), RecordingPolicy(name = "newcomer"))
            log.add(orderEvent(OrderPlaced("b"), eventId = "e-2", orderId = "o-2"))

            restarted.tickForTest()

            assertEquals(listOf("newcomer/e-2/0"), queues.pending("newcomer").map { it.id.value })
        }

    @Test
    fun `the reader only reads while leader`() =
        runBlocking {
            leader = false
            val reactor = reactor(RecordingPolicy())
            log.add(orderEvent(OrderPlaced("a")))

            reactor.tickForTest()

            assertTrue(queues.published.isEmpty())
            assertEquals(EventLogPosition.START, position)
        }

    @Test
    fun `the position is saved after each event`() =
        runBlocking {
            val reactor = reactor(RecordingPolicy())
            log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1"))
            log.add(orderEvent(OrderPlaced("b"), eventId = "e-2", orderId = "o-2"))

            reactor.tickForTest()

            assertEquals(log.events.map { it.position }, saves)
        }

    @Test
    fun `a queue that fails to publish stops the batch, parking nothing and saving no position`() =
        runBlocking {
            val flaky = FlakyQueues(queues)
            val reactor = reactor(RecordingPolicy(), queues = flaky)
            log.add(orderEvent(OrderPlaced("a")))
            flaky.failNext = true

            assertFailsWith<IllegalStateException> { reactor.tickForTest() }
            assertTrue(queues.published.isEmpty())
            assertEquals(EventLogPosition.START, position)
            reactor.tickForTest()
            assertEquals(listOf("confirmations/e-1/0"), queues.pending("confirmations").map { it.id.value })
        }

    @Test
    fun `when one event policy's queue fails, the event is read again and the other event policy's trigger is queued again under the same id`() =
        runBlocking {
            val flaky = FlakyQueues(queues)
            val reactor = reactor(RecordingPolicy(name = "a"), RecordingPolicy(name = "b"), queues = flaky)
            log.add(orderEvent(OrderPlaced("book")))
            flaky.failNextOn = "b"

            assertFailsWith<IllegalStateException> { reactor.tickForTest() }
            assertEquals(listOf("a/e-1/0"), queues.published.map { it.id.value })
            assertEquals(EventLogPosition.START, position)
            reactor.tickForTest()

            assertEquals(listOf("a/e-1/0", "a/e-1/0", "b/e-1/0"), queues.published.map { it.id.value })
            assertEquals(listOf("a/e-1/0"), queues.pending("a").map { it.id.value })
            assertEquals(log.events.single().position, position)
        }

    @Test
    fun `kotmod's internal events never reach an event policy`() =
        runBlocking {
            val reactor = reactor(RecordingPolicy(mapping = { _, _ -> error("must not be called") }))
            log.add(persistedEvent(globalOffset = 1, eventType = ProcessEventSerialization.COMMAND_REQUESTED))

            reactor.tickForTest()

            assertTrue(queues.published.isEmpty())
        }

    @Test
    fun `start fixes a new reactor's starting position before it returns`() =
        runBlocking {
            leader = false
            val reactor = reactor(RecordingPolicy())

            reactor.start()
            try {
                assertEquals(1, positionReads)
            } finally {
                reactor.stop()
            }
        }

    @Test
    fun `a stopped reactor can be started again`() =
        runBlocking {
            leader = false
            val reactor = reactor(RecordingPolicy())

            reactor.start()
            reactor.start()
            reactor.stop()
            reactor.start()
            try {
                assertEquals(2, positionReads)
            } finally {
                reactor.stop()
            }
        }
}

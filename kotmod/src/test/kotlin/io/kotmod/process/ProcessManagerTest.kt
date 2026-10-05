package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.support.ClosedWindow
import io.kotmod.support.NoOrder
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.Order
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.Window
import io.kotmod.support.WindowInput
import io.kotmod.support.persistedEvent
import io.kotmod.support.testOrders
import io.kotmod.support.windowEventSerialization
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Instant

class ProcessManagerTest {
    private val windowType = AggregateType("Window")
    private val closeAt = Instant.parse("2026-10-05T10:30:00Z")
    private var now = Instant.parse("2026-10-05T10:00:00Z")
    private val queues = ManualQueues()
    private val processBackend = StubPersistenceBackend<DomainEvent>()
    private val windows = StubRepository<Window>()
    private val orderRepository = StubRepository<Order>()
    private val orderBackend = StubPersistenceBackend<OrderEvent>()
    private val orders = AggregateManager(testOrders, orderRepository, orderBackend, NoOrder)
    private val log = EventLog()
    private var position = EventLogPosition.START
    private val translated = mutableListOf<EventId>()

    /** The event log the poller reads: foreign events added by tests, plus the process's own stream copied from its backend. */
    private inner class EventLog : DomainEventPollingBackend {
        val events = mutableListOf<PersistedEvent>()
        private var copied = 0

        fun add(event: PersistedEvent) {
            val offset = events.size + 1L
            events += event.copy(position = EventLogPosition(offset, offset))
        }

        fun copyProcessStream() {
            val serialization = ProcessEventSerialization(windowEventSerialization())
            processBackend.events.drop(copied).forEach { pending ->
                add(persistedEvent(globalOffset = 0).copy(metadata = pending.metadata, serialized = serialization.serialize(pending.event)))
            }
            copied = processBackend.events.size
        }

        override fun readEventsAfter(
            position: EventLogPosition,
            limit: Int,
        ): List<PersistedEvent> = events.filter { it.position > position }.take(limit)
    }

    private fun manager(
        targets: List<ProcessTarget<WindowInput>> = listOf(target(orders) { _, rejection -> ReleaseBlocked(rejection) }),
        inputOrdering: ReactionOrdering = ReactionOrdering.Unordered,
        start: Boolean = true,
    ) = ProcessManager(
        type = windowType,
        repository = windows,
        persistence = processBackend,
        polling = log,
        initial = NoWindow,
        inputSerializer = WindowInput.serializer(),
        eventSerialization = windowEventSerialization(),
        translate = { event ->
            translated += event.metadata.eventId
            if (event.metadata.aggregateType == AggregateType("Order")) {
                AggregateId("window-${event.metadata.aggregateId.value}") to Opened(event.metadata.aggregateId.value, closeAt.epochSeconds)
            } else {
                null
            }
        },
        targets = targets,
        queues = queues,
        inputOrdering = inputOrdering,
        getPosition = { position },
        savePosition = { position = it },
        isLeader = { true },
        clock = { now },
    ).also { if (start) it.startExecutorsForTest() }

    private data class OrderGoneInternal(
        val orderId: String,
    ) : DomainEvent

    private data class OrderGone(
        val orderId: String,
    ) : PublicDomainEvent

    private val contractEvents = mutableListOf<PersistedEvent>()

    private val contract =
        PublicEventContract<OrderGoneInternal, OrderGone>(
            backend =
                object : DomainEventPollingBackend {
                    override fun readEventsAfter(
                        position: EventLogPosition,
                        limit: Int,
                    ): List<PersistedEvent> = contractEvents.filter { it.position > position }.take(limit)
                },
            serialization =
                object : DataSerializationContext<OrderGoneInternal> {
                    override fun serialize(event: OrderGoneInternal) = SerializedEvent("OrderGone", 1, event.orderId)

                    override fun deserialize(serialized: SerializedEvent) = OrderGoneInternal(serialized.payload)
                },
            internalToPublic = { OrderGone(it.orderId) },
            getPosition = { EventLogPosition.START },
            savePosition = {},
            isLeader = { true },
        )

    private fun publishOrderGone(
        eventId: String,
        orderId: String,
    ) {
        val offset = contractEvents.size + 1L
        contractEvents +=
            persistedEvent(globalOffset = offset, eventId = eventId, aggregateId = orderId, eventType = "OrderGone", eventPayload = orderId)
    }

    private fun ProcessManager<Window, WindowInput, *>.subscribeToOrders(name: String = "orders") =
        subscribeTo(name, contract) { envelope ->
            val orderId = envelope.event.orderId
            if (orderId == "skip") null else AggregateId("window-$orderId") to Opened(orderId, closeAt.epochSeconds)
        }

    private suspend fun ProcessManager<Window, WindowInput, *>.openWindowFor(orderId: String) {
        log.add(persistedEvent(globalOffset = 0, eventId = "e-$orderId", aggregateType = "Order", aggregateId = orderId))
        tickForTest()
        queues.deliver("inputs")
        log.copyProcessStream()
        tickForTest()
    }

    private suspend fun ProcessManager<Window, WindowInput, *>.elapse() {
        now = closeAt
        queues.deliver("internal")
        log.copyProcessStream()
        tickForTest()
    }

    @Test
    fun `the manager asks for its three channels`() {
        manager()
        assertEquals(listOf("inputs" to false, "internal" to false, "commands" to false), queues.channels)
    }

    @Test
    fun `a translated event starts the process and its timeout is scheduled with notBefore`() =
        runBlocking {
            val pm = manager()

            pm.openWindowFor("o-1")

            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
            val scheduled = queues.pending("internal").single()
            assertTrue(scheduled.id.value.startsWith("sched-"))
            assertEquals(closeAt, scheduled.notBefore)
        }

    @Test
    fun `a timeout delivered early waits, then runs and requests the command`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")

            assertIs<ReactionOutcome.Wait>(queues.deliver("internal").single())
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])

            pm.elapse()

            assertEquals(ClosedWindow("o-1"), windows.store[AggregateId("window-o-1")])
            val command = queues.pending("commands").single()
            assertTrue(command.id.value.startsWith("cmd-"))
            assertEquals("\"ship\"", (command.trigger as CommandTrigger).command)
        }

    @Test
    fun `an accepted command changes the target and returns nothing to the process`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("o-1"))
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            queues.deliver("commands")

            assertEquals(ShippedOrder("o-1"), orderRepository.store[AggregateId("o-1")])
            assertTrue(queues.pending("internal").none { it.id.value.startsWith("rejected-") })
        }

    @Test
    fun `a rejected command comes back to the process as its own input`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            queues.deliver("commands")
            queues.deliver("internal")

            assertEquals(ClosedWindow("o-1", blocked = OrderNotFound), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `a redelivered command gets the same answer and its feedback is applied once`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()
            val command = queues.pending("commands").single()

            queues.deliver("commands")
            queues.redeliver(command)
            queues.deliver("commands")

            assertEquals(1, orderBackend.commands.size)
            assertEquals(1, queues.pending("internal").count { it.id.value.startsWith("rejected-") })
            queues.deliver("internal")
            assertEquals(ClosedWindow("o-1", blocked = OrderNotFound), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `the process's own events are never translated`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            assertEquals(listOf(EventId("e-o-1")), translated)
        }

    @Test
    fun `a command for an unregistered aggregate type is retried, and nothing is recorded`() =
        runBlocking {
            val pm = manager(targets = emptyList())
            pm.openWindowFor("o-1")
            now = closeAt

            assertIs<ReactionOutcome.Retry>(queues.deliver("internal").single())
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `ordered inputs are stamped with their source aggregate`() =
        runBlocking {
            val pm = manager(inputOrdering = ReactionOrdering.PerAggregate())
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1"))

            pm.tickForTest()

            assertEquals("Order/o-1", queues.pending("inputs").single().ordering?.key)
            assertEquals(listOf("inputs" to true, "internal" to false, "commands" to false), queues.channels)
        }

    @Test
    fun `two targets for the same aggregate type are refused`() {
        assertFailsWith<IllegalArgumentException> {
            manager(targets = listOf(target(orders) { _, r -> ReleaseBlocked(r) }, target(orders) { _, r -> ReleaseBlocked(r) }))
        }
    }

    @Test
    fun `a contract subscription asks for its own channel, ordered like the inputs`() {
        manager(start = false).subscribeToOrders()
        manager(inputOrdering = ReactionOrdering.PerAggregate(), start = false).subscribeToOrders("ordered-orders")

        assertEquals("contract-orders" to false, queues.channels[3])
        assertEquals("contract-ordered-orders" to true, queues.channels[7])
    }

    @Test
    fun `a translated public event becomes an input on the contract channel and starts the process`() =
        runBlocking {
            val pm = manager(start = false)
            pm.subscribeToOrders()
            pm.startExecutorsForTest()
            publishOrderGone(eventId = "p-1", orderId = "o-1")

            contract.tickForTest()

            assertEquals(EventReactionId("in-p-1"), queues.pending("contract-orders").single().id)
            queues.deliver("contract-orders")
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `a public event translated to null dispatches nothing`() =
        runBlocking {
            val pm = manager(start = false)
            pm.subscribeToOrders()
            pm.startExecutorsForTest()
            publishOrderGone(eventId = "p-1", orderId = "skip")

            contract.tickForTest()

            assertTrue(queues.pending("contract-orders").isEmpty())
        }

    @Test
    fun `subscribing after the executors have started is refused`() {
        val pm = manager()

        assertFailsWith<IllegalStateException> { pm.subscribeToOrders() }
    }

    @Test
    fun `two subscriptions with the same name are refused`() {
        val pm = manager(start = false)
        pm.subscribeToOrders()

        assertFailsWith<IllegalArgumentException> { pm.subscribeToOrders() }
    }

    @Test
    fun `another process manager's envelopes are never translated`() =
        runBlocking {
            val pm = manager()
            log.add(persistedEvent(globalOffset = 0, eventId = "c-1", aggregateType = "Other", eventType = ProcessEventSerialization.COMMAND_REQUESTED))
            log.add(persistedEvent(globalOffset = 0, eventId = "s-1", aggregateType = "Other", eventType = ProcessEventSerialization.INPUT_SCHEDULED))

            pm.tickForTest()

            assertTrue(translated.isEmpty())
            assertTrue(queues.published.isEmpty())
        }
}

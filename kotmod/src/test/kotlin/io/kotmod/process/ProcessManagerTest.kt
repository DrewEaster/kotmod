package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.DomainPersistenceBackend
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.HandledCommand
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.InMemoryReactionRows
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionRow
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.event.reaction.RowKind
import io.kotmod.event.reaction.TaskPayload
import io.kotmod.scheduling.Cancellable
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskOutcome
import io.kotmod.scheduling.TaskQueue
import io.kotmod.scheduling.TaskScheduler
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
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class ProcessManagerTest {
    private val windowType = AggregateType("Window")
    private val closeAt = Instant.parse("2026-10-05T10:30:00Z")
    private var now = Instant.parse("2026-10-05T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val rows = InMemoryReactionRows { now }
    private val processBackend = StubPersistenceBackend<DomainEvent>()
    private val windows = StubRepository<Window>()

    /** The next this-many inputs fail before the process records anything, as a database outage would. */
    private var failInputs = 0

    private val persistence =
        object : DomainPersistenceBackend<DomainEvent> by processBackend {
            override fun findHandledCommand(
                type: AggregateType,
                id: AggregateId,
                commandId: CommandId,
            ): HandledCommand? {
                if (failInputs > 0) {
                    failInputs--
                    error("database down")
                }
                return processBackend.findHandledCommand(type, id, commandId)
            }
        }
    private val orderRepository = StubRepository<Order>()
    private val orderBackend = StubPersistenceBackend<OrderEvent>()
    private val orders = AggregateManager(testOrders, orderRepository, orderBackend, NoOrder)
    private val log = EventLog()
    private var position = EventLogPosition.START
    private var leader = true
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
        scheduler: TaskScheduler = this.scheduler,
        rows: ReactionRows = this.rows,
    ) = ProcessManager(
        type = windowType,
        repository = windows,
        persistence = persistence,
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
        scheduler = scheduler,
        rows = rows,
        inputOrdering = inputOrdering,
        getPosition = { position },
        savePosition = { position = it },
        isLeader = { leader },
        sweepEvery = 10.minutes,
        sweepIdle = 30.minutes,
        clock = { now },
    ).also { if (start) it.startQueuesForTest() }

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
        sequence: Long = 1,
    ) {
        val offset = contractEvents.size + 1L
        contractEvents +=
            persistedEvent(
                globalOffset = offset,
                eventId = eventId,
                aggregateId = orderId,
                eventType = "OrderGone",
                eventPayload = orderId,
                sequence = sequence,
            )
    }

    private fun ProcessManager<Window, WindowInput, *>.subscribeToOrders(name: String = "orders") =
        subscribeTo(name, contract) { envelope ->
            val orderId = envelope.event.orderId
            if (orderId == "skip") null else AggregateId("window-$orderId") to Opened(orderId, closeAt.epochSeconds)
        }

    private fun queue(name: String) = scheduler.queue(name)

    /** Delivers each task pending in [name] once, in order; tasks added meanwhile wait for the next call. */
    private suspend fun deliverPending(name: String): List<TaskOutcome> = List(queue(name).pending.size) { checkNotNull(queue(name).deliverNext()) }

    /** The unordered work pending in [name]: reaction id to stored item JSON. */
    private fun unordered(name: String): List<TaskPayload.Unordered> = queue(name).pending.map { ReactionTasks.decode(it.payload) as TaskPayload.Unordered }

    /** The input ids the process has handled (accepted or ignored), in the order it handled them. */
    private fun handledInputs(): List<String> = processBackend.commands.keys.map { it.commandId.value }

    private suspend fun ProcessManager<Window, WindowInput, *>.openWindowFor(orderId: String) {
        log.add(persistedEvent(globalOffset = 0, eventId = "e-$orderId", aggregateType = "Order", aggregateId = orderId))
        tickForTest()
        deliverPending("Window-inputs")
        log.copyProcessStream()
        tickForTest()
    }

    private suspend fun ProcessManager<Window, WindowInput, *>.elapse() {
        now = closeAt
        deliverPending("Window-internal")
        log.copyProcessStream()
        tickForTest()
    }

    /** A scheduler that records which queues are asked for and how often each is subscribed to. */
    private inner class Watching : TaskScheduler {
        val asked = mutableListOf<String>()
        var subscriptions = 0

        override fun queue(name: String): TaskQueue {
            asked += name
            val queue = scheduler.queue(name)
            return object : TaskQueue by queue {
                override fun subscribe(handler: suspend (name: String, payload: String) -> TaskOutcome): Cancellable {
                    subscriptions++
                    return queue.subscribe(handler)
                }
            }
        }
    }

    @Test
    fun `the manager runs on three task queues named after its type`() {
        val watching = Watching()

        manager(scheduler = watching, start = false)

        assertEquals(listOf("Window-inputs", "Window-internal", "Window-commands"), watching.asked)
    }

    @Test
    fun `a translated event starts the process and its timeout is scheduled for its time`() =
        runBlocking {
            val pm = manager()

            pm.openWindowFor("o-1")

            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
            val scheduled = queue("Window-internal").pending.single()
            assertTrue(scheduled.name.startsWith("sched-"))
            assertEquals(closeAt, scheduled.at)
            assertEquals(closeAt.toString(), unordered("Window-internal").single().notBefore)
        }

    @Test
    fun `a timeout delivered early waits, then runs and requests the command`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")

            assertEquals(TaskOutcome.RunAgain(closeAt, queue("Window-internal").pending.single().payload), deliverPending("Window-internal").single())
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])

            pm.elapse()

            assertEquals(ClosedWindow("o-1"), windows.store[AggregateId("window-o-1")])
            val command = unordered("Window-commands").single()
            assertTrue(command.reactionId.startsWith("cmd-"))
            assertEquals("\"ship\"", Json.decodeFromString(CommandTrigger.serializer(), command.item).command)
        }

    @Test
    fun `an accepted command changes the target and returns nothing to the process`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("o-1"))
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            deliverPending("Window-commands")

            assertEquals(ShippedOrder("o-1"), orderRepository.store[AggregateId("o-1")])
            assertTrue(queue("Window-internal").pending.none { it.name.startsWith("rejected-") })
        }

    @Test
    fun `a rejected command comes back to the process as its own input`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            deliverPending("Window-commands")
            deliverPending("Window-internal")

            assertEquals(ClosedWindow("o-1", blocked = OrderNotFound), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `a redelivered command gets the same answer and its feedback is applied once`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()
            val command = queue("Window-commands").pending.single()

            deliverPending("Window-commands")
            queue("Window-commands").schedule(command.name, command.payload, command.at)
            deliverPending("Window-commands")

            assertEquals(1, orderBackend.commands.size)
            assertEquals(1, queue("Window-internal").pending.count { it.name.startsWith("rejected-") })
            deliverPending("Window-internal")
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

            val outcome = assertIs<TaskOutcome.RunAgain>(deliverPending("Window-internal").single())
            assertEquals(now + 1.seconds, outcome.at)
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `ordered inputs join their source aggregate's line in kotmod's rows`() =
        runBlocking {
            val pm = manager(inputOrdering = ReactionOrdering.PerAggregate())
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1", sequence = 3))

            pm.tickForTest()

            val row = rows.rows("Window-inputs").single()
            assertEquals(listOf("Order/o-1", 3L, 0, "in-e-1"), listOf(row.key, row.sequence, row.ordinal, row.reactionId))
            assertEquals(listOf(ReactionTasks.frontName("Order/o-1", "in-e-1")), queue("Window-inputs").pending.map { it.name })
        }

    @Test
    fun `ordered inputs run in their source aggregate's order on a first-in-first-out scheduler`() =
        runBlocking {
            val pm = manager(inputOrdering = ReactionOrdering.PerAggregate())
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1", sequence = 1))
            log.add(persistedEvent(globalOffset = 0, eventId = "e-2", aggregateType = "Order", aggregateId = "o-1", sequence = 2))
            pm.tickForTest()
            failInputs = 1

            queue("Window-inputs").deliverAll()

            assertEquals(listOf("in-e-1", "in-e-2"), handledInputs())
        }

    @Test
    fun `unordered inputs may overtake one another`() =
        runBlocking {
            val pm = manager()
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1", sequence = 1))
            log.add(persistedEvent(globalOffset = 0, eventId = "e-2", aggregateType = "Order", aggregateId = "o-1", sequence = 2))
            pm.tickForTest()
            failInputs = 1

            queue("Window-inputs").deliverAll()

            assertEquals(listOf("in-e-2", "in-e-1"), handledInputs())
        }

    @Test
    fun `ordered inputs from the process manager's own reader and from a contract subscription use separate lines`() =
        runBlocking {
            val pm = manager(inputOrdering = ReactionOrdering.PerAggregate(), start = false)
            pm.subscribeToOrders()
            pm.startQueuesForTest()
            // The same aggregate and sequence reach the process through both readers.
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1", sequence = 1))
            publishOrderGone(eventId = "p-1", orderId = "o-1", sequence = 1)

            pm.tickForTest()
            contract.tickForTest()

            assertEquals(listOf("in-e-1"), rows.rows("Window-inputs").map { it.reactionId })
            assertEquals(listOf("in-p-1"), rows.rows("Window-contract-orders").map { it.reactionId })
            queue("Window-inputs").deliverAll()
            queue("Window-contract-orders").deliverAll()
            assertEquals(listOf("in-e-1", "in-p-1"), handledInputs())
        }

    @Test
    fun `a target kept in a val narrower than the input type can be one of the targets`() {
        val shipping = target(orders) { _, rejection -> ReleaseBlocked(rejection) }

        manager(targets = listOf(shipping))
    }

    @Test
    fun `starting twice starts the queues once, and stopping stops them`() =
        runBlocking<Unit> {
            val watching = Watching()
            val pm = manager(scheduler = watching, start = false)

            pm.start()
            pm.start()
            pm.stop()

            assertEquals(3, watching.subscriptions)
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1"))
            pm.tickForTest()
            assertFailsWith<IllegalStateException> { queue("Window-inputs").deliverNext() }
        }

    @Test
    fun `two targets for the same aggregate type are refused`() {
        assertFailsWith<IllegalArgumentException> {
            manager(targets = listOf(target(orders) { _, r -> ReleaseBlocked(r) }, target(orders) { _, r -> ReleaseBlocked(r) }))
        }
    }

    @Test
    fun `a contract subscription runs on its own task queue, named after the subscription`() {
        val watching = Watching()

        manager(scheduler = watching, start = false).subscribeToOrders()

        assertEquals(listOf("Window-inputs", "Window-internal", "Window-commands", "Window-contract-orders"), watching.asked)
    }

    @Test
    fun `a translated public event becomes an input on the contract's queue and starts the process`() =
        runBlocking {
            val pm = manager(start = false)
            pm.subscribeToOrders()
            pm.startQueuesForTest()
            publishOrderGone(eventId = "p-1", orderId = "o-1")

            contract.tickForTest()

            assertEquals(listOf("in-p-1"), queue("Window-contract-orders").pending.map { it.name })
            queue("Window-contract-orders").deliverAll()
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `a public event translated to null queues nothing`() =
        runBlocking {
            val pm = manager(start = false)
            pm.subscribeToOrders()
            pm.startQueuesForTest()
            publishOrderGone(eventId = "p-1", orderId = "skip")

            contract.tickForTest()

            assertTrue(queue("Window-contract-orders").pending.isEmpty())
        }

    @Test
    fun `subscribing after the queues have started is refused`() {
        val pm = manager()

        assertFailsWith<IllegalStateException> { pm.subscribeToOrders() }
    }

    @Test
    fun `subscribing to a contract that has started is refused, and creates no queue`() {
        val watching = Watching()
        val pm = manager(scheduler = watching, start = false)
        contract.start()
        runBlocking { contract.stop() }

        assertFailsWith<IllegalStateException> { pm.subscribeToOrders() }
        assertEquals(listOf("Window-inputs", "Window-internal", "Window-commands"), watching.asked)
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
            listOf("Window-inputs", "Window-internal", "Window-commands").forEach { assertTrue(queue(it).pending.isEmpty()) }
        }

    /** Puts an input at the front of [key]'s line in [queue]'s rows, with no task: as if its backend lost the task. */
    private fun lostFront(
        queue: String,
        key: String,
        inputId: String,
    ) = rows.inLine(queue, key) {
        insert(
            ReactionRow(
                queue,
                inputId,
                RowKind.ORDERED,
                key,
                1,
                0,
                Json.encodeToString(InputTrigger.serializer(), InputTrigger("window-o-9", Json.encodeToString(WindowInput.serializer(), Opened("o-9", closeAt.epochSeconds)), inputId)),
            ),
        )
    }

    @Test
    fun `the first tick sweeps every queue, then at most every 10 minutes`() =
        runBlocking {
            val pm = manager(inputOrdering = ReactionOrdering.PerAggregate(), start = false)
            pm.subscribeToOrders()
            pm.startQueuesForTest()
            lostFront("Window-inputs", "Order/o-9", "lost-1")
            lostFront("Window-contract-orders", "Order/o-9", "lost-2")
            val lost1 = ReactionTasks.frontName("Order/o-9", "lost-1")
            val lost2 = ReactionTasks.frontName("Order/o-9", "lost-2")
            now += 31.minutes

            leader = false
            pm.tickForTest()
            assertTrue(queue("Window-inputs").pending.isEmpty(), "only the leader sweeps")

            leader = true
            pm.tickForTest()
            assertEquals(listOf(lost1), queue("Window-inputs").pending.map { it.name }, "the first tick sweeps")
            assertEquals(listOf(lost2), queue("Window-contract-orders").pending.map { it.name }, "contract queues are swept too")

            queue("Window-inputs").lose(lost1)
            now += 9.minutes
            pm.tickForTest()
            assertTrue(queue("Window-inputs").pending.isEmpty(), "no sweep before 10 minutes")

            now += 1.minutes
            pm.tickForTest()
            assertEquals(listOf(lost1), queue("Window-inputs").pending.map { it.name }, "a sweep once 10 minutes have passed")
            queue("Window-inputs").deliverAll()
            assertEquals(OpenWindow("o-9"), windows.store[AggregateId("window-o-9")])
        }

    @Test
    fun `a sweep that fails for one queue still sweeps the others and doesn't stop reading`() =
        runBlocking {
            val failing =
                object : ReactionRows by rows {
                    override fun stale(
                        queue: String,
                        now: Instant,
                        before: Instant,
                    ): List<ReactionRow> = if (queue == "Window-inputs") error("database down") else rows.stale(queue, now, before)
                }
            val pm = manager(inputOrdering = ReactionOrdering.PerAggregate(), start = false, rows = failing)
            pm.subscribeToOrders()
            pm.startQueuesForTest()
            lostFront("Window-contract-orders", "Order/o-9", "lost")
            now += 31.minutes
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1"))

            pm.tickForTest()

            assertEquals(log.events.single().position, position)
            assertEquals(listOf(ReactionTasks.frontName("Order/o-1", "in-e-1")), queue("Window-inputs").pending.map { it.name })
            assertEquals(listOf(ReactionTasks.frontName("Order/o-9", "lost")), queue("Window-contract-orders").pending.map { it.name })
        }
}

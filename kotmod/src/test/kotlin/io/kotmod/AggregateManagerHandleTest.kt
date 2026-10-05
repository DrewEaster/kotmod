package io.kotmod

import io.kotmod.support.CancelOrder
import io.kotmod.support.CancelledOrder
import io.kotmod.support.DecideWith
import io.kotmod.support.Order
import io.kotmod.support.OrderAlreadyExists
import io.kotmod.support.OrderCommand
import io.kotmod.support.OrderCommands
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.OrderNotPending
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderRejection
import io.kotmod.support.OrderShipped
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ShipOrder
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.ship
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class AggregateManagerHandleTest {
    private val type = AggregateType("Order")
    private val id = AggregateId("o-1")
    private lateinit var backend: StubPersistenceBackend<OrderEvent>
    private lateinit var repo: StubRepository<Order>
    private lateinit var orders: AggregateManager<Order, OrderEvent, OrderCommand, OrderRejection>

    @BeforeTest
    fun setUp() {
        backend = StubPersistenceBackend()
        repo = StubRepository()
        orders = manager()
    }

    private fun manager(maxConflictRetries: Int = 5) =
        AggregateManager(type, repo, backend, OrderCommands, maxConflictRetries = maxConflictRetries)

    private fun version() = backend.metas[StubPersistenceBackend.Key(type, id)]?.version

    @Test
    fun `an accepted create saves state, meta at version 1, events and the command`() =
        runTest {
            val result = orders.handle(id, PlaceOrder("book"), commandId = CommandId("c-1"))

            assertEquals(CommandResult.Accepted(PendingOrder("book")), result)
            assertEquals(PendingOrder("book"), repo.store[id])
            assertEquals(1L, version())
            assertEquals(listOf(OrderPlaced("book")), backend.events.map { it.event })
            assertEquals(HandledCommand.Accepted, backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-1"))])
        }

    @Test
    fun `an accepted command on an existing aggregate advances its version`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))

            val result = orders.handle(id, ShipOrder)

            assertEquals(CommandResult.Accepted(ShippedOrder("book")), result)
            assertEquals(2L, version())
        }

    @Test
    fun `events carry the command id as causation, the correlation id and per-aggregate sequences`() =
        runTest {
            orders.handle(id, DecideWith { accept(PendingOrder("book"), OrderPlaced("book"), OrderPlaced("pen")) }, commandId = CommandId("c-1"), correlationId = CorrelationId("flow-1"))
            orders.handle(id, ShipOrder)

            assertEquals(listOf(1L, 2L, 3L), backend.events.map { it.metadata.sequence })
            assertEquals(CommandId("c-1"), backend.events.first().metadata.causationId)
            assertEquals(CorrelationId("flow-1"), backend.events.first().metadata.correlationId)
        }

    @Test
    fun `accepting with no events still bumps the version, saves state and records the command`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))

            val result = orders.handle(id, DecideWith { state -> accept(state!!) }, commandId = CommandId("noop"))

            assertEquals(CommandResult.Accepted(PendingOrder("book")), result)
            assertEquals(2L, version())
            assertEquals(1, backend.events.size)
            assertEquals(HandledCommand.Accepted, backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("noop"))])
        }

    @Test
    fun `a rejection records the rejection and writes nothing else`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, ShipOrder)

            val result = orders.handle(id, CancelOrder("too late"), commandId = CommandId("c-cancel"))

            assertEquals(CommandResult.Rejected(OrderNotPending("ShippedOrder")), result)
            assertEquals(ShippedOrder("book"), repo.store[id])
            assertEquals(2L, version())
            assertEquals(2, backend.events.size)
            val recorded = backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-cancel"))]
            assertIs<HandledCommand.Rejected>(recorded)
            assertEquals(OrderNotPending::class.java.name, recorded.type)
        }

    @Test
    fun `a command on a missing aggregate is rejected through otherwise, and recorded`() =
        runTest {
            val result = orders.handle(id, ShipOrder, commandId = CommandId("c-1"))

            assertEquals(CommandResult.Rejected(OrderNotFound), result)
            assertNull(version())
            assertIs<HandledCommand.Rejected>(backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-1"))])
        }

    @Test
    fun `creating an existing aggregate is rejected through otherwise`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))

            assertEquals(CommandResult.Rejected(OrderAlreadyExists), orders.handle(id, PlaceOrder("again")))
        }

    @Test
    fun `a repeated accepted command id returns the current state without deciding again`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, CancelOrder("changed mind"), commandId = CommandId("c-cancel"))
            var decided = false

            val again = orders.handle(id, DecideWith { decided = true; error("must not decide") }, commandId = CommandId("c-cancel"))

            assertEquals(CommandResult.Accepted(CancelledOrder("book", "changed mind")), again)
            assertEquals(false, decided)
            assertEquals(2L, version())
        }

    @Test
    fun `a repeated rejected command id returns the same rejection even if the state now allows it`() =
        runTest {
            val first = orders.handle(id, ShipOrder, commandId = CommandId("c-ship"))
            orders.handle(id, PlaceOrder("book"))

            val again = orders.handle(id, ShipOrder, commandId = CommandId("c-ship"))

            assertEquals(CommandResult.Rejected(OrderNotFound), first)
            assertEquals(first, again)
            assertEquals(PendingOrder("book"), repo.store[id])
        }

    @Test
    fun `without a command id every call gets a fresh one and is recorded`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, ShipOrder)

            assertEquals(2, backend.commands.size)
        }

    @Test
    fun `all writes happen inside one backend transaction per command`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, CancelOrder("x"))
            orders.handle(id, ShipOrder)

            assertEquals(emptyList(), backend.writesOutsideTransaction)
            assertEquals(3, backend.transactionsCommitted)
        }

    @Test
    fun `a decision may suspend`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))

            val result =
                orders.handle(
                    id,
                    DecideWith { state ->
                        kotlinx.coroutines.delay(1)
                        (state as PendingOrder).ship()
                    },
                )

            assertEquals(CommandResult.Accepted(ShippedOrder("book")), result)
        }

    @Test
    fun `an exception from the decision writes nothing and propagates`() =
        runTest {
            assertFailsWith<IllegalStateException> { orders.handle(id, DecideWith { error("bug") }) }

            assertNull(version())
            assertEquals(0, backend.commands.size)
        }

    @Test
    fun `a conflict is retried by re-reading and deciding again`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            backend.conflictsToInject = 2
            var decisions = 0

            val result = orders.handle(id, DecideWith { state -> decisions++; (state as PendingOrder).ship() })

            assertEquals(CommandResult.Accepted(ShippedOrder("book")), result)
            assertEquals(3, decisions)
        }

    @Test
    fun `a conflict thrown by the decision propagates without a retry`() =
        runTest {
            var decisions = 0

            assertFailsWith<OptimisticConcurrencyException> {
                orders.handle(
                    id,
                    DecideWith {
                        decisions++
                        throw OptimisticConcurrencyException(type, id, 0)
                    },
                )
            }

            assertEquals(1, decisions)
        }

    @Test
    fun `when retries run out the last conflict exception is rethrown`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            backend.conflictsToInject = 6

            assertFailsWith<OptimisticConcurrencyException> { orders.handle(id, ShipOrder) }
            assertEquals(0, backend.conflictsToInject)
        }

    @Test
    fun `maxConflictRetries of 0 does not retry`() =
        runTest {
            orders = manager(maxConflictRetries = 0)
            orders.handle(id, PlaceOrder("book"))
            backend.conflictsToInject = 1

            assertFailsWith<OptimisticConcurrencyException> { orders.handle(id, ShipOrder) }
        }

    @Test
    fun `a negative maxConflictRetries is refused`() {
        assertFailsWith<IllegalArgumentException> { manager(maxConflictRetries = -1) }
    }

    @Test
    fun `inside an outer transaction a conflict is not retried`() {
        runBlocking { orders.handle(id, PlaceOrder("book")) }
        backend.conflictsToInject = 1

        assertFailsWith<OptimisticConcurrencyException> {
            backend.inTransaction { runBlocking { orders.handle(id, ShipOrder) } }
        }
    }

    @Test
    fun `an unreadable recorded rejection throws RejectionDeserializationException`() =
        runTest {
            backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-old"))] =
                HandledCommand.Rejected("com.example.RenamedRejection", """{"type":"com.example.RenamedRejection"}""")

            val failure = assertFailsWith<RejectionDeserializationException> { orders.handle(id, ShipOrder, commandId = CommandId("c-old")) }

            assertEquals("com.example.RenamedRejection", failure.rejectionType)
            assertEquals(CommandId("c-old"), failure.commandId)
        }

    @Test
    fun `bookkeeping without repository state throws AggregateNotFoundException`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            repo.store.clear()

            assertFailsWith<AggregateNotFoundException> { orders.handle(id, ShipOrder) }
        }

    @Test
    fun `accepted events are appended in command order`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, ShipOrder)

            assertEquals(listOf(OrderPlaced("book"), OrderShipped("book")), backend.events.map { it.event })
        }
}

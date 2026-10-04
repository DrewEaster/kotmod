package io.kotmod

import io.kotmod.support.Order
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.PendingOrder
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.StubTransacter
import io.kotmod.support.ship
import kotlinx.coroutines.test.runTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AggregateManagerDedupTest {
    private lateinit var backend: StubPersistenceBackend<OrderEvent>
    private lateinit var repo: StubRepository<Order>
    private lateinit var orders: AggregateManager<Order, OrderEvent>

    @BeforeTest
    fun setUp() {
        backend = StubPersistenceBackend()
        repo = StubRepository()
        orders =
            AggregateManager(
                aggregateType = AggregateType("Order"),
                repository = repo,
                backend = backend,
                transacter = StubTransacter(),
            )
    }

    @Test
    fun `duplicate create commandId returns current state without re-running`() =
        runTest {
            val id = AggregateId("o-1")
            val cmd = CommandId("cmd-1")
            orders.create(id, commandId = cmd) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            val second =
                orders.create(id, commandId = cmd) {
                    error("should not be invoked")
                }
            assertTrue(second is PendingOrder)
        }

    @Test
    fun `duplicate execute commandId returns current state without re-running`() =
        runTest {
            val id = AggregateId("o-1")
            val cmd = CommandId("cmd-ship")
            orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            orders.execute<PendingOrder>(id, commandId = cmd) { it.ship() }
            var ran = false
            val current =
                orders.execute(id, commandId = cmd) {
                    ran = true
                    it to emptyList()
                }
            assertFalse(ran, "lambda should not have been invoked on duplicate commandId")
            assertTrue(current is ShippedOrder)
            assertEquals(2L, backend.metas[StubPersistenceBackend.Key(AggregateType("Order"), id)]!!.version)
        }

    @Test
    fun `auto-generated commandIds are always unique and recorded`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            orders.execute<PendingOrder>(id) { it.ship() }
            assertEquals(2, backend.commands.size)
        }
}

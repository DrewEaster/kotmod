package io.kotmod

import io.kotmod.support.CancelledOrder
import io.kotmod.support.Order
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.PendingOrder
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.StubTransacter
import io.kotmod.support.cancel
import io.kotmod.support.ship
import kotlinx.coroutines.test.runTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class AggregateManagerExecuteTest {
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

    private suspend fun placeOrder(id: AggregateId) {
        orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
    }

    @Test
    fun `execute un-narrowed loads current state and applies command`() =
        runTest {
            val id = AggregateId("o-1")
            placeOrder(id)
            val result =
                orders.execute(id) { current ->
                    require(current is PendingOrder)
                    current.cancel("sold out")
                }
            val cancelled = result as CancelledOrder
            assertEquals("sold out", cancelled.reason)
        }

    @Test
    fun `execute narrowed via reified T calls command directly`() =
        runTest {
            val id = AggregateId("o-1")
            placeOrder(id)
            val result = orders.execute<PendingOrder>(id) { it.ship() }
            assertTrue(result is ShippedOrder)
        }

    @Test
    fun `execute narrowed with wrong state throws UnexpectedAggregateStateException`() =
        runTest {
            val id = AggregateId("o-1")
            placeOrder(id)
            orders.execute<PendingOrder>(id) { it.ship() }
            assertFailsWith<UnexpectedAggregateStateException> {
                orders.execute<PendingOrder>(id) { it.cancel("too late") }
            }
        }

    @Test
    fun `execute against missing aggregate throws AggregateNotFoundException`() =
        runTest {
            assertFailsWith<AggregateNotFoundException> {
                orders.execute(AggregateId("ghost")) { current -> current to emptyList() }
            }
        }

    @Test
    fun `execute bumps version`() =
        runTest {
            val id = AggregateId("o-1")
            placeOrder(id)
            orders.execute<PendingOrder>(id) { it.ship() }
            val meta = backend.metas[StubPersistenceBackend.Key(AggregateType("Order"), id)]!!
            assertEquals(2L, meta.version)
        }

    @Test
    fun `execute supports suspend lambda`() =
        runTest {
            val id = AggregateId("o-1")
            placeOrder(id)
            val result =
                orders.execute<PendingOrder>(id) {
                    kotlinx.coroutines.delay(1)
                    it.ship()
                }
            assertTrue(result is ShippedOrder)
        }

    @Test
    fun `execute with concurrent version bump throws OptimisticConcurrencyException`() =
        runTest {
            val id = AggregateId("o-1")
            placeOrder(id) // meta is now at version=1
            val key = StubPersistenceBackend.Key(AggregateType("Order"), id)
            assertFailsWith<OptimisticConcurrencyException> {
                orders.execute<PendingOrder>(id) {
                    // Simulate a concurrent writer between the read and write phases
                    backend.metas[key] = backend.metas[key]!!.copy(version = 99)
                    it.ship()
                }
            }
        }
}

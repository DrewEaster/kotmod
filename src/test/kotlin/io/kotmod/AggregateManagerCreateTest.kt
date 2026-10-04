package io.kotmod

import io.kotmod.support.Order
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.PendingOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.StubTransacter
import kotlinx.coroutines.test.runTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class AggregateManagerCreateTest {
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
    fun `create returns the new state`() =
        runTest {
            val id = AggregateId("o-1")
            val state =
                orders.create(id, commandId = CommandId("cmd-1")) {
                    PendingOrder("widgets") to listOf(OrderPlaced("widgets"))
                }
            assertTrue(state is PendingOrder)
            assertEquals("widgets", state.name)
        }

    @Test
    fun `create persists state via repository`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            assertNotNull(repo.store[id])
        }

    @Test
    fun `create inserts meta at version 1`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            val meta = backend.metas[StubPersistenceBackend.Key(AggregateType("Order"), id)]
            assertNotNull(meta)
            assertEquals(1L, meta.version)
        }

    @Test
    fun `create appends events`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            assertEquals(1, backend.events.size)
            assertTrue(backend.events[0].event is OrderPlaced)
        }

    @Test
    fun `create records command as handled`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id, commandId = CommandId("cmd-1")) {
                PendingOrder("widgets") to listOf(OrderPlaced("widgets"))
            }
            assertTrue(backend.commands.any { it.commandId == CommandId("cmd-1") })
        }

    @Test
    fun `create with empty events still writes meta and state`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) { PendingOrder("widgets") to emptyList() }
            assertNotNull(repo.store[id])
            assertNotNull(backend.metas[StubPersistenceBackend.Key(AggregateType("Order"), id)])
            assertEquals(0, backend.events.size)
        }

    @Test
    fun `create twice with same id throws AggregateAlreadyExistsException`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            assertFailsWith<AggregateAlreadyExistsException> {
                orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            }
        }

    @Test
    fun `create generates a command id when none supplied`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) { PendingOrder("widgets") to listOf(OrderPlaced("widgets")) }
            assertEquals(1, backend.commands.size)
        }

    @Test
    fun `create supports suspend lambda`() =
        runTest {
            val id = AggregateId("o-1")
            orders.create(id) {
                kotlinx.coroutines.delay(1)
                PendingOrder("widgets") to listOf(OrderPlaced("widgets"))
            }
            assertNotNull(repo.store[id])
        }
}

package io.kotmod.querty.example

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.CommandResult
import io.kotmod.jdbc.transaction
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.reaction.ReactionContext
import io.querty.jdbc.execute
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Instant

class QuertyExampleIntegrationTest : IntegrationTest() {
    private val orders by lazy {
        AggregateManager(
            kind = Orders,
            repository = QuertyOrderRepository(jdbc),
            backend = PostgresDomainPersistenceBackend(jdbc, Orders.eventSerialization),
            initial = NoOrder,
        )
    }

    private val customers by lazy {
        AggregateManager(
            kind = Customers,
            repository = QuertyCustomerRepository(jdbc),
            backend = PostgresDomainPersistenceBackend(jdbc, Customers.eventSerialization),
            initial = NoCustomer,
        )
    }

    private val readModel by lazy { OrderReadModel(jdbc) }

    @BeforeEach
    fun prepareExampleTables() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                if (!schemaApplied) {
                    stmt.execute(migration())
                    schemaApplied = true
                }
                stmt.execute("TRUNCATE order_status, orders, customers")
            }
        }
    }

    @Test
    fun `orders round-trip through the Querty repository`() =
        runBlocking {
            registerAlice()
            val repository = QuertyOrderRepository(jdbc)

            orders.handle(AggregateId("order-1"), PlaceOrder("alice", "book", JAN_1))
            orders.handle(AggregateId("order-1"), ShipOrder)
            orders.handle(AggregateId("order-2"), PlaceOrder("alice", "pen", JAN_2))
            orders.handle(AggregateId("order-2"), CancelOrder("changed my mind"))
            orders.handle(AggregateId("order-3"), PlaceOrder("alice", "ink", JAN_3))

            assertEquals(ShippedOrder(CustomerId("alice"), "book", JAN_1), repository.get(AggregateId("order-1")))
            assertEquals(
                CancelledOrder(CustomerId("alice"), "pen", JAN_2, "changed my mind"),
                repository.get(AggregateId("order-2")),
            )
            assertEquals(PendingOrder(CustomerId("alice"), "ink", JAN_3), repository.get(AggregateId("order-3")))
            assertNull(repository.get(AggregateId("order-unknown")))
        }

    @Test
    fun `customers round-trip through the Querty repository`() =
        runBlocking {
            val repository = QuertyCustomerRepository(jdbc)

            val result = customers.handle(AggregateId("alice"), RegisterCustomer("Alice", "alice@example.com", JAN_1))
            val again = customers.handle(AggregateId("alice"), RegisterCustomer("Alice", "alice@example.com", JAN_1))

            val alice = RegisteredCustomer("Alice", "alice@example.com", JAN_1)
            assertEquals(CommandResult.Accepted(alice), result)
            assertEquals(CommandResult.Rejected(CustomerAlreadyRegistered), again)
            assertEquals(alice, repository.get(AggregateId("alice")))
            assertNull(repository.get(AggregateId("bob")))
        }

    @Test
    fun `repository writes join the command's transaction`() =
        runBlocking {
            assertFailsWith<IllegalStateException> {
                jdbc.transaction {
                    customers.handle(AggregateId("alice"), RegisterCustomer("Alice", "alice@example.com", JAN_1))
                    orders.handle(AggregateId("order-1"), PlaceOrder("alice", "book", JAN_1))
                    error("failed after both commands were saved")
                }
            }

            assertEquals(0, countRows("orders"))
            assertEquals(0, countRows("customers"))
        }

    @Test
    fun `projection keeps the newest status even when an older change runs late`() =
        runBlocking {
            val projection = OrderStatusProjection(jdbc)
            val context = ReactionContext(reactionId = "reaction-1", attempt = 1)

            projection.handle(StatusChanged("order-1", "shipped", 2), context)
            projection.handle(StatusChanged("order-1", "placed", 1), context)

            val stored = jdbc.withConnection { conn -> FindOrderStatus("order-1").execute(conn) }
            assertEquals(FindOrderStatus.Row("order-1", "shipped", 2), stored)
        }

    @Test
    fun `customer orders include the customer name`() =
        runBlocking {
            registerAlice()
            orders.handle(AggregateId("order-1"), PlaceOrder("alice", "book", JAN_1))
            orders.handle(AggregateId("order-2"), PlaceOrder("alice", "pen", JAN_2))

            val rows = readModel.customerOrders(CustomerId("alice"))

            assertEquals(listOf(OrderId("order-2"), OrderId("order-1")), rows.map { it.id })
            assertTrue(rows.all { it.customerName == "Alice" && it.customerEmail == "alice@example.com" })
            assertEquals(OrderStatus.PENDING, rows.first().status)
            assertEquals(JAN_2, rows.first().placedAt)
        }

    @Test
    fun `customer orders for a customer whose row is missing`() {
        dataSource.connection.use { conn ->
            // Skips the foreign key to customers, so the order points at a customer that doesn't exist.
            conn.createStatement().use { it.execute("SET session_replication_role = replica") }
            SaveOrder(OrderId("order-1"), CustomerId("ghost"), OrderStatus.PENDING, "book", null, JAN_1).execute(conn)
        }

        val error = assertFailsWith<IllegalStateException> { readModel.customerOrders(CustomerId("ghost")) }

        assertTrue("customer_name" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `search applies optional filters`() =
        runBlocking {
            registerAlice()
            orders.handle(AggregateId("order-1"), PlaceOrder("alice", "Red Book", JAN_1))
            orders.handle(AggregateId("order-2"), PlaceOrder("alice", "pen", JAN_2))
            orders.handle(AggregateId("order-2"), ShipOrder)
            orders.handle(AggregateId("order-3"), PlaceOrder("alice", "blue book", JAN_3))

            fun search(
                status: OrderStatus? = null,
                itemContains: String? = null,
                limit: Long = 10,
                offset: Long = 0,
            ) = readModel.search(status, itemContains, limit, offset).map { it.id.value }

            assertEquals(listOf("order-3", "order-2", "order-1"), search())
            assertEquals(listOf("order-2"), search(status = OrderStatus.SHIPPED))
            assertEquals(listOf("order-3", "order-1"), search(itemContains = "BOOK"))
            assertEquals(listOf("order-3"), search(status = OrderStatus.PENDING, itemContains = "blue"))
            assertEquals(listOf("order-2"), search(limit = 1, offset = 1))
        }

    @Test
    fun `byIds returns only the requested orders`() =
        runBlocking {
            registerAlice()
            orders.handle(AggregateId("order-1"), PlaceOrder("alice", "book", JAN_1))
            orders.handle(AggregateId("order-2"), PlaceOrder("alice", "pen", JAN_2))
            orders.handle(AggregateId("order-3"), PlaceOrder("alice", "ink", JAN_3))

            val rows = readModel.byIds(listOf(OrderId("order-3"), OrderId("order-1"), OrderId("order-unknown")))

            assertEquals(listOf(OrderId("order-1"), OrderId("order-3")), rows.map { it.id })
        }

    private suspend fun registerAlice() {
        customers.handle(AggregateId("alice"), RegisterCustomer("Alice", "alice@example.com", JAN_1))
    }

    private fun countRows(table: String): Int =
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT count(*) FROM $table").use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }

    private companion object {
        val JAN_1 = Instant.parse("2026-01-01T10:00:00Z")
        val JAN_2 = Instant.parse("2026-01-02T10:00:00Z")
        val JAN_3 = Instant.parse("2026-01-03T10:00:00Z")

        var schemaApplied = false

        fun migration(): String =
            checkNotNull(QuertyExampleIntegrationTest::class.java.getResource("/db/migration/V1__orders_and_customers.sql")) {
                "example schema resource missing"
            }.readText()
    }
}

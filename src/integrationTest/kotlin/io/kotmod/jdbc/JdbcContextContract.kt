package io.kotmod.jdbc

import io.kotmod.AggregateAlreadyExistsException
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.Repository
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.Order
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped
import io.kotmod.support.PendingOrder
import io.kotmod.support.ShippedOrder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import io.kotmod.postgres.support.IntegrationTest
import org.junit.jupiter.api.BeforeEach
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Behaviour every [JdbcContext] implementation must have. Subclass it and implement [createJdbcContext]. */
abstract class JdbcContextContract : IntegrationTest() {
    protected abstract fun createJdbcContext(dataSource: DataSource): JdbcContext

    protected lateinit var context: JdbcContext

    @BeforeEach
    fun setUpContract() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS jdbc_probe (id TEXT PRIMARY KEY)")
                stmt.execute("TRUNCATE jdbc_probe")
            }
        }
        context = createJdbcContext(dataSource)
    }

    protected fun insertProbe(id: String) {
        context.withConnection { conn ->
            conn.prepareStatement("INSERT INTO jdbc_probe (id) VALUES (?)").use { ps ->
                ps.setString(1, id)
                ps.executeUpdate()
            }
        }
    }

    protected fun committedProbes(): List<String> =
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id FROM jdbc_probe ORDER BY id").use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1)) }
                }
            }
        }

    @Test
    fun `inTransaction commits when the block completes`() {
        context.inTransaction { insertProbe("a") }
        assertEquals(listOf("a"), committedProbes())
    }

    @Test
    fun `inTransaction rolls back and rethrows when the block throws`() {
        val failure = assertFailsWith<IllegalStateException> { context.inTransaction { insertProbe("a"); error("boom") } }
        assertEquals("boom", failure.message)
        assertEquals(emptyList(), committedProbes())
    }

    @Test
    fun `nested inTransaction joins the outer transaction`() {
        assertFailsWith<IllegalStateException> {
            context.inTransaction {
                context.inTransaction { insertProbe("a") }
                error("outer fails")
            }
        }
        assertEquals(emptyList(), committedProbes())
    }

    @Test
    fun `withConnection inside a transaction uses the transaction's connection`() {
        context.inTransaction {
            val first = context.withConnection { it }
            context.withConnection { conn ->
                assertSame(first, conn)
                assertFalse(conn.autoCommit)
            }
        }
    }

    @Test
    fun `withConnection outside a transaction is auto-commit`() {
        context.withConnection { assertTrue(it.autoCommit) }
        insertProbe("a")
        assertEquals(listOf("a"), committedProbes())
    }

    @Test
    fun `isInTransaction reflects the current thread`() {
        assertFalse(context.isInTransaction())
        context.inTransaction { assertTrue(context.isInTransaction()) }
        assertFalse(context.isInTransaction())
    }

    private class ProbeOrderRepository(
        private val jdbc: JdbcContext,
    ) : Repository<Order> {
        override fun get(id: AggregateId): Order? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, name FROM outer_tx_state WHERE id = ?").use { ps ->
                    ps.setString(1, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) null else if (rs.getString(1) == "PENDING") PendingOrder(rs.getString(2)) else ShippedOrder(rs.getString(2))
                    }
                }
            }

        override fun save(
            id: AggregateId,
            state: Order,
        ) {
            val (status, name) =
                when (state) {
                    is PendingOrder -> "PENDING" to state.name
                    is ShippedOrder -> "SHIPPED" to state.name
                    else -> error("unsupported state $state")
                }
            jdbc.withConnection { conn ->
                conn
                    .prepareStatement(
                        "INSERT INTO outer_tx_state (id, status, name) VALUES (?, ?, ?) " +
                            "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, name = EXCLUDED.name",
                    ).use { ps ->
                        ps.setString(1, id.value)
                        ps.setString(2, status)
                        ps.setString(3, name)
                        ps.executeUpdate()
                    }
            }
        }
    }

    private fun manager(
        type: String,
        jdbc: JdbcContext = context,
    ) = AggregateManager(
        aggregateType = AggregateType(type),
        repository = ProbeOrderRepository(jdbc),
        backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
    )

    private fun count(table: String): Int =
        dataSource.connection.use { conn ->
            conn.createStatement().use { it.executeQuery("SELECT COUNT(*) FROM $table").use { rs -> rs.next(); rs.getInt(1) } }
        }

    @BeforeEach
    fun setUpOuterTransactionTable() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS outer_tx_state (id TEXT PRIMARY KEY, status TEXT NOT NULL, name TEXT NOT NULL)")
                stmt.execute("TRUNCATE outer_tx_state")
            }
        }
    }

    @Test
    fun `transaction commits commands on two aggregates together`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")

            context.transaction {
                orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                invoices.create(AggregateId("i-1")) { PendingOrder("invoice") to listOf(OrderPlaced("invoice")) }
            }

            assertEquals(2, count("ddd_aggregate_root"))
            assertEquals(2, count("ddd_domain_event"))
            assertEquals(2, count("ddd_command_history"))
            assertEquals(2, count("outer_tx_state"))
        }

    @Test
    fun `a failing second command rolls back the first command's state, events and command record`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            invoices.create(AggregateId("i-1")) { PendingOrder("invoice") to listOf(OrderPlaced("invoice")) }

            assertFailsWith<AggregateAlreadyExistsException> {
                context.transaction {
                    orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    invoices.create(AggregateId("i-1")) { PendingOrder("again") to listOf(OrderPlaced("again")) }
                }
            }

            assertEquals(1, count("ddd_aggregate_root"))
            assertEquals(1, count("ddd_domain_event"))
            assertEquals(1, count("ddd_command_history"))
            assertEquals(1, count("outer_tx_state"))
        }

    @Test
    fun `an optimistic concurrency conflict rolls back the whole transaction`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
            invoices.create(AggregateId("i-1")) { PendingOrder("invoice") to listOf(OrderPlaced("invoice")) }

            assertFailsWith<OptimisticConcurrencyException> {
                context.transaction {
                    orders.execute<PendingOrder>(AggregateId("o-1")) { ShippedOrder(it.name) to listOf(OrderShipped(it.name)) }
                    invoices.execute<PendingOrder>(AggregateId("i-1")) { order ->
                        // Someone else changes the invoice between our read and our write.
                        dataSource.connection.use { conn ->
                            conn.createStatement().use {
                                it.execute("UPDATE ddd_aggregate_root SET aggregate_version = aggregate_version + 1 WHERE aggregate_id = 'i-1'")
                            }
                        }
                        ShippedOrder(order.name) to listOf(OrderShipped(order.name))
                    }
                }
            }

            assertEquals(2, count("ddd_domain_event"))
            assertEquals(PendingOrder("book"), ProbeOrderRepository(context).get(AggregateId("o-1")))
        }

    @Test
    fun `commands inside a transaction see earlier uncommitted writes`() =
        runBlocking {
            val orders = manager("Order")

            val shipped =
                context.transaction {
                    orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    orders.execute<PendingOrder>(AggregateId("o-1")) { ShippedOrder(it.name) to listOf(OrderShipped(it.name)) }
                }

            assertEquals(ShippedOrder("book"), shipped)
            assertEquals(2, count("ddd_domain_event"))
        }

    @Test
    fun `nested transaction joins the outer one`() =
        runBlocking {
            val orders = manager("Order")

            assertFailsWith<IllegalStateException> {
                context.transaction {
                    context.transaction {
                        orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    }
                    error("outer fails")
                }
            }

            assertEquals(0, count("ddd_domain_event"))
        }

    @Test
    fun `switching threads inside a transaction fails loudly and commits nothing`() =
        runBlocking {
            val orders = manager("Order")

            val failure =
                assertFailsWith<IllegalStateException> {
                    context.transaction {
                        withContext(Dispatchers.Default) {
                            orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                        }
                    }
                }

            assertTrue(failure.message!!.contains("don't switch threads"))
            assertEquals(0, count("ddd_domain_event"))
        }

    @Test
    fun `using a different JdbcContext inside a transaction fails loudly`() =
        runBlocking {
            val other = manager("Order", jdbc = createJdbcContext(dataSource))

            val failure =
                assertFailsWith<IllegalStateException> {
                    context.transaction {
                        other.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    }
                }

            assertTrue(failure.message!!.contains("different JdbcContext"))
            assertEquals(0, count("ddd_domain_event"))
        }

    @Test
    fun `cancelling the caller rolls back the transaction`() =
        runBlocking {
            val orders = manager("Order")
            val written = CompletableDeferred<Unit>()

            val running =
                async(Dispatchers.Default) {
                    context.transaction {
                        orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                        written.complete(Unit)
                        awaitCancellation()
                    }
                }
            written.await()
            running.cancelAndJoin()

            assertEquals(0, count("ddd_domain_event"))
            assertEquals(0, count("outer_tx_state"))
        }
}

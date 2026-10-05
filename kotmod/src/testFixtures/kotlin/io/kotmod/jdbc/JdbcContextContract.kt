package io.kotmod.jdbc

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandResult
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.Repository
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.DecideWith
import io.kotmod.support.Order
import io.kotmod.support.NoOrder
import io.kotmod.support.OrderRejection
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ShipOrder
import io.kotmod.support.ShippedOrder
import io.kotmod.support.ship
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.currentCoroutineContext
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
        initial = NoOrder,
        rejectionSerializer = OrderRejection.serializer(),
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
                orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                invoices.handle(AggregateId("i-1"), PlaceOrder("invoice"))
            }

            assertEquals(2, count("ddd_aggregate_root"))
            assertEquals(2, count("ddd_domain_event"))
            assertEquals(2, count("ddd_command_history"))
            assertEquals(2, count("outer_tx_state"))
        }

    @Test
    fun `throwing on a rejection rolls back the first command's state, events and command record, and the rejection`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            invoices.handle(AggregateId("i-1"), PlaceOrder("invoice"))

            assertFailsWith<IllegalStateException> {
                context.transaction {
                    orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                    val again = invoices.handle(AggregateId("i-1"), PlaceOrder("again"))
                    if (again is CommandResult.Rejected) error("rejected: ${again.rejection}")
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
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
            invoices.handle(AggregateId("i-1"), PlaceOrder("invoice"))

            assertFailsWith<OptimisticConcurrencyException> {
                context.transaction {
                    orders.handle(AggregateId("o-1"), ShipOrder)
                    invoices.handle(
                        AggregateId("i-1"),
                        DecideWith { order ->
                            // Someone else changes the invoice between our read and our write.
                            bumpVersionElsewhere("i-1")
                            (order as PendingOrder).ship()
                        },
                    )
                }
            }

            assertEquals(2, count("ddd_domain_event"))
            assertEquals(PendingOrder("book"), ProbeOrderRepository(context).get(AggregateId("o-1")))
        }

    private fun bumpVersionElsewhere(aggregateId: String) {
        dataSource.connection.use { conn ->
            conn.createStatement().use {
                it.execute("UPDATE ddd_aggregate_root SET aggregate_version = aggregate_version + 1 WHERE aggregate_id = '$aggregateId'")
            }
        }
    }

    @Test
    fun `commands inside a transaction see earlier uncommitted writes`() =
        runBlocking {
            val orders = manager("Order")

            val shipped =
                context.transaction {
                    orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                    orders.handle(AggregateId("o-1"), ShipOrder)
                }

            assertEquals(CommandResult.Accepted(ShippedOrder("book")), shipped)
            assertEquals(2, count("ddd_domain_event"))
        }

    @Test
    fun `nested transaction joins the outer one`() =
        runBlocking {
            val orders = manager("Order")

            val failure =
                assertFailsWith<IllegalStateException> {
                    context.transaction {
                        context.transaction {
                            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                        }
                        error("outer fails")
                    }
                }

            assertEquals("outer fails", failure.message)
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
                            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
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
                        other.handle(AggregateId("o-1"), PlaceOrder("book"))
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
                        orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                        written.complete(Unit)
                        awaitCancellation()
                    }
                }
            written.await()
            running.cancelAndJoin()

            assertEquals(0, count("ddd_domain_event"))
            assertEquals(0, count("outer_tx_state"))
        }

    @Test
    fun `switching threads inside a transaction for your own SQL fails loudly and commits nothing`() =
        runBlocking {
            assertFailsWith<IllegalStateException> {
                context.transaction {
                    insertProbe("a")
                    withContext(Dispatchers.IO) { insertProbe("b") }
                }
            }

            assertEquals(emptyList(), committedProbes())
        }

    @Test
    fun `transaction keeps the caller's coroutine context`() =
        runBlocking {
            val name =
                withContext(CoroutineName("request-42")) {
                    context.transaction { currentCoroutineContext()[CoroutineName]?.name }
                }

            assertEquals("request-42", name)
        }

    @Test
    fun `carrying on after a nested inTransaction failed rolls back and fails loudly`() {
        assertFailsWith<TransactionRolledBackException> {
            context.inTransaction {
                try {
                    context.inTransaction {
                        insertProbe("a")
                        error("inner fails")
                    }
                } catch (_: IllegalStateException) {
                    // carry on regardless
                }
                insertProbe("b")
            }
        }

        assertEquals(emptyList(), committedProbes())
    }

    @Test
    fun `carrying on after a command failed inside transaction rolls back and fails loudly`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            invoices.handle(AggregateId("i-1"), PlaceOrder("invoice"))

            assertFailsWith<TransactionRolledBackException> {
                context.transaction {
                    orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                    try {
                        invoices.handle(
                            AggregateId("i-1"),
                            DecideWith { order ->
                                bumpVersionElsewhere("i-1")
                                (order as PendingOrder).ship()
                            },
                        )
                    } catch (_: OptimisticConcurrencyException) {
                        // carry on regardless
                    }
                }
            }

            assertEquals(1, count("ddd_domain_event"))
            assertEquals(1, count("outer_tx_state"))
        }

    @Test
    fun `a command called from blocking code inside an open inTransaction joins it`() {
        val orders = manager("Order")

        val failure =
            assertFailsWith<IllegalStateException> {
                context.inTransaction {
                    insertProbe("app")
                    runBlocking {
                        orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                    }
                    error("app fails after kotmod ran")
                }
            }

        assertEquals("app fails after kotmod ran", failure.message)
        assertEquals(emptyList(), committedProbes())
        assertEquals(0, count("ddd_domain_event"))
    }
}

package io.kotmod.postgres

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.CommandId
import io.kotmod.CommandResult
import io.kotmod.Repository
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.transaction
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.reject
import io.kotmod.support.DecideWith
import io.kotmod.support.NoOrder
import io.kotmod.support.Order
import io.kotmod.support.OrderAlreadyExists
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ShipOrder
import io.kotmod.support.ShippedOrder
import io.kotmod.support.placeOrder
import io.kotmod.support.ship
import io.kotmod.support.testOrderKind
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class AggregateManagerIntegrationTest : IntegrationTest() {
    private class OrderTable(
        private val jdbc: JdbcContext,
    ) : Repository<Order> {
        override fun get(id: AggregateId): Order? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, name FROM handle_test_order WHERE id = ?").use { ps ->
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
                        "INSERT INTO handle_test_order (id, status, name) VALUES (?, ?, ?) " +
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

    private lateinit var orders: AggregateManager<Order, io.kotmod.support.OrderCommand, io.kotmod.support.OrderEvent, io.kotmod.support.OrderRejection>

    @BeforeEach
    fun setUp() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS handle_test_order (id TEXT PRIMARY KEY, status TEXT NOT NULL, name TEXT NOT NULL)")
                stmt.execute("TRUNCATE handle_test_order")
            }
        }
        orders =
            AggregateManager(
                testOrderKind(),
                OrderTable(jdbc),
                PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
                NoOrder,
            )
    }

    private fun count(sql: String): Int =
        dataSource.connection.use { conn -> conn.createStatement().use { it.executeQuery(sql).use { rs -> rs.next(); rs.getInt(1) } } }

    /** A decision that waits until [parties] callers have read, so they all decide on the same snapshot. Only the first [parties] decisions wait. */
    private fun rendezvous(parties: Int): () -> Unit {
        val barrier = CyclicBarrier(parties)
        val calls = AtomicInteger()
        return { if (calls.incrementAndGet() <= parties) barrier.await(10, TimeUnit.SECONDS) }
    }

    @Test
    fun `two callers sending the same command id at once get one acceptance and one set of events`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
            val meet = rendezvous(2)
            val ship = DecideWith { state -> meet(); (state as PendingOrder).ship() }

            val results =
                (1..2).map { async(Dispatchers.IO) { orders.handle(AggregateId("o-1"), ship, commandId = CommandId("ship-1")) } }.awaitAll()

            assertEquals(List(2) { CommandResult.Accepted(ShippedOrder("book")) }, results)
            assertEquals(2, count("SELECT COUNT(*) FROM ddd_domain_event"))
            assertEquals(2, count("SELECT COUNT(*) FROM ddd_command_history"))
        }

    @Test
    fun `two callers rejecting the same command id at once get the same recorded rejection`() =
        runBlocking {
            val meet = rendezvous(2)
            val refuse = DecideWith { meet(); reject(OrderAlreadyExists) }

            val results =
                (1..2).map { async(Dispatchers.IO) { orders.handle(AggregateId("o-1"), refuse, commandId = CommandId("c-1")) } }.awaitAll()

            assertEquals(List(2) { CommandResult.Rejected(OrderAlreadyExists) }, results)
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_command_history WHERE rejection_type IS NOT NULL"))
        }

    @Test
    fun `two callers creating the same aggregate with different command ids get one acceptance and one rejection`() =
        runBlocking {
            val meet = rendezvous(2)
            val create = DecideWith { state -> meet(); if (state == null) placeOrder("book") else reject(OrderAlreadyExists) }

            val results =
                (1..2).map { n -> async(Dispatchers.IO) { orders.handle(AggregateId("o-1"), create, commandId = CommandId("create-$n")) } }.awaitAll()

            assertEquals(setOf(CommandResult.Accepted(PendingOrder("book")), CommandResult.Rejected(OrderAlreadyExists)), results.toSet())
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_aggregate_root"))
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_domain_event"))
        }

    @Test
    fun `a rejection inside an outer transaction commits with it when the caller carries on`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))

            val result = jdbc.transaction { orders.handle(AggregateId("o-1"), PlaceOrder("again"), commandId = CommandId("again")) }

            assertEquals(CommandResult.Rejected(OrderAlreadyExists), result)
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_command_history WHERE command_id = 'again' AND rejection_type IS NOT NULL"))
        }

    @Test
    fun `a recorded rejection survives in Postgres and is returned for the same command id`() =
        runBlocking {
            val first = orders.handle(AggregateId("o-1"), ShipOrder, commandId = CommandId("ship-early"))
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))

            val again = orders.handle(AggregateId("o-1"), ShipOrder, commandId = CommandId("ship-early"))

            assertEquals(first, again)
            assertEquals(PendingOrder("book"), OrderTable(jdbc).get(AggregateId("o-1")))
        }
}

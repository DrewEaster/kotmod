package io.kotmod.event.reaction.dbscheduler

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.Repository
import io.kotmod.jdbc.JdbcContext
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.process.ProcessManager
import io.kotmod.process.target
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.support.CancelOrder
import io.kotmod.support.CancelledOrder
import io.kotmod.support.ClosedWindow
import io.kotmod.support.NoOrder
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.Order
import io.kotmod.support.OrderNotPending
import io.kotmod.support.OrderRejection
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShippedOrder
import io.kotmod.support.Window
import io.kotmod.support.WindowInput
import io.kotmod.support.testOrders
import io.kotmod.support.windowEventSerialization
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class ProcessManagerIntegrationTest : IntegrationTest() {
    private class OrderTable(
        private val jdbc: JdbcContext,
    ) : Repository<Order> {
        override fun get(id: AggregateId): Order? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, name, reason FROM pm_test_order WHERE id = ?").use { ps ->
                    ps.setString(1, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) {
                            null
                        } else {
                            when (rs.getString(1)) {
                                "PENDING" -> PendingOrder(rs.getString(2))
                                "SHIPPED" -> ShippedOrder(rs.getString(2))
                                else -> CancelledOrder(rs.getString(2), rs.getString(3))
                            }
                        }
                    }
                }
            }

        override fun save(
            id: AggregateId,
            state: Order,
        ) {
            val (status, name, reason) =
                when (state) {
                    is PendingOrder -> Triple("PENDING", state.name, null)
                    is ShippedOrder -> Triple("SHIPPED", state.name, null)
                    is CancelledOrder -> Triple("CANCELLED", state.name, state.reason)
                }
            jdbc.withConnection { conn ->
                conn
                    .prepareStatement(
                        "INSERT INTO pm_test_order (id, status, name, reason) VALUES (?, ?, ?, ?) " +
                            "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, name = EXCLUDED.name, reason = EXCLUDED.reason",
                    ).use { ps ->
                        ps.setString(1, id.value)
                        ps.setString(2, status)
                        ps.setString(3, name)
                        ps.setString(4, reason)
                        ps.executeUpdate()
                    }
            }
        }
    }

    private class WindowTable(
        private val jdbc: JdbcContext,
    ) : Repository<Window> {
        override fun get(id: AggregateId): Window? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, order_id, blocked FROM pm_test_window WHERE id = ?").use { ps ->
                    ps.setString(1, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) {
                            null
                        } else if (rs.getString(1) == "OPEN") {
                            OpenWindow(rs.getString(2))
                        } else {
                            ClosedWindow(rs.getString(2), rs.getString(3)?.let { Json.decodeFromString(OrderRejection.serializer(), it) })
                        }
                    }
                }
            }

        override fun save(
            id: AggregateId,
            state: Window,
        ) {
            val (status, orderId, blocked) =
                when (state) {
                    is OpenWindow -> Triple("OPEN", state.orderId, null)
                    is ClosedWindow -> Triple("CLOSED", state.orderId, state.blocked?.let { Json.encodeToString(OrderRejection.serializer(), it) })
                }
            jdbc.withConnection { conn ->
                conn
                    .prepareStatement(
                        "INSERT INTO pm_test_window (id, status, order_id, blocked) VALUES (?, ?, ?, ?) " +
                            "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, order_id = EXCLUDED.order_id, blocked = EXCLUDED.blocked",
                    ).use { ps ->
                        ps.setString(1, id.value)
                        ps.setString(2, status)
                        ps.setString(3, orderId)
                        ps.setString(4, blocked)
                        ps.executeUpdate()
                    }
            }
        }
    }

    @BeforeEach
    fun createTables() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS pm_test_order (id TEXT PRIMARY KEY, status TEXT NOT NULL, name TEXT NOT NULL, reason TEXT)")
                stmt.execute("CREATE TABLE IF NOT EXISTS pm_test_window (id TEXT PRIMARY KEY, status TEXT NOT NULL, order_id TEXT NOT NULL, blocked TEXT)")
                stmt.execute("TRUNCATE pm_test_order, pm_test_window")
            }
        }
    }

    private val orders by lazy {
        AggregateManager(testOrders, OrderTable(jdbc), PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()), NoOrder)
    }

    /** Runs a window process: every OrderPlaced opens a window that closes [closeAfter] later. */
    private suspend fun runningWindows(
        closeAfter: Duration,
        clock: () -> Instant = { Clock.System.now() },
        block: suspend () -> Unit,
    ) {
        val queues = DbSchedulerProcessManagerQueues("windows", jdbc)
        val offsets = PostgresOffsetManager(jdbc)
        val windows =
            ProcessManager(
                type = AggregateType("Window"),
                repository = WindowTable(jdbc),
                jdbc = jdbc,
                initial = NoWindow,
                inputSerializer = WindowInput.serializer(),
                eventSerialization = windowEventSerialization(),
                translate = { event ->
                    if (event.metadata.aggregateType == testOrders.type && event.serialized.type == "io.kotmod.support.OrderPlaced") {
                        val orderId = event.metadata.aggregateId.value
                        AggregateId("window-$orderId") to Opened(orderId, (Clock.System.now() + closeAfter).epochSeconds)
                    } else {
                        null
                    }
                },
                targets = listOf(target(orders) { _, rejection -> ReleaseBlocked(rejection) }),
                queues = queues,
                inputOrdering = ReactionOrdering.PerAggregate(),
                getPosition = { offsets.getPosition("windows") },
                savePosition = { offsets.savePosition("windows", it) },
                isLeader = { true },
                clock = clock,
            )
        val scheduler = testScheduler(dataSource, *queues.tasks.toTypedArray())
        queues.bind(scheduler)
        windows.start()
        scheduler.start()
        try {
            block()
        } finally {
            scheduler.stop()
            windows.stop()
        }
    }

    private fun window(orderId: String) = WindowTable(jdbc).get(AggregateId("window-$orderId"))

    private fun order(orderId: String) = OrderTable(jdbc).get(AggregateId(orderId))

    @Test
    fun `a placed order opens a window that closes on time and ships the order`() =
        runBlocking {
            runningWindows(closeAfter = 3.seconds) {
                orders.handle(AggregateId("o-1"), PlaceOrder("o-1"))

                eventually { window("o-1") == OpenWindow("o-1") }
                delay(1_000)
                assertEquals(PendingOrder("o-1"), order("o-1"), "shipped before the window closed")
                eventually { order("o-1") == ShippedOrder("o-1") }
                eventually { window("o-1") == ClosedWindow("o-1") }
            }
        }

    @Test
    fun `a refused command comes back to the process as its own input`() =
        runBlocking {
            runningWindows(closeAfter = 2.seconds) {
                orders.handle(AggregateId("o-2"), PlaceOrder("o-2"))
                orders.handle(AggregateId("o-2"), CancelOrder("changed mind"))

                eventually { window("o-2") == ClosedWindow("o-2", blocked = OrderNotPending("CancelledOrder")) }
                assertEquals(CancelledOrder("o-2", "changed mind"), order("o-2"))
            }
        }

    @Test
    fun `a timeout that arrives before the process's clock says it is due waits and runs later`() =
        runBlocking {
            // The process manager's clock lags 2 seconds behind db-scheduler's, so the timeout arrives "early".
            runningWindows(closeAfter = 1.seconds, clock = { Clock.System.now() - 2.seconds }) {
                orders.handle(AggregateId("o-3"), PlaceOrder("o-3"))

                eventually { window("o-3") == OpenWindow("o-3") }
                delay(1_500)
                assertEquals(OpenWindow("o-3"), window("o-3"), "ran before its notBefore by the process's clock")
                eventually { window("o-3") == ClosedWindow("o-3") }
            }
        }
}

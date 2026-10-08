package io.kotmod.readme

// Keep in sync with README.md (Quickstart, steps 1, 3, 4 and 5).

import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.CommandResult
import io.kotmod.jdbc.DataSourceJdbcContext
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.reaction.EventReactor
import io.kotmod.scheduling.dbscheduler.DbSchedulerTaskScheduler
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class QuickstartTest : IntegrationTest() {
    @BeforeEach
    fun createOrdersTable() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(ORDERS_TABLE_DDL)
                stmt.execute("TRUNCATE orders")
            }
        }
    }

    @Test
    fun `quickstart places and ships an order and sends one confirmation, ignoring other aggregate types`() =
        runBlocking {
            val sentConfirmations = CopyOnWriteArrayList<String>()

            fun sendConfirmation(orderId: String) {
                sentConfirmations += orderId
            }

            val jdbc = DataSourceJdbcContext(dataSource)

            val orders =
                AggregateManager(
                    kind = Orders,
                    repository = OrderRepository(jdbc),
                    backend = PostgresDomainPersistenceBackend(jdbc, Orders.eventSerialization),
                    initial = NoOrder,
                )

            val scheduler = DbSchedulerTaskScheduler()

            val reactor = EventReactor(jdbc, scheduler, isLeader = { true })
            reactor.register(OrderNotifications(::sendConfirmation))

            val dbScheduler =
                Scheduler
                    .create(dataSource, *scheduler.tasks.toTypedArray())
                    .threads(4)
                    .enableImmediateExecution()
                    .build()
            scheduler.bind(dbScheduler)

            reactor.start()
            dbScheduler.start()

            try {
                // Another aggregate type writing to the same event log, as an app with an audit log would.
                recordView(auditLog(jdbc), AggregateId("order-1"), viewer = "support", requestId = "view-1")

                val orderId = AggregateId("order-1")

                orders.handle(orderId, PlaceOrder("book"))

                val result = orders.handle(orderId, ShipOrder)

                assertEquals(CommandResult.Accepted(ShippedOrder("book")), result)
                eventually(15.seconds) { sentConfirmations.isNotEmpty() }
                delay(500) // give a duplicate time to show up
            } finally {
                dbScheduler.stop()
                reactor.stop()
            }

            assertEquals(listOf("order-1"), sentConfirmations.toList())
        }

    private companion object {
        const val ORDERS_TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS orders (
                id     TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                item   TEXT NOT NULL,
                reason TEXT
            )
        """
    }
}

package io.kotmod.readme

// Keep in sync with README.md (Quickstart, steps 1, 3, 4 and 5).

import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionCompletionResult
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.RetrySignal
import io.kotmod.event.reaction.dbscheduler.DbSchedulerEventReactions
import io.kotmod.jdbc.DataSourceJdbcContext
import io.kotmod.outbox.AggregateEventOutbox
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
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

            val serialization =
                jsonDataSerializationContext<OrderEvent> {
                    +OrderPlaced.serializer().toEventSerializer()
                    +OrderShipped.serializer().toEventSerializer()
                    +OrderCancelled.serializer().toEventSerializer()
                }

            val orderType = AggregateType("Order")

            val orders =
                AggregateManager(
                    aggregateType = orderType,
                    repository = OrderRepository(jdbc),
                    backend = PostgresDomainPersistenceBackend(jdbc, serialization),
                )

            // Another aggregate type writing to the same event log, as an app with an audit log would.
            recordView(auditLog(jdbc), AggregateId("order-1"), viewer = "support", requestId = "view-1")

            val orderId = AggregateId("order-1")

            orders.create(orderId) { placeOrder("book") }

            val shipped = orders.execute<PendingOrder>(orderId) { it.ship() }

            val notifications = DbSchedulerEventReactions("order-notifications", OrderNotificationSerializer)

            val scheduler =
                Scheduler
                    .create(dataSource, notifications.task)
                    .threads(4)
                    .enableImmediateExecution()
                    .build()

            val executor =
                EventReactionExecutor<OrderNotification, Unit>(
                    sink = notifications.sink(scheduler),
                    source = notifications.source,
                    createExecutionContext = { _, _ -> },
                    execute = { _, _, trigger, _, _ ->
                        when (trigger) {
                            is SendOrderConfirmation -> sendConfirmation(trigger.orderId)
                        }
                        EventReactionExecutionResult.EventReactionExecutionCompleted
                    },
                    failureRetryHandler = { _, _, _, retryCount, _, ex ->
                        if (retryCount < 5) {
                            RetrySignal.Retry(BackoffStrategy().calculateBackoff(retryCount))
                        } else {
                            RetrySignal.DoNotRetry(
                                EventReactionCompletionResult.EventReactionFailed(ex.message ?: "failed", allowManualRetry = true),
                            )
                        }
                    },
                    timeoutRetryHandler = { _, _, _, retryCount, _ ->
                        RetrySignal.Retry(BackoffStrategy().calculateBackoff(retryCount))
                    },
                    onCompletion = { id, _, _, _, _, result ->
                        println("Reaction ${id.value} finished: $result")
                    },
                )

            val offsets = PostgresOffsetManager(jdbc)

            val outbox =
                AggregateEventOutbox<OrderNotification>(
                    backend = PostgresDomainPollingBackend(jdbc),
                    executor = executor,
                    eventToReactions = { event ->
                        if (event.metadata.aggregateType != orderType) {
                            // The event log holds every aggregate type's events; only order events can be read here.
                            emptyList()
                        } else {
                            when (serialization.deserialize(event.serialized)) {
                                is OrderPlaced ->
                                    listOf(
                                        EventReaction(
                                            id = EventReactionId("confirmation-${event.metadata.eventId.value}"),
                                            trigger = SendOrderConfirmation(orderId = event.metadata.aggregateId.value),
                                        ),
                                    )
                                else -> emptyList()
                            }
                        }
                    },
                    getPosition = { offsets.getPosition("order-notifications") },
                    savePosition = { offsets.savePosition("order-notifications", it) },
                    isLeader = { true },
                )

            executor.start()
            scheduler.start()
            outbox.start()

            try {
                eventually(15.seconds) { sentConfirmations.isNotEmpty() }
                delay(500) // give a duplicate time to show up
            } finally {
                outbox.stop()
                scheduler.stop()
                executor.stop()
            }

            assertEquals(ShippedOrder("book"), shipped)
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

package io.kotmod.readme

// Keep in sync with README.md (Guides). These must compile; QuickstartTest runs the quickstart.

import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.EventId
import io.kotmod.EventProducer
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.PublicDomainEvent
import io.kotmod.UnexpectedAggregateStateException
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.dbscheduler.DbSchedulerEventReactions
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.transaction
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import javax.sql.DataSource
import kotlin.time.Duration

// Guide: Aggregates and commands

suspend fun cancelOrder(
    orders: AggregateManager<Order, OrderEvent>,
    orderId: AggregateId,
    reason: String,
    requestId: String,
): Order =
    try {
        orders.execute<PendingOrder>(orderId, commandId = CommandId(requestId)) { order ->
            CancelledOrder(order.item, reason) to listOf(OrderCancelled(order.item, reason))
        }
    } catch (e: UnexpectedAggregateStateException) {
        throw IllegalStateException("Only pending orders can be cancelled", e)
    }

suspend fun <T> retryOnConflict(
    attempts: Int = 3,
    command: suspend () -> T,
): T {
    repeat(attempts - 1) {
        try {
            return command()
        } catch (e: OptimisticConcurrencyException) {
            // Someone else changed the aggregate first: run the command again against the latest state.
        }
    }
    return command()
}

suspend fun shipAndInvoice(
    jdbc: JdbcContext,
    orders: AggregateManager<Order, OrderEvent>,
    invoices: AggregateManager<Order, OrderEvent>,
    orderId: AggregateId,
) {
    jdbc.transaction {
        orders.execute<PendingOrder>(orderId) { order ->
            ShippedOrder(order.item) to listOf(OrderShipped(order.item))
        }
        invoices.create(AggregateId("invoice-${orderId.value}")) {
            PendingOrder("invoice") to listOf(OrderPlaced("invoice"))
        }
    }
}

// Guide: Event-only aggregates

@Serializable
sealed interface AuditEvent : DomainEvent

@Serializable
data class OrderViewed(
    val viewer: String,
) : AuditEvent

fun auditLog(jdbc: JdbcContext): EventProducer<AuditEvent> =
    EventProducer(
        aggregateType = AggregateType("OrderAuditLog"),
        backend =
            PostgresDomainPersistenceBackend(
                jdbc,
                jsonDataSerializationContext<AuditEvent> { +OrderViewed.serializer().toEventSerializer() },
            ),
    )

suspend fun recordView(
    auditLog: EventProducer<AuditEvent>,
    orderId: AggregateId,
    viewer: String,
    requestId: String,
) {
    auditLog.emit(orderId, listOf(OrderViewed(viewer)), commandId = CommandId(requestId))
}

// Guide: Event serialization and schema migrations

val orderEventSerialization =
    jsonDataSerializationContext<OrderEvent> {
        +OrderPlaced.serializer().toEventSerializer()
        // OrderShipped used to be called OrderDispatched, in another package.
        +OrderShipped.serializer().toEventSerializer(initialClassName = "com.example.orders.OrderDispatched") {
            migrateClassName(OrderShipped::class.qualifiedName!!)
        }
        // OrderCancelled gained a `reason` field; older events get a default.
        +OrderCancelled.serializer().toEventSerializer {
            migrateFormat { json -> JsonObject(json + ("reason" to JsonPrimitive("not recorded"))) }
        }
    }

// Guide: Durable reactions with db-scheduler

@Serializable
sealed interface BillingTrigger : EventReactionTrigger

@Serializable
data class ChargeCustomer(
    val orderId: String,
    override val timeout: Duration? = null,
) : BillingTrigger

object BillingTriggerSerializer : EventReactionTriggerSerializer<BillingTrigger> {
    override suspend fun serialize(trigger: BillingTrigger): String = Json.encodeToString(BillingTrigger.serializer(), trigger)

    override suspend fun deserialize(serializedTrigger: String): BillingTrigger =
        Json.decodeFromString(BillingTrigger.serializer(), serializedTrigger)
}

fun sharedScheduler(
    dataSource: DataSource,
    billing: DbSchedulerEventReactions<BillingTrigger>,
    notifications: DbSchedulerEventReactions<OrderNotification>,
): Scheduler =
    Scheduler
        .create(dataSource, billing.task, notifications.task)
        .threads(10)
        .enableImmediateExecution()
        .build()

fun cancelPendingConfirmation(
    scheduler: Scheduler,
    notifications: DbSchedulerEventReactions<OrderNotification>,
    eventId: EventId,
) {
    scheduler.cancel(notifications.task.instanceId("confirmation-${eventId.value}"))
}

// Guide: Publishing events to other contexts

@Serializable
sealed interface OrderPublicEvent : PublicDomainEvent

@Serializable
data class OrderPlacedV1(
    val item: String,
) : OrderPublicEvent

fun orderContract(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    offsets: PostgresOffsetManager,
    billingExecutor: EventReactionExecutor<BillingTrigger, *>,
): PublicEventContract<OrderEvent, OrderPublicEvent> {
    val contract =
        PublicEventContract<OrderEvent, OrderPublicEvent>(
            backend = PostgresDomainPollingBackend(jdbc),
            serialization = serialization,
            internalToPublic = { event ->
                when (event) {
                    is OrderPlaced -> OrderPlacedV1(event.item)
                    else -> null
                }
            },
            getOffset = { offsets.getOffset("order-contract") },
            saveOffset = { offsets.saveOffset("order-contract", it) },
            isLeader = { true },
        )

    contract.subscribe(billingExecutor) { envelope ->
        when (envelope.event) {
            is OrderPlacedV1 ->
                listOf(
                    EventReaction<BillingTrigger>(
                        id = EventReactionId("charge-${envelope.metadata.eventId.value}"),
                        trigger = ChargeCustomer(orderId = envelope.metadata.aggregateId.value),
                    ),
                )
        }
    }
    return contract
}

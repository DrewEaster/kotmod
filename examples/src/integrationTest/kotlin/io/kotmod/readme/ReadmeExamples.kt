package io.kotmod.readme

// Keep in sync with README.md (Guides). These must compile; QuickstartTest runs the quickstart.

import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.AggregateId
import io.kotmod.AggregateAlreadyExistsException
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandAlreadyRecordedException
import io.kotmod.CommandId
import io.kotmod.CommandResult
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.EventId
import io.kotmod.EventProducer
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.Repository
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.dbscheduler.DbSchedulerEventReactions
import io.kotmod.event.reaction.dbscheduler.DbSchedulerProcessManagerQueues
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.transaction
import io.kotmod.outbox.AggregateEventOutbox
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresLeaderElection
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.process.ProcessInitialState
import io.kotmod.process.ProcessManager
import io.kotmod.process.ProcessOutcome
import io.kotmod.process.ProcessState
import io.kotmod.process.ignore
import io.kotmod.process.schedule
import io.kotmod.process.target
import io.kotmod.process.transition
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.sql.DriverManager
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Instant

// Guide: Aggregates and commands

suspend fun cancelOrder(
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    orderId: AggregateId,
    reason: String,
    requestId: String,
): String =
    when (val result = orders.handle(orderId, CancelOrder(reason), commandId = CommandId(requestId))) {
        is CommandResult.Accepted -> "Cancelled"
        is CommandResult.Rejected ->
            when (result.rejection) {
                OrderAlreadyShipped -> "Too late: the order has shipped"
                CancellationReasonMissing -> "Please give a reason"
                else -> "Can't cancel: ${result.rejection}"
            }
    }

suspend fun shipAndInvoice(
    jdbc: JdbcContext,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    invoices: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    orderId: AggregateId,
) {
    jdbc.transaction {
        // A rejection is a value: throw to roll the whole transaction back.
        val shipped = orders.handle(orderId, ShipOrder)
        if (shipped is CommandResult.Rejected) throw IllegalStateException("Can't ship: ${shipped.rejection}")
        val invoiced = invoices.handle(AggregateId("invoice-${orderId.value}"), PlaceOrder("invoice"))
        if (invoiced is CommandResult.Rejected) throw IllegalStateException("Can't invoice: ${invoiced.rejection}")
    }
}

suspend fun <T> retryTransaction(
    jdbc: JdbcContext,
    attempts: Int = 3,
    block: suspend () -> T,
): T {
    repeat(attempts - 1) {
        try {
            return jdbc.transaction { block() }
        } catch (e: OptimisticConcurrencyException) {
            // Another writer got there first: run the whole transaction again.
        } catch (e: AggregateAlreadyExistsException) {
        } catch (e: CommandAlreadyRecordedException) {
        }
    }
    return jdbc.transaction { block() }
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

// Guide: Ordered reactions

fun orderedNotifications(jdbc: JdbcContext): DbSchedulerEventReactions<OrderNotification> =
    DbSchedulerEventReactions("order-notifications", OrderNotificationSerializer, jdbc = jdbc)

fun orderedOutbox(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    offsets: PostgresOffsetManager,
    executor: EventReactionExecutor<OrderNotification, *>,
): AggregateEventOutbox<OrderNotification> =
    AggregateEventOutbox(
        backend = PostgresDomainPollingBackend(jdbc),
        executor = executor,
        eventToReactions = { event ->
            when (serialization.deserialize(event.serialized)) {
                is OrderPlaced -> listOf(EventReaction(EventReactionId("confirmation-${event.metadata.eventId.value}"), SendOrderConfirmation(event.metadata.aggregateId.value)))
                else -> emptyList()
            }
        },
        getPosition = { offsets.getPosition("order-notifications") },
        savePosition = { offsets.savePosition("order-notifications", it) },
        isLeader = { true },
        ordering = ReactionOrdering.PerAggregate(onGiveUp = OnGiveUp.BlockAggregate),
    )

// Guide: Delayed reactions

fun reviewReminderOutbox(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    offsets: PostgresOffsetManager,
    executor: EventReactionExecutor<OrderNotification, *>,
): AggregateEventOutbox<OrderNotification> =
    AggregateEventOutbox(
        backend = PostgresDomainPollingBackend(jdbc),
        executor = executor,
        eventToReactions = { event ->
            if (event.metadata.aggregateType != Orders.type) {
                emptyList()
            } else {
                when (serialization.deserialize(event.serialized)) {
                    is OrderShipped ->
                        listOf(
                            EventReaction(
                                id = EventReactionId("review-reminder-${event.metadata.eventId.value}"),
                                trigger = SendReviewReminder(orderId = event.metadata.aggregateId.value),
                                notBefore = event.metadata.timestamp + 7.days,
                            ),
                        )
                    else -> emptyList()
                }
            }
        },
        getPosition = { offsets.getPosition("review-reminders") },
        savePosition = { offsets.savePosition("review-reminders", it) },
        isLeader = { true },
    )

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
            getPosition = { offsets.getPosition("order-contract") },
            savePosition = { offsets.savePosition("order-contract", it) },
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

fun leaderElection(
    url: String,
    user: String,
    password: String,
): PostgresLeaderElection {
    val election = PostgresLeaderElection({ DriverManager.getConnection(url, user, password) }, "order-service")
    election.start()
    return election
}

fun outboxWithLeaderElection(
    jdbc: JdbcContext,
    election: PostgresLeaderElection,
    offsets: PostgresOffsetManager,
    executor: EventReactionExecutor<OrderNotification, *>,
): AggregateEventOutbox<OrderNotification> =
    AggregateEventOutbox(
        backend = PostgresDomainPollingBackend(jdbc),
        executor = executor,
        eventToReactions = { emptyList() },
        getPosition = { offsets.getPosition("order-notifications") },
        savePosition = { offsets.savePosition("order-notifications", it) },
        isLeader = election::isLeader,
    )

// Guide: Process managers

@Serializable
sealed interface DispatchDeadlineInput

@Serializable
data class OrderWasPlaced(
    val orderId: String,
    val placedAt: Instant,
) : DispatchDeadlineInput

@Serializable
data object OrderWasShipped : DispatchDeadlineInput

@Serializable
data object DeadlinePassed : DispatchDeadlineInput

@Serializable
data class CancellationRefused(
    val rejection: OrderRejection,
) : DispatchDeadlineInput

@Serializable
sealed interface DispatchDeadlineEvent : DomainEvent

@Serializable
data class DispatchDeadlineMissed(
    val orderId: String,
) : DispatchDeadlineEvent

typealias DispatchDeadlineOutcome = ProcessOutcome<DispatchDeadline, DispatchDeadlineEvent, DispatchDeadlineInput>

sealed interface DispatchDeadline : ProcessState<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent>

object NoDispatchDeadline : ProcessInitialState<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent> {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            is OrderWasPlaced ->
                transition(
                    AwaitingDispatch(input.orderId),
                    schedule = listOf(schedule(DeadlinePassed, at = input.placedAt + 2.days)),
                )
            OrderWasShipped, DeadlinePassed, is CancellationRefused -> ignore()
        }
}

data class AwaitingDispatch(
    val orderId: String,
) : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            OrderWasShipped -> transition(Dispatched)
            DeadlinePassed ->
                transition(
                    Missed,
                    events = listOf(DispatchDeadlineMissed(orderId)),
                    commands = listOf(Orders.command(AggregateId(orderId), CancelOrder("not shipped within 2 days"))),
                )
            is OrderWasPlaced, is CancellationRefused -> ignore()
        }
}

data object Dispatched : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome = ignore()
}

data object Missed : DispatchDeadline {
    // A refused cancellation means the order had shipped before the process heard about it.
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            is CancellationRefused -> transition(Dispatched)
            OrderWasShipped, DeadlinePassed, is OrderWasPlaced -> ignore()
        }
}

fun translateOrderEvent(
    event: PersistedEvent,
    serialization: DataSerializationContext<OrderEvent>,
): Pair<AggregateId, DispatchDeadlineInput>? {
    if (event.metadata.aggregateType != Orders.type) return null
    val deadline = AggregateId("deadline-${event.metadata.aggregateId.value}")
    return when (serialization.deserialize(event.serialized)) {
        is OrderPlaced -> deadline to OrderWasPlaced(event.metadata.aggregateId.value, event.metadata.timestamp)
        is OrderShipped -> deadline to OrderWasShipped
        else -> null
    }
}

fun dispatchDeadlines(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    deadlines: Repository<DispatchDeadline>,
    deadlineEvents: DataSerializationContext<DispatchDeadlineEvent>,
    queues: DbSchedulerProcessManagerQueues,
): ProcessManager<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent> {
    val offsets = PostgresOffsetManager(jdbc)
    return ProcessManager(
        type = AggregateType("DispatchDeadline"),
        repository = deadlines,
        jdbc = jdbc,
        initial = NoDispatchDeadline,
        inputSerializer = DispatchDeadlineInput.serializer(),
        inputOrdering = ReactionOrdering.PerAggregate(),
        eventSerialization = deadlineEvents,
        translate = { event -> translateOrderEvent(event, serialization) },
        targets = listOf(target(orders) { _, rejection -> CancellationRefused(rejection) }),
        queues = queues,
        getPosition = { offsets.getPosition("dispatch-deadlines") },
        savePosition = { offsets.savePosition("dispatch-deadlines", it) },
        isLeader = { true },
    )
}

fun startDispatchDeadlines(
    dataSource: DataSource,
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    deadlines: Repository<DispatchDeadline>,
    deadlineEvents: DataSerializationContext<DispatchDeadlineEvent>,
): Scheduler {
    val queues = DbSchedulerProcessManagerQueues("dispatch-deadlines", jdbc)
    val process = dispatchDeadlines(jdbc, serialization, orders, deadlines, deadlineEvents, queues)
    val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
    queues.bind(scheduler)
    process.start()
    scheduler.start()
    return scheduler
}

package io.kotmod.readme

// Keep in sync with README.md (Guides). These must compile; QuickstartTest runs the quickstart.

import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.task.TaskInstanceId
import io.kotmod.AggregateAlreadyExistsException
import io.kotmod.AggregateId
import io.kotmod.AggregateKind
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
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.dbscheduler.DbSchedulerQueues
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.transaction
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
import io.kotmod.reaction.EventReactor
import io.kotmod.reaction.ReactionContext
import io.kotmod.reaction.EventPolicy
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.sql.DriverManager
import javax.sql.DataSource
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

// Guide: Event policies

// Not shown in the README: a minimal customer aggregate kind for the SalesFeed example.

@Serializable
sealed interface CustomerCommand

@Serializable
data class RegisterCustomer(
    val email: String,
) : CustomerCommand

@Serializable
sealed interface CustomerEvent : DomainEvent

@Serializable
data class CustomerRegistered(
    val email: String,
) : CustomerEvent

@Serializable
sealed interface CustomerRejection

@Serializable
data object CustomerAlreadyRegistered : CustomerRejection

object Customers : AggregateKind<CustomerCommand, CustomerEvent, CustomerRejection>(
    type = AggregateType("Customer"),
    commandSerializer = CustomerCommand.serializer(),
    eventSerialization =
        jsonDataSerializationContext<CustomerEvent> {
            +CustomerRegistered.serializer().toEventSerializer()
        },
    rejectionSerializer = CustomerRejection.serializer(),
)

@Serializable
sealed interface SalesFeedPost

@Serializable
data class NewCustomer(
    val customerId: String,
    val email: String,
) : SalesFeedPost

@Serializable
data class NewOrder(
    val orderId: String,
    val item: String,
) : SalesFeedPost

class SalesFeed(
    private val post: suspend (message: String) -> Unit,
) : EventPolicy<SalesFeedPost>(
        name = "sales-feed",
        triggers = SalesFeedPost.serializer(),
    ) {
    init {
        on(Customers) { event, metadata ->
            when (event) {
                is CustomerRegistered -> trigger(NewCustomer(metadata.aggregateId.value, event.email))
            }
        }
        on(Orders) { event, metadata ->
            if (event is OrderPlaced) trigger(NewOrder(metadata.aggregateId.value, event.item))
        }
    }

    override suspend fun handle(
        trigger: SalesFeedPost,
        context: ReactionContext,
    ) = when (trigger) {
        is NewCustomer -> post("New customer: ${trigger.email}")
        is NewOrder -> post("New order ${trigger.orderId}: ${trigger.item}")
    }
}

@Serializable
data class SendReviewReminder(
    val orderId: String,
)

class ReviewReminders :
    EventPolicy<SendReviewReminder>(
        name = "review-reminders",
        triggers = SendReviewReminder.serializer(),
    ) {
    init {
        on(Orders) { event, metadata ->
            if (event is OrderShipped) {
                trigger(SendReviewReminder(metadata.aggregateId.value), notBefore = metadata.timestamp + 7.days)
            }
        }
    }

    override suspend fun handle(
        trigger: SendReviewReminder,
        context: ReactionContext,
    ) {
        println("Asking for a review of order ${trigger.orderId}")
    }
}

// Guide: Running event policies on db-scheduler

fun startReactions(
    dataSource: DataSource,
    jdbc: JdbcContext,
    election: PostgresLeaderElection,
): Pair<EventReactor, Scheduler> {
    val queues = DbSchedulerQueues(jdbc)
    val reactor = EventReactor(jdbc, queues, isLeader = election::isLeader)
    reactor.register(OrderNotifications(::sendConfirmation))
    reactor.register(ReviewReminders())
    reactor.register(OrderStatusProjection(jdbc))
    val scheduler =
        Scheduler
            .create(dataSource, *queues.tasks.toTypedArray())
            .threads(10)
            .enableImmediateExecution()
            .build()
    queues.bind(scheduler)
    reactor.start()
    scheduler.start()
    return reactor to scheduler
}

fun cancelPendingConfirmation(
    scheduler: Scheduler,
    eventId: EventId,
) {
    scheduler.cancel(TaskInstanceId.of("order-notifications", "order-notifications/${eventId.value}/0"))
}

// Guide: Ordered event policies

@Serializable
sealed interface OrderStatusChange

@Serializable
data class StatusChanged(
    val orderId: String,
    val status: String,
) : OrderStatusChange

class OrderStatusProjection(
    private val jdbc: JdbcContext,
) : EventPolicy<OrderStatusChange>(
        name = "order-status-projection",
        triggers = OrderStatusChange.serializer(),
    ) {
    init {
        on(Orders) { event, metadata ->
            val status =
                when (event) {
                    is OrderPlaced -> "placed"
                    is OrderShipped -> "shipped"
                    is OrderCancelled -> "cancelled"
                }
            trigger(StatusChanged(metadata.aggregateId.value, status))
        }
    }

    override val ordering = ReactionOrdering.PerAggregate(onGiveUp = OnGiveUp.BlockAggregate)

    override suspend fun handle(
        trigger: OrderStatusChange,
        context: ReactionContext,
    ) = when (trigger) {
        is StatusChanged -> saveStatus(trigger.orderId, trigger.status)
    }

    private fun saveStatus(
        orderId: String,
        status: String,
    ) {
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO order_status (id, status) VALUES (?, ?) " +
                        "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status",
                ).use { ps ->
                    ps.setString(1, orderId)
                    ps.setString(2, status)
                    ps.executeUpdate()
                }
        }
    }
}

fun retryBlockedProjection(
    scheduler: Scheduler,
    queues: DbSchedulerQueues,
) {
    for (blocked in queues.blockedReactions(scheduler, "order-status-projection")) {
        println("${blocked.key} is held back by ${blocked.reactionId.value} at sequence ${blocked.sequence}")
        queues.retryBlocked(scheduler, "order-status-projection", blocked.reactionId)
    }
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
    offsets: PostgresOffsetManager,
): PublicEventContract<OrderEvent, OrderPublicEvent> =
    PublicEventContract(
        backend = PostgresDomainPollingBackend(jdbc),
        serialization = Orders.eventSerialization,
        internalToPublic = { event ->
            when (event) {
                is OrderPlaced -> OrderPlacedV1(event.item)
                else -> null
            }
        },
        getPosition = { offsets.getPosition("order-contract") },
        savePosition = { offsets.savePosition("order-contract", it) },
        isLeader = { true },
        aggregateTypes = setOf(Orders.type),
    )

// Guide: Consuming another context's events

@Serializable
sealed interface BillingTrigger

@Serializable
data class ChargeCustomer(
    val orderId: String,
) : BillingTrigger

interface PaymentGateway {
    suspend fun charge(
        orderId: String,
        idempotencyKey: String,
    )
}

class CustomerBilling(
    orderEvents: PublicEventContract<*, OrderPublicEvent>,
    private val gateway: PaymentGateway,
) : EventPolicy<BillingTrigger>(
        name = "customer-billing",
        triggers = BillingTrigger.serializer(),
    ) {
    init {
        on(orderEvents) { event, metadata ->
            when (event) {
                is OrderPlacedV1 -> trigger(ChargeCustomer(metadata.aggregateId.value))
            }
        }
    }

    override suspend fun handle(
        trigger: BillingTrigger,
        context: ReactionContext,
    ) = when (trigger) {
        is ChargeCustomer -> gateway.charge(trigger.orderId, idempotencyKey = context.reactionId)
    }
}

fun startBilling(
    dataSource: DataSource,
    jdbc: JdbcContext,
    orderEvents: PublicEventContract<*, OrderPublicEvent>,
    gateway: PaymentGateway,
): Scheduler {
    val queues = DbSchedulerQueues(jdbc)
    val reactor = EventReactor(jdbc, queues, isLeader = { true }, name = "billing-reactor")
    reactor.register(CustomerBilling(orderEvents, gateway))
    val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
    queues.bind(scheduler)
    reactor.start()
    scheduler.start()
    return scheduler
}

// Running in production

fun leaderElection(
    url: String,
    user: String,
    password: String,
): PostgresLeaderElection {
    val election = PostgresLeaderElection({ DriverManager.getConnection(url, user, password) }, "order-service")
    election.start()
    return election
}

fun reactorWithLeaderElection(
    jdbc: JdbcContext,
    queues: DbSchedulerQueues,
    election: PostgresLeaderElection,
): EventReactor = EventReactor(jdbc, queues, isLeader = election::isLeader)

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
data object OrderWasCancelled : DispatchDeadlineInput

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
            OrderWasShipped, OrderWasCancelled, DeadlinePassed, is CancellationRefused -> ignore()
        }
}

data class AwaitingDispatch(
    val orderId: String,
) : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            OrderWasShipped -> transition(Dispatched)
            OrderWasCancelled -> transition(Abandoned)
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

data object Abandoned : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome = ignore()
}

data object Missed : DispatchDeadline {
    // A cancellation refused because the order has shipped: it shipped before the process heard about it.
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            is CancellationRefused ->
                when (input.rejection) {
                    OrderAlreadyShipped -> transition(Dispatched)
                    else -> ignore()
                }
            OrderWasShipped, OrderWasCancelled, DeadlinePassed, is OrderWasPlaced -> ignore()
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
        is OrderCancelled -> deadline to OrderWasCancelled
    }
}

fun dispatchDeadlines(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    deadlines: Repository<DispatchDeadline>,
    deadlineEvents: DataSerializationContext<DispatchDeadlineEvent>,
    queues: DbSchedulerQueues,
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
    val queues = DbSchedulerQueues(jdbc)
    val process = dispatchDeadlines(jdbc, serialization, orders, deadlines, deadlineEvents, queues)
    val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
    queues.bind(scheduler)
    process.start()
    scheduler.start()
    return scheduler
}

// Guide: When timeouts go stale

@Serializable
sealed interface PaymentReminderInput

@Serializable
data class InvoiceIssued(
    val invoiceId: String,
    val dueAt: Instant,
) : PaymentReminderInput

@Serializable
data class DueDateMoved(
    val until: Instant,
) : PaymentReminderInput

@Serializable
data object InvoicePaid : PaymentReminderInput

@Serializable
data class PaymentOverdue(
    val dueAt: Instant,
) : PaymentReminderInput

@Serializable
sealed interface PaymentReminderEvent : DomainEvent

@Serializable
data class InvoiceWentOverdue(
    val invoiceId: String,
) : PaymentReminderEvent

typealias PaymentReminderOutcome = ProcessOutcome<PaymentReminder, PaymentReminderEvent, PaymentReminderInput>

sealed interface PaymentReminder : ProcessState<PaymentReminder, PaymentReminderInput, PaymentReminderEvent>

object NoPaymentReminder : ProcessInitialState<PaymentReminder, PaymentReminderInput, PaymentReminderEvent> {
    override suspend fun handle(input: PaymentReminderInput): PaymentReminderOutcome =
        when (input) {
            is InvoiceIssued ->
                transition(
                    AwaitingPayment(input.invoiceId, input.dueAt),
                    schedule = listOf(schedule(PaymentOverdue(input.dueAt), at = input.dueAt)),
                )
            is DueDateMoved, InvoicePaid, is PaymentOverdue -> ignore()
        }
}

data class AwaitingPayment(
    val invoiceId: String,
    val dueAt: Instant,
) : PaymentReminder {
    override suspend fun handle(input: PaymentReminderInput): PaymentReminderOutcome =
        when (input) {
            is DueDateMoved ->
                transition(
                    copy(dueAt = input.until),
                    schedule = listOf(schedule(PaymentOverdue(input.until), at = input.until)),
                )
            is PaymentOverdue ->
                if (input.dueAt != dueAt) {
                    ignore()
                } else {
                    transition(Overdue, events = listOf(InvoiceWentOverdue(invoiceId)))
                }
            InvoicePaid -> transition(Paid)
            is InvoiceIssued -> ignore()
        }
}

data object Paid : PaymentReminder {
    // Nothing left to wait for: any timeout that still arrives is stale.
    override suspend fun handle(input: PaymentReminderInput): PaymentReminderOutcome = ignore()
}

data object Overdue : PaymentReminder {
    override suspend fun handle(input: PaymentReminderInput): PaymentReminderOutcome = ignore()
}

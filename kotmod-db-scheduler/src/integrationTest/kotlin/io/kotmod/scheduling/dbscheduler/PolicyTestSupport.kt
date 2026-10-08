package io.kotmod.scheduling.dbscheduler

import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.AggregateId
import io.kotmod.AggregateKind
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.jdbc.JdbcContext
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.StartFrom
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.reaction.EventReactor
import io.kotmod.reaction.ParkedMapping
import io.kotmod.reaction.ReactionOperations
import io.kotmod.reaction.FailureDecision
import io.kotmod.reaction.ReactionContext
import io.kotmod.reaction.ReactionResult
import io.kotmod.reaction.EventPolicy
import io.kotmod.reaction.Retry
import io.kotmod.reaction.TriggerScope
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.testOrders
import kotlinx.serialization.Serializable
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Serializable
sealed interface Work

@Serializable
data class Confirm(
    val orderId: String,
    val sequence: Long,
) : Work

@Serializable
data class Remind(
    val orderId: String,
) : Work

@Serializable
data class Flag(
    val customerId: String,
) : Work

/** What an event policy saw, across threads. [handled] records triggers whose `handle` returned normally, in order. */
class Seen {
    val handled = CopyOnWriteArrayList<Work>()
    val contexts = CopyOnWriteArrayList<ReactionContext>()
    val failures = CopyOnWriteArrayList<Throwable>()
    val completions = CopyOnWriteArrayList<Pair<Work, ReactionResult>>()
}

/**
 * An event policy over test orders for end-to-end tests. [mapping] is its `on(kind)` block (by default a [Confirm] for every
 * event), [work] runs inside `handle`, and [decide] is its failure handling (by default, retry after 100ms).
 */
class OrderWork(
    name: String,
    val seen: Seen = Seen(),
    override val ordering: ReactionOrdering = ReactionOrdering.Unordered,
    override val timeout: Duration = 60.seconds,
    kind: AggregateKind<*, OrderEvent, *>? = testOrders,
    private val mapping: TriggerScope<Work>.(OrderEvent, EventMetadata) -> Unit = { _, metadata ->
        trigger(Confirm(metadata.aggregateId.value, metadata.sequence))
    },
    private val work: suspend (Work, ReactionContext) -> Unit = { _, _ -> },
    private val decide: (Work, Int, Throwable) -> FailureDecision = { _, _, _ -> Retry(100.milliseconds) },
) : EventPolicy<Work>(name, Work.serializer()) {
    init {
        if (kind != null) on(kind) { event, metadata -> mapping(this, event, metadata) }
    }

    /** Declares a contract source from outside, as only an `init` block normally would. */
    fun <P : PublicDomainEvent> listenTo(
        contract: PublicEventContract<*, P>,
        block: TriggerScope<Work>.(P, EventMetadata) -> Unit,
    ) = on(contract, block)

    override suspend fun handle(
        trigger: Work,
        context: ReactionContext,
    ) {
        seen.contexts += context
        work(trigger, context)
        seen.handled += trigger
    }

    override fun onFailure(
        trigger: Work,
        attempt: Int,
        error: Throwable,
    ): FailureDecision {
        seen.failures += error
        return decide(trigger, attempt, error)
    }

    override suspend fun onCompletion(
        trigger: Work,
        result: ReactionResult,
    ) {
        seen.completions += trigger to result
    }
}

/** Appends [event] to order [orderId]'s history at [sequence], as a command would. */
fun JdbcContext.appendOrderEvent(
    eventId: String,
    orderId: String,
    sequence: Long,
    event: OrderEvent = OrderPlaced("book"),
) {
    PostgresDomainPersistenceBackend(this, orderEventSerialization()).appendEvents(
        listOf(
            PendingEvent(
                EventMetadata(EventId(eventId), AggregateType("Order"), AggregateId(orderId), CommandId("cmd-$eventId"), null, Clock.System.now(), sequence),
                event,
            ),
        ),
    )
}

// The payments context: its internal events, and the contract publishing declined payments to other contexts.

@Serializable
sealed interface PaymentEvent : DomainEvent

@Serializable
data class PaymentDeclinedEvent(
    val customerId: String,
) : PaymentEvent

@Serializable
data class PaymentTaken(
    val customerId: String,
) : PaymentEvent

data class CustomerPaymentDeclined(
    val customerId: String,
) : PublicDomainEvent

val paymentEventSerialization: DataSerializationContext<PaymentEvent> =
    jsonDataSerializationContext<PaymentEvent> {
        +PaymentDeclinedEvent.serializer().toEventSerializer()
        +PaymentTaken.serializer().toEventSerializer()
    }

fun JdbcContext.appendPaymentEvent(
    eventId: String,
    customerId: String,
    sequence: Long = 1,
    event: PaymentEvent = PaymentDeclinedEvent(customerId),
) {
    PostgresDomainPersistenceBackend(this, paymentEventSerialization).appendEvents(
        listOf(
            PendingEvent(
                EventMetadata(EventId(eventId), AggregateType("Payment"), AggregateId(customerId), CommandId("cmd-$eventId"), null, Clock.System.now(), sequence),
                event,
            ),
        ),
    )
}

/** The payments context's contract: declined payments, as [CustomerPaymentDeclined]. */
fun paymentContract(jdbc: JdbcContext): PublicEventContract<PaymentEvent, CustomerPaymentDeclined> {
    val offsets = PostgresOffsetManager(jdbc)
    return PublicEventContract(
        backend = PostgresDomainPollingBackend(jdbc),
        serialization = paymentEventSerialization,
        internalToPublic = { event -> (event as? PaymentDeclinedEvent)?.let { CustomerPaymentDeclined(it.customerId) } },
        getPosition = { offsets.getPosition("payments-contract", StartFrom.Beginning) },
        savePosition = { offsets.savePosition("payments-contract", it) },
        isLeader = { true },
        aggregateTypes = setOf(AggregateType("Payment")),
        pollInterval = 50.milliseconds,
    )
}

/**
 * Registers [policies] on one reactor and one scheduler, starts them in the documented order (reactor, contract,
 * scheduler), runs [block] with the scheduler and the operator tools, then stops them in reverse. Events appended
 * inside [block] are seen; earlier ones are not.
 */
suspend fun runningReactor(
    dataSource: DataSource,
    jdbc: JdbcContext,
    policies: List<OrderWork>,
    contract: PublicEventContract<*, *>? = null,
    block: suspend (scheduler: Scheduler, operations: ReactionOperations) -> Unit,
) {
    val tasks = DbSchedulerTaskScheduler()
    val reactor = EventReactor(jdbc, tasks, isLeader = { true }, pollInterval = 50.milliseconds)
    policies.forEach { reactor.register(it) }
    val scheduler = testScheduler(dataSource, tasks.tasks)
    tasks.bind(scheduler)
    reactor.start()
    contract?.start()
    scheduler.start()
    try {
        block(scheduler, ReactionOperations(jdbc, tasks))
    } finally {
        scheduler.stop()
        contract?.stop()
        reactor.stop()
    }
}

/** [policy]'s parked mapping of event [eventId], while it is still parked. */
fun ReactionOperations.parked(
    policy: String,
    eventId: String,
): ParkedMapping? = parkedMappings(policy).singleOrNull { it.eventId == EventId(eventId) }

/** The pending (not running) instances of [policy]'s task for aggregate line [key] (`<aggregateType>/<aggregateId>`). */
fun Scheduler.lineInstances(
    policy: String,
    key: String,
): List<String> = instanceIds(policy).filter { it.startsWith("line/$key/") }

/** The ids of [queue]'s rows in `ddd_reaction_row`, by line and place in line. */
fun JdbcContext.reactionRowIds(queue: String): List<String> =
    withConnection { conn ->
        conn
            .prepareStatement(
                "SELECT reaction_id FROM ddd_reaction_row WHERE queue_name = ? " +
                    "ORDER BY line_key NULLS LAST, line_sequence, line_ordinal, reaction_id",
            ).use { ps ->
                ps.setString(1, queue)
                ps.executeQuery().use { rs -> generateSequence { if (rs.next()) rs.getString(1) else null }.toList() }
            }
    }

/**
 * Runs [change] (an operator tool), trying again while it is refused because the work is running at that moment, as
 * an operator would.
 */
suspend fun retryWhileRunning(change: suspend () -> Unit) {
    withTimeout(10.seconds) {
        while (true) {
            try {
                change()
                return@withTimeout
            } catch (e: IllegalStateException) {
                delay(100)
            }
        }
    }
}

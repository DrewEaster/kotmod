package io.kotmod.reaction

import io.kotmod.AggregateKind
import io.kotmod.AggregateType
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.persistedEvent
import io.kotmod.support.testOrders
import kotlinx.serialization.Serializable
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@Serializable
sealed interface Notice

@Serializable
data class Confirm(
    val orderId: String,
) : Notice

@Serializable
data class Flag(
    val customerId: String,
) : Notice

/** An order event as the event log holds it. */
internal fun orderEvent(
    event: OrderEvent,
    eventId: String = "e-1",
    orderId: String = "o-1",
    sequence: Long = 1,
    offset: Long = 1,
): PersistedEvent =
    persistedEvent(globalOffset = offset, eventId = eventId, aggregateId = orderId, sequence = sequence)
        .copy(serialized = orderEventSerialization().serialize(event))

/** Another context's internal payment event, and the public event its contract publishes for a declined payment. */
internal data class PaymentRecorded(
    val customerId: String,
    val declined: Boolean,
) : DomainEvent

internal data class PaymentDeclined(
    val customerId: String,
) : PublicDomainEvent

internal fun paymentEvent(
    customerId: String,
    declined: Boolean = true,
    eventId: String = "p-1",
    sequence: Long = 1,
): PersistedEvent =
    persistedEvent(
        globalOffset = 1,
        eventId = eventId,
        aggregateType = "Payment",
        aggregateId = customerId,
        eventType = "PaymentRecorded",
        eventPayload = "$customerId:$declined",
        sequence = sequence,
    )

/** The payments context's contract over [backend]: declined payments become [PaymentDeclined]. */
internal fun paymentContract(
    backend: DomainEventPollingBackend,
    aggregateTypes: Set<AggregateType>? = setOf(AggregateType("Payment")),
): PublicEventContract<PaymentRecorded, PaymentDeclined> {
    var position = EventLogPosition.START
    return PublicEventContract(
        backend = backend,
        serialization =
            object : DataSerializationContext<PaymentRecorded> {
                override fun serialize(event: PaymentRecorded) = SerializedEvent("PaymentRecorded", 1, "${event.customerId}:${event.declined}")

                override fun deserialize(serialized: SerializedEvent): PaymentRecorded {
                    val (customerId, declined) = serialized.payload.split(":")
                    return PaymentRecorded(customerId, declined.toBooleanStrict())
                }
            },
        internalToPublic = { if (it.declined) PaymentDeclined(it.customerId) else null },
        getPosition = { position },
        savePosition = { position = it },
        isLeader = { true },
        aggregateTypes = aggregateTypes,
    )
}

/** An event log in memory: the reactor's and contracts' polling backend, and the parked mappings' read by id. */
internal class InMemoryLog : DomainEventPollingBackend {
    val events = mutableListOf<PersistedEvent>()

    /** Appends [event] at the next position and returns it as stored. */
    fun add(event: PersistedEvent): PersistedEvent {
        val offset = events.size + 1L
        val stored = event.copy(position = EventLogPosition(offset, offset))
        events += stored
        return stored
    }

    override fun readEventsAfter(
        position: EventLogPosition,
        limit: Int,
    ): List<PersistedEvent> = events.filter { it.position > position }.take(limit)

    fun readEvent(id: EventId): PersistedEvent? = events.firstOrNull { it.metadata.eventId == id }
}

/**
 * A use case over test orders that records what happens to it. [mapping] is its `on(kind)` block (by default a
 * [Confirm] for each placed order); [failWith], [decide], [work] and [onCompleted] script `handle`, `onFailure` and
 * `onCompletion`.
 */
internal class RecordingUseCase(
    name: String = "confirmations",
    override val ordering: ReactionOrdering = ReactionOrdering.Unordered,
    override val timeout: Duration = 60.seconds,
    kind: AggregateKind<*, OrderEvent, *>? = testOrders,
    private val mapping: TriggerScope<Notice>.(OrderEvent, EventMetadata) -> Unit = { event, metadata ->
        if (event is OrderPlaced) trigger(Confirm(metadata.aggregateId.value))
    },
) : Reactions<Notice>(name, Notice.serializer()) {
    val handled = mutableListOf<Pair<Notice, ReactionContext>>()
    val completions = mutableListOf<Pair<Notice, ReactionResult>>()
    val failures = mutableListOf<Pair<Int, Throwable>>()

    /** The exception `handle` throws for a trigger and context, or `null` to succeed. */
    var failWith: (Notice, ReactionContext) -> Throwable? = { _, _ -> null }

    /** The failure policy. */
    var decide: (Notice, Int, Throwable) -> FailureDecision = { _, attempt, _ -> Retry(backoff(attempt)) }

    /** Runs inside `handle`, before [failWith]. */
    var work: suspend (Notice) -> Unit = {}

    /** Runs inside `onCompletion`, after recording. */
    var onCompleted: (ReactionResult) -> Unit = {}

    init {
        if (kind != null) on(kind) { event, metadata -> mapping(this, event, metadata) }
    }

    /** Declares a contract source from outside, as only an `init` block normally would. */
    fun <P : PublicDomainEvent> listenTo(
        contract: PublicEventContract<*, P>,
        block: TriggerScope<Notice>.(P, EventMetadata) -> Unit,
    ) = on(contract, block)

    override suspend fun handle(
        trigger: Notice,
        context: ReactionContext,
    ) {
        handled += trigger to context
        work(trigger)
        failWith(trigger, context)?.let { throw it }
    }

    override fun onFailure(
        trigger: Notice,
        attempt: Int,
        error: Throwable,
    ): FailureDecision {
        failures += attempt to error
        return decide(trigger, attempt, error)
    }

    override suspend fun onCompletion(
        trigger: Notice,
        result: ReactionResult,
    ) {
        completions += trigger to result
        onCompleted(result)
    }
}

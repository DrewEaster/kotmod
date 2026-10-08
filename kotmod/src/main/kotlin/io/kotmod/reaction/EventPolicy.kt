package io.kotmod.reaction

import io.kotmod.AggregateKind
import io.kotmod.AggregateType
import io.kotmod.DomainEvent
import io.kotmod.EventMetadata
import io.kotmod.PublicDomainEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.ReactionOrdering
import kotlinx.serialization.KSerializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/**
 * An event policy: follow-up work in your application, reacting to events. It owns the whole reaction — which events
 * it reacts to, the triggers they produce, and how each trigger is handled — and runs on an [EventReactor].
 *
 * Declare its sources in `init`: `on(kind) { event, metadata -> … }` for one of this context's aggregate kinds and
 * `on(contract) { event, metadata -> … }` for another context's public events. Inside the block, `trigger(t)` queues a
 * trigger (`trigger(t, notBefore = instant)` delays it). Keep the block deterministic: the same event must produce the
 * same triggers in the same order, because kotmod numbers them to recognise a re-read event. An aggregate type can
 * reach a policy through only one source.
 *
 * [handle] does the work: returning means done; throwing, or running longer than [timeout], is a failure that
 * [onFailure] decides on. [onCompletion] is told how the work ended. Delivery is at least once, so [handle] must be
 * idempotent; [ReactionContext.reactionId] is a stable idempotency key.
 *
 * @param T the trigger type: plain `@Serializable` data.
 * @param name names the policy's queue and its rows in kotmod's reaction table, so keep it stable across releases.
 *   Unique within a reactor.
 * @param triggers serializes the triggers while they wait to run.
 */
abstract class EventPolicy<T : Any>(
    val name: String,
    val triggers: KSerializer<T>,
) {
    init {
        require(name.isNotBlank()) { "An event policy's name must not be blank" }
    }

    private val declared = mutableListOf<ReactionSource<T>>()

    /** Set when the policy is registered with a reactor; its sources can't change after that. */
    @Volatile
    internal var registered: Boolean = false

    internal val sources: List<ReactionSource<T>> get() = declared

    /** Whether an aggregate's work runs one at a time in event order ([ReactionOrdering.PerAggregate]); unordered by default. */
    open val ordering: ReactionOrdering = ReactionOrdering.Unordered

    /**
     * How long one attempt at [handle] may run; longer is a failure, reported to [onFailure] as a [ReactionTimeoutException].
     * A `TimeoutCancellationException` escaping [handle] from the app's own inner `withTimeout` is reported the same way.
     * [onFailure] and [onCompletion] run outside the timeout; keep them short. An ordered policy's work is protected from
     * a duplicate run for this timeout plus 30 seconds, and that covers them too.
     */
    open val timeout: Duration = 60.seconds

    /** Reacts to [kind]'s events, typed with its event serialization. Call it from `init`. */
    protected fun <E : DomainEvent> on(
        kind: AggregateKind<*, E, *>,
        block: TriggerScope<T>.(event: E, metadata: EventMetadata) -> Unit,
    ) {
        declare(KindSource(kind, block))
    }

    /**
     * Reacts to another context's public events, published by [contract]. Call it from `init`.
     *
     * The contract's own reader feeds this policy, so the contract must be started (after the policy is registered) or
     * nothing is delivered. It must read the same event log (database) as the reactor: a parked mapping re-reads its
     * event through the reactor, and an event missing there would be retried forever.
     */
    protected fun <P : PublicDomainEvent> on(
        contract: PublicEventContract<*, P>,
        block: TriggerScope<T>.(event: P, metadata: EventMetadata) -> Unit,
    ) {
        declare(ContractSource(contract, block))
    }

    /** Does the work for [trigger]. Returning normally means done; throwing is a failure. */
    abstract suspend fun handle(
        trigger: T,
        context: ReactionContext,
    )

    /** Decides what happens after attempt [attempt] failed with [error]: by default, retry with [backoff], forever. */
    open fun onFailure(
        trigger: T,
        attempt: Int,
        error: Throwable,
    ): FailureDecision = Retry(backoff(attempt))

    /** Told how the work for [trigger] ended. Does nothing by default. */
    open suspend fun onCompletion(
        trigger: T,
        result: ReactionResult,
    ) {}

    /** Capped exponential backoff for retry [attempt]: 1s, 2s, 4s… up to 10 minutes. */
    protected fun backoff(attempt: Int): Duration = BackoffStrategy().calculateBackoff(attempt)

    /** The source that delivers events of [type], if any. */
    internal fun sourceFor(type: AggregateType): ReactionSource<T>? = declared.firstOrNull { it.covers(type) }

    /** The aggregate kind source that delivers events of [type], if any: the reactor's own reader feeds only these. */
    internal fun kindSourceFor(type: AggregateType): ReactionSource<T>? = declared.firstOrNull { it is KindSource<*, *> && it.covers(type) }

    private fun declare(source: ReactionSource<T>) {
        check(!registered) { "Event policy $name is already registered with a reactor: declare its sources in its init block" }
        val clash = declared.firstOrNull { it.overlaps(source) }
        require(clash == null) {
            "Event policy $name listens to the same aggregate type through ${clash?.description} and ${source.description}; " +
                "an aggregate type can reach a policy through only one source"
        }
        declared += source
    }
}

/**
 * Marks kotmod's reaction DSL scopes, so a block nested in one (such as a lambda with its own [TriggerScope]) can't call
 * the outer scope's [TriggerScope.trigger] by accident: name the outer receiver explicitly (`this@on.trigger(…)`).
 */
@DslMarker
annotation class EventPolicyDsl

/** The receiver of an event policy's `on(...)` block: [trigger] queues work for the event being read. */
@EventPolicyDsl
class TriggerScope<T : Any> internal constructor() {
    internal val produced = mutableListOf<ProducedTrigger<T>>()

    /** Queues [trigger] for this event; with [notBefore] it doesn't run before that time (not allowed when ordered). */
    fun trigger(
        trigger: T,
        notBefore: Instant? = null,
    ) {
        produced += ProducedTrigger(trigger, notBefore)
    }
}

/** A trigger an event policy's block produced, before it is queued. */
internal data class ProducedTrigger<out T : Any>(
    val trigger: T,
    val notBefore: Instant?,
)


package io.kotmod.reaction

import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.event.reaction.ReactionQueues
import io.kotmod.event.reaction.stampFor
import io.kotmod.process.JsonTriggerSerializer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Instant

/** What an event policy's queue holds. kotmod's own envelope around the app's triggers; the queue only stores it. */
@Serializable
internal sealed interface PolicyItem : EventReactionTrigger

/** One of the policy's triggers, as JSON written with its `triggers` serializer. */
@Serializable
@SerialName("trigger")
internal data class TriggerItem(
    val trigger: String,
    @Transient override val timeout: Duration? = null,
) : PolicyItem

/**
 * An event the policy couldn't map to triggers (its block threw, the event couldn't be deserialized, or an ordered
 * policy produced a delayed trigger). Running it reads the event again and retries the mapping with the current code.
 */
@Serializable
@SerialName("parked")
internal data class ParkedMapping(
    val eventId: String,
    val aggregateType: String,
    val aggregateId: String,
    val source: String,
    @Transient override val timeout: Duration? = null,
) : PolicyItem

private val log = LoggerFactory.getLogger("io.kotmod.reaction.PolicyRuntime")

/**
 * Runs one event policy on its own queue, named after it and ordered when the policy is.
 *
 * It maps events to triggers, numbering them `<policy>/<eventId>/<n>` so a re-read event is recognised. A mapping that
 * fails is parked in the queue as `<policy>/<eventId>/mapping` (stamped like the event's first trigger, so an ordered
 * aggregate's later work waits behind it) instead of stopping the reader. It handles each delivered item: a trigger
 * with the policy's timeout and failure handling, a parked mapping by reading its event again (with [readEvent]) and
 * retrying the mapping, with capped backoff, forever.
 */
internal class PolicyRuntime<T : Any>(
    val policy: EventPolicy<T>,
    queues: ReactionQueues,
    private val readEvent: (EventId) -> PersistedEvent?,
    private val clock: () -> Instant = { Clock.System.now() },
) {
    private val ordered = policy.ordering is ReactionOrdering.PerAggregate
    private val channel = queues.channel(policy.name, JsonTriggerSerializer(PolicyItem.serializer()), ordered)
    private val backoff = BackoffStrategy()

    @Volatile
    private var subscription: Cancellable? = null

    init {
        require(!ordered || channel.sink.supportsOrdering) { "Event policy ${policy.name} is ordered, so its queue must support ordering" }
    }

    /** Starts handling the queue's deliveries. Does nothing if already started. */
    fun start() {
        if (subscription != null) return
        subscription = channel.source.subscribe { id, _, item, attempt, notBefore -> deliver(id, item, attempt, notBefore) }
    }

    /** Stops handling deliveries. */
    fun stop() {
        subscription?.cancel()
        subscription = null
    }

    /** Queues [trigger] as reaction [id]. */
    suspend fun publish(
        id: EventReactionId,
        trigger: T,
        ordering: DispatchOrdering?,
        notBefore: Instant?,
    ) = channel.sink.publish(id, TriggerItem(encode(trigger)), ordering, notBefore)

    /** Maps [event], read by the reactor, if one of this policy's aggregate kind sources covers its type. */
    suspend fun routeLocal(event: PersistedEvent) {
        val source = policy.kindSourceFor(event.metadata.aggregateType) ?: return
        mapAndPublish(event.metadata, source) { source.map(event) }
    }

    /**
     * Runs [map] (this policy's block for the event described by [metadata], from [source]) and queues what it
     * triggered. If mapping fails — [map] throws, a trigger can't be serialized, or an ordered policy produced a
     * delayed trigger — the event is parked instead, and nothing it triggered is queued. A failure to queue propagates,
     * so the reader stops and reads the event again.
     */
    suspend fun mapAndPublish(
        metadata: EventMetadata,
        source: ReactionSource<T>,
        map: () -> List<ProducedTrigger<T>>,
    ) {
        val items =
            try {
                encodeAll(map())
            } catch (e: Throwable) {
                rethrowIfCancelled(e)
                park(metadata, source, e)
                return
            }
        publishAll(metadata, items)
    }

    /**
     * Rethrows [error] if it is a [CancellationException] and this coroutine was cancelled. A CancellationException
     * thrown while the coroutine is still active came from the app's code (a mapping block or `handle`): it is a
     * failure like any other.
     */
    private suspend fun rethrowIfCancelled(error: Throwable) {
        if (error is CancellationException && !currentCoroutineContext().isActive) throw error
    }

    private fun encode(trigger: T): String = Json.encodeToString(policy.triggers, trigger)

    private fun encodeAll(produced: List<ProducedTrigger<T>>): List<Pair<TriggerItem, Instant?>> {
        check(!ordered || produced.none { it.notBefore != null }) {
            "Event policy ${policy.name} is ordered, so it can't produce delayed triggers: a delayed trigger would hold back " +
                "every later reaction of its aggregate"
        }
        return produced.map { TriggerItem(encode(it.trigger)) to it.notBefore }
    }

    private suspend fun publishAll(
        metadata: EventMetadata,
        items: List<Pair<TriggerItem, Instant?>>,
    ) {
        items.forEachIndexed { n, (item, notBefore) ->
            val id = EventReactionId("${policy.name}/${metadata.eventId.value}/$n")
            channel.sink.publish(id, item, policy.ordering.stampFor(metadata, n), notBefore)
        }
    }

    private suspend fun park(
        metadata: EventMetadata,
        source: ReactionSource<T>,
        error: Throwable,
    ) {
        log.error(
            "Event policy {} couldn't map event {} of {}/{} from {}; parking it in its queue and moving on",
            policy.name,
            metadata.eventId.value,
            metadata.aggregateType.value,
            metadata.aggregateId.value,
            source.description,
            error,
        )
        channel.sink.publish(
            EventReactionId("${policy.name}/${metadata.eventId.value}/mapping"),
            ParkedMapping(metadata.eventId.value, metadata.aggregateType.value, metadata.aggregateId.value, source.description),
            policy.ordering.stampFor(metadata, 0),
            null,
        )
    }

    private suspend fun deliver(
        id: EventReactionId,
        item: PolicyItem,
        attempt: Int,
        notBefore: Instant?,
    ): ReactionOutcome {
        val now = clock()
        if (notBefore != null && now < notBefore) return ReactionOutcome.Wait(notBefore - now)
        return when (item) {
            // A trigger that can't be decoded throws here, back to the queue, which retries it until a fix is deployed.
            is TriggerItem -> runTrigger(id, Json.decodeFromString(policy.triggers, item.trigger), attempt)
            is ParkedMapping -> runParked(id, item, attempt)
        }
    }

    private suspend fun runParked(
        id: EventReactionId,
        item: ParkedMapping,
        attempt: Int,
    ): ReactionOutcome =
        try {
            remap(item)
            ReactionOutcome.Finished(gaveUp = false)
        } catch (e: Throwable) {
            rethrowIfCancelled(e)
            val delay = backoff.calculateBackoff(attempt)
            log.error(
                "Parked mapping {} of event policy {} (event {} of {}/{} from {}) failed again; retrying in {} [attempt={}]",
                id.value,
                policy.name,
                item.eventId,
                item.aggregateType,
                item.aggregateId,
                item.source,
                delay,
                attempt,
                e,
            )
            ReactionOutcome.Retry(delay)
        }

    private suspend fun remap(item: ParkedMapping) {
        val event =
            withContext(Dispatchers.IO) { readEvent(EventId(item.eventId)) }
                ?: error("Event ${item.eventId} is not in the event log")
        val source = policy.sourceFor(event.metadata.aggregateType)
        if (source == null) {
            log.warn(
                "Event policy {} no longer listens to aggregate type {}; dropping its parked mapping of event {}",
                policy.name,
                event.metadata.aggregateType.value,
                item.eventId,
            )
            return
        }
        publishAll(event.metadata, encodeAll(source.map(event)))
    }

    private suspend fun runTrigger(
        id: EventReactionId,
        trigger: T,
        attempt: Int,
    ): ReactionOutcome {
        // handle's exception is caught inside withTimeout and carried out as a value, so only a genuine timeout or
        // cancellation crosses the timeout boundary (which keeps the app's exception from being copied by stack-trace recovery).
        val error: Throwable? =
            try {
                withTimeout(policy.timeout) {
                    try {
                        policy.handle(trigger, ReactionContext(id.value, attempt))
                        null
                    } catch (e: Throwable) {
                        // A timeout or a cancellation (e.g. the scheduler is stopping) is not a failure: it crosses the
                        // boundary, and an interrupted attempt is redelivered by the queue. A CancellationException
                        // thrown while the attempt is still running came from the app's code: a failure like any other.
                        rethrowIfCancelled(e)
                        e
                    }
                }
            } catch (e: TimeoutCancellationException) {
                ReactionTimeoutException(policy.timeout)
            }
        if (error == null) return finish(id, trigger, ReactionResult.Completed, attempt)
        log.error("Reaction {} of event policy {} failed [attempt={}]", id.value, policy.name, attempt, error)
        val decision =
            try {
                policy.onFailure(trigger, attempt, error)
            } catch (e: Throwable) {
                rethrowIfCancelled(e)
                log.error("onFailure of event policy {} threw for reaction {}; retrying the reaction", policy.name, id.value, e)
                return ReactionOutcome.Retry(backoff.calculateBackoff(attempt))
            }
        return when (decision) {
            is Retry -> ReactionOutcome.Retry(decision.delay)
            GiveUp -> {
                log.warn("Event policy {} gave up on reaction {} [attempt={}]", policy.name, id.value, attempt)
                finish(id, trigger, ReactionResult.GaveUp(error), attempt)
            }
        }
    }

    private suspend fun finish(
        id: EventReactionId,
        trigger: T,
        result: ReactionResult,
        attempt: Int,
    ): ReactionOutcome =
        try {
            policy.onCompletion(trigger, result)
            ReactionOutcome.Finished(gaveUp = result is ReactionResult.GaveUp)
        } catch (e: Throwable) {
            rethrowIfCancelled(e)
            log.error("onCompletion of event policy {} threw for reaction {}; retrying the reaction", policy.name, id.value, e)
            ReactionOutcome.Retry(backoff.calculateBackoff(attempt))
        }
}

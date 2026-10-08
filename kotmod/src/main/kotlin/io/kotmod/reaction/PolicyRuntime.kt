package io.kotmod.reaction

import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.Delivery
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ItemResult
import io.kotmod.event.reaction.LEASE_MARGIN
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.OrderedItem
import io.kotmod.event.reaction.Produced
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionQueue
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.lineKey
import io.kotmod.event.reaction.rethrowIfCancelled
import io.kotmod.scheduling.TaskScheduler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Instant

/** What an event policy's queue holds. kotmod's own envelope around the app's triggers. */
@Serializable
internal sealed interface PolicyItem

/** One of the policy's triggers, as JSON written with its `triggers` serializer. */
@Serializable
@SerialName("trigger")
internal data class TriggerItem(
    val trigger: String,
) : PolicyItem

/**
 * An event the policy couldn't map to triggers (its block threw, the event couldn't be deserialized, or an ordered
 * policy produced a delayed trigger). Running it reads the event again and retries the mapping with the current code.
 */
@Serializable
@SerialName("parked")
internal data class ParkedItem(
    val eventId: String,
    val aggregateType: String,
    val aggregateId: String,
    val source: String,
) : PolicyItem

private val log = LoggerFactory.getLogger("io.kotmod.reaction.PolicyRuntime")

/**
 * Runs one event policy on its own [ReactionQueue], named after it, with tasks on the [scheduler]'s queue of that name.
 *
 * It maps events to triggers, numbering them `<policy>/<eventId>/<n>` so a re-read event is recognised. An unordered
 * policy queues each trigger as a task; an ordered one adds them to their aggregate's line in [rows]. A mapping that
 * fails is parked as `<policy>/<eventId>/mapping` instead of stopping the reader: in the event's place in its line for
 * an ordered policy (so the aggregate's later work waits behind it), or as a kept row for an unordered one. It handles
 * each delivered item: a trigger with the policy's timeout and failure handling, a parked mapping by reading its event
 * again (with [readEvent]) and retrying the mapping, with capped backoff, forever. A recovered mapping's triggers take
 * its place in line, or are queued as unordered work.
 */
internal class PolicyRuntime<T : Any>(
    val policy: EventPolicy<T>,
    scheduler: TaskScheduler,
    rows: ReactionRows,
    private val readEvent: (EventId) -> PersistedEvent?,
    private val clock: () -> Instant = { Clock.System.now() },
) {
    private val ordered = policy.ordering is ReactionOrdering.PerAggregate
    private val onGiveUp = (policy.ordering as? ReactionOrdering.PerAggregate)?.onGiveUp
    private val queue =
        ReactionQueue(policy.name, PolicyItem.serializer(), scheduler.queue(policy.name), rows, policy.timeout + LEASE_MARGIN, clock)
    private val backoff = BackoffStrategy()

    /** Starts handling the queue's deliveries. Does nothing if already started. */
    fun start() = queue.start(::deliver)

    /** Stops handling deliveries. */
    fun stop() = queue.stop()

    /** Schedules again any of the policy's line fronts and kept rows idle for [idleFor] (tasks a backend lost). */
    suspend fun sweep(idleFor: Duration) = queue.sweep(idleFor)

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
        val produced =
            try {
                produce(metadata, map())
            } catch (e: Throwable) {
                rethrowIfCancelled(e)
                park(metadata, source, e)
                return
            }
        queueAll(metadata, produced)
    }

    /** Numbers [triggers] `<policy>/<eventId>/<n>`; refuses delayed triggers for an ordered policy. */
    private fun produce(
        metadata: EventMetadata,
        triggers: List<ProducedTrigger<T>>,
    ): List<Produced<PolicyItem>> {
        check(!ordered || triggers.none { it.notBefore != null }) {
            "Event policy ${policy.name} is ordered, so it can't produce delayed triggers: a delayed trigger would hold back " +
                "every later reaction of its aggregate"
        }
        return triggers.mapIndexed { n, produced ->
            Produced(
                EventReactionId("${policy.name}/${metadata.eventId.value}/$n"),
                TriggerItem(Json.encodeToString(policy.triggers, produced.trigger)),
                produced.notBefore,
            )
        }
    }

    private suspend fun queueAll(
        metadata: EventMetadata,
        produced: List<Produced<PolicyItem>>,
    ) {
        if (ordered) {
            queue.publishOrdered(produced.mapIndexed { n, p -> OrderedItem(p.id, lineKey(metadata), metadata.sequence, n, p.item) })
        } else {
            produced.forEach { queue.publish(it) }
        }
    }

    private suspend fun park(
        metadata: EventMetadata,
        source: ReactionSource<T>,
        error: Throwable,
    ) {
        log.error(
            "Event policy {} couldn't map event {} of {}/{} from {}; parking it and moving on",
            policy.name,
            metadata.eventId.value,
            metadata.aggregateType.value,
            metadata.aggregateId.value,
            source.description,
            error,
        )
        val id = EventReactionId("${policy.name}/${metadata.eventId.value}/mapping")
        val item = ParkedItem(metadata.eventId.value, metadata.aggregateType.value, metadata.aggregateId.value, source.description)
        if (ordered) {
            queue.publishOrdered(listOf(OrderedItem(id, lineKey(metadata), metadata.sequence, 0, item)))
        } else {
            queue.keep(id, item)
        }
    }

    private suspend fun deliver(delivery: Delivery<PolicyItem>): ItemResult<PolicyItem> =
        when (val item = delivery.item) {
            // A trigger that can't be decoded throws here, back to the backend, which delivers it again until a fix is deployed.
            is TriggerItem -> runTrigger(delivery.id, Json.decodeFromString(policy.triggers, item.trigger), delivery.attempt)
            is ParkedItem -> runParked(delivery.id, item, delivery.attempt)
        }

    private suspend fun runParked(
        id: EventReactionId,
        item: ParkedItem,
        attempt: Int,
    ): ItemResult<PolicyItem> =
        try {
            remap(item)
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
            ItemResult.Retry(delay)
        }

    private suspend fun remap(item: ParkedItem): ItemResult<PolicyItem> {
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
            return ItemResult.Completed
        }
        return ItemResult.Replaced(produce(event.metadata, source.map(event)))
    }

    private suspend fun runTrigger(
        id: EventReactionId,
        trigger: T,
        attempt: Int,
    ): ItemResult<PolicyItem> {
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
                        // boundary, and an interrupted attempt is delivered again. A CancellationException thrown while
                        // the attempt is still running came from the app's code: a failure like any other.
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
                return ItemResult.Retry(backoff.calculateBackoff(attempt))
            }
        return when (decision) {
            is Retry -> ItemResult.Retry(decision.delay)
            GiveUp -> {
                log.warn("Event policy {} gave up on reaction {} [attempt={}]", policy.name, id.value, attempt)
                finish(id, trigger, ReactionResult.GaveUp(error), attempt)
            }
        }
    }

    /**
     * Tells `onCompletion` how the reaction ended, then finishes it; a reaction that gave up under
     * [OnGiveUp.BlockAggregate] blocks its line instead.
     */
    private suspend fun finish(
        id: EventReactionId,
        trigger: T,
        result: ReactionResult,
        attempt: Int,
    ): ItemResult<PolicyItem> =
        try {
            policy.onCompletion(trigger, result)
            if (result is ReactionResult.GaveUp && onGiveUp == OnGiveUp.BlockAggregate) ItemResult.Blocked else ItemResult.Completed
        } catch (e: Throwable) {
            rethrowIfCancelled(e)
            log.error("onCompletion of event policy {} threw for reaction {}; retrying the reaction", policy.name, id.value, e)
            ItemResult.Retry(backoff.calculateBackoff(attempt))
        }
}

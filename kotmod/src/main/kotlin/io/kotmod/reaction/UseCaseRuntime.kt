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

/** What a use case's queue holds. kotmod's own envelope around the app's triggers; the queue only stores it. */
@Serializable
internal sealed interface UseCaseItem : EventReactionTrigger

/** One of the use case's triggers, as JSON written with its `triggers` serializer. */
@Serializable
@SerialName("trigger")
internal data class TriggerItem(
    val trigger: String,
    @Transient override val timeout: Duration? = null,
) : UseCaseItem

/**
 * An event the use case couldn't map to triggers (its block threw, the event couldn't be deserialized, or an ordered
 * use case produced a delayed trigger). Running it reads the event again and retries the mapping with the current code.
 */
@Serializable
@SerialName("parked")
internal data class ParkedMapping(
    val eventId: String,
    val aggregateType: String,
    val aggregateId: String,
    val source: String,
    @Transient override val timeout: Duration? = null,
) : UseCaseItem

private val log = LoggerFactory.getLogger("io.kotmod.reaction.UseCaseRuntime")

/**
 * Runs one use case on its own queue, named after it and ordered when the use case is.
 *
 * It maps events to triggers, numbering them `<useCase>/<eventId>/<n>` so a re-read event is recognised. A mapping that
 * fails is parked in the queue as `<useCase>/<eventId>/mapping` (stamped like the event's first trigger, so an ordered
 * aggregate's later work waits behind it) instead of stopping the reader. It handles each delivered item: a trigger
 * with the use case's timeout and failure policy, a parked mapping by reading its event again (with [readEvent]) and
 * retrying the mapping, with capped backoff, forever.
 */
internal class UseCaseRuntime<T : Any>(
    val useCase: Reactions<T>,
    queues: ReactionQueues,
    private val readEvent: (EventId) -> PersistedEvent?,
    private val clock: () -> Instant = { Clock.System.now() },
) {
    private val ordered = useCase.ordering is ReactionOrdering.PerAggregate
    private val channel = queues.channel(useCase.name, JsonTriggerSerializer(UseCaseItem.serializer()), ordered)
    private val backoff = BackoffStrategy()

    @Volatile
    private var subscription: Cancellable? = null

    init {
        require(!ordered || channel.sink.supportsOrdering) { "Use case ${useCase.name} is ordered, so its queue must support ordering" }
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

    /** Maps [event], read by the reactor, if one of this use case's aggregate kind sources covers its type. */
    suspend fun routeLocal(event: PersistedEvent) {
        val source = useCase.kindSourceFor(event.metadata.aggregateType) ?: return
        mapAndPublish(event.metadata, source) { source.map(event) }
    }

    /**
     * Runs [map] (this use case's block for the event described by [metadata], from [source]) and queues what it
     * triggered. If mapping fails — [map] throws, a trigger can't be serialized, or an ordered use case produced a
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
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                park(metadata, source, e)
                return
            }
        publishAll(metadata, items)
    }

    private fun encode(trigger: T): String = Json.encodeToString(useCase.triggers, trigger)

    private fun encodeAll(produced: List<ProducedTrigger<T>>): List<Pair<TriggerItem, Instant?>> {
        check(!ordered || produced.none { it.notBefore != null }) {
            "Use case ${useCase.name} is ordered, so it can't produce delayed triggers: a delayed trigger would hold back " +
                "every later reaction of its aggregate"
        }
        return produced.map { TriggerItem(encode(it.trigger)) to it.notBefore }
    }

    private suspend fun publishAll(
        metadata: EventMetadata,
        items: List<Pair<TriggerItem, Instant?>>,
    ) {
        items.forEachIndexed { n, (item, notBefore) ->
            val id = EventReactionId("${useCase.name}/${metadata.eventId.value}/$n")
            channel.sink.publish(id, item, useCase.ordering.stampFor(metadata, n), notBefore)
        }
    }

    private suspend fun park(
        metadata: EventMetadata,
        source: ReactionSource<T>,
        error: Throwable,
    ) {
        log.error(
            "Use case {} couldn't map event {} of {}/{} from {}; parking it in its queue and moving on",
            useCase.name,
            metadata.eventId.value,
            metadata.aggregateType.value,
            metadata.aggregateId.value,
            source.description,
            error,
        )
        channel.sink.publish(
            EventReactionId("${useCase.name}/${metadata.eventId.value}/mapping"),
            ParkedMapping(metadata.eventId.value, metadata.aggregateType.value, metadata.aggregateId.value, source.description),
            useCase.ordering.stampFor(metadata, 0),
            null,
        )
    }

    private suspend fun deliver(
        id: EventReactionId,
        item: UseCaseItem,
        attempt: Int,
        notBefore: Instant?,
    ): ReactionOutcome {
        val now = clock()
        if (notBefore != null && now < notBefore) return ReactionOutcome.Wait(notBefore - now)
        return when (item) {
            // A trigger that can't be decoded throws here, back to the queue, which retries it until a fix is deployed.
            is TriggerItem -> runTrigger(id, Json.decodeFromString(useCase.triggers, item.trigger), attempt)
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
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            val delay = backoff.calculateBackoff(attempt)
            log.error(
                "Parked mapping {} of use case {} (event {} of {}/{} from {}) failed again; retrying in {} [attempt={}]",
                id.value,
                useCase.name,
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
        val source = useCase.sourceFor(event.metadata.aggregateType)
        if (source == null) {
            log.warn(
                "Use case {} no longer listens to aggregate type {}; dropping its parked mapping of event {}",
                useCase.name,
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
                withTimeout(useCase.timeout) {
                    try {
                        useCase.handle(trigger, ReactionContext(id.value, attempt))
                        null
                    } catch (e: CancellationException) {
                        // Not a failure: the attempt was interrupted (e.g. the scheduler is stopping); the queue redelivers it.
                        throw e
                    } catch (e: Throwable) {
                        e
                    }
                }
            } catch (e: TimeoutCancellationException) {
                ReactionTimeoutException(useCase.timeout)
            }
        if (error == null) return finish(id, trigger, ReactionResult.Completed, attempt)
        log.error("Reaction {} of use case {} failed [attempt={}]", id.value, useCase.name, attempt, error)
        val decision =
            try {
                useCase.onFailure(trigger, attempt, error)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                log.error("onFailure of use case {} threw for reaction {}; retrying the reaction", useCase.name, id.value, e)
                return ReactionOutcome.Retry(backoff.calculateBackoff(attempt))
            }
        return when (decision) {
            is Retry -> ReactionOutcome.Retry(decision.delay)
            GiveUp -> {
                log.warn("Use case {} gave up on reaction {} [attempt={}]", useCase.name, id.value, attempt)
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
            useCase.onCompletion(trigger, result)
            ReactionOutcome.Finished(gaveUp = result is ReactionResult.GaveUp)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            log.error("onCompletion of use case {} threw for reaction {}; retrying the reaction", useCase.name, id.value, e)
            ReactionOutcome.Retry(backoff.calculateBackoff(attempt))
        }
}

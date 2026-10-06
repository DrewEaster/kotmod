package io.kotmod.contract

import io.kotmod.AggregateType
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.PublicEventEnvelope
import io.kotmod.outbox.DomainEventPoller
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.stampFor
import io.kotmod.process.ProcessEventSerialization
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Publishes a bounded context's internal domain events to other contexts as public events.
 *
 * While running, it polls the event log, deserializes each event with [serialization], maps it with
 * [internalToPublic] (returning `null` keeps an event private) and passes the public event to every
 * subscriber. Each subscriber turns it into event reactions for its own executor. Positions, leadership and
 * redelivery work as in [io.kotmod.outbox.AggregateEventOutbox].
 *
 * The event log holds every aggregate's events, process managers' facts included. kotmod's own internal events are
 * always skipped, but without [aggregateTypes] [serialization] must be able to read every other event type in the log.
 *
 * @param I the internal domain event type.
 * @param E the public event type.
 * @param aggregateTypes the aggregate types whose events this contract publishes; events of other types are skipped
 *   without being deserialized. With a filter, adding aggregate types or process managers to the context never affects
 *   this contract. `null` (the default) reads every aggregate type.
 */
class PublicEventContract<I : DomainEvent, E : PublicDomainEvent>(
    private val backend: DomainEventPollingBackend,
    private val serialization: DataSerializationContext<I>,
    private val internalToPublic: (I) -> E?,
    getPosition: () -> EventLogPosition,
    savePosition: (EventLogPosition) -> Unit,
    isLeader: () -> Boolean,
    internal val aggregateTypes: Set<AggregateType>? = null,
    pollInterval: Duration = 500.milliseconds,
    batchSize: Int = 100,
) {
    private data class Subscription<T : EventReactionTrigger, E : PublicDomainEvent>(
        val executor: EventReactionExecutor<T, *>,
        val ordering: ReactionOrdering,
        val block: (PublicEventEnvelope<E>) -> List<EventReaction<T>>,
    ) {
        /** Dispatches this subscription's reactions, numbering them from [firstOrdinal]; returns the next ordinal. */
        suspend fun fanOut(
            envelope: PublicEventEnvelope<E>,
            position: EventLogPosition,
            firstOrdinal: Int,
            log: Logger,
        ): Int {
            val reactions = block(envelope)
            reactions.forEachIndexed { index, reaction ->
                log.debug(
                    "Dispatching event reaction {} for DDD event {} [position={}]",
                    reaction.id.value,
                    envelope.metadata.eventId.value,
                    position,
                )
                executor.dispatch(reaction.id, reaction.trigger, ordering.stampFor(envelope.metadata, firstOrdinal + index), reaction.notBefore)
            }
            return firstOrdinal + reactions.size
        }
    }

    private val log = LoggerFactory.getLogger(PublicEventContract::class.java)
    private val subscriptions = mutableListOf<Subscription<*, E>>()
    private var started = false

    /**
     * Registers a subscriber: [block] maps each public event to reactions dispatched to [executor].
     * Reactions are stamped per [ordering]; ordered subscriptions need an executor whose sink supports
     * ordering. Ordered subscriptions of this contract may share an executor and then share ordering for an
     * aggregate, but that executor may not be fed ordered reactions by any other outbox or contract. Must be
     * called before [start].
     */
    fun <T : EventReactionTrigger> subscribe(
        executor: EventReactionExecutor<T, *>,
        ordering: ReactionOrdering = ReactionOrdering.Unordered,
        block: (PublicEventEnvelope<E>) -> List<EventReaction<T>>,
    ) {
        check(!started) { "subscribe() must be called before start()" }
        require(ordering == ReactionOrdering.Unordered || executor.supportsOrdering) {
            "An ordered subscription needs an executor whose sink supports ordering"
        }
        if (ordering != ReactionOrdering.Unordered) executor.claimOrderedSource(this)
        subscriptions += Subscription(executor, ordering, block)
    }

    private val poller =
        DomainEventPoller(
            backend = backend,
            getPosition = getPosition,
            savePosition = savePosition,
            isLeader = isLeader,
            pollInterval = pollInterval,
            batchSize = batchSize,
            loggerName = "PublicEventContract",
            handleEvent = ::handleEvent,
        )

    /** Starts polling in the background. No further subscribers can be added afterwards. */
    fun start() {
        started = true
        poller.start()
    }

    /** Stops polling and waits for the current poll to finish. */
    suspend fun stop() = poller.stop()

    /** Runs a single poll. For tests only. */
    internal suspend fun tickForTest() = poller.tickForTest()

    /**
     * Converts [event] to the public event this contract publishes for it, or `null` if it publishes none (kotmod's
     * internal events, aggregate types outside [aggregateTypes], and events [internalToPublic] keeps private). Throws if
     * [serialization] or [internalToPublic] does.
     */
    internal fun toPublic(event: PersistedEvent): PublicEventEnvelope<E>? {
        // A process manager's internal envelopes are only for that process manager; no app serialization can read them.
        if (ProcessEventSerialization.isEnvelope(event.serialized.type)) return null
        if (aggregateTypes != null && event.metadata.aggregateType !in aggregateTypes) return null
        val internal: I = serialization.deserialize(event.serialized)
        val public: E = internalToPublic(internal) ?: return null
        return PublicEventEnvelope(event.metadata, public)
    }

    private suspend fun handleEvent(envelope: PersistedEvent) {
        val publicEnvelope = toPublic(envelope) ?: return
        // One ordinal counter across all subscriptions: ordered subscriptions sharing an executor share ordering
        // for an aggregate, so anything dispatched later for this event must sort later.
        var ordinal = 0
        for (subscription in subscriptions) {
            ordinal = subscription.fanOut(publicEnvelope, envelope.position, ordinal, log)
        }
    }
}

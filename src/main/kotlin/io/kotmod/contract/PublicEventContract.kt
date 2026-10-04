package io.kotmod.contract

import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.PublicEventEnvelope
import io.kotmod.outbox.DomainEventPoller
import io.kotmod.DomainEventPollingBackend
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionTrigger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Publishes a bounded context's internal domain events to other contexts as public events.
 *
 * While running, it polls the event log, deserializes each event with [serialization], maps it with
 * [internalToPublic] (returning `null` keeps an event private) and passes the public event to every
 * subscriber. Each subscriber turns it into event reactions for its own executor. Offsets, leadership
 * and redelivery work as in [io.kotmod.outbox.AggregateEventOutbox].
 *
 * @param I the internal domain event type.
 * @param E the public event type.
 */
class PublicEventContract<I : DomainEvent, E : PublicDomainEvent>(
    private val backend: DomainEventPollingBackend,
    private val serialization: DataSerializationContext<I>,
    private val internalToPublic: (I) -> E?,
    getOffset: () -> Long,
    saveOffset: (Long) -> Unit,
    isLeader: () -> Boolean,
    pollInterval: Duration = 500.milliseconds,
    batchSize: Int = 100,
) {
    private data class Subscription<T : EventReactionTrigger, E : PublicDomainEvent>(
        val executor: EventReactionExecutor<T, *>,
        val block: (PublicEventEnvelope<E>) -> List<EventReaction<T>>,
    ) {
        suspend fun fanOut(
            envelope: PublicEventEnvelope<E>,
            globalOffset: Long,
            log: Logger,
        ) {
            val reactions = block(envelope)
            for (reaction in reactions) {
                log.debug(
                    "Dispatching event reaction {} for DDD event {} [offset={}]",
                    reaction.id.value,
                    envelope.metadata.eventId.value,
                    globalOffset,
                )
                executor.dispatch(reaction.id, reaction.trigger)
            }
        }
    }

    private val log = LoggerFactory.getLogger(PublicEventContract::class.java)
    private val subscriptions = mutableListOf<Subscription<*, E>>()
    private var started = false

    /**
     * Registers a subscriber: [block] maps each public event to reactions dispatched to [executor].
     * Must be called before [start].
     */
    fun <T : EventReactionTrigger> subscribe(
        executor: EventReactionExecutor<T, *>,
        block: (PublicEventEnvelope<E>) -> List<EventReaction<T>>,
    ) {
        check(!started) { "subscribe() must be called before start()" }
        subscriptions += Subscription(executor, block)
    }

    private val poller =
        DomainEventPoller(
            backend = backend,
            getOffset = getOffset,
            saveOffset = saveOffset,
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

    private suspend fun handleEvent(envelope: PersistedEvent) {
        val internal: I = serialization.deserialize(envelope.serialized)
        val public: E = internalToPublic(internal) ?: return
        val publicEnvelope = PublicEventEnvelope(envelope.metadata, public)
        for (subscription in subscriptions) {
            subscription.fanOut(publicEnvelope, envelope.globalOffset, log)
        }
    }
}

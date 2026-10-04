package io.kotmod.outbox

import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionTrigger
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Turns domain events into event reactions (the transactional outbox pattern).
 *
 * While running, it polls the event log after the position returned by [getPosition], calls
 * [eventToReactions] for each event and dispatches the resulting reactions to [executor]. The position is
 * saved after all of an event's reactions are dispatched, so an event is never skipped; after a crash
 * it may be dispatched again, which is why reaction ids should be deterministic. Polling only happens
 * while [isLeader] returns `true`, so run one instance per cluster.
 *
 * @param pollInterval pause between polls.
 * @param batchSize maximum number of events read per poll.
 */
class AggregateEventOutbox<T : EventReactionTrigger>(
    private val backend: DomainEventPollingBackend,
    private val executor: EventReactionExecutor<T, *>,
    private val eventToReactions: (PersistedEvent) -> List<EventReaction<T>>,
    getPosition: () -> EventLogPosition,
    savePosition: (EventLogPosition) -> Unit,
    isLeader: () -> Boolean,
    pollInterval: Duration = 500.milliseconds,
    batchSize: Int = 100,
) {
    private val log = LoggerFactory.getLogger(AggregateEventOutbox::class.java)

    private val poller =
        DomainEventPoller(
            backend = backend,
            batchSize = batchSize,
            getPosition = getPosition,
            savePosition = savePosition,
            isLeader = isLeader,
            pollInterval = pollInterval,
            loggerName = "AggregateEventOutbox",
            handleEvent = { envelope ->
                val reactions = eventToReactions(envelope)
                for ((id, trigger) in reactions) {
                    log.debug(
                        "Dispatching event reaction {} for DDD event {} [position={}]",
                        id.value,
                        envelope.metadata.eventId.value,
                        envelope.position,
                    )
                    executor.dispatch(id, trigger)
                }
            },
        )

    /** Starts polling in the background. Does nothing if already started. */
    fun start() = poller.start()

    /** Stops polling and waits for the current poll to finish. */
    suspend fun stop() = poller.stop()

    /** Runs a single poll. For tests only. */
    internal suspend fun tickForTest() = poller.tickForTest()
}

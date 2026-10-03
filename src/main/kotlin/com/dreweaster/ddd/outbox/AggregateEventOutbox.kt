package com.dreweaster.ddd.outbox

import com.dreweaster.ddd.DomainEventPollingBackend
import com.dreweaster.ddd.PersistedEvent
import com.dreweaster.ddd.event.reaction.EventReaction
import com.dreweaster.ddd.event.reaction.EventReactionExecutor
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class AggregateEventOutbox<T : EventReactionTrigger>(
    private val backend: DomainEventPollingBackend,
    private val executor: EventReactionExecutor<T, *>,
    private val eventToReactions: (PersistedEvent) -> List<EventReaction<T>>,
    getOffset: () -> Long,
    saveOffset: (Long) -> Unit,
    isLeader: () -> Boolean,
    pollInterval: Duration = 500.milliseconds,
    batchSize: Int = 100,
) {
    private val log = LoggerFactory.getLogger(AggregateEventOutbox::class.java)

    private val poller =
        DomainEventPoller(
            backend = backend,
            batchSize = batchSize,
            getOffset = getOffset,
            saveOffset = saveOffset,
            isLeader = isLeader,
            pollInterval = pollInterval,
            loggerName = "AggregateEventOutbox",
            handleEvent = { envelope ->
                val reactions = eventToReactions(envelope)
                for ((id, trigger) in reactions) {
                    log.debug(
                        "Dispatching event reaction {} for DDD event {} [offset={}]",
                        id.value,
                        envelope.metadata.eventId.value,
                        envelope.globalOffset,
                    )
                    executor.dispatch(id, trigger)
                }
            },
        )

    fun start() = poller.start()

    suspend fun stop() = poller.stop()

    /** Visible for unit tests. Runs a single poll cycle without the loop wrapper. */
    internal suspend fun tickForTest() = poller.tickForTest()
}

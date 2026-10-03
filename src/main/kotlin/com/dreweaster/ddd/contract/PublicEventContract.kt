package com.dreweaster.ddd.contract

import com.dreweaster.ddd.DataSerializationContext
import com.dreweaster.ddd.DomainEvent
import com.dreweaster.ddd.PublicDomainEvent
import com.dreweaster.ddd.PublicEventEnvelope
import com.dreweaster.ddd.outbox.DomainEventPoller
import app.cash.sqldelight.driver.jdbc.JdbcDriver
import com.dreweaster.ddd.DomainEventPollingBackend
import com.dreweaster.ddd.PersistedEvent
import com.dreweaster.ddd.event.reaction.EventReaction
import com.dreweaster.ddd.event.reaction.EventReactionExecutor
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

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

    fun start() {
        started = true
        poller.start()
    }

    suspend fun stop() = poller.stop()

    /** Visible for unit tests. Runs a single poll cycle without the loop wrapper. */
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

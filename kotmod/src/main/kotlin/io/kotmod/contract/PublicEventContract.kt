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
import io.kotmod.process.ProcessEventSerialization
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Publishes a bounded context's internal domain events to other contexts as public events.
 *
 * While running, it polls the event log, deserializes each event with [serialization], maps it with
 * [internalToPublic] (returning `null` keeps an event private) and hands the public event to its listeners, in the
 * order they registered: event policies listening with `on(contract)` and process managers subscribed with
 * `subscribeTo`. Each listener queues its own work in its own queue. Register them all before [start]. Positions,
 * leadership and redelivery work as for the reactor.
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
    private var started = false

    /** Throws if this contract has started, so a listener can check every contract before attaching to any. */
    internal fun ensureCanListen() =
        check(!started) { "An event policy or process manager must be registered before the contract it listens to starts" }

    private val listeners = mutableListOf<suspend (PublicEventEnvelope<E>) -> Unit>()

    /**
     * Feeds each public event to [listener] (an event policy's `on(contract)` source, or a process manager's
     * `subscribeTo`), after the listeners registered before it. Must be called before [start].
     */
    internal fun listen(listener: suspend (PublicEventEnvelope<E>) -> Unit) {
        ensureCanListen()
        listeners += listener
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

    /** Starts polling in the background. No further listeners can be added afterwards. */
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
        // A listener's publish failure makes the poller read the whole event again; safe, as trigger ids are deterministic.
        for (listener in listeners) listener(publicEnvelope)
    }
}

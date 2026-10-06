package io.kotmod.reaction

import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.ReactionQueues
import io.kotmod.jdbc.JdbcContext
import io.kotmod.outbox.DomainEventPoller
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.process.ProcessEventSerialization
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

/**
 * Runs a context's use cases. It reads the event log once, after one saved position, and hands each event to every
 * registered use case that listens to its aggregate type with `on(kind)`. It queues each use case's triggers on that
 * use case's own queue from [queues] (named after the use case). Use cases listening to another context's contract with
 * `on(contract)` are fed by that contract's own reader instead.
 *
 * - If a use case can't map an event (its block throws, the event can't be deserialized, or an ordered use case produces
 *   a delayed trigger), the event is parked in that use case's queue and the reactor moves on. Other use cases still
 *   get their triggers for the event.
 * - The position is saved after each event, once every use case's triggers for it are queued. After a crash the event
 *   is read again and queued with the same ids, which a queue recognises while the work is pending.
 * - It reads only while [isLeader]; run one active reactor per context. kotmod's internal events never reach a use
 *   case.
 *
 * Register every use case, then start the reactor before the queue's scheduler, and stop it after. A new reactor
 * starts at the head of the event log as of its first [start]: it sees events written from then on, not history.
 *
 * @param name the consumer name its position is saved under.
 */
class EventReactor internal constructor(
    private val queues: ReactionQueues,
    polling: DomainEventPollingBackend,
    private val readEvent: (EventId) -> PersistedEvent?,
    private val getPosition: () -> EventLogPosition,
    savePosition: (EventLogPosition) -> Unit,
    isLeader: () -> Boolean,
    val name: String,
    pollInterval: Duration,
    batchSize: Int,
    private val clock: () -> Instant,
) {
    constructor(
        jdbc: JdbcContext,
        queues: ReactionQueues,
        isLeader: () -> Boolean,
        name: String = "reactor",
        pollInterval: Duration = 500.milliseconds,
        batchSize: Int = 100,
    ) : this(
        queues = queues,
        polling = PostgresDomainPollingBackend(jdbc),
        readEvent = PostgresDomainPollingBackend(jdbc)::readEvent,
        getPosition = { PostgresOffsetManager(jdbc).getPosition(name) },
        savePosition = { PostgresOffsetManager(jdbc).savePosition(name, it) },
        isLeader = isLeader,
        name = name,
        pollInterval = pollInterval,
        batchSize = batchSize,
        clock = { Clock.System.now() },
    )

    private val runtimes = mutableListOf<UseCaseRuntime<*>>()

    /** Set by the first start; no use case can be registered after it. */
    @Volatile
    private var started = false

    /** Whether the reader is running (between [start] and [stop]). */
    @Volatile
    private var running = false

    private val poller =
        DomainEventPoller(
            backend = polling,
            getPosition = getPosition,
            savePosition = savePosition,
            isLeader = isLeader,
            pollInterval = pollInterval,
            batchSize = batchSize,
            loggerName = "EventReactor($name)",
            handleEvent = ::route,
        )

    /**
     * Registers [useCase]: it gets its own queue, named after it, and its `on(contract)` sources start listening to their
     * contracts. Must be called before [start], before reading the queues' tasks, and before those contracts start.
     */
    fun <T : Any> register(useCase: Reactions<T>) {
        check(!started) { "Use case ${useCase.name} was registered after reactor $name started: register every use case before start()" }
        require(runtimes.none { it.useCase.name == useCase.name }) { "Reactor $name already has a use case named ${useCase.name}" }
        check(!useCase.registered) { "Use case ${useCase.name} is already registered with a reactor" }
        runtimes += UseCaseRuntime(useCase, queues, readEvent, clock)
        useCase.registered = true
    }

    /**
     * Starts handling the use cases' queues and reading the event log. A new reactor's starting position is fixed before
     * this returns, so events committed afterwards are always seen. Does nothing if already running; a stopped reactor
     * can be started again.
     */
    fun start() {
        if (running) return
        startUseCases()
        getPosition()
        poller.start()
        running = true
    }

    /** Stops reading, then stops handling the use cases' queues. */
    suspend fun stop() {
        poller.stop()
        runtimes.forEach { it.stop() }
        running = false
    }

    internal fun startUseCasesForTest() = startUseCases()

    private fun startUseCases() {
        started = true
        runtimes.forEach { it.start() }
    }

    internal suspend fun tickForTest() = poller.tickForTest()

    private suspend fun route(event: PersistedEvent) {
        // A process manager's internal envelopes are only for that process manager.
        if (ProcessEventSerialization.isEnvelope(event.serialized.type)) return
        for (runtime in runtimes) runtime.routeLocal(event)
    }
}

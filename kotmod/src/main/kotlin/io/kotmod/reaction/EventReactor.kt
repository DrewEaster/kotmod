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
 * Runs a context's event policies. It reads the event log once, after one saved position, and hands each event to
 * every registered policy that listens to its aggregate type with `on(kind)`. It queues each policy's triggers on that
 * policy's own queue from [queues] (named after the policy). Policies listening to another context's contract with
 * `on(contract)` are fed by that contract's own reader instead.
 *
 * - If a policy can't map an event (its block throws, the event can't be deserialized, or an ordered policy produces
 *   a delayed trigger), the event is parked in that policy's queue and the reactor moves on. Other policies still
 *   get their triggers for the event.
 * - The position is saved after each event, once every policy's triggers for it are queued. After a crash the event
 *   is read again and queued with the same ids, which a queue recognises while the work is pending.
 * - It reads only while [isLeader]; run one active reactor per context. kotmod's internal events never reach an
 *   event policy.
 *
 * Register every event policy, then start the reactor before the queue's scheduler, and stop it after. A new reactor
 * starts at the head of the event log as of its first [start]: it sees events written from then on, not history.
 * [register], [start] and [stop] are meant to be called from one thread, at startup and shutdown; they are not
 * safe to call concurrently.
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

    private val runtimes = mutableListOf<PolicyRuntime<*>>()

    /** Set by the first start; no event policy can be registered after it. */
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
     * Registers [policy]: it gets its own queue, named after it, and its `on(contract)` sources start listening to their
     * contracts. Must be called before [start], before reading the queues' tasks, and before those contracts start.
     * Each such contract must read the same event log (database) as this reactor, because a parked mapping re-reads its
     * event through the reactor, and a contract that is never started delivers nothing to its event policies.
     */
    fun <T : Any> register(policy: EventPolicy<T>) {
        check(!started) { "Event policy ${policy.name} was registered after reactor $name started: register every policy before start()" }
        require(runtimes.none { it.policy.name == policy.name }) { "Reactor $name already has an event policy named ${policy.name}" }
        check(!policy.registered) { "Event policy ${policy.name} is already registered with a reactor" }
        // Check every contract first, so a refused registration attaches nothing and creates no queue.
        val contractSources = policy.sources.filterIsInstance<ContractSource<T, *>>()
        contractSources.forEach { it.ensureCanFeed() }
        val runtime = PolicyRuntime(policy, queues, readEvent, clock)
        contractSources.forEach { it.feed(runtime) }
        runtimes += runtime
        policy.registered = true
    }

    /**
     * Starts handling the policies' queues and reading the event log. A new reactor's starting position is fixed before
     * this returns, so events committed afterwards are always seen. Does nothing if already running; a stopped reactor
     * can be started again.
     */
    fun start() {
        if (running) return
        startPolicies()
        getPosition()
        poller.start()
        running = true
    }

    /** Stops reading, then stops handling the policies' queues. */
    suspend fun stop() {
        poller.stop()
        runtimes.forEach { it.stop() }
        running = false
    }

    internal fun startPoliciesForTest() = startPolicies()

    private fun startPolicies() {
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

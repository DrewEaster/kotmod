package io.kotmod.reaction

import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.rethrowIfCancelled
import io.kotmod.jdbc.JdbcContext
import io.kotmod.outbox.DomainEventPoller
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.PostgresReactionRows
import io.kotmod.process.ProcessEventSerialization
import io.kotmod.scheduling.TaskScheduler
import org.slf4j.LoggerFactory
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant

/**
 * Runs a context's event policies. It reads the event log once, after one saved position, and hands each event to
 * every registered policy that listens to its aggregate type with `on(kind)`. It queues each policy's triggers on that
 * policy's queue from [scheduler] (named after the policy). An ordered policy keeps each aggregate's work in a line in
 * kotmod's `ddd_reaction_row` table, and only the front of each line is scheduled. Policies listening to another
 * context's contract with `on(contract)` are fed by that contract's own reader instead.
 *
 * - If a policy can't map an event (its block throws, the event can't be deserialized, or an ordered policy produces
 *   a delayed trigger), the event is parked in that policy's rows (for an ordered policy, in the event's place in its
 *   aggregate's line) and the reactor moves on. Other policies still get their triggers for the event.
 * - The position is saved after each event, once every policy's triggers for it are queued. After a crash the event
 *   is read again and queued with the same ids, which are recognised while the work is pending.
 * - It reads only while [isLeader]; run one active reactor per context. kotmod's internal events never reach an
 *   event policy.
 * - On the leader, on its first read and then about every 10 minutes, a repair sweep schedules again any line front or
 *   parked mapping that has sat idle for a while, in case the scheduler lost its task. A failed sweep is logged and
 *   tried again next time; it never stops the reading.
 *
 * Register every event policy, then start the reactor before the scheduler, and stop it after. A new reactor
 * starts at the head of the event log as of its first [start]: it sees events written from then on, not history.
 * [register], [start] and [stop] are meant to be called from one thread, at startup and shutdown; they are not
 * safe to call concurrently.
 *
 * @param name the consumer name its position is saved under.
 */
class EventReactor internal constructor(
    private val scheduler: TaskScheduler,
    private val rows: ReactionRows,
    polling: DomainEventPollingBackend,
    private val readEvent: (EventId) -> PersistedEvent?,
    private val getPosition: () -> EventLogPosition,
    savePosition: (EventLogPosition) -> Unit,
    isLeader: () -> Boolean,
    val name: String,
    pollInterval: Duration,
    batchSize: Int,
    private val sweepEvery: Duration = 10.minutes,
    private val sweepIdle: Duration = 30.minutes,
    private val clock: () -> Instant,
) {
    constructor(
        jdbc: JdbcContext,
        scheduler: TaskScheduler,
        isLeader: () -> Boolean,
        name: String = "reactor",
        pollInterval: Duration = 500.milliseconds,
        batchSize: Int = 100,
    ) : this(
        scheduler = scheduler,
        rows = PostgresReactionRows(jdbc),
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

    private val log = LoggerFactory.getLogger("EventReactor($name)")

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
            afterTick = ::sweepIfDue,
        )

    /** When the last repair sweep started; touched only by the reader's loop. */
    private var lastSweep: Instant? = null

    /**
     * Registers [policy]: it gets its own queue from the scheduler, named after it, and its `on(contract)` sources start
     * listening to their contracts. Must be called before [start], before the scheduler runs tasks, and before those
     * contracts start.
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
        val runtime = PolicyRuntime(policy, scheduler, rows, readEvent, clock)
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

    /** Runs the repair sweep on the first tick, then at most every [sweepEvery]. */
    private suspend fun sweepIfDue() {
        val now = clock()
        val last = lastSweep
        if (last != null && now - last < sweepEvery) return
        lastSweep = now
        runtimes.forEach { runtime ->
            try {
                runtime.sweep(sweepIdle)
            } catch (e: Exception) {
                rethrowIfCancelled(e)
                log.warn("Repair sweep of event policy {} failed; trying again next time", runtime.policy.name, e)
            }
        }
    }
}

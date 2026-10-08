package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.DomainPersistenceBackend
import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.PublicEventEnvelope
import io.kotmod.Repository
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OrderedItem
import io.kotmod.event.reaction.Produced
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionQueue
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.lineKey
import io.kotmod.event.reaction.rethrowIfCancelled
import io.kotmod.jdbc.JdbcContext
import io.kotmod.outbox.DomainEventPoller
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresReactionRows
import io.kotmod.scheduling.TaskScheduler
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant

/**
 * A long-running workflow: it reacts to events (translated into its own inputs), keeps its own state per process
 * instance, records its own facts, requests commands to aggregates of this context, and schedules inputs to itself,
 * never doing anything synchronously.
 *
 * - [translate] is the anti-corruption layer for this context's events: it turns an event into the process id and
 *   input it is for, or `null` to ignore it. [subscribeTo] does the same for another context's public events.
 * - Each instance is decided by its state ([ProcessState.handle]), or by [initial] for an instance that doesn't exist
 *   yet. A transition is recorded in one transaction: the state, the process's own events, the commands it requests
 *   and the inputs it schedules.
 * - Requested commands are run against [targets] through their `AggregateManager.handle`; a rejection comes back as an
 *   input, via the target's mapping.
 * - Scheduled inputs (timeouts) are delivered at their time.
 *
 * It reads the event log with its own poller (only while [isLeader]), and runs its work on queues from [scheduler]:
 * `<type>-inputs`, `<type>-internal` (scheduled inputs and rejection feedback), `<type>-commands`, and
 * `<type>-contract-<name>` per [subscribeTo]. Each input or command may run for 60 seconds; failures and timeouts are
 * retried with capped backoff and never given up. Ordered inputs wait in their source aggregate's line in kotmod's
 * `ddd_reaction_row` table (one line per queue, so inputs from its own reader and from each contract are ordered
 * separately). On the leader, on its first read and then about every 10 minutes, a repair sweep schedules again any
 * line front that has sat idle for 30 minutes, in case the scheduler lost its task. Start it before the scheduler, and
 * stop it after.
 *
 * @param type the process manager's aggregate type; its instances and events are recorded under it.
 * @param repository stores each instance's state, in the same transaction as its events.
 * @param inputOrdering whether inputs from one source aggregate are delivered in that aggregate's order. Inputs are
 *   retried until they succeed, so `onGiveUp` never applies.
 */
class ProcessManager<S : ProcessState<S, I, E>, I : Any, E : DomainEvent> internal constructor(
    private val type: AggregateType,
    repository: Repository<S>,
    persistence: DomainPersistenceBackend<DomainEvent>,
    polling: DomainEventPollingBackend,
    initial: ProcessInitialState<S, I, E>,
    private val inputSerializer: KSerializer<I>,
    eventSerialization: DataSerializationContext<E>,
    private val translate: (PersistedEvent) -> Pair<AggregateId, I>?,
    targets: List<ProcessTarget<I>>,
    private val scheduler: TaskScheduler,
    private val rows: ReactionRows,
    inputOrdering: ReactionOrdering = ReactionOrdering.Unordered,
    getPosition: () -> EventLogPosition,
    savePosition: (EventLogPosition) -> Unit,
    isLeader: () -> Boolean,
    pollInterval: Duration = 500.milliseconds,
    batchSize: Int = 100,
    private val sweepEvery: Duration = 10.minutes,
    private val sweepIdle: Duration = 30.minutes,
    private val clock: () -> Instant = { Clock.System.now() },
) {
    constructor(
        type: AggregateType,
        repository: Repository<S>,
        jdbc: JdbcContext,
        initial: ProcessInitialState<S, I, E>,
        inputSerializer: KSerializer<I>,
        eventSerialization: DataSerializationContext<E>,
        translate: (PersistedEvent) -> Pair<AggregateId, I>?,
        targets: List<ProcessTarget<I>>,
        scheduler: TaskScheduler,
        inputOrdering: ReactionOrdering = ReactionOrdering.Unordered,
        getPosition: () -> EventLogPosition,
        savePosition: (EventLogPosition) -> Unit,
        isLeader: () -> Boolean,
        pollInterval: Duration = 500.milliseconds,
        batchSize: Int = 100,
        clock: () -> Instant = { Clock.System.now() },
    ) : this(
        type,
        repository,
        PostgresDomainPersistenceBackend(jdbc, ProcessEventSerialization(eventSerialization)),
        PostgresDomainPollingBackend(jdbc, includeProcessEnvelopes = true),
        initial,
        inputSerializer,
        eventSerialization,
        translate,
        targets,
        scheduler,
        PostgresReactionRows(jdbc, clock),
        inputOrdering,
        getPosition,
        savePosition,
        isLeader,
        pollInterval,
        batchSize,
        clock = clock,
    )

    private val targetsByType: Map<AggregateType, ProcessTarget<I>> =
        targets.associateBy { it.type }.also { byType ->
            require(byType.size == targets.size) { "A process manager can have only one target per aggregate type" }
        }

    private val streamSerialization = ProcessEventSerialization(eventSerialization)
    private val instances = ProcessInstances(type, repository, persistence, streamSerialization, initial, inputSerializer, targetsByType.keys)
    private val ordered = inputOrdering is ReactionOrdering.PerAggregate

    private val inputs = processQueue("${type.value}-inputs", InputTrigger.serializer(), scheduler, rows, clock)
    private val internal = processQueue("${type.value}-internal", InputTrigger.serializer(), scheduler, rows, clock)
    private val commands = processQueue("${type.value}-commands", CommandTrigger.serializer(), scheduler, rows, clock)
    private val contractQueues = mutableListOf<ReactionQueue<InputTrigger>>()
    private val subscriptionNames = mutableSetOf<String>()
    private var started = false

    /** When the last repair sweep started; touched only by the poller's loop. */
    private var lastSweep: Instant? = null

    private val log = LoggerFactory.getLogger("ProcessManager(${type.value})")

    private val poller =
        DomainEventPoller(
            backend = polling,
            getPosition = getPosition,
            savePosition = savePosition,
            isLeader = isLeader,
            pollInterval = pollInterval,
            batchSize = batchSize,
            loggerName = "ProcessManager(${type.value})",
            handleEvent = ::route,
            afterTick = ::sweepIfDue,
        )

    /**
     * Delivers another context's public events to this process manager: [translate] turns each one into the process id
     * and input it is for, or `null` to ignore it. [name] names the queue (`<type>-contract-<name>`), must be unique within
     * this process manager and must stay the same across restarts. Must be called before [start] and before [contract]
     * starts.
     */
    fun <P : PublicDomainEvent> subscribeTo(
        name: String,
        contract: PublicEventContract<*, P>,
        translate: (PublicEventEnvelope<P>) -> Pair<AggregateId, I>?,
    ) {
        check(!started) { "subscribeTo() must be called before start()" }
        require(name !in subscriptionNames) { "This process manager already subscribes to a contract named $name" }
        // Check the contract first, so a refused subscription creates no queue.
        contract.ensureCanListen()
        val queue = processQueue("${type.value}-contract-$name", InputTrigger.serializer(), scheduler, rows, clock)
        contract.listen { envelope ->
            val (processId, input) = translate(envelope) ?: return@listen
            val inputId = "in-${envelope.metadata.eventId.value}"
            queue.queueInput(envelope.metadata, InputTrigger(processId.value, encode(input), inputId))
        }
        subscriptionNames += name
        contractQueues += queue
    }

    /** Starts handling its queues, then the poller. Does nothing if already started. */
    fun start() {
        if (started) return
        startQueuesForTest()
        poller.start()
    }

    /** Stops the poller, then stops handling its queues. */
    suspend fun stop() {
        poller.stop()
        allQueues().forEach { it.stop() }
    }

    internal fun startQueuesForTest() {
        started = true
        inputs.startProcess(::deliverInput)
        internal.startProcess(::deliverInput)
        commands.startProcess(::runCommand)
        contractQueues.forEach { it.startProcess(::deliverInput) }
    }

    /**
     * Schedules again every line front of its queues idle for [idleFor] (a task the scheduler lost). A queue whose
     * sweep fails is logged and doesn't stop the others.
     */
    internal suspend fun sweep(idleFor: Duration) {
        allQueues().forEach { queue ->
            try {
                queue.sweep(idleFor)
            } catch (e: Exception) {
                rethrowIfCancelled(e)
                log.warn("Repair sweep of queue {} failed; trying again next time", queue.name, e)
            }
        }
    }

    /** Runs the repair sweep on the first tick, then at most every [sweepEvery]. */
    private suspend fun sweepIfDue() {
        val now = clock()
        val last = lastSweep
        if (last != null && now - last < sweepEvery) return
        lastSweep = now
        sweep(sweepIdle)
    }

    private fun allQueues(): List<ReactionQueue<*>> = listOf(inputs, internal, commands) + contractQueues

    /** Queues [trigger], caused by the event described by [metadata]: in that event's aggregate's line if ordered. */
    private suspend fun ReactionQueue<InputTrigger>.queueInput(
        metadata: EventMetadata,
        trigger: InputTrigger,
    ) {
        val id = EventReactionId(trigger.inputId)
        if (ordered) {
            publishOrdered(listOf(OrderedItem(id, lineKey(metadata), metadata.sequence, 0, trigger)))
        } else {
            publish(Produced(id, trigger))
        }
    }

    internal suspend fun tickForTest() = poller.tickForTest()

    private fun encode(input: I): String = Json.encodeToString(inputSerializer, input)

    private suspend fun route(event: PersistedEvent) {
        val eventId = event.metadata.eventId.value
        if (event.metadata.aggregateType == type) {
            val processId = event.metadata.aggregateId.value
            when (event.serialized.type) {
                ProcessEventSerialization.COMMAND_REQUESTED -> {
                    val requested = streamSerialization.deserialize(event.serialized) as CommandRequested
                    commands.publish(
                        Produced(
                            EventReactionId("cmd-$eventId"),
                            CommandTrigger(processId, requested.targetType, requested.targetId, requested.command, "${type.value}-$eventId"),
                        ),
                    )
                }
                ProcessEventSerialization.INPUT_SCHEDULED -> {
                    val scheduled = streamSerialization.deserialize(event.serialized) as InputScheduled
                    val inputId = "sched-$eventId"
                    internal.publish(Produced(EventReactionId(inputId), InputTrigger(processId, scheduled.input, inputId), Instant.parse(scheduled.at)))
                }
                else -> Unit // the process's own facts are never fed back to it
            }
            return
        }
        // Another process manager's envelopes are its own business, never an input to this one.
        if (ProcessEventSerialization.isEnvelope(event.serialized.type)) return
        val (processId, input) = translate(event) ?: return
        val inputId = "in-$eventId"
        inputs.queueInput(event.metadata, InputTrigger(processId.value, encode(input), inputId))
    }

    private suspend fun deliverInput(trigger: InputTrigger) {
        instances.deliver(AggregateId(trigger.processId), Json.decodeFromString(inputSerializer, trigger.input), trigger.inputId)
    }

    private suspend fun runCommand(trigger: CommandTrigger) {
        val target =
            checkNotNull(targetsByType[AggregateType(trigger.targetType)]) {
                "No target for aggregate type ${trigger.targetType}; register it with target(manager) { … }"
            }
        val feedback =
            target.send(
                AggregateId(trigger.targetId),
                trigger.command,
                CommandId(trigger.commandId),
                CorrelationId("${type.value}/${trigger.processId}"),
            ) ?: return
        val inputId = "rejected-${trigger.commandId}"
        internal.publish(Produced(EventReactionId(inputId), InputTrigger(trigger.processId, encode(feedback), inputId)))
    }
}

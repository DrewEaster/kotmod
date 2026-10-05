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
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.PublicEventEnvelope
import io.kotmod.Repository
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.stampFor
import io.kotmod.jdbc.JdbcContext
import io.kotmod.outbox.DomainEventPoller
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
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
 * It reads the event log with its own poller (only while [isLeader]), and runs its work on channels from [queues]:
 * `inputs`, `internal` (scheduled inputs and rejection feedback), `commands`, and one per [subscribeTo]. Failures are
 * retried with capped backoff and never given up. Start it before the queue's scheduler, and stop it after.
 *
 * @param type the process manager's aggregate type; its instances and events are recorded under it.
 * @param repository stores each instance's state, in the same transaction as its events.
 * @param inputOrdering whether inputs from one source aggregate are delivered in that aggregate's order.
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
    private val queues: ProcessManagerQueues,
    private val inputOrdering: ReactionOrdering = ReactionOrdering.Unordered,
    getPosition: () -> EventLogPosition,
    savePosition: (EventLogPosition) -> Unit,
    isLeader: () -> Boolean,
    pollInterval: Duration = 500.milliseconds,
    batchSize: Int = 100,
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
        queues: ProcessManagerQueues,
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
        queues,
        inputOrdering,
        getPosition,
        savePosition,
        isLeader,
        pollInterval,
        batchSize,
        clock,
    )

    private val targetsByType: Map<AggregateType, ProcessTarget<I>> =
        targets.associateBy { it.type }.also { byType ->
            require(byType.size == targets.size) { "A process manager can have only one target per aggregate type" }
        }

    private val instances = ProcessInstances(type, repository, persistence, initial, inputSerializer, targetsByType.keys)
    private val streamSerialization = ProcessEventSerialization(eventSerialization)
    private val inputTriggers = JsonTriggerSerializer(InputTrigger.serializer())
    private val ordered = inputOrdering != ReactionOrdering.Unordered

    private val inputs = processExecutor(queues.channel("inputs", inputTriggers, ordered), clock, ::deliverInput)
    private val internal = processExecutor(queues.channel("internal", inputTriggers, ordered = false), clock, ::deliverInput)
    private val commands =
        processExecutor(queues.channel("commands", JsonTriggerSerializer(CommandTrigger.serializer()), ordered = false), clock, ::runCommand)
    private val contractExecutors = mutableListOf<EventReactionExecutor<InputTrigger, Unit>>()
    private val subscriptionNames = mutableSetOf<String>()
    private var started = false

    init {
        require(!ordered || inputs.supportsOrdering) { "Ordered process inputs need a queue that supports ordering" }
        if (ordered) inputs.claimOrderedSource(this)
    }

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
        )

    /**
     * Delivers another context's public events to this process manager: [translate] turns each one into the process id
     * and input it is for, or `null` to ignore it. [name] names the channel, must be unique within this process manager
     * and must stay the same across restarts. Must be called before [start].
     */
    fun <P : PublicDomainEvent> subscribeTo(
        name: String,
        contract: PublicEventContract<*, P>,
        translate: (PublicEventEnvelope<P>) -> Pair<AggregateId, I>?,
    ) {
        check(!started) { "subscribeTo() must be called before start()" }
        require(name !in subscriptionNames) { "This process manager already subscribes to a contract named $name" }
        val executor = processExecutor(queues.channel("contract-$name", inputTriggers, ordered), clock, ::deliverInput)
        contract.subscribe(executor, inputOrdering) { envelope ->
            val (processId, input) = translate(envelope) ?: return@subscribe emptyList()
            val inputId = "in-${envelope.metadata.eventId.value}"
            listOf(EventReaction(EventReactionId(inputId), InputTrigger(processId.value, encode(input), inputId)))
        }
        subscriptionNames += name
        contractExecutors += executor
    }

    /** Starts the channels' executors and the poller. Does nothing if already started. */
    fun start() {
        if (started) return
        startExecutorsForTest()
        poller.start()
    }

    /** Stops the poller, then the channels' executors. */
    suspend fun stop() {
        poller.stop()
        (listOf(inputs, internal, commands) + contractExecutors).forEach { it.stop() }
    }

    internal fun startExecutorsForTest() {
        started = true
        (listOf(inputs, internal, commands) + contractExecutors).forEach { it.start() }
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
                    commands.dispatch(
                        EventReactionId("cmd-$eventId"),
                        CommandTrigger(processId, requested.targetType, requested.targetId, requested.command, "${type.value}-$eventId"),
                    )
                }
                ProcessEventSerialization.INPUT_SCHEDULED -> {
                    val scheduled = streamSerialization.deserialize(event.serialized) as InputScheduled
                    val inputId = "sched-$eventId"
                    internal.dispatch(EventReactionId(inputId), InputTrigger(processId, scheduled.input, inputId), notBefore = Instant.parse(scheduled.at))
                }
                else -> Unit // the process's own facts are never fed back to it
            }
            return
        }
        // Another process manager's envelopes are its own business, never an input to this one.
        if (ProcessEventSerialization.isEnvelope(event.serialized.type)) return
        val (processId, input) = translate(event) ?: return
        val inputId = "in-$eventId"
        inputs.dispatch(EventReactionId(inputId), InputTrigger(processId.value, encode(input), inputId), inputOrdering.stampFor(event.metadata, 0))
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
        internal.dispatch(EventReactionId(inputId), InputTrigger(trigger.processId, encode(feedback), inputId))
    }
}

package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateKind
import io.kotmod.AggregateManager
import io.kotmod.AggregateState
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainPersistenceBackend
import io.kotmod.InitialState
import io.kotmod.Outcome
import io.kotmod.Repository
import io.kotmod.SerializedEvent
import io.kotmod.reject
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/** kotmod's own events in a process manager's stream: its intents, carried out later by its channels. */
internal sealed interface ProcessEnvelope : DomainEvent

/** The process asked for [command] (JSON) to be run against aggregate [targetType]/[targetId]. */
@Serializable
internal data class CommandRequested(
    val targetType: String,
    val targetId: String,
    val command: String,
) : ProcessEnvelope

/** The process scheduled [input] (JSON) to be delivered to itself at [at] (ISO-8601). */
@Serializable
internal data class InputScheduled(
    val input: String,
    val at: String,
) : ProcessEnvelope

/** Stores a process manager's stream: kotmod's envelopes itself, everything else through the app's [events]. */
internal class ProcessEventSerialization<E : DomainEvent>(
    private val events: DataSerializationContext<E>,
) : DataSerializationContext<DomainEvent> {
    override fun serialize(event: DomainEvent): SerializedEvent =
        when (event) {
            is ProcessEnvelope ->
                when (event) {
                    is CommandRequested -> SerializedEvent(COMMAND_REQUESTED, 1, Json.encodeToString(CommandRequested.serializer(), event))
                    is InputScheduled -> SerializedEvent(INPUT_SCHEDULED, 1, Json.encodeToString(InputScheduled.serializer(), event))
                }
            else -> {
                @Suppress("UNCHECKED_CAST")
                events.serialize(event as E)
            }
        }

    override fun deserialize(serialized: SerializedEvent): DomainEvent =
        when (serialized.type) {
            COMMAND_REQUESTED -> Json.decodeFromString(CommandRequested.serializer(), serialized.payload)
            INPUT_SCHEDULED -> Json.decodeFromString(InputScheduled.serializer(), serialized.payload)
            else -> events.deserialize(serialized)
        }

    companion object {
        const val COMMAND_REQUESTED = "io.kotmod.process.CommandRequested"
        const val INPUT_SCHEDULED = "io.kotmod.process.InputScheduled"

        internal val ENVELOPE_TYPES = setOf(COMMAND_REQUESTED, INPUT_SCHEDULED)

        /**
         * Whether [type] is one of kotmod's envelope types. Only the process manager that wrote an envelope reads it;
         * outboxes, contracts and other process managers skip it.
         */
        fun isEnvelope(type: String): Boolean = type in ENVELOPE_TYPES
    }
}

/** The core rejection that records an ignored input. */
@Serializable
internal data object Ignored

/** Turns a process decision into a core outcome: a transition is accepted with its events and envelopes; ignore is a rejection. */
internal class ProcessDecider<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    private val inputSerializer: KSerializer<I>,
    private val targetTypes: Set<AggregateType>,
) {
    fun toOutcome(outcome: ProcessOutcome<S, E, I>): Outcome<Held<S, I, E>, DomainEvent, Ignored> =
        when (outcome) {
            ProcessOutcome.Ignore -> reject(Ignored)
            is ProcessOutcome.Transition -> {
                outcome.commands.forEach { requested ->
                    require(requested.kind.type in targetTypes) {
                        "The process requested a command for aggregate type ${requested.kind.type.value}, which isn't one " +
                            "of its targets: register it with target(manager) { … }"
                    }
                }
                val envelopes =
                    outcome.commands.map { CommandRequested(it.kind.type.value, it.targetId.value, it.encodeCommand()) } +
                        outcome.schedule.map { InputScheduled(Json.encodeToString(inputSerializer, it.input), it.at.toString()) }
                Outcome.Accept(Held(outcome.state, this), outcome.events + envelopes)
            }
        }
}

/** A process state seen by the core as an aggregate state. */
internal class Held<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    val state: S,
    private val decider: ProcessDecider<S, I, E>,
) : AggregateState<Held<S, I, E>, I, DomainEvent, Ignored> {
    override suspend fun handle(command: I): Outcome<Held<S, I, E>, DomainEvent, Ignored> = decider.toOutcome(state.handle(command))
}

private class HeldInitial<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    private val initial: ProcessInitialState<S, I, E>,
    private val decider: ProcessDecider<S, I, E>,
) : InitialState<Held<S, I, E>, I, DomainEvent, Ignored> {
    override suspend fun handle(command: I): Outcome<Held<S, I, E>, DomainEvent, Ignored> = decider.toOutcome(initial.handle(command))
}

private class HeldRepository<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    private val repository: Repository<S>,
    private val decider: ProcessDecider<S, I, E>,
) : Repository<Held<S, I, E>> {
    override fun get(id: AggregateId): Held<S, I, E>? = repository.get(id)?.let { Held(it, decider) }

    override fun save(
        id: AggregateId,
        state: Held<S, I, E>,
    ) = repository.save(id, state.state)
}

/** Persists process instances of one [type]: each instance is an aggregate whose commands are its inputs. */
internal class ProcessInstances<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    type: AggregateType,
    repository: Repository<S>,
    backend: DomainPersistenceBackend<DomainEvent>,
    eventSerialization: DataSerializationContext<DomainEvent>,
    initial: ProcessInitialState<S, I, E>,
    inputSerializer: KSerializer<I>,
    targetTypes: Set<AggregateType>,
) {
    private val decider = ProcessDecider<S, I, E>(inputSerializer, targetTypes)
    private val manager =
        AggregateManager(
            AggregateKind(type, inputSerializer, eventSerialization, Ignored.serializer()),
            HeldRepository(repository, decider),
            backend,
            HeldInitial(initial, decider),
        )

    /** Applies [input] to process [processId] once: a redelivery with the same [inputId] is recognised and skipped. */
    suspend fun deliver(
        processId: AggregateId,
        input: I,
        inputId: String,
    ) {
        manager.handle(processId, input, commandId = CommandId(inputId))
    }
}

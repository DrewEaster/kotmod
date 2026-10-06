package io.kotmod.reaction

import io.kotmod.AggregateKind
import io.kotmod.AggregateType
import io.kotmod.DomainEvent
import io.kotmod.EventMetadata
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.PublicEventEnvelope
import io.kotmod.contract.PublicEventContract

/** Where a use case's events come from: one of this context's aggregate kinds, or another context's contract. */
internal sealed class ReactionSource<T : Any> {
    /** The aggregate types whose events this source delivers; `null` means every type. */
    abstract val aggregateTypes: Set<AggregateType>?

    /** Names the source in logs and error messages. */
    abstract val description: String

    fun covers(type: AggregateType): Boolean = aggregateTypes?.contains(type) ?: true

    fun overlaps(other: ReactionSource<T>): Boolean {
        val mine = aggregateTypes ?: return true
        val theirs = other.aggregateTypes ?: return true
        return mine.any { it in theirs }
    }

    /** Reads [event] as this source delivers it and runs the use case's block for it; throws if either fails. */
    abstract fun map(event: PersistedEvent): List<ProducedTrigger<T>>
}

/** One of this context's aggregate kinds: its events are deserialized with the kind's event serialization. */
internal class KindSource<T : Any, E : DomainEvent>(
    private val kind: AggregateKind<*, E, *>,
    private val block: TriggerScope<T>.(E, EventMetadata) -> Unit,
) : ReactionSource<T>() {
    override val aggregateTypes: Set<AggregateType> = setOf(kind.type)
    override val description: String = "aggregate kind ${kind.type.value}"

    override fun map(event: PersistedEvent): List<ProducedTrigger<T>> {
        val typed = kind.eventSerialization.deserialize(event.serialized)
        val scope = TriggerScope<T>()
        scope.block(typed, event.metadata)
        return scope.produced
    }
}

/** Another context's contract: its public events, each with the metadata of the event it came from. */
internal class ContractSource<T : Any, P : PublicDomainEvent>(
    private val contract: PublicEventContract<*, P>,
    private val block: TriggerScope<T>.(P, EventMetadata) -> Unit,
) : ReactionSource<T>() {
    override val aggregateTypes: Set<AggregateType>? = contract.aggregateTypes
    override val description: String = "contract on ${aggregateTypes?.joinToString { it.value } ?: "every aggregate type"}"

    override fun map(event: PersistedEvent): List<ProducedTrigger<T>> = contract.toPublic(event)?.let(::mapPublic) ?: emptyList()

    /** Has the contract's reader map each of its public events with this source and queue the result on [runtime]. */
    fun feed(runtime: UseCaseRuntime<T>) {
        contract.listen { envelope -> runtime.mapAndPublish(envelope.metadata, this) { mapPublic(envelope) } }
    }

    fun mapPublic(envelope: PublicEventEnvelope<P>): List<ProducedTrigger<T>> {
        val scope = TriggerScope<T>()
        scope.block(envelope.event, envelope.metadata)
        return scope.produced
    }
}

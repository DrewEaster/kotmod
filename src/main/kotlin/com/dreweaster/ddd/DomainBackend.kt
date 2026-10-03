package com.dreweaster.ddd

interface DomainPersistenceBackend<E : DomainEvent> {
    fun loadMeta(
        type: AggregateType,
        id: AggregateId,
    ): AggregateMeta?

    fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
    )

    fun appendEvents(events: List<PendingEvent<E>>)

    fun wasCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): Boolean

    fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    )
}

data class PersistedEvent(
    val globalOffset: Long,
    val metadata: EventMetadata,
    val serialized: SerializedEvent,
    val attributes: Map<String, String> = emptyMap(),
)

interface DomainEventPollingBackend {
    fun readEventsAfter(
        lastOffset: Long,
        limit: Int,
    ): List<PersistedEvent>
}

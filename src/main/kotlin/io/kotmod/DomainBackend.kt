package io.kotmod

/**
 * Storage used by [AggregateManager] and [EventProducer] for everything except aggregate state:
 * aggregate versions, domain events and the history of handled commands. All methods are called
 * inside the caller's transaction. [io.kotmod.postgres.PostgresDomainPersistenceBackend] is the
 * Postgres implementation.
 */
interface DomainPersistenceBackend<E : DomainEvent> {
    /**
     * Runs [block] in a transaction, joining one already open on this thread. [AggregateManager] and
     * [EventProducer] make all of a command's writes inside it.
     */
    fun <R> inTransaction(block: () -> R): R

    /** Returns the bookkeeping for aggregate [type]/[id], or `null` if it does not exist. */
    fun loadMeta(
        type: AggregateType,
        id: AggregateId,
    ): AggregateMeta?

    /**
     * Records a change to aggregate [type]/[id]. With [expectedVersion] `null` the aggregate is created at
     * version 1, throwing [AggregateAlreadyExistsException] if it exists; otherwise its version is advanced
     * from [expectedVersion], throwing [OptimisticConcurrencyException] if the stored version differs.
     */
    fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
    )

    /** Appends [events] to the event log in order. */
    fun appendEvents(events: List<PendingEvent<E>>)

    /** Returns whether [commandId] has already been applied to aggregate [type]/[id]. */
    fun wasCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): Boolean

    /** Records that [commandId] has been applied to aggregate [type]/[id]. */
    fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    )
}

/**
 * A domain event as read back from the event log, still in serialized form.
 *
 * @property globalOffset position in the log across all aggregates; strictly increasing.
 * @property attributes extra columns a backend chose to expose with the event.
 */
data class PersistedEvent(
    val globalOffset: Long,
    val metadata: EventMetadata,
    val serialized: SerializedEvent,
    val attributes: Map<String, String> = emptyMap(),
)

/** Reads the event log in order, for consumers such as [io.kotmod.outbox.AggregateEventOutbox] and [io.kotmod.contract.PublicEventContract]. */
interface DomainEventPollingBackend {
    /** Returns up to [limit] events whose global offset is greater than [lastOffset], oldest first. */
    fun readEventsAfter(
        lastOffset: Long,
        limit: Int,
    ): List<PersistedEvent>
}

package io.kotmod

import app.cash.sqldelight.Transacter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.time.toKotlinInstant

private sealed class ReadPhaseResult {
    data object Dedup : ReadPhaseResult()

    data class Proceed(
        val existingVersion: Long?,
    ) : ReadPhaseResult()
}

/**
 * Records domain events for aggregates of one [AggregateType] that have no stored state.
 *
 * Use it where something needs an event log and idempotent commands but not a [Repository]. Each call
 * is idempotent per command id, versions the aggregate with optimistic concurrency, and appends its
 * events in one transaction.
 */
class EventProducer<E : DomainEvent>(
    private val aggregateType: AggregateType,
    private val backend: DomainPersistenceBackend<E>,
    private val transacter: Transacter,
) {
    /**
     * Appends [events] for aggregate [id], creating the aggregate's bookkeeping on first use.
     *
     * Does nothing if [commandId] has already been handled for [id]. Throws
     * [OptimisticConcurrencyException] if another call changed the aggregate concurrently.
     */
    suspend fun emit(
        id: AggregateId,
        events: List<E>,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
    ) {
        val resolvedCommandId = commandId ?: CommandId(randomId())

        // Phase 1: Read — dedup check, load existing version
        val readResult =
            withContext(Dispatchers.IO) {
                if (backend.wasCommandHandled(aggregateType, id, resolvedCommandId)) {
                    ReadPhaseResult.Dedup
                } else {
                    ReadPhaseResult.Proceed(backend.loadMeta(aggregateType, id)?.version)
                }
            }
        if (readResult is ReadPhaseResult.Dedup) return
        val existingVersion = (readResult as ReadPhaseResult.Proceed).existingVersion

        // Phase 2: Write — tight transaction
        withContext(Dispatchers.IO) {
            transacter.transactionWithResult {
                backend.saveMeta(aggregateType, id, expectedVersion = existingVersion)
                if (events.isNotEmpty()) {
                    backend.appendEvents(
                        wrap(
                            id = id,
                            causationId = resolvedCommandId,
                            correlationId = correlationId,
                            events = events,
                        ),
                    )
                }
                backend.recordCommandHandled(aggregateType, id, resolvedCommandId)
            }
        }
    }

    private fun wrap(
        id: AggregateId,
        causationId: CommandId,
        correlationId: CorrelationId?,
        events: List<E>,
    ): List<PendingEvent<E>> {
        val now =
            java.time.Instant
                .now()
                .toKotlinInstant()
        return events.map { event ->
            PendingEvent(
                metadata =
                    EventMetadata(
                        eventId = EventId(randomId()),
                        aggregateType = aggregateType,
                        aggregateId = id,
                        causationId = causationId,
                        correlationId = correlationId,
                        timestamp = now,
                    ),
                event = event,
            )
        }
    }
}

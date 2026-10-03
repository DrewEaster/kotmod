package com.dreweaster.ddd

import app.cash.sqldelight.Transacter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.time.toKotlinInstant

class AggregateManager<S : Any, E : DomainEvent>(
    @PublishedApi internal val aggregateType: AggregateType,
    private val repository: Repository<S>,
    private val backend: DomainPersistenceBackend<E>,
    private val transacter: Transacter,
) {
    suspend fun create(
        id: AggregateId,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
        block: suspend () -> Pair<S, List<E>>,
    ): S {
        val resolvedCommandId = commandId ?: CommandId(randomId())

        // Phase 1: Read — dedup check
        val dedupResult: S? =
            withContext(Dispatchers.IO) {
                if (backend.wasCommandHandled(aggregateType, id, resolvedCommandId)) {
                    repository.get(id) ?: throw AggregateNotFoundException(
                        aggregateType,
                        id
                    )
                } else {
                    null
                }
            }
        if (dedupResult != null) return dedupResult

        // Phase 2: Command — invoke user lambda
        val (newState, events) = block()

        // Phase 3: Write — tight transaction
        withContext(Dispatchers.IO) {
            transacter.transactionWithResult {
                backend.saveMeta(aggregateType, id, expectedVersion = null)
                repository.save(id, newState)
                if (events.isNotEmpty()) {
                    backend.appendEvents(
                        wrapPending(
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
        return newState
    }

    suspend fun execute(
        id: AggregateId,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
        block: suspend (S) -> Pair<S, List<E>>,
    ): S = executeCore(id, commandId, correlationId) { current -> block(current) }

    @JvmName("executeNarrowed")
    suspend inline fun <reified T : S> execute(
        id: AggregateId,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
        crossinline block: suspend (T) -> Pair<S, List<E>>,
    ): S =
        executeCore(id, commandId, correlationId) { current ->
            val narrowed =
                current as? T
                    ?: throw UnexpectedAggregateStateException(
                        aggregateType = aggregateType,
                        aggregateId = id,
                        expected = T::class.simpleName ?: "?",
                        actual = current::class.simpleName ?: "?",
                    )
            block(narrowed)
        }

    @PublishedApi
    internal suspend fun executeCore(
        id: AggregateId,
        commandId: CommandId?,
        correlationId: CorrelationId?,
        block: suspend (S) -> Pair<S, List<E>>,
    ): S {
        val resolvedCommandId = commandId ?: CommandId(randomId())

        // Phase 1: Read — dedup check, load meta, load state
        val readResult =
            withContext(Dispatchers.IO) {
                if (backend.wasCommandHandled(aggregateType, id, resolvedCommandId)) {
                    val current = repository.get(id) ?: throw AggregateNotFoundException(
                        aggregateType,
                        id
                    )
                    return@withContext ReadResult.Dedup(current)
                }
                val meta =
                    backend.loadMeta(aggregateType, id)
                        ?: throw AggregateNotFoundException(aggregateType, id)
                val current =
                    repository.get(id)
                        ?: throw AggregateNotFoundException(aggregateType, id)
                ReadResult.Proceed(meta, current)
            }

        when (readResult) {
            is ReadResult.Dedup<S> -> return readResult.state
            is ReadResult.Proceed<S> -> {
                val meta = readResult.meta
                val currentState = readResult.state

                // Phase 2: Command — invoke user lambda
                val (newState, events) = block(currentState)

                // Phase 3: Write — tight transaction
                withContext(Dispatchers.IO) {
                    transacter.transactionWithResult {
                        backend.saveMeta(aggregateType, id, expectedVersion = meta.version)
                        repository.save(id, newState)
                        if (events.isNotEmpty()) {
                            backend.appendEvents(
                                wrapPending(
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
                return newState
            }
        }
    }

    @PublishedApi
    internal sealed class ReadResult<S> {
        class Dedup<S>(
            val state: S,
        ) : ReadResult<S>()

        class Proceed<S>(
            val meta: AggregateMeta,
            val state: S,
        ) : ReadResult<S>()
    }

    @PublishedApi
    internal fun wrapPending(
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

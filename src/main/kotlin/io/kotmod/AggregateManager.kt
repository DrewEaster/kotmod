package io.kotmod

import io.kotmod.jdbc.databaseWork
import kotlin.time.toKotlinInstant

/**
 * Runs commands against aggregates of one [AggregateType] whose state is stored by a [Repository].
 *
 * Each command runs in three phases:
 * 1. **Read:** if the command id has already been handled, return the current state without doing
 *    anything else (commands are idempotent). Otherwise load the aggregate's version and state.
 * 2. **Command:** call the app's block, which returns the new state and the events it raised. No
 *    database work happens during this phase.
 * 3. **Write:** in one transaction (the backend's [DomainPersistenceBackend.inTransaction]), advance the
 *    aggregate's version (optimistic concurrency), save the new state, append the events and record the
 *    command as handled.
 *
 * Events appended here are later picked up by [io.kotmod.outbox.AggregateEventOutbox] and
 * [io.kotmod.contract.PublicEventContract].
 *
 * @param S the aggregate's state type.
 * @param E the aggregate's domain event type.
 */
class AggregateManager<S : Any, E : DomainEvent>(
    @PublishedApi internal val aggregateType: AggregateType,
    private val repository: Repository<S>,
    private val backend: DomainPersistenceBackend<E>,
) {
    /**
     * Creates aggregate [id] from the state and events returned by [block].
     *
     * If [commandId] has already been handled for [id], the stored state is returned and [block] is not
     * called. Throws [AggregateAlreadyExistsException] if the aggregate already exists. A random command id
     * is used when none is given, which makes the call non-idempotent.
     *
     * @return the new state.
     */
    suspend fun create(
        id: AggregateId,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
        block: suspend () -> Pair<S, List<E>>,
    ): S {
        val resolvedCommandId = commandId ?: CommandId(randomId())

        // Phase 1: Read — dedup check
        val dedupResult: S? =
            databaseWork {
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
        databaseWork {
            backend.inTransaction {
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

    /**
     * Applies a command to existing aggregate [id]: [block] receives the current state and returns the new
     * state and the events it raised.
     *
     * If [commandId] has already been handled for [id], the stored state is returned and [block] is not
     * called. Throws [AggregateNotFoundException] if the aggregate does not exist and
     * [OptimisticConcurrencyException] if it changed concurrently.
     *
     * @return the new state.
     */
    suspend fun execute(
        id: AggregateId,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
        block: suspend (S) -> Pair<S, List<E>>,
    ): S = executeCore(id, commandId, correlationId) { current -> block(current) }

    /**
     * Like [execute], but only applies the command when the current state is of subtype [T], throwing
     * [UnexpectedAggregateStateException] otherwise. Useful for state machines, e.g. a command that only
     * applies to a pending order.
     */
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
            databaseWork {
                if (backend.wasCommandHandled(aggregateType, id, resolvedCommandId)) {
                    val current = repository.get(id) ?: throw AggregateNotFoundException(
                        aggregateType,
                        id
                    )
                    return@databaseWork ReadResult.Dedup(current)
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
                databaseWork {
                    backend.inTransaction {
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

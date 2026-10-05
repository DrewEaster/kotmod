package io.kotmod

import io.kotmod.jdbc.KotmodTransaction
import io.kotmod.jdbc.databaseWork
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.serialization.json.Json
import kotlin.time.toKotlinInstant

/**
 * Runs commands against aggregates of one [AggregateType] whose state is stored by a [Repository].
 *
 * [handle] is the only way to change an aggregate. Each command runs in three phases:
 * 1. **Read:** if the command id has already been handled, return the recorded answer (the current state if
 *    it was accepted, the same rejection if it was rejected). Otherwise load the aggregate's version and state.
 * 2. **Decide:** [commands] routes the command to a pure function, which accepts it (new state and events) or
 *    rejects it with one of the aggregate's rejection types. No database work happens in this phase.
 * 3. **Write:** in one transaction, either advance the aggregate's version (optimistic concurrency), save the
 *    new state, append the events and record the command as accepted; or record the rejection.
 *
 * If the write loses a race with another writer, [handle] starts again from the read, up to
 * [maxConflictRetries] times, except inside an outer transaction, where the conflict propagates.
 *
 * Events appended here are later picked up by [io.kotmod.outbox.AggregateEventOutbox] and
 * [io.kotmod.contract.PublicEventContract].
 *
 * @param S the aggregate's state type.
 * @param E the aggregate's domain event type.
 * @param C the aggregate's command type.
 * @param R the aggregate's rejection type.
 */
class AggregateManager<S : Any, E : DomainEvent, C : Any, R : Any>(
    private val aggregateType: AggregateType,
    private val repository: Repository<S>,
    private val backend: DomainPersistenceBackend<E>,
    private val commands: CommandHandlers<S, C, E, R>,
    private val maxConflictRetries: Int = 5,
) {
    init {
        require(maxConflictRetries >= 0) { "maxConflictRetries must not be negative" }
    }

    /**
     * Handles [command] for aggregate [id] and returns whether it was accepted (with the new state) or rejected
     * (with the rejection).
     *
     * If [commandId] has already been handled for [id], the recorded answer is returned without deciding again:
     * the current state if it was accepted, or the same rejection if it was rejected. A random command id is
     * used when none is given, which makes the call non-idempotent. [correlationId] is stored with every event.
     */
    suspend fun handle(
        id: AggregateId,
        command: C,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
    ): CommandResult<S, R> {
        val resolvedCommandId = commandId ?: CommandId(randomId())
        val retries = if (inOuterTransaction()) 0 else maxConflictRetries
        var attempt = 0
        while (true) {
            try {
                return handleOnce(id, command, resolvedCommandId, correlationId)
            } catch (e: DddException) {
                if (!e.isConflict() || attempt >= retries) throw e
                attempt++
            }
        }
    }

    private suspend fun inOuterTransaction(): Boolean =
        currentCoroutineContext()[KotmodTransaction] != null || backend.isInTransaction()

    private fun DddException.isConflict(): Boolean =
        this is OptimisticConcurrencyException ||
            this is AggregateAlreadyExistsException ||
            this is CommandAlreadyRecordedException

    private suspend fun handleOnce(
        id: AggregateId,
        command: C,
        commandId: CommandId,
        correlationId: CorrelationId?,
    ): CommandResult<S, R> {
        // Phase 1: Read — the recorded answer, or the current version and state
        val read =
            databaseWork(backend::isInTransaction) {
                when (val handled = backend.findHandledCommand(aggregateType, id, commandId)) {
                    HandledCommand.Accepted -> Read.Answered<S, R>(CommandResult.Accepted(requireState(id)))
                    is HandledCommand.Rejected -> Read.Answered<S, R>(CommandResult.Rejected(decodeRejection(id, commandId, handled)))
                    null -> {
                        val meta = backend.loadMeta(aggregateType, id)
                        Read.Undecided<S, R>(meta, if (meta == null) null else requireState(id))
                    }
                }
            }
        val undecided =
            when (read) {
                is Read.Answered -> return read.result
                is Read.Undecided -> read
            }

        // Phase 2: Decide — pure, no database work
        val outcome = commands.handlerFor(command).decide(undecided.state)

        // Phase 3: Write — tight transaction
        databaseWork(backend::isInTransaction) {
            backend.inTransaction {
                when (outcome) {
                    is Outcome.Accept -> {
                        val lastSequence =
                            backend.saveMeta(aggregateType, id, expectedVersion = undecided.meta?.version, eventCount = outcome.events.size)
                        repository.save(id, outcome.state)
                        if (outcome.events.isNotEmpty()) {
                            backend.appendEvents(
                                wrapPending(
                                    id = id,
                                    causationId = commandId,
                                    correlationId = correlationId,
                                    events = outcome.events,
                                    firstSequence = lastSequence - outcome.events.size + 1,
                                ),
                            )
                        }
                        backend.recordCommandHandled(aggregateType, id, commandId)
                    }
                    is Outcome.Reject ->
                        backend.recordCommandRejected(
                            aggregateType,
                            id,
                            commandId,
                            rejectionType = outcome.rejection::class.java.name,
                            payload = Json.encodeToString(commands.rejectionSerializer, outcome.rejection),
                        )
                }
            }
        }
        return when (outcome) {
            is Outcome.Accept -> CommandResult.Accepted(outcome.state)
            is Outcome.Reject -> CommandResult.Rejected(outcome.rejection)
        }
    }

    private fun requireState(id: AggregateId): S = repository.get(id) ?: throw AggregateNotFoundException(aggregateType, id)

    private fun decodeRejection(
        id: AggregateId,
        commandId: CommandId,
        handled: HandledCommand.Rejected,
    ): R =
        try {
            Json.decodeFromString(commands.rejectionSerializer, handled.payload)
        } catch (e: IllegalArgumentException) {
            // kotlinx.serialization's SerializationException is an IllegalArgumentException.
            throw RejectionDeserializationException(aggregateType, id, commandId, handled.type, e)
        }

    private sealed interface Read<S, R> {
        class Answered<S, R>(
            val result: CommandResult<S, R>,
        ) : Read<S, R>

        class Undecided<S, R>(
            val meta: AggregateMeta?,
            val state: S?,
        ) : Read<S, R>
    }

    private fun wrapPending(
        id: AggregateId,
        causationId: CommandId,
        correlationId: CorrelationId?,
        events: List<E>,
        firstSequence: Long,
    ): List<PendingEvent<E>> {
        val now =
            java.time.Instant
                .now()
                .toKotlinInstant()
        return events.mapIndexed { index, event ->
            PendingEvent(
                metadata =
                    EventMetadata(
                        eventId = EventId(randomId()),
                        aggregateType = aggregateType,
                        aggregateId = id,
                        causationId = causationId,
                        correlationId = correlationId,
                        timestamp = now,
                        sequence = firstSequence + index,
                    ),
                event = event,
            )
        }
    }
}

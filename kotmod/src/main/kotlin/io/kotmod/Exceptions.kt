package io.kotmod

/** Base class for all exceptions thrown by the library. */
sealed class DddException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

/**
 * Thrown when an aggregate's bookkeeping exists, or one of its commands was accepted, but its [Repository] has no
 * state for it: the state was deleted or saved somewhere else.
 */
class AggregateNotFoundException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
) : DddException("Aggregate not found: ${aggregateType.value}/${aggregateId.value}")

/** Thrown when creating an aggregate whose type and id are already taken. */
class AggregateAlreadyExistsException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
) : DddException("Aggregate already exists: ${aggregateType.value}/${aggregateId.value}")

/**
 * Thrown when an aggregate was changed by someone else between being read and being written: its stored version
 * no longer matches [expectedVersion]. [AggregateManager.handle] retries automatically; you only see this when its
 * retries run out, or inside an outer transaction.
 */
class OptimisticConcurrencyException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val expectedVersion: Long,
) : DddException("Optimistic concurrency conflict on ${aggregateType.value}/${aggregateId.value} (expected version $expectedVersion)")

/**
 * Thrown by a [DomainPersistenceBackend] when [commandId] is already recorded for an aggregate, which happens when
 * two writers handle the same command id at the same time. [AggregateManager.handle] retries, and the retry
 * returns the recorded answer.
 */
class CommandAlreadyRecordedException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val commandId: CommandId,
) : DddException("Command ${commandId.value} is already recorded for ${aggregateType.value}/${aggregateId.value}")

/**
 * Thrown by [AggregateManager.handle] when a command id was rejected earlier but the recorded rejection can no
 * longer be read, usually because its class was renamed or reshaped. Keep old names with `@SerialName`.
 */
class RejectionDeserializationException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val commandId: CommandId,
    val rejectionType: String,
    cause: Throwable,
) : DddException(
        "Recorded rejection $rejectionType for command ${commandId.value} on ${aggregateType.value}/${aggregateId.value} " +
            "can't be read; keep renamed rejection classes readable with @SerialName",
        cause,
    )

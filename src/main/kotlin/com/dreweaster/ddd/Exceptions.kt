package com.dreweaster.ddd

sealed class DddException(
    message: String,
) : RuntimeException(message)

class AggregateNotFoundException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
) : DddException("Aggregate not found: ${aggregateType.value}/${aggregateId.value}")

class AggregateAlreadyExistsException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
) : DddException("Aggregate already exists: ${aggregateType.value}/${aggregateId.value}")

class OptimisticConcurrencyException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val expectedVersion: Long,
) : DddException("Optimistic concurrency conflict on ${aggregateType.value}/${aggregateId.value} (expected version $expectedVersion)")

class UnexpectedAggregateStateException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val expected: String,
    val actual: String,
) : DddException("Aggregate ${aggregateType.value}/${aggregateId.value} is in state $actual but $expected was expected")

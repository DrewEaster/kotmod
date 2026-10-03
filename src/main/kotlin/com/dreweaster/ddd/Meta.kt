package com.dreweaster.ddd

import kotlin.time.Instant

/**
 * Bookkeeping the library keeps for every aggregate instance, independent of its state.
 *
 * @property version incremented on every successful change; used for optimistic concurrency.
 * @property createdAt when the aggregate was first created.
 * @property updatedAt when the aggregate was last changed.
 */
data class AggregateMeta(
    val version: Long,
    val createdAt: Instant,
    val updatedAt: Instant,
)

/** An event that has been produced by a command but not yet persisted, together with its [metadata]. */
data class PendingEvent<E : DomainEvent>(
    val metadata: EventMetadata,
    val event: E,
)

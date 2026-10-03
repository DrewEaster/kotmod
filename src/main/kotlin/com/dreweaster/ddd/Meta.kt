package com.dreweaster.ddd

import kotlin.time.Instant

data class AggregateMeta(
    val version: Long,
    val createdAt: Instant,
    val updatedAt: Instant,
)

data class PendingEvent<E : DomainEvent>(
    val metadata: EventMetadata,
    val event: E,
)

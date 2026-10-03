package com.dreweaster.ddd

import kotlin.time.Instant

data class EventMetadata(
    val eventId: EventId,
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val causationId: CommandId,
    val correlationId: CorrelationId?,
    val timestamp: Instant,
)

data class PublicEventEnvelope<out E>(
    val metadata: EventMetadata,
    val event: E,
)

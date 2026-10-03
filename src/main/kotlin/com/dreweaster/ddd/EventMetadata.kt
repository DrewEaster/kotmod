package com.dreweaster.ddd

import kotlin.time.Instant

/**
 * Facts about a persisted domain event that are not part of the event itself.
 *
 * @property eventId unique id of this event.
 * @property aggregateType type of the aggregate that raised it.
 * @property aggregateId the aggregate instance that raised it.
 * @property causationId the command that caused it.
 * @property correlationId the wider flow it belongs to, if any.
 * @property timestamp when it was raised.
 */
data class EventMetadata(
    val eventId: EventId,
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val causationId: CommandId,
    val correlationId: CorrelationId?,
    val timestamp: Instant,
)

/** A public event delivered to [com.dreweaster.ddd.contract.PublicEventContract] subscribers, with the [metadata] of the domain event it came from. */
data class PublicEventEnvelope<out E>(
    val metadata: EventMetadata,
    val event: E,
)

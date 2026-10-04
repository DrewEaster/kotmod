package io.kotmod.support

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.PersistedEvent
import io.kotmod.SerializedEvent
import kotlin.time.Instant

internal fun persistedEvent(
    globalOffset: Long,
    eventId: String = "e-$globalOffset",
    aggregateType: String = "Order",
    aggregateId: String = "o-1",
    causationId: String = "cmd-1",
    correlationId: String? = null,
    eventType: String = "OrderPlaced",
    eventVersion: Int = 1,
    eventPayload: String = "{}",
    timestamp: Instant = Instant.parse("2026-04-18T10:00:00Z"),
    sequence: Long = 1,
) = PersistedEvent(
    position = EventLogPosition(transactionId = 1, globalOffset = globalOffset),
    metadata =
        EventMetadata(
            eventId = EventId(eventId),
            aggregateType = AggregateType(aggregateType),
            aggregateId = AggregateId(aggregateId),
            causationId = CommandId(causationId),
            correlationId = correlationId?.let(::CorrelationId),
            timestamp = timestamp,
            sequence = sequence,
        ),
    serialized = SerializedEvent(eventType, eventVersion, eventPayload),
)

/** In-memory stand-in for getPosition/savePosition that records every save. */
internal class RecordingOffsets(
    initial: EventLogPosition,
) {
    var current: EventLogPosition = initial
        private set
    val saved = mutableListOf<EventLogPosition>()

    fun get(): EventLogPosition = current

    fun save(position: EventLogPosition) {
        saved += position
        current = position
    }
}

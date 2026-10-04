package io.kotmod.support

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.EventId
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
) = PersistedEvent(
    globalOffset = globalOffset,
    metadata =
        EventMetadata(
            eventId = EventId(eventId),
            aggregateType = AggregateType(aggregateType),
            aggregateId = AggregateId(aggregateId),
            causationId = CommandId(causationId),
            correlationId = correlationId?.let(::CorrelationId),
            timestamp = timestamp,
        ),
    serialized = SerializedEvent(eventType, eventVersion, eventPayload),
)

/** In-memory stand-in for getOffset/saveOffset that records every save. */
internal class RecordingOffsets(
    initial: Long,
) {
    var current: Long = initial
        private set
    val saved = mutableListOf<Long>()

    fun get(): Long = current

    fun save(offset: Long) {
        saved += offset
        current = offset
    }
}

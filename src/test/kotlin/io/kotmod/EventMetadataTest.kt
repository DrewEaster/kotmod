package io.kotmod

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Instant

class EventMetadataTest {
    @Test
    fun `EventMetadata carries the six envelope fields`() {
        val ts = Instant.parse("2026-04-20T10:00:00Z")
        val metadata =
            EventMetadata(
                eventId = EventId("e-1"),
                aggregateType = AggregateType("Order"),
                aggregateId = AggregateId("o-1"),
                causationId = CommandId("cmd-1"),
                correlationId = CorrelationId("corr-1"),
                timestamp = ts,
            )

        assertEquals(EventId("e-1"), metadata.eventId)
        assertEquals(AggregateType("Order"), metadata.aggregateType)
        assertEquals(AggregateId("o-1"), metadata.aggregateId)
        assertEquals(CommandId("cmd-1"), metadata.causationId)
        assertEquals(CorrelationId("corr-1"), metadata.correlationId)
        assertEquals(ts, metadata.timestamp)
    }

    @Test
    fun `EventMetadata allows null correlationId`() {
        val metadata =
            EventMetadata(
                eventId = EventId("e-1"),
                aggregateType = AggregateType("Order"),
                aggregateId = AggregateId("o-1"),
                causationId = CommandId("cmd-1"),
                correlationId = null,
                timestamp = Instant.parse("2026-04-20T10:00:00Z"),
            )
        assertNull(metadata.correlationId)
    }

    @Test
    fun `PublicEventEnvelope composes metadata and payload`() {
        val metadata =
            EventMetadata(
                eventId = EventId("e-1"),
                aggregateType = AggregateType("Order"),
                aggregateId = AggregateId("o-1"),
                causationId = CommandId("cmd-1"),
                correlationId = null,
                timestamp = Instant.parse("2026-04-20T10:00:00Z"),
            )
        val envelope = PublicEventEnvelope(metadata = metadata, event = "some-payload")

        assertEquals(metadata, envelope.metadata)
        assertEquals("some-payload", envelope.event)
    }

    @Test
    fun `PublicEventEnvelope is covariant in E`() {
        val metadata =
            EventMetadata(
                eventId = EventId("e-1"),
                aggregateType = AggregateType("Order"),
                aggregateId = AggregateId("o-1"),
                causationId = CommandId("cmd-1"),
                correlationId = null,
                timestamp = Instant.parse("2026-04-20T10:00:00Z"),
            )
        val specific: PublicEventEnvelope<String> = PublicEventEnvelope(metadata, "payload")
        val widened: PublicEventEnvelope<CharSequence> = specific
        assertEquals("payload", widened.event)
    }
}

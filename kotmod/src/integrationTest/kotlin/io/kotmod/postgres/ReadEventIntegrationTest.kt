package io.kotmod.postgres

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.SerializedEvent
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.process.ProcessEventSerialization
import io.kotmod.support.OrderPlaced
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Instant

class ReadEventIntegrationTest : IntegrationTest() {
    private fun appendOrderPlaced(eventId: String) {
        PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()).appendEvents(
            listOf(
                PendingEvent(
                    EventMetadata(EventId(eventId), AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-$eventId"), null, Instant.parse("2026-10-06T10:00:00Z"), 1),
                    OrderPlaced("book"),
                ),
            ),
        )
    }

    private fun insertEnvelope(eventId: String) {
        dataSource.connection.use { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence, causation_id, event_id, " +
                        "event_type, event_version, event_payload, event_timestamp) VALUES ('Window', 'w-1', 1, 'cmd', ?, ?, 1, '{}', now())",
                ).use { ps ->
                    ps.setString(1, eventId)
                    ps.setString(2, ProcessEventSerialization.COMMAND_REQUESTED)
                    ps.executeUpdate()
                }
        }
    }

    @Test
    fun `an event is read back by its id`() {
        appendOrderPlaced("e-1")

        val backend = PostgresDomainPollingBackend(jdbc)
        val event = checkNotNull(backend.readEvent(EventId("e-1")))

        assertEquals(backend.readEventsAfter(EventLogPosition.START, 10).single(), event)
        assertEquals(AggregateId("o-1"), event.metadata.aggregateId)
        assertEquals(OrderPlaced("book"), orderEventSerialization().deserialize(event.serialized))
    }

    @Test
    fun `an unknown id reads as null`() {
        assertNull(PostgresDomainPollingBackend(jdbc).readEvent(EventId("missing")))
    }

    @Test
    fun `kotmod's internal events are only read by the process manager's backend`() {
        insertEnvelope("c-1")

        assertNull(PostgresDomainPollingBackend(jdbc).readEvent(EventId("c-1")))
        assertEquals(
            SerializedEvent(ProcessEventSerialization.COMMAND_REQUESTED, 1, "{}"),
            PostgresDomainPollingBackend(jdbc, includeProcessEnvelopes = true).readEvent(EventId("c-1"))?.serialized,
        )
    }
}

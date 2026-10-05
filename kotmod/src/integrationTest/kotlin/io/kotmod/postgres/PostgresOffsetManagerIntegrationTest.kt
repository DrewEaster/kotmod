package io.kotmod.postgres

import io.kotmod.EventLogPosition
import io.kotmod.postgres.support.IntegrationTest
import org.junit.jupiter.api.BeforeEach
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals

class PostgresOffsetManagerIntegrationTest : IntegrationTest() {
    private lateinit var offsets: PostgresOffsetManager

    @BeforeEach
    fun createOffsetManager() {
        offsets = PostgresOffsetManager(jdbc)
    }

    private fun insert(
        conn: Connection,
        eventId: String,
    ) = conn
        .prepareStatement(
            "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence, causation_id, " +
                "event_id, event_type, event_version, event_payload, event_timestamp) " +
                "VALUES ('Order', ?, 1, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
        ).use { ps ->
            ps.setString(1, "agg-$eventId")
            ps.setString(2, eventId)
            ps.executeUpdate()
        }

    private fun insert(eventId: String) = dataSource.connection.use { insert(it, eventId) }

    private fun readAfter(position: EventLogPosition) =
        PostgresDomainPollingBackend(jdbc).readEventsAfter(position, 100).map { it.metadata.eventId.value }

    @Test
    fun `a new consumer does not read events committed before its first getPosition`() {
        insert("before")
        val position = offsets.getPosition("orders-outbox")
        insert("after")

        assertEquals(listOf("after"), readAfter(position))
    }

    @Test
    fun `a new consumer reads an event in a transaction still open at its first getPosition`() {
        insert("before")
        dataSource.connection.use { open ->
            open.autoCommit = false
            insert(open, "in-flight")
            val position = offsets.getPosition("orders-outbox")
            open.commit()
            insert("after")

            assertEquals(listOf("in-flight", "after"), readAfter(position))
        }
    }

    @Test
    fun `the first position is saved and returned on later calls`() {
        val first = offsets.getPosition("orders-outbox")
        insert("later")

        assertEquals(first, offsets.getPosition("orders-outbox"))
        assertEquals(listOf("later"), readAfter(offsets.getPosition("orders-outbox")))
    }

    @Test
    fun `StartFrom Beginning on a new consumer returns START and reads everything`() {
        insert("before")
        val position = offsets.getPosition("orders-outbox", StartFrom.Beginning)

        assertEquals(EventLogPosition.START, position)
        assertEquals(listOf("before"), readAfter(position))
    }

    @Test
    fun `a saved position wins over startFrom`() {
        offsets.savePosition("orders-outbox", EventLogPosition(7, 10))

        assertEquals(EventLogPosition(7, 10), offsets.getPosition("orders-outbox", StartFrom.Beginning))
        assertEquals(EventLogPosition(7, 10), offsets.getPosition("orders-outbox", StartFrom.Latest))
    }

    @Test
    fun `savePosition then getPosition round-trips`() {
        offsets.savePosition("orders-outbox", EventLogPosition(7, 10))
        assertEquals(EventLogPosition(7, 10), offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `savePosition overwrites the previous position`() {
        offsets.savePosition("orders-outbox", EventLogPosition(7, 10))
        offsets.savePosition("orders-outbox", EventLogPosition(9, 42))
        assertEquals(EventLogPosition(9, 42), offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `positions are independent per consumer`() {
        offsets.savePosition("orders-outbox", EventLogPosition(9, 42))
        offsets.savePosition("public-contract", EventLogPosition(3, 3))

        assertEquals(EventLogPosition(9, 42), offsets.getPosition("orders-outbox"))
        assertEquals(EventLogPosition(3, 3), offsets.getPosition("public-contract"))
        assertEquals(EventLogPosition.START, offsets.getPosition("someone-else", StartFrom.Beginning))
    }
}

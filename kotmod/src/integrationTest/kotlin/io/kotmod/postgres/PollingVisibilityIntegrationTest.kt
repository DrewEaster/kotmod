package io.kotmod.postgres

import io.kotmod.EventLogPosition
import io.kotmod.postgres.support.IntegrationTest
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class PollingVisibilityIntegrationTest : IntegrationTest() {
    /** A consumer that reads the event log the way the poller does and records what it saw. */
    private inner class Consumer(
        var position: EventLogPosition = EventLogPosition.START,
    ) {
        val seen = mutableListOf<String>()

        fun poll(limit: Int = 100): Int {
            val events = PostgresDomainPollingBackend(jdbc).readEventsAfter(position, limit)
            for (event in events) {
                seen += event.metadata.eventId.value
                position = event.position
            }
            return events.size
        }
    }

    private fun openTransaction(): Connection = dataSource.connection.apply { autoCommit = false }

    private fun assignTransactionId(conn: Connection) {
        conn.createStatement().use { it.executeQuery("SELECT pg_current_xact_id()").close() }
    }

    private fun insertEvent(
        conn: Connection,
        eventId: String,
    ) {
        conn
            .prepareStatement(
                "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, causation_id, event_id, " +
                    "event_type, event_version, event_payload, event_timestamp) " +
                    "VALUES ('Order', ?, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
            ).use { ps ->
                ps.setString(1, "agg-$eventId")
                ps.setString(2, eventId)
                ps.executeUpdate()
            }
    }

    @Test
    fun `an event committed after a later one is still delivered`() {
        val consumer = Consumer()
        val a = openTransaction()
        val b = openTransaction()
        try {
            insertEvent(a, "e-1") // offset 1, still in flight
            insertEvent(b, "e-2") // offset 2
            b.commit()
            consumer.poll()
            assertEquals(emptyList(), consumer.seen) // a is still in flight, so nothing may be read yet
            a.commit()
            consumer.poll()
        } finally {
            a.close()
            b.close()
        }

        assertEquals(listOf("e-1", "e-2"), consumer.seen)
    }

    @Test
    fun `an event with a lower offset from a later transaction is still delivered`() {
        val consumer = Consumer()
        val a = openTransaction()
        val b = openTransaction()
        try {
            assignTransactionId(a)
            assignTransactionId(b) // b's transaction id is later than a's
            insertEvent(b, "e-b") // offset 1
            insertEvent(a, "e-a") // offset 2
            a.commit()
            consumer.poll()
            b.commit()
            consumer.poll()
        } finally {
            a.close()
            b.close()
        }

        assertEquals(listOf("e-a", "e-b"), consumer.seen)
    }

    @Test
    fun `a rolled-back transaction does not block delivery`() {
        val consumer = Consumer()
        openTransaction().use { a ->
            insertEvent(a, "e-rolled-back")
            a.rollback()
        }
        openTransaction().use { b ->
            insertEvent(b, "e-2")
            b.commit()
        }

        consumer.poll()

        assertEquals(listOf("e-2"), consumer.seen)
    }

    @Test
    fun `a consumer with no saved position reads every event`() {
        openTransaction().use { a ->
            insertEvent(a, "e-1")
            insertEvent(a, "e-2")
            a.commit()
        }

        val consumer = Consumer()
        consumer.poll()

        assertEquals(listOf("e-1", "e-2"), consumer.seen)
    }

    @Test
    fun `one transaction's events are read exactly once across batches and restarts`() {
        openTransaction().use { a ->
            (1..5).forEach { insertEvent(a, "e-$it") }
            a.commit()
        }

        val seen = mutableListOf<String>()
        var saved = EventLogPosition.START
        while (true) {
            // A fresh consumer each batch, resuming from the saved position, as after a restart.
            val consumer = Consumer(saved)
            if (consumer.poll(limit = 2) == 0) break
            seen += consumer.seen
            saved = consumer.position
        }

        assertEquals((1..5).map { "e-$it" }, seen)
    }

    @Test
    fun `a saved position ahead of the server's transaction counter fails loudly`() {
        // As after restoring the database onto a new server: the saved position's transaction id is from the old one.
        val currentXid =
            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint").use { rs ->
                        rs.next()
                        rs.getLong(1)
                    }
                }
            }
        openTransaction().use { a ->
            insertEvent(a, "e-new")
            a.commit()
        }

        val failure =
            assertFailsWith<IllegalStateException> {
                PostgresDomainPollingBackend(jdbc).readEventsAfter(EventLogPosition(currentXid + 1_000_000, 1), 100)
            }

        assertTrue(failure.message!!.contains("ahead of"), failure.message)
    }
}

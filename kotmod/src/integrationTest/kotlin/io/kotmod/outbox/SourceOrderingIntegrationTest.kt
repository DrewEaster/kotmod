package io.kotmod.outbox

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.EventLogPosition
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.support.ConnectionJdbcContext
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.postgres.support.orderEventSerialization
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

class SourceOrderingIntegrationTest : IntegrationTest() {
    private fun open(): Connection = dataSource.connection.apply { autoCommit = false }

    private fun assignTransactionId(conn: Connection) {
        conn.createStatement().use { it.executeQuery("SELECT pg_current_xact_id()").close() }
    }

    /** Writes [aggregateId]'s next event on [conn] through the real saveMeta, as a command would. */
    private fun insert(
        conn: Connection,
        aggregateId: String,
        sequence: Long,
    ) {
        val backend = PostgresDomainPersistenceBackend(ConnectionJdbcContext(conn), orderEventSerialization())
        val type = AggregateType("Order")
        val id = AggregateId(aggregateId)
        assertEquals(sequence, backend.saveMeta(type, id, expectedVersion = backend.loadMeta(type, id)?.version, eventCount = 1))
        conn
            .prepareStatement(
                "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence, causation_id, " +
                    "event_id, event_type, event_version, event_payload, event_timestamp) " +
                    "VALUES ('Order', ?, ?, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
            ).use { ps ->
                ps.setString(1, aggregateId)
                ps.setLong(2, sequence)
                ps.setString(3, "$aggregateId#$sequence")
                ps.executeUpdate()
            }
    }

    private fun flagged(aggregateId: String): Boolean =
        dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT has_out_of_order_events FROM ddd_aggregate_root WHERE aggregate_type = 'Order' AND aggregate_id = ?")
                .use { ps ->
                    ps.setString(1, aggregateId)
                    ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
                }
        }

    private fun poller(dispatched: MutableList<String>): DomainEventPoller {
        val position = AtomicReference(EventLogPosition.START)
        return DomainEventPoller(
            backend = PostgresDomainPollingBackend(jdbc),
            getPosition = { position.get() },
            savePosition = { position.set(it) },
            isLeader = { true },
            pollInterval = 50.milliseconds,
            batchSize = 100,
            loggerName = "SourceOrderingIntegrationTest",
            handleEvent = { event -> dispatched += "${event.metadata.aggregateId.value}#${event.metadata.sequence}" },
        )
    }

    @Test
    fun `an inverted pair is dispatched in sequence order, each once`() =
        runBlocking {
            dataSource.connection.use { insert(it, "A", 1) }
            val t1 = open()
            insert(t1, "B", 1) // t1 gets the earlier transaction id
            open().use { t2 ->
                insert(t2, "A", 2)
                t2.commit()
            }
            insert(t1, "A", 3) // later in A's history, earlier transaction id
            t1.commit()
            t1.close()
            assertEquals(true, flagged("A"), "the inverted write flags the aggregate")

            val dispatched = CopyOnWriteArrayList<String>()
            val poller = poller(dispatched)
            poller.start()
            try {
                eventually { dispatched.size >= 4 }
                delay(300)
            } finally {
                poller.stop()
            }

            val forA = dispatched.filter { it.startsWith("A#") }
            assertEquals(listOf("A#1", "A#2", "A#3"), forA)
            assertEquals(4, dispatched.size)
        }

    @Test
    fun `the poller waits for an earlier event that is not yet readable`() =
        runBlocking {
            val t1 = open()
            insert(t1, "B", 1) // t1: earliest transaction id
            val blocker = open()
            assignTransactionId(blocker) // an unrelated transaction, id between t1 and t2, left open
            open().use { t2 ->
                insert(t2, "A", 1)
                t2.commit()
            }
            insert(t1, "A", 2)
            t1.commit()
            t1.close()

            val dispatched = CopyOnWriteArrayList<String>()
            val poller = poller(dispatched)
            poller.start()
            try {
                delay(500)
                assertEquals(false, dispatched.contains("A#2"), "A#2 must wait for A#1, which is held back")
                blocker.rollback()
                blocker.close()
                eventually { dispatched.size >= 3 }
            } finally {
                poller.stop()
            }

            assertEquals(listOf("A#1", "A#2"), dispatched.filter { it.startsWith("A#") })
        }

    @Test
    fun `an event pulled forward once is never pulled forward again by a later event`() =
        runBlocking {
            // Transaction ids (and so log positions) end up ordered ta < tb < tc ...
            val ta = open()
            insert(ta, "X", 1)
            val tb = open()
            insert(tb, "Y", 1)
            val tc = open()
            insert(tc, "Z", 1)
            // ... but A's events are written in the opposite order: A#1 in tc, A#2 in ta, A#3 in tb.
            insert(tc, "A", 1)
            tc.commit()
            tc.close()
            insert(ta, "A", 2)
            ta.commit()
            ta.close()
            insert(tb, "A", 3)
            tb.commit()
            tb.close()

            val dispatched = CopyOnWriteArrayList<String>()
            val poller = poller(dispatched)
            poller.start()
            try {
                eventually { dispatched.size >= 6 }
                delay(300)
            } finally {
                poller.stop()
            }

            assertEquals(listOf("A#1", "A#2", "A#3"), dispatched.filter { it.startsWith("A#") })
            assertEquals(6, dispatched.size)
        }
}

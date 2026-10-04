package io.kotmod.postgres

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.EventLogPosition
import io.kotmod.EventProducer
import io.kotmod.SequenceCheck
import io.kotmod.postgres.support.ConnectionJdbcContext
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.OrderPlaced
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class OutOfOrderFlagIntegrationTest : IntegrationTest() {
    private val order = AggregateType("Order")

    private fun open(): Connection = dataSource.connection.apply { autoCommit = false }

    /** Writes A's next event on [conn] through the real saveMeta, returning its sequence. */
    private fun write(
        conn: Connection,
        aggregateId: String,
    ): Long {
        val backend = PostgresDomainPersistenceBackend(ConnectionJdbcContext(conn), orderEventSerialization())
        val id = AggregateId(aggregateId)
        val sequence = backend.saveMeta(order, id, expectedVersion = backend.loadMeta(order, id)?.version, eventCount = 1)
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
        return sequence
    }

    private fun flagged(aggregateId: String): Boolean =
        dataSource.connection.use { conn ->
            conn
                .prepareStatement("SELECT has_out_of_order_events FROM ddd_aggregate_root WHERE aggregate_type = 'Order' AND aggregate_id = ?")
                .use { ps ->
                    ps.setString(1, aggregateId)
                    ps.executeQuery().use { rs ->
                        rs.next()
                        rs.getBoolean(1)
                    }
                }
        }

    @Test
    fun `writes in transaction order leave the aggregate unflagged`() {
        repeat(3) { open().use { conn -> write(conn, "A").also { conn.commit() } } }
        open().use { outer ->
            write(outer, "B") // an outer transaction that writes A later, with nothing in between
            write(outer, "A")
            outer.commit()
        }

        assertEquals(false, flagged("A"))
        assertEquals(false, flagged("B"))
    }

    @Test
    fun `EventProducer writes leave the aggregate unflagged`() =
        runBlocking {
            val producer = EventProducer(order, PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()))
            producer.emit(AggregateId("p-1"), listOf(OrderPlaced("a")), CommandId("c-1"))
            producer.emit(AggregateId("p-1"), listOf(OrderPlaced("b"), OrderPlaced("c")), CommandId("c-2"))

            assertEquals(false, flagged("p-1"))
        }

    @Test
    fun `a write from an older transaction after a newer one flags the aggregate`() {
        open().use { conn -> write(conn, "A").also { conn.commit() } }
        open().use { t1 ->
            write(t1, "B") // t1 gets the earlier transaction id
            open().use { t2 ->
                write(t2, "A")
                t2.commit()
            }
            assertEquals(false, flagged("A"))
            write(t1, "A") // later in A's history, earlier transaction id
            t1.commit()
        }

        assertEquals(true, flagged("A"))
        assertEquals(false, flagged("B"))
    }

    @Test
    fun `checkSequence treats an unflagged aggregate as in order without the full check`() {
        // Raw rows that look inverted (A#2 sits before A#1 in the log) but whose aggregate is not flagged:
        // only the fast path answers InOrder for A#2.
        dataSource.connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    "INSERT INTO ddd_aggregate_root (aggregate_type, aggregate_id, aggregate_version, last_sequence, " +
                        "created_at, updated_at) VALUES ('Order', 'A', 2, 2, now(), now())",
                )
            }
            for (sequence in listOf(2L, 1L)) {
                conn
                    .prepareStatement(
                        "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence, causation_id, " +
                            "event_id, event_type, event_version, event_payload, event_timestamp) " +
                            "VALUES ('Order', 'A', ?, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
                    ).use { ps ->
                        ps.setLong(1, sequence)
                        ps.setString(2, "A#$sequence")
                        ps.executeUpdate()
                    }
            }
        }
        val polling = PostgresDomainPollingBackend(jdbc)
        val first = polling.readEventsAfter(EventLogPosition.START, 10).first()
        assertEquals(2L, first.metadata.sequence)

        assertIs<SequenceCheck.InOrder>(polling.checkSequence(first, EventLogPosition.START))
    }
}

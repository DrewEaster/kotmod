package io.kotmod.outbox

import io.kotmod.EventLogPosition
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.postgres.support.recordingExecutor
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class SourceOrderingIntegrationTest : IntegrationTest() {
    private data class Seen(
        val label: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private fun open(): Connection = dataSource.connection.apply { autoCommit = false }

    private fun assignTransactionId(conn: Connection) {
        conn.createStatement().use { it.executeQuery("SELECT pg_current_xact_id()").close() }
    }

    private fun insert(
        conn: Connection,
        aggregateId: String,
        sequence: Long,
    ) {
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

    private fun outbox(dispatched: MutableList<String>): AggregateEventOutbox<Seen> {
        val position = AtomicReference(EventLogPosition.START)
        return AggregateEventOutbox(
            backend = PostgresDomainPollingBackend(jdbc),
            executor = recordingExecutor<Seen> { _, trigger -> dispatched += trigger.label },
            eventToReactions = { event ->
                val label = "${event.metadata.aggregateId.value}#${event.metadata.sequence}"
                listOf(EventReaction(EventReactionId("r-$label"), Seen(label)))
            },
            getPosition = { position.get() },
            savePosition = { position.set(it) },
            isLeader = { true },
            pollInterval = 50.milliseconds,
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

            val dispatched = CopyOnWriteArrayList<String>()
            val outbox = outbox(dispatched)
            outbox.start()
            try {
                eventually { dispatched.size >= 4 }
                delay(300)
            } finally {
                outbox.stop()
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
            val outbox = outbox(dispatched)
            outbox.start()
            try {
                delay(500)
                assertEquals(false, dispatched.contains("A#2"), "A#2 must wait for A#1, which is held back")
                blocker.rollback()
                blocker.close()
                eventually { dispatched.size >= 3 }
            } finally {
                outbox.stop()
            }

            assertEquals(listOf("A#1", "A#2"), dispatched.filter { it.startsWith("A#") })
        }
}

package io.kotmod.postgres

import io.kotmod.EventLogPosition
import io.kotmod.SequenceCheck
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.process.ProcessEventSerialization
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/**
 * A process manager's stream interleaves its own facts with kotmod's internal envelopes (requested commands and
 * scheduled inputs), sharing one sequence. The public polling backend hides the envelopes everywhere; the process
 * manager's own backend sees them.
 */
class ProcessEnvelopeVisibilityIntegrationTest : IntegrationTest() {
    private val public = PostgresDomainPollingBackend(jdbc)
    private val withEnvelopes = PostgresDomainPollingBackend(jdbc, includeProcessEnvelopes = true)

    private fun root(
        type: String,
        id: String,
        flagged: Boolean,
    ) = dataSource.connection.use { conn ->
        conn
            .prepareStatement(
                "INSERT INTO ddd_aggregate_root (aggregate_type, aggregate_id, aggregate_version, last_sequence, " +
                    "has_out_of_order_events, created_at, updated_at) VALUES (?, ?, 1, 10, ?, now(), now())",
            ).use { ps ->
                ps.setString(1, type)
                ps.setString(2, id)
                ps.setBoolean(3, flagged)
                ps.executeUpdate()
            }
    }

    /** Inserts one event in its own transaction (or on [conn]); its event id is `<id>#<sequence>`. */
    private fun insert(
        id: String,
        sequence: Long,
        eventType: String,
        type: String = "Proc",
        conn: Connection? = null,
    ) {
        fun run(c: Connection) =
            c
                .prepareStatement(
                    "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence, causation_id, " +
                        "event_id, event_type, event_version, event_payload, event_timestamp) " +
                        "VALUES (?, ?, ?, 'cmd', ?, ?, 1, '{}', now())",
                ).use { ps ->
                    ps.setString(1, type)
                    ps.setString(2, id)
                    ps.setLong(3, sequence)
                    ps.setString(4, "$id#$sequence")
                    ps.setString(5, eventType)
                    ps.executeUpdate()
                }
        if (conn != null) run(conn) else dataSource.connection.use { run(it) }
    }

    private fun fact(
        id: String,
        sequence: Long,
        type: String = "Proc",
    ) = insert(id, sequence, "ProcFact", type)

    private fun commandRequested(
        id: String,
        sequence: Long,
    ) = insert(id, sequence, ProcessEventSerialization.COMMAND_REQUESTED)

    private fun inputScheduled(
        id: String,
        sequence: Long,
    ) = insert(id, sequence, ProcessEventSerialization.INPUT_SCHEDULED)

    private fun List<io.kotmod.PersistedEvent>.ids() = map { it.metadata.eventId.value }

    @Test
    fun `the public backend returns only facts, and the process manager's backend returns everything`() {
        fact("p", 1)
        commandRequested("p", 2)
        inputScheduled("p", 3)
        fact("p", 4)
        fact("o", 1, type = "Order")

        assertEquals(listOf("p#1", "p#4", "o#1"), public.readEventsAfter(EventLogPosition.START, 10).ids())
        assertEquals(
            listOf("p#1", "p#2", "p#3", "p#4", "o#1"),
            withEnvelopes.readEventsAfter(EventLogPosition.START, 10).ids(),
        )
    }

    @Test
    fun `a consumer reading in small batches moves past hidden envelopes`() {
        commandRequested("p", 1)
        inputScheduled("p", 2)
        commandRequested("p", 3)
        fact("p", 4)
        commandRequested("p", 5)
        fact("o", 1, type = "Order")

        var position = EventLogPosition.START
        val seen = mutableListOf<String>()
        repeat(5) {
            for (event in public.readEventsAfter(position, 1)) {
                seen += event.metadata.eventId.value
                position = event.position
            }
        }

        assertEquals(listOf("p#4", "o#1"), seen)
    }

    @Test
    fun `checkSequence on an out-of-order aggregate pulls forward only earlier facts`() {
        root("Proc", "p", flagged = true)
        fact("p", 4)
        inputScheduled("p", 3)
        commandRequested("p", 2)
        fact("p", 1)

        val first = public.readEventsAfter(EventLogPosition.START, 10).first()
        assertEquals("p#4", first.metadata.eventId.value)
        val check = public.checkSequence(first, EventLogPosition.START)
        assertIs<SequenceCheck.HandleEarlierFirst>(check)
        assertEquals(listOf("p#1"), check.earlier.ids())

        val internalCheck = withEnvelopes.checkSequence(first, EventLogPosition.START)
        assertIs<SequenceCheck.HandleEarlierFirst>(internalCheck)
        assertEquals(listOf("p#1", "p#2", "p#3"), internalCheck.earlier.ids())
    }

    @Test
    fun `checkSequence never waits for an envelope that is not readable yet`() {
        root("Proc", "p", flagged = true)
        fact("p", 2)
        dataSource.connection.use { open ->
            open.autoCommit = false
            open.createStatement().use { it.executeQuery("SELECT pg_current_xact_id()").close() }
            commandRequested("p", 1) // committed, but behind the open transaction, so not readable yet

            val first = public.readEventsAfter(EventLogPosition.START, 10).single()
            assertEquals("p#2", first.metadata.eventId.value)
            assertIs<SequenceCheck.InOrder>(public.checkSequence(first, EventLogPosition.START))
            assertIs<SequenceCheck.WaitForEarlier>(withEnvelopes.checkSequence(first, EventLogPosition.START))
            open.rollback()
        }
    }

    @Test
    fun `an envelope already passed in the log does not make a later-read earlier fact look handled`() {
        root("Proc", "p", flagged = true)
        commandRequested("p", 2)
        fact("o", 1, type = "Order")
        fact("p", 1)

        val events = public.readEventsAfter(EventLogPosition.START, 10)
        assertEquals(listOf("o#1", "p#1"), events.ids())
        assertIs<SequenceCheck.InOrder>(public.checkSequence(events[1], events[0].position))
        assertIs<SequenceCheck.AlreadyHandled>(withEnvelopes.checkSequence(events[1], events[0].position))
    }
}

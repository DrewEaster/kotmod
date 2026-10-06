package io.kotmod.outbox

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.StartFrom
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.BeforeEach
import java.sql.Connection
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class DomainEventPollerIntegrationTest : IntegrationTest() {
    private lateinit var backend: PostgresDomainPersistenceBackend<OrderEvent>
    private lateinit var offsets: PostgresOffsetManager

    @BeforeEach
    fun createBackends() {
        backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization())
        offsets = PostgresOffsetManager(jdbc)
    }

    private fun seed(
        eventId: String,
        aggregateId: String,
    ) {
        val type = AggregateType("Order")
        backend.saveMeta(type, AggregateId(aggregateId), expectedVersion = null, eventCount = 1)
        backend.appendEvents(
            listOf(
                PendingEvent(
                    metadata =
                        EventMetadata(
                            eventId = EventId(eventId),
                            aggregateType = type,
                            aggregateId = AggregateId(aggregateId),
                            causationId = CommandId("cmd-$eventId"),
                            correlationId = null,
                            timestamp = kotlin.time.Instant.parse("2026-04-18T10:00:00Z"),
                            sequence = 1,
                        ),
                    event = OrderPlaced("widgets-$aggregateId"),
                ),
            ),
        )
    }

    /** A poller that records each event's id and saves its position as consumer [CONSUMER]. */
    private fun newPoller(
        handled: MutableList<String>,
        isLeader: () -> Boolean = { true },
    ) = DomainEventPoller(
        backend = PostgresDomainPollingBackend(jdbc),
        getPosition = { offsets.getPosition(CONSUMER, StartFrom.Beginning) },
        savePosition = { offsets.savePosition(CONSUMER, it) },
        isLeader = isLeader,
        pollInterval = 50.milliseconds,
        batchSize = 10,
        loggerName = "DomainEventPollerIntegrationTest",
        handleEvent = { event -> handled += event.metadata.eventId.value },
    )

    private suspend fun waitForPosition(offset: Long) {
        withTimeout(5.seconds) {
            while (offsets.getPosition(CONSUMER, StartFrom.Beginning).globalOffset < offset) delay(50)
        }
    }

    @Test
    fun `the poller hands over every event in log order and saves its position`() =
        runBlocking {
            seed("e-1", "o-1")
            seed("e-2", "o-2")
            val handled = CopyOnWriteArrayList<String>()
            val poller = newPoller(handled)

            poller.start()
            try {
                waitForPosition(2)
            } finally {
                poller.stop()
            }

            assertEquals(listOf("e-1", "e-2"), handled.toList())
            assertEquals(2L, offsets.getPosition(CONSUMER, StartFrom.Beginning).globalOffset)
        }

    @Test
    fun `the poller resumes from the saved position`() =
        runBlocking {
            seed("e-1", "o-1")
            seed("e-2", "o-2")
            offsets.savePosition(CONSUMER, PostgresDomainPollingBackend(jdbc).readEventsAfter(EventLogPosition.START, 1).single().position)
            val handled = CopyOnWriteArrayList<String>()
            val poller = newPoller(handled)

            poller.start()
            try {
                waitForPosition(2)
            } finally {
                poller.stop()
            }

            assertEquals(listOf("e-2"), handled.toList())
        }

    @Test
    fun `the poller skips ticks when isLeader returns false`() =
        runBlocking {
            seed("e-1", "o-1")
            val handled = CopyOnWriteArrayList<String>()
            val poller = newPoller(handled, isLeader = { false })

            poller.start()
            try {
                delay(300)
            } finally {
                poller.stop()
            }

            assertEquals(emptyList(), handled.toList())
            assertEquals(EventLogPosition.START, offsets.getPosition(CONSUMER, StartFrom.Beginning))
        }

    @Test
    fun `the poller delivers an event committed after a later one`() =
        runBlocking {
            fun insert(
                conn: Connection,
                eventId: String,
            ) = conn
                .prepareStatement(
                    "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence, causation_id, event_id, " +
                        "event_type, event_version, event_payload, event_timestamp) " +
                        "VALUES ('Order', ?, 1, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
                ).use { ps ->
                    ps.setString(1, "agg-$eventId")
                    ps.setString(2, eventId)
                    ps.executeUpdate()
                }

            val handled = CopyOnWriteArrayList<String>()
            val position = AtomicReference(EventLogPosition.START)
            val poller =
                DomainEventPoller(
                    backend = PostgresDomainPollingBackend(jdbc),
                    getPosition = { position.get() },
                    savePosition = { position.set(it) },
                    isLeader = { true },
                    pollInterval = 50.milliseconds,
                    batchSize = 100,
                    loggerName = "DomainEventPollerIntegrationTest",
                    handleEvent = { event -> handled += event.metadata.eventId.value },
                )

            val a = dataSource.connection.apply { autoCommit = false }
            try {
                insert(a, "e-1") // in flight
                dataSource.connection.use { b -> insert(b, "e-2") } // auto-commit
                poller.start()
                delay(300)
                assertEquals(emptyList(), handled.toList()) // e-2 is held back behind e-1's transaction
                a.commit()
                eventually { handled.size == 2 }
            } finally {
                poller.stop()
                a.close()
            }

            assertEquals(listOf("e-1", "e-2"), handled.toList())
        }

    private companion object {
        const val CONSUMER = "poller"
    }
}

package io.kotmod.outbox

import io.kotmod.EventLogPosition
import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.postgres.support.recordingExecutor
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import io.kotmod.postgres.support.eventually
import java.util.concurrent.atomic.AtomicReference
import java.sql.Connection

class AggregateEventOutboxIntegrationTest : IntegrationTest() {
    private data class FakeTrigger(
        val eventId: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

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
        correlationId: CorrelationId? = null,
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
                            correlationId = correlationId,
                            timestamp = kotlin.time.Instant.parse("2026-04-18T10:00:00Z"),
                            sequence = 1,
                        ),
                    event = OrderPlaced("widgets-$aggregateId"),
                ),
            ),
        )
    }

    private fun newOutbox(
        onDispatch: suspend (EventReactionId, FakeTrigger) -> Unit,
        isLeader: () -> Boolean = { true },
    ) = AggregateEventOutbox(
        backend = PostgresDomainPollingBackend(jdbc),
        executor = recordingExecutor(onDispatch),
        eventToReactions = { event ->
            listOf(
                EventReaction(
                    id = EventReactionId("reaction-${event.metadata.eventId.value}"),
                    trigger = FakeTrigger(eventId = event.metadata.eventId.value),
                ),
            )
        },
        getPosition = { offsets.getPosition(CONSUMER) },
        savePosition = { offsets.savePosition(CONSUMER, it) },
        isLeader = isLeader,
        pollInterval = 50.milliseconds,
        batchSize = 10,
    )

    @Test
    fun `outbox dispatches every event in offset order and persists the offset`() =
        runBlocking {
            seed("e-1", "o-1")
            seed("e-2", "o-2", CorrelationId("corr-2"))

            // CopyOnWriteArrayList gives cross-thread visibility without explicit synchronisation.
            val captured = CopyOnWriteArrayList<Pair<EventReactionId, FakeTrigger>>()
            val outbox = newOutbox(onDispatch = { id, trigger -> captured += id to trigger })

            outbox.start()
            try {
                withTimeout(5.seconds) {
                    while (offsets.getPosition(CONSUMER).globalOffset < 2) delay(50)
                }
            } finally {
                outbox.stop()
            }

            assertEquals(
                listOf(
                    EventReactionId("reaction-e-1") to FakeTrigger("e-1"),
                    EventReactionId("reaction-e-2") to FakeTrigger("e-2"),
                ),
                captured.toList(),
            )
            assertEquals(2L, offsets.getPosition(CONSUMER).globalOffset)
        }

    @Test
    fun `outbox resumes from the persisted position`() =
        runBlocking {
            seed("e-1", "o-1")
            seed("e-2", "o-2")
            offsets.savePosition(CONSUMER, PostgresDomainPollingBackend(jdbc).readEventsAfter(EventLogPosition.START, 1).single().position)

            val captured = CopyOnWriteArrayList<EventReactionId>()
            val outbox = newOutbox(onDispatch = { id, _ -> captured += id })

            outbox.start()
            try {
                withTimeout(5.seconds) {
                    while (offsets.getPosition(CONSUMER).globalOffset < 2) delay(50)
                }
            } finally {
                outbox.stop()
            }

            assertEquals(listOf(EventReactionId("reaction-e-2")), captured.toList())
        }

    @Test
    fun `outbox skips ticks when isLeader returns false`() =
        runBlocking {
            seed("e-1", "o-1")

            val dispatched = CopyOnWriteArrayList<EventReactionId>()
            val outbox = newOutbox(onDispatch = { id, _ -> dispatched += id }, isLeader = { false })

            outbox.start()
            try {
                delay(300)
            } finally {
                outbox.stop()
            }

            assertEquals(emptyList(), dispatched.toList())
            assertEquals(EventLogPosition.START, offsets.getPosition(CONSUMER))
        }


    @Test
    fun `outbox delivers an event committed after a later one`() =
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

            val dispatched = CopyOnWriteArrayList<EventReactionId>()
            val position = AtomicReference(EventLogPosition.START)
            val outbox =
                AggregateEventOutbox(
                    backend = PostgresDomainPollingBackend(jdbc),
                    executor = recordingExecutor<FakeTrigger> { id, _ -> dispatched += id },
                    eventToReactions = { event ->
                        listOf(EventReaction(EventReactionId("reaction-${event.metadata.eventId.value}"), FakeTrigger(event.metadata.eventId.value)))
                    },
                    getPosition = { position.get() },
                    savePosition = { position.set(it) },
                    isLeader = { true },
                    pollInterval = 50.milliseconds,
                )

            val a = dataSource.connection.apply { autoCommit = false }
            try {
                insert(a, "e-1") // in flight
                dataSource.connection.use { b -> insert(b, "e-2") } // auto-commit
                outbox.start()
                delay(300)
                assertEquals(emptyList(), dispatched.toList()) // e-2 is held back behind e-1's transaction
                a.commit()
                eventually { dispatched.size == 2 }
            } finally {
                outbox.stop()
                a.close()
            }

            assertEquals(listOf(EventReactionId("reaction-e-1"), EventReactionId("reaction-e-2")), dispatched.toList())
        }

    private companion object {
        const val CONSUMER = "orders-outbox"
    }
}

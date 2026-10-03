package com.dreweaster.ddd.outbox

import com.dreweaster.ddd.AggregateId
import com.dreweaster.ddd.AggregateType
import com.dreweaster.ddd.CommandId
import com.dreweaster.ddd.CorrelationId
import com.dreweaster.ddd.EventId
import com.dreweaster.ddd.EventMetadata
import com.dreweaster.ddd.PendingEvent
import com.dreweaster.ddd.event.reaction.EventReaction
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.postgres.PostgresDomainPersistenceBackend
import com.dreweaster.ddd.postgres.PostgresDomainPollingBackend
import com.dreweaster.ddd.postgres.PostgresOffsetManager
import com.dreweaster.ddd.postgres.support.IntegrationTest
import com.dreweaster.ddd.postgres.support.orderEventSerialization
import com.dreweaster.ddd.postgres.support.recordingExecutor
import com.dreweaster.ddd.support.OrderEvent
import com.dreweaster.ddd.support.OrderPlaced
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

class AggregateEventOutboxIntegrationTest : IntegrationTest() {
    private data class FakeTrigger(
        val eventId: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private lateinit var backend: PostgresDomainPersistenceBackend<OrderEvent>
    private lateinit var offsets: PostgresOffsetManager

    @BeforeEach
    fun createBackends() {
        backend = PostgresDomainPersistenceBackend(driver, orderEventSerialization())
        offsets = PostgresOffsetManager(driver)
    }

    private fun seed(
        eventId: String,
        aggregateId: String,
        correlationId: CorrelationId? = null,
    ) {
        val type = AggregateType("Order")
        backend.saveMeta(type, AggregateId(aggregateId), expectedVersion = null)
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
        backend = PostgresDomainPollingBackend(driver),
        executor = recordingExecutor(onDispatch),
        eventToReactions = { event ->
            listOf(
                EventReaction(
                    id = EventReactionId("reaction-${event.metadata.eventId.value}"),
                    trigger = FakeTrigger(eventId = event.metadata.eventId.value),
                ),
            )
        },
        getOffset = { offsets.getOffset(CONSUMER) },
        saveOffset = { offsets.saveOffset(CONSUMER, it) },
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
                    while (offsets.getOffset(CONSUMER) < 2) delay(50)
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
            assertEquals(2L, offsets.getOffset(CONSUMER))
        }

    @Test
    fun `outbox resumes from the persisted offset`() =
        runBlocking {
            seed("e-1", "o-1")
            seed("e-2", "o-2")
            offsets.saveOffset(CONSUMER, 1)

            val captured = CopyOnWriteArrayList<EventReactionId>()
            val outbox = newOutbox(onDispatch = { id, _ -> captured += id })

            outbox.start()
            try {
                withTimeout(5.seconds) {
                    while (offsets.getOffset(CONSUMER) < 2) delay(50)
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
            assertEquals(PostgresOffsetManager.INITIAL_OFFSET, offsets.getOffset(CONSUMER))
        }

    private companion object {
        const val CONSUMER = "orders-outbox"
    }
}

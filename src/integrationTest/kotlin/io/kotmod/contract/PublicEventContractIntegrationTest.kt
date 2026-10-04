package io.kotmod.contract

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.PublicEventEnvelope
import io.kotmod.SerializedEvent
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.recordingExecutor
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

class PublicEventContractIntegrationTest : IntegrationTest() {
    private sealed interface TestInternalEvent : DomainEvent {
        data class OrderOpened(
            val orderId: String,
        ) : TestInternalEvent

        data class OrderClosed(
            val orderId: String,
        ) : TestInternalEvent

        data class InternalNote(
            val note: String,
        ) : TestInternalEvent
    }

    private sealed interface TestPublicEvent : PublicDomainEvent {
        data class OrderOpenedPublic(
            val orderId: String,
        ) : TestPublicEvent

        data class OrderClosedPublic(
            val orderId: String,
        ) : TestPublicEvent
    }

    /** Minimal round-trip serialization using a naive `type + key=value` payload. */
    private class TestSerialization : DataSerializationContext<TestInternalEvent> {
        override fun serialize(event: TestInternalEvent): SerializedEvent =
            when (event) {
                is TestInternalEvent.OrderOpened -> SerializedEvent("OrderOpened", 1, "orderId=${event.orderId}")
                is TestInternalEvent.OrderClosed -> SerializedEvent("OrderClosed", 1, "orderId=${event.orderId}")
                is TestInternalEvent.InternalNote -> SerializedEvent("InternalNote", 1, "note=${event.note}")
            }

        override fun deserialize(serialized: SerializedEvent): TestInternalEvent {
            val fields =
                serialized.payload.split("|").associate {
                    val (k, v) = it.split("=", limit = 2)
                    k to v
                }
            return when (serialized.type) {
                "OrderOpened" -> TestInternalEvent.OrderOpened(fields.getValue("orderId"))
                "OrderClosed" -> TestInternalEvent.OrderClosed(fields.getValue("orderId"))
                "InternalNote" -> TestInternalEvent.InternalNote(fields.getValue("note"))
                else -> error("unknown event type ${serialized.type}")
            }
        }
    }

    private data class FakeTrigger(
        val eventId: String,
        val subscriberName: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private lateinit var backend: PostgresDomainPersistenceBackend<TestInternalEvent>
    private lateinit var offsets: PostgresOffsetManager

    @BeforeEach
    fun createBackends() {
        backend = PostgresDomainPersistenceBackend(driver, TestSerialization())
        offsets = PostgresOffsetManager(driver)
    }

    private fun seed(
        eventId: String,
        orderId: String,
        event: TestInternalEvent,
    ) {
        backend.saveMeta(type = AggregateType("Order"), id = AggregateId(orderId), expectedVersion = null)
        backend.appendEvents(
            listOf(
                PendingEvent(
                    metadata =
                        EventMetadata(
                            eventId = EventId(eventId),
                            aggregateType = AggregateType("Order"),
                            aggregateId = AggregateId(orderId),
                            causationId = CommandId("cmd-$eventId"),
                            correlationId = null,
                            timestamp = kotlin.time.Instant.parse("2026-04-20T10:00:00Z"),
                        ),
                    event = event,
                ),
            ),
        )
    }

    @Test
    fun `contract fans out public events to two subscribers and filters internal-only events`() =
        runBlocking {
            seed("e-1", "o-1", TestInternalEvent.OrderOpened("o-1"))
            seed("e-2", "o-2", TestInternalEvent.OrderClosed("o-2"))
            seed("e-3", "o-3", TestInternalEvent.InternalNote("private")) // filtered

            val capturedA = CopyOnWriteArrayList<Pair<EventReactionId, FakeTrigger>>()
            val capturedB = CopyOnWriteArrayList<Pair<EventReactionId, FakeTrigger>>()

            val contract =
                PublicEventContract(
                    backend = PostgresDomainPollingBackend(driver),
                    serialization = TestSerialization(),
                    internalToPublic = { event: TestInternalEvent ->
                        when (event) {
                            is TestInternalEvent.OrderOpened -> TestPublicEvent.OrderOpenedPublic(event.orderId)
                            is TestInternalEvent.OrderClosed -> TestPublicEvent.OrderClosedPublic(event.orderId)
                            is TestInternalEvent.InternalNote -> null
                        }
                    },
                    getOffset = { offsets.getOffset(CONSUMER) },
                    saveOffset = { offsets.saveOffset(CONSUMER, it) },
                    isLeader = { true },
                    pollInterval = 50.milliseconds,
                    batchSize = 10,
                )

            fun reactionsFor(
                subscriberName: String,
                envelope: PublicEventEnvelope<TestPublicEvent>,
            ) = listOf(
                EventReaction(
                    id = EventReactionId("$subscriberName-${envelope.metadata.eventId.value}"),
                    trigger = FakeTrigger(eventId = envelope.metadata.eventId.value, subscriberName = subscriberName),
                ),
            )

            contract.subscribe(recordingExecutor<FakeTrigger> { id, trigger -> capturedA += id to trigger }) { reactionsFor("A", it) }
            contract.subscribe(recordingExecutor<FakeTrigger> { id, trigger -> capturedB += id to trigger }) { reactionsFor("B", it) }

            contract.start()
            try {
                // The filtered event still advances the cursor, so offset 3 means all three were handled.
                withTimeout(5.seconds) {
                    while (offsets.getOffset(CONSUMER) < 3) delay(50)
                }
            } finally {
                contract.stop()
            }

            assertEquals(
                listOf(
                    EventReactionId("A-e-1") to FakeTrigger("e-1", "A"),
                    EventReactionId("A-e-2") to FakeTrigger("e-2", "A"),
                ),
                capturedA.toList(),
            )
            assertEquals(
                listOf(
                    EventReactionId("B-e-1") to FakeTrigger("e-1", "B"),
                    EventReactionId("B-e-2") to FakeTrigger("e-2", "B"),
                ),
                capturedB.toList(),
            )
        }

    private companion object {
        const val CONSUMER = "public-contract"
    }
}

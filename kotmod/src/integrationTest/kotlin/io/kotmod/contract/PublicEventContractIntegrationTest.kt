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
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.StartFrom
import io.kotmod.postgres.support.IntegrationTest
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
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

    private lateinit var backend: PostgresDomainPersistenceBackend<TestInternalEvent>
    private lateinit var offsets: PostgresOffsetManager

    @BeforeEach
    fun createBackends() {
        backend = PostgresDomainPersistenceBackend(jdbc, TestSerialization())
        offsets = PostgresOffsetManager(jdbc)
    }

    private fun seed(
        eventId: String,
        orderId: String,
        event: TestInternalEvent,
    ) {
        backend.saveMeta(type = AggregateType("Order"), id = AggregateId(orderId), expectedVersion = null, eventCount = 1)
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
                            sequence = 1,
                        ),
                    event = event,
                ),
            ),
        )
    }

    @Test
    fun `contract hands public events to two listeners and filters internal-only events`() =
        runBlocking {
            seed("e-1", "o-1", TestInternalEvent.OrderOpened("o-1"))
            seed("e-2", "o-2", TestInternalEvent.OrderClosed("o-2"))
            seed("e-3", "o-3", TestInternalEvent.InternalNote("private")) // filtered

            val heard = CopyOnWriteArrayList<Pair<String, PublicEventEnvelope<TestPublicEvent>>>()

            val contract =
                PublicEventContract(
                    backend = PostgresDomainPollingBackend(jdbc),
                    serialization = TestSerialization(),
                    internalToPublic = { event: TestInternalEvent ->
                        when (event) {
                            is TestInternalEvent.OrderOpened -> TestPublicEvent.OrderOpenedPublic(event.orderId)
                            is TestInternalEvent.OrderClosed -> TestPublicEvent.OrderClosedPublic(event.orderId)
                            is TestInternalEvent.InternalNote -> null
                        }
                    },
                    getPosition = { offsets.getPosition(CONSUMER, StartFrom.Beginning) },
                    savePosition = { offsets.savePosition(CONSUMER, it) },
                    isLeader = { true },
                    pollInterval = 50.milliseconds,
                    batchSize = 10,
                )

            contract.listen { heard += "A" to it }
            contract.listen { heard += "B" to it }

            contract.start()
            try {
                // The filtered event still advances the cursor, so offset 3 means all three were handled.
                withTimeout(5.seconds) {
                    while (offsets.getPosition(CONSUMER, StartFrom.Beginning).globalOffset < 3) delay(50)
                }
            } finally {
                contract.stop()
            }

            assertEquals(
                listOf(
                    "A" to "e-1" to TestPublicEvent.OrderOpenedPublic("o-1"),
                    "B" to "e-1" to TestPublicEvent.OrderOpenedPublic("o-1"),
                    "A" to "e-2" to TestPublicEvent.OrderClosedPublic("o-2"),
                    "B" to "e-2" to TestPublicEvent.OrderClosedPublic("o-2"),
                ),
                heard.map { (listener, envelope) -> listener to envelope.metadata.eventId.value to envelope.event },
            )
        }

    private companion object {
        const val CONSUMER = "public-contract"
    }
}

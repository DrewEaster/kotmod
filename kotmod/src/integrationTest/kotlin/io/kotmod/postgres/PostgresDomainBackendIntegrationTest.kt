package io.kotmod.postgres

import io.kotmod.AggregateAlreadyExistsException
import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.PendingEvent
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.OrderCancelled
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class PostgresDomainBackendIntegrationTest : IntegrationTest() {
    private lateinit var backend: PostgresDomainPersistenceBackend<OrderEvent>
    private lateinit var pollingBackend: PostgresDomainPollingBackend

    @BeforeEach
    fun createBackends() {
        backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization())
        pollingBackend = PostgresDomainPollingBackend(jdbc)
    }

    private fun metadata(
        eventId: String,
        aggregateId: String = "o-1",
        correlationId: CorrelationId? = null,
        sequence: Long = 1,
    ) = EventMetadata(
        eventId = EventId(eventId),
        aggregateType = AggregateType("Order"),
        aggregateId = AggregateId(aggregateId),
        causationId = CommandId("cmd-1"),
        correlationId = correlationId,
        timestamp = kotlin.time.Instant.parse("2026-04-18T10:00:00Z"),
        sequence = sequence,
    )

    @Test
    fun `loadMeta returns null when aggregate does not exist`() =
        runTest {
            val meta = backend.loadMeta(AggregateType("Order"), AggregateId("o-1"))
            assertNull(meta)
        }

    @Test
    fun `saveMeta numbers events contiguously within each aggregate`() =
        runTest {
            val order = AggregateType("Order")
            assertEquals(2L, backend.saveMeta(order, AggregateId("o-1"), expectedVersion = null, eventCount = 2))
            assertEquals(3L, backend.saveMeta(order, AggregateId("o-1"), expectedVersion = 1, eventCount = 1))
            assertEquals(3L, backend.saveMeta(order, AggregateId("o-1"), expectedVersion = 2, eventCount = 0))
            assertEquals(1L, backend.saveMeta(order, AggregateId("o-2"), expectedVersion = null, eventCount = 1))
        }

    @Test
    fun `a duplicate sequence within an aggregate is rejected`() =
        runTest {
            fun pending(eventId: String): PendingEvent<OrderEvent> = PendingEvent(metadata(eventId, sequence = 1), OrderPlaced("x"))
            backend.appendEvents(listOf(pending("e-1")))
            assertFailsWith<java.sql.SQLException> { backend.appendEvents(listOf(pending("e-2"))) }
        }

    @Test
    fun `saveMeta with null expectedVersion inserts at version 1`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            backend.saveMeta(type, id, expectedVersion = null, eventCount = 0)
            val meta = backend.loadMeta(type, id)
            assertNotNull(meta)
            assertEquals(1L, meta.version)
        }

    @Test
    fun `saveMeta with null expectedVersion twice throws AggregateAlreadyExistsException`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            backend.saveMeta(type, id, expectedVersion = null, eventCount = 0)
            assertFailsWith<AggregateAlreadyExistsException> {
                backend.saveMeta(type, id, expectedVersion = null, eventCount = 0)
            }
        }

    @Test
    fun `saveMeta with matching expectedVersion increments version`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            backend.saveMeta(type, id, expectedVersion = null, eventCount = 0)
            backend.saveMeta(type, id, expectedVersion = 1L, eventCount = 0)
            val meta = backend.loadMeta(type, id)
            assertEquals(2L, meta!!.version)
        }

    @Test
    fun `saveMeta with stale expectedVersion throws OptimisticConcurrencyException`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            backend.saveMeta(type, id, expectedVersion = null, eventCount = 0)
            assertFailsWith<OptimisticConcurrencyException> {
                backend.saveMeta(type, id, expectedVersion = 99L, eventCount = 0)
            }
        }

    @Test
    fun `appendEvents writes rows with serialized payload and metadata`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            val now = kotlin.time.Instant.parse("2026-04-18T10:00:00Z")

            fun metadata(
                eventId: String,
                correlation: CorrelationId?,
            ) = EventMetadata(
                eventId = EventId(eventId),
                aggregateType = type,
                aggregateId = id,
                causationId = CommandId("cmd-1"),
                correlationId = correlation,
                timestamp = now,
                sequence = if (eventId == "e-1") 1 else 2,
            )
            backend.appendEvents(
                listOf(
                    PendingEvent(metadata("e-1", CorrelationId("corr-1")), OrderPlaced("widgets")),
                    PendingEvent(metadata("e-2", CorrelationId("corr-1")), OrderCancelled("widgets", "sold out")),
                ),
            )

            dataSource.connection.use { conn ->
                conn
                    .createStatement()
                    .executeQuery(
                        "SELECT event_id, event_type, event_version, event_payload, causation_id, correlation_id " +
                            "FROM ddd_domain_event ORDER BY global_offset",
                    ).use { rs ->
                        assertTrue(rs.next())
                        assertEquals("e-1", rs.getString("event_id"))
                        assertEquals("io.kotmod.support.OrderPlaced", rs.getString("event_type"))
                        assertEquals(1, rs.getInt("event_version"))
                        assertEquals("""{"name":"widgets"}""", rs.getString("event_payload"))
                        assertEquals("cmd-1", rs.getString("causation_id"))
                        assertEquals("corr-1", rs.getString("correlation_id"))

                        assertTrue(rs.next())
                        assertEquals("e-2", rs.getString("event_id"))
                        assertEquals("io.kotmod.support.OrderCancelled", rs.getString("event_type"))
                        assertEquals("""{"name":"widgets","reason":"sold out"}""", rs.getString("event_payload"))
                    }
            }
        }

    @Test
    fun `appendEvents with null correlation writes NULL column`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            val now = kotlin.time.Instant.parse("2026-04-18T10:00:00Z")

            backend.appendEvents(
                listOf(
                    PendingEvent(
                        metadata =
                            EventMetadata(
                                eventId = EventId("e-1"),
                                aggregateType = type,
                                aggregateId = id,
                                causationId = CommandId("cmd-1"),
                                correlationId = null,
                                timestamp = now,
                                sequence = 1,
                            ),
                        event = OrderPlaced("widgets"),
                    ),
                ),
            )

            dataSource.connection.use { conn ->
                conn
                    .createStatement()
                    .executeQuery(
                        "SELECT correlation_id FROM ddd_domain_event",
                    ).use { rs ->
                        assertTrue(rs.next())
                        rs.getString("correlation_id")
                        assertTrue(rs.wasNull())
                    }
            }
        }

    @Test
    fun `appendEvents with empty list is a no-op`() =
        runTest {
            backend.appendEvents(emptyList())

            dataSource.connection.use { conn ->
                conn.createStatement().executeQuery("SELECT COUNT(*) FROM ddd_domain_event").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(0, rs.getInt(1))
                }
            }
        }

    @Test
    fun `wasCommandHandled returns false when unseen`() =
        runTest {
            val seen = backend.wasCommandHandled(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-1"))
            assertEquals(false, seen)
        }

    @Test
    fun `recordCommandHandled then wasCommandHandled returns true`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            val cmd = CommandId("cmd-1")

            backend.recordCommandHandled(type, id, cmd)
            val seen = backend.wasCommandHandled(type, id, cmd)

            assertTrue(seen)
        }

    @Test
    fun `wasCommandHandled is scoped per type, id, and commandId`() =
        runTest {
            backend.recordCommandHandled(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-1"))

            assertEquals(false, backend.wasCommandHandled(AggregateType("Order"), AggregateId("o-2"), CommandId("cmd-1")))
            assertEquals(false, backend.wasCommandHandled(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-2")))
            assertEquals(false, backend.wasCommandHandled(AggregateType("Widget"), AggregateId("o-1"), CommandId("cmd-1")))
        }

    @Test
    fun `backend operations inside a JdbcContext transaction roll back together`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            val now = kotlin.time.Instant.parse("2026-04-18T10:00:00Z")

            try {
                jdbc.inTransaction {
                    backend.saveMeta(type, id, expectedVersion = null, eventCount = 1)
                    backend.appendEvents(
                        listOf(
                            PendingEvent(
                                metadata =
                                    EventMetadata(
                                        eventId = EventId("e-1"),
                                        aggregateType = type,
                                        aggregateId = id,
                                        causationId = CommandId("cmd-1"),
                                        correlationId = null,
                                        timestamp = now,
                                        sequence = 1,
                                    ),
                                event = OrderPlaced("widgets"),
                            ),
                        ),
                    )
                    backend.recordCommandHandled(type, id, CommandId("cmd-1"))
                    throw RuntimeException("simulated failure")
                }
            } catch (e: RuntimeException) {
                // expected
            }

            assertNull(backend.loadMeta(type, id))
            dataSource.connection.use { conn ->
                conn.createStatement().executeQuery("SELECT COUNT(*) FROM ddd_domain_event").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(0, rs.getInt(1))
                }
                conn.createStatement().executeQuery("SELECT COUNT(*) FROM ddd_command_history").use { rs ->
                    assertTrue(rs.next())
                    assertEquals(0, rs.getInt(1))
                }
            }
        }

    @Test
    fun `readEventsAfter returns events after the given offset in offset order`() =
        runTest {
            backend.appendEvents(
                listOf(
                    PendingEvent(metadata("e-1", correlationId = CorrelationId("corr-1")), OrderPlaced("widgets")),
                    PendingEvent(metadata("e-2", sequence = 2), OrderCancelled("widgets", "sold out")),
                    PendingEvent(metadata("e-3", aggregateId = "o-2"), OrderPlaced("gadgets")),
                ),
            )

            val all = pollingBackend.readEventsAfter(EventLogPosition.START, limit = 10)
            assertEquals(listOf(1L, 2L, 3L), all.map { it.position.globalOffset })
            assertEquals(listOf("e-1", "e-2", "e-3"), all.map { it.metadata.eventId.value })
            assertEquals(listOf(1L, 2L, 1L), all.map { it.metadata.sequence })

            val first = all.first()
            assertEquals(metadata("e-1", correlationId = CorrelationId("corr-1")), first.metadata)
            assertEquals("io.kotmod.support.OrderPlaced", first.serialized.type)
            assertEquals("""{"name":"widgets"}""", first.serialized.payload)
            assertNull(all[1].metadata.correlationId)

            assertEquals(listOf("e-2", "e-3"), pollingBackend.readEventsAfter(all[0].position, limit = 10).map { it.metadata.eventId.value })
            assertEquals(listOf("e-1", "e-2"), pollingBackend.readEventsAfter(EventLogPosition.START, limit = 2).map { it.metadata.eventId.value })
            assertEquals(emptyList(), pollingBackend.readEventsAfter(all[2].position, limit = 10))
        }
}

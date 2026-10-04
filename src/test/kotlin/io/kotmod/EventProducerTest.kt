package io.kotmod

import io.kotmod.support.OrderCancelled
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.StubPersistenceBackend
import kotlinx.coroutines.test.runTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EventProducerTest {
    private lateinit var backend: StubPersistenceBackend<OrderEvent>
    private lateinit var producer: EventProducer<OrderEvent>

    @BeforeTest
    fun setUp() {
        backend = StubPersistenceBackend()
        producer =
            EventProducer(
                aggregateType = AggregateType("Audit"),
                backend = backend,
            )
    }

    @Test
    fun `first emit creates meta at version 1 and appends events`() =
        runTest {
            val id = AggregateId("a-1")
            producer.emit(id, events = listOf(OrderPlaced("audit")), commandId = CommandId("c-1"))
            val meta = backend.metas[StubPersistenceBackend.Key(AggregateType("Audit"), id)]
            assertEquals(1L, meta!!.version)
            assertEquals(1, backend.events.size)
        }

    @Test
    fun `subsequent emit increments version`() =
        runTest {
            val id = AggregateId("a-1")
            producer.emit(id, listOf(OrderPlaced("audit")))
            producer.emit(id, listOf(OrderCancelled("audit", "meh")))
            val meta = backend.metas[StubPersistenceBackend.Key(AggregateType("Audit"), id)]
            assertEquals(2L, meta!!.version)
            assertEquals(2, backend.events.size)
        }

    @Test
    fun `duplicate commandId is a no-op`() =
        runTest {
            val id = AggregateId("a-1")
            val cmd = CommandId("c-1")
            producer.emit(id, listOf(OrderPlaced("audit")), commandId = cmd)
            producer.emit(id, listOf(OrderPlaced("audit")), commandId = cmd)
            val meta = backend.metas[StubPersistenceBackend.Key(AggregateType("Audit"), id)]
            assertEquals(1L, meta!!.version)
            assertEquals(1, backend.events.size)
        }

    @Test
    fun `emit with empty events still writes meta and records command`() =
        runTest {
            val id = AggregateId("a-1")
            producer.emit(id, events = emptyList(), commandId = CommandId("c-1"))
            val meta = backend.metas[StubPersistenceBackend.Key(AggregateType("Audit"), id)]
            assertEquals(1L, meta!!.version)
            assertEquals(0, backend.events.size)
            assertTrue(backend.commands.any { it.commandId == CommandId("c-1") })
        }
}

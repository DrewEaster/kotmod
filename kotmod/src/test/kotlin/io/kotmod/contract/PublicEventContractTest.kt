package io.kotmod.contract

import io.kotmod.AggregateType
import io.kotmod.EventLogPosition
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.PersistedEvent
import io.kotmod.SequenceCheck
import io.kotmod.PublicEventEnvelope
import io.kotmod.support.RecordingOffsets
import io.kotmod.support.persistedEvent
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class PublicEventContractTest {
    // ---- Test fixtures --------------------------------------------------

    private sealed interface TestInternalEvent : DomainEvent {
        data class Opened(
            val id: String,
        ) : TestInternalEvent

        data class Closed(
            val id: String,
        ) : TestInternalEvent

        data class InternalOnly(
            val id: String,
        ) : TestInternalEvent
    }

    private sealed interface TestPublicEvent : PublicDomainEvent {
        data class OpenedPublic(
            val id: String,
        ) : TestPublicEvent

        data class ClosedPublic(
            val id: String,
        ) : TestPublicEvent
    }

    /** Serializer whose deserialize() is counted so fan-out can assert "once per event". */
    private class CountingSerialization : DataSerializationContext<TestInternalEvent> {
        var deserializeCalls: Int = 0

        override fun serialize(event: TestInternalEvent): SerializedEvent = SerializedEvent(event::class.simpleName!!, 1, event.toString())

        override fun deserialize(serialized: SerializedEvent): TestInternalEvent {
            deserializeCalls++
            // payload is the toString() form: "Opened(id=e-10)" etc. Parse "id=..." from it.
            val id =
                Regex("""id=([^)]+)""").find(serialized.payload)?.groupValues?.get(1)
                    ?: error("bad payload: ${serialized.payload}")
            return when (serialized.type) {
                "Opened" -> TestInternalEvent.Opened(id)
                "Closed" -> TestInternalEvent.Closed(id)
                "InternalOnly" -> TestInternalEvent.InternalOnly(id)
                else -> error("unknown type ${serialized.type}")
            }
        }
    }

    private val backend: DomainEventPollingBackend = mockk()

    /** What the listeners saw, in order: each listener's name and the event id. */
    private val heard = mutableListOf<Pair<String, String>>()
    private val offsets = RecordingOffsets(initial = EventLogPosition(1, 9))

    private fun givenEvents(vararg events: PersistedEvent) {
        every { backend.readEventsAfter(EventLogPosition(1, 9), any()) } returns events.toList()
        every { backend.checkSequence(any(), any()) } returns SequenceCheck.InOrder
    }

    private fun newContract(
        serialization: DataSerializationContext<TestInternalEvent> = CountingSerialization(),
        internalToPublic: (TestInternalEvent) -> TestPublicEvent? = { ev ->
            when (ev) {
                is TestInternalEvent.Opened -> TestPublicEvent.OpenedPublic(ev.id)
                is TestInternalEvent.Closed -> TestPublicEvent.ClosedPublic(ev.id)
                is TestInternalEvent.InternalOnly -> null
            }
        },
        isLeader: () -> Boolean = { true },
        aggregateTypes: Set<AggregateType>? = null,
    ) = PublicEventContract(
        backend = backend,
        serialization = serialization,
        internalToPublic = internalToPublic,
        getPosition = offsets::get,
        savePosition = offsets::save,
        isLeader = isLeader,
        aggregateTypes = aggregateTypes,
        pollInterval = 50.milliseconds,
        batchSize = 100,
    )

    /** Registers a listener named [name] that records each envelope it hears, then runs [then]. */
    private fun PublicEventContract<TestInternalEvent, TestPublicEvent>.recording(
        name: String,
        then: (PublicEventEnvelope<TestPublicEvent>) -> Unit = {},
    ) = listen { envelope ->
        heard += name to envelope.metadata.eventId.value
        then(envelope)
    }

    // ---- Tests ----------------------------------------------------------

    @Test
    fun `tick is a no-op when isLeader returns false`() {
        val contract = newContract(isLeader = { false })
        contract.recording("A")

        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        verify(exactly = 0) { backend.readEventsAfter(any(), any()) }
        assertTrue(heard.isEmpty())
    }

    @Test
    fun `a listener receives the typed public event in its envelope`() {
        givenEvents(
            persistedEvent(
                globalOffset = 10,
                eventId = "e-10",
                eventType = "Opened",
                eventPayload = "Opened(id=doc-1)",
            ),
        )

        val contract = newContract()
        val seen = mutableListOf<PublicEventEnvelope<TestPublicEvent>>()
        contract.listen { seen += it }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(listOf<TestPublicEvent>(TestPublicEvent.OpenedPublic("doc-1")), seen.map { it.event })
        assertEquals("e-10", seen.single().metadata.eventId.value)
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `every listener hears each event once, in registration order`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
            persistedEvent(globalOffset = 11, eventId = "e-11", eventType = "Closed", eventPayload = "Closed(id=doc-1)"),
        )

        val contract = newContract()
        contract.recording("A")
        contract.recording("B")
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(listOf("A" to "e-10", "B" to "e-10", "A" to "e-11", "B" to "e-11"), heard)
        assertEquals(listOf(10L, 11L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `internalToPublic returning null filters the event — no listener hears it, cursor advances`() {
        givenEvents(
            persistedEvent(
                globalOffset = 10,
                eventId = "e-10",
                eventType = "InternalOnly",
                eventPayload = "InternalOnly(id=x-1)",
            ),
        )

        val contract = newContract()
        contract.recording("A")
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertTrue(heard.isEmpty())
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `a process manager's internal envelope events are skipped without deserializing them`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "io.kotmod.process.CommandRequested", eventPayload = "{}"),
            persistedEvent(globalOffset = 11, eventId = "e-11", eventType = "io.kotmod.process.InputScheduled", eventPayload = "{}"),
        )

        val counting = CountingSerialization()
        val contract = newContract(serialization = counting)
        contract.recording("A")
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(0, counting.deserializeCalls)
        assertTrue(heard.isEmpty())
        assertEquals(listOf(10L, 11L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `with an aggregate type filter, events of other types are skipped without deserializing them`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", aggregateType = "Shipment", eventType = "Unknown", eventPayload = "?"),
            persistedEvent(globalOffset = 11, eventId = "e-11", aggregateType = "Document", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
        )

        val counting = CountingSerialization()
        val contract = newContract(serialization = counting, aggregateTypes = setOf(AggregateType("Document")))
        val seen = mutableListOf<TestPublicEvent>()
        contract.listen { seen += it.event }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(1, counting.deserializeCalls)
        assertEquals(listOf<TestPublicEvent>(TestPublicEvent.OpenedPublic("doc-1")), seen)
        assertEquals(listOf(10L, 11L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `without an aggregate type filter, events of every type are published`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", aggregateType = "Shipment", eventType = "Opened", eventPayload = "Opened(id=s-1)"),
            persistedEvent(globalOffset = 11, eventId = "e-11", aggregateType = "Document", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
        )

        val contract = newContract()
        val seen = mutableListOf<TestPublicEvent>()
        contract.listen { seen += it.event }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(listOf<TestPublicEvent>(TestPublicEvent.OpenedPublic("s-1"), TestPublicEvent.OpenedPublic("doc-1")), seen)
    }

    @Test
    fun `a listener that throws halts the batch — cursor does not advance`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
            persistedEvent(globalOffset = 11, eventId = "e-11", eventType = "Opened", eventPayload = "Opened(id=doc-2)"),
        )

        val contract = newContract()
        contract.recording("A") { error("simulated mapping failure") }
        val caught = runCatching { kotlinx.coroutines.runBlocking { contract.tickForTest() } }
        assertFalse(caught.isSuccess)

        assertEquals(listOf("A" to "e-10"), heard)
        assertEquals(emptyList(), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `a failure in the second listener re-reads the whole event, so the first hears it again`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
        )

        var failures = 1
        val contract = newContract()
        contract.recording("A")
        contract.recording("B") {
            if (failures > 0) {
                failures--
                throw RuntimeException("queue offline")
            }
        }
        val caught = runCatching { kotlinx.coroutines.runBlocking { contract.tickForTest() } }
        assertFalse(caught.isSuccess)
        assertEquals(emptyList(), offsets.saved.map { it.globalOffset })

        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(listOf("A" to "e-10", "B" to "e-10", "A" to "e-10", "B" to "e-10"), heard)
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `listen after start throws IllegalStateException`() {
        val contract = newContract()
        contract.start()

        assertFailsWith<IllegalStateException> {
            contract.listen { }
        }
        kotlinx.coroutines.runBlocking { contract.stop() }
    }

    @Test
    fun `deserialization happens once per event regardless of listener count`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
        )

        val counting = CountingSerialization()
        val contract = newContract(serialization = counting)
        contract.recording("A")
        contract.recording("B")
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(1, counting.deserializeCalls)
    }
}

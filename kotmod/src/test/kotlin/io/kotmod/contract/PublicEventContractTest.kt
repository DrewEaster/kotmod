package io.kotmod.contract

import io.kotmod.EventLogPosition
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.PersistedEvent
import io.kotmod.SequenceCheck
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.support.RecordingOffsets
import io.kotmod.support.persistedEvent
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifySequence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.time.Duration
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

    private data class FakeTrigger(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

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
    private val executorA: EventReactionExecutor<FakeTrigger, Any> = mockk(relaxed = true)
    private val executorB: EventReactionExecutor<FakeTrigger, Any> = mockk(relaxed = true)
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
    ) = PublicEventContract(
        backend = backend,
        serialization = serialization,
        internalToPublic = internalToPublic,
        getPosition = offsets::get,
        savePosition = offsets::save,
        isLeader = isLeader,
        pollInterval = 50.milliseconds,
        batchSize = 100,
    )

    // ---- Tests ----------------------------------------------------------

    @Test
    fun `tick is a no-op when isLeader returns false`() {
        val contract = newContract(isLeader = { false })
        contract.subscribe(executorA) { error("should not be invoked") }

        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        verify(exactly = 0) { backend.readEventsAfter(any(), any()) }
        coVerify(exactly = 0) { executorA.dispatch(any(), any(), any()) }
    }

    @Test
    fun `single subscriber receives typed public event and its reactions are dispatched`() {
        givenEvents(
            persistedEvent(
                globalOffset = 10,
                eventId = "e-10",
                eventType = "Opened",
                eventPayload = "Opened(id=doc-1)",
            ),
        )

        val contract = newContract()
        val seen = mutableListOf<TestPublicEvent>()
        contract.subscribe(executorA) { envelope ->
            seen += envelope.event
            listOf(
                EventReaction(EventReactionId("reaction-${envelope.metadata.eventId.value}-a"), FakeTrigger("a")),
                EventReaction(EventReactionId("reaction-${envelope.metadata.eventId.value}-b"), FakeTrigger("b")),
            )
        }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals<List<TestPublicEvent>>(listOf(TestPublicEvent.OpenedPublic("doc-1")), seen)
        coVerifySequence {
            executorA.dispatch(EventReactionId("reaction-e-10-a"), FakeTrigger("a"), null)
            executorA.dispatch(EventReactionId("reaction-e-10-b"), FakeTrigger("b"), null)
        }
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `multi-subscriber fan-out invokes each subscriber once per event`() {
        givenEvents(
            persistedEvent(
                globalOffset = 10,
                eventId = "e-10",
                eventType = "Opened",
                eventPayload = "Opened(id=doc-1)",
            ),
        )

        val contract = newContract()
        contract.subscribe(executorA) {
            listOf(
                EventReaction(EventReactionId("A-${it.metadata.eventId.value}-1"), FakeTrigger("A1")),
                EventReaction(EventReactionId("A-${it.metadata.eventId.value}-2"), FakeTrigger("A2")),
            )
        }
        contract.subscribe(executorB) {
            listOf(
                EventReaction(EventReactionId("B-${it.metadata.eventId.value}-1"), FakeTrigger("B1")),
                EventReaction(EventReactionId("B-${it.metadata.eventId.value}-2"), FakeTrigger("B2")),
            )
        }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        coVerifySequence {
            executorA.dispatch(EventReactionId("A-e-10-1"), FakeTrigger("A1"), null)
            executorA.dispatch(EventReactionId("A-e-10-2"), FakeTrigger("A2"), null)
        }
        coVerifySequence {
            executorB.dispatch(EventReactionId("B-e-10-1"), FakeTrigger("B1"), null)
            executorB.dispatch(EventReactionId("B-e-10-2"), FakeTrigger("B2"), null)
        }
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `subscriber block returning an empty list is valid and cursor advances`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
        )

        val contract = newContract()
        contract.subscribe(executorA) { emptyList() }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        coVerify(exactly = 0) { executorA.dispatch(any(), any(), any()) }
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `internalToPublic returning null filters the event — no dispatch, cursor advances`() {
        givenEvents(
            persistedEvent(
                globalOffset = 10,
                eventId = "e-10",
                eventType = "InternalOnly",
                eventPayload = "InternalOnly(id=x-1)",
            ),
        )

        val contract = newContract()
        contract.subscribe(executorA) { error("should not be invoked — event filtered out") }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        coVerify(exactly = 0) { executorA.dispatch(any(), any(), any()) }
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `subscriber block that throws halts the batch — cursor does not advance`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
            persistedEvent(globalOffset = 11, eventId = "e-11", eventType = "Opened", eventPayload = "Opened(id=doc-2)"),
        )

        val contract = newContract()
        contract.subscribe(executorA) { error("simulated mapping failure") }
        val caught = runCatching { kotlinx.coroutines.runBlocking { contract.tickForTest() } }
        assertFalse(caught.isSuccess)

        coVerify(exactly = 0) { executorA.dispatch(any(), any(), any()) }
        assertEquals(emptyList(), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `dispatch failure on the second subscriber for one event halts before cursor advance`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
        )

        coEvery { executorA.dispatch(any(), any(), any()) } returns Unit
        coEvery { executorB.dispatch(any(), any(), any()) } throws RuntimeException("sink offline")

        val contract = newContract()
        contract.subscribe(executorA) { listOf(EventReaction(EventReactionId("A"), FakeTrigger("A"))) }
        contract.subscribe(executorB) { listOf(EventReaction(EventReactionId("B"), FakeTrigger("B"))) }
        val caught = runCatching { kotlinx.coroutines.runBlocking { contract.tickForTest() } }
        assertFalse(caught.isSuccess)

        coVerify(exactly = 1) { executorA.dispatch(EventReactionId("A"), FakeTrigger("A"), null) }
        coVerify(exactly = 1) { executorB.dispatch(EventReactionId("B"), FakeTrigger("B"), null) }
        assertEquals(emptyList(), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `subscribe after start throws IllegalStateException`() {
        val contract = newContract()
        contract.start()

        assertFailsWith<IllegalStateException> {
            contract.subscribe(executorA) { emptyList() }
        }
    }

    @Test
    fun `deserialization happens once per event regardless of subscriber count`() {
        givenEvents(
            persistedEvent(globalOffset = 10, eventId = "e-10", eventType = "Opened", eventPayload = "Opened(id=doc-1)"),
        )

        val counting = CountingSerialization()
        val contract = newContract(serialization = counting)
        contract.subscribe(executorA) { listOf(EventReaction(EventReactionId("A"), FakeTrigger("A"))) }
        contract.subscribe(executorB) { listOf(EventReaction(EventReactionId("B"), FakeTrigger("B"))) }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        assertEquals(1, counting.deserializeCalls)
    }

    @Test
    fun `a contract stamps only its ordered subscriptions`() {
        every { executorA.supportsOrdering } returns true
        givenEvents(persistedEvent(globalOffset = 10, aggregateId = "o-1", sequence = 2, eventType = "Opened", eventPayload = "Opened(id=doc-1)"))
        val contract = newContract()
        contract.subscribe(executorA, ordering = ReactionOrdering.PerAggregate()) { listOf(EventReaction(EventReactionId("A"), FakeTrigger("A"))) }
        contract.subscribe(executorB) { listOf(EventReaction(EventReactionId("B"), FakeTrigger("B"))) }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        coVerify { executorA.dispatch(EventReactionId("A"), FakeTrigger("A"), DispatchOrdering("Order/o-1", 2, 0, OnGiveUp.ContinueWithNext)) }
        coVerify { executorB.dispatch(EventReactionId("B"), FakeTrigger("B"), null) }
    }
}

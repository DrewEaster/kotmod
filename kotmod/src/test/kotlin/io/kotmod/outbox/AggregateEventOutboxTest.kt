package io.kotmod.outbox

import io.kotmod.EventLogPosition
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
import io.mockk.coVerifyOrder
import io.mockk.coVerifySequence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.time.Duration

class AggregateEventOutboxTest {
    private data class FakeTrigger(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private val backend: DomainEventPollingBackend = mockk()
    private val executor: EventReactionExecutor<FakeTrigger, Any> = mockk(relaxed = true)
    private val offsets = RecordingOffsets(initial = EventLogPosition(1, 9))

    private fun givenEvents(vararg events: PersistedEvent) {
        every { backend.readEventsAfter(EventLogPosition(1, 9), any()) } returns events.toList()
        every { backend.checkSequence(any(), any()) } returns SequenceCheck.InOrder
    }

    private fun newOutbox(
        eventToReactions: (PersistedEvent) -> List<EventReaction<FakeTrigger>>,
        isLeader: () -> Boolean = { true },
    ) = AggregateEventOutbox(
        backend = backend,
        executor = executor,
        eventToReactions = eventToReactions,
        getPosition = offsets::get,
        savePosition = offsets::save,
        isLeader = isLeader,
    )

    @Test
    fun `tick is a no-op when isLeader returns false`() {
        val outbox = newOutbox(eventToReactions = { error("should not be invoked when not leader") }, isLeader = { false })
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        verify(exactly = 0) { backend.readEventsAfter(any(), any()) }
        coVerify(exactly = 0) { executor.dispatch(any(), any(), any()) }
    }

    @Test
    fun `tick calls eventToReactions for each event and dispatches each EventReaction`() {
        givenEvents(persistedEvent(globalOffset = 10, eventId = "e-1"), persistedEvent(globalOffset = 11, eventId = "e-2"))

        val mappingCalls = mutableListOf<String>()
        val outbox =
            newOutbox(eventToReactions = { event ->
                mappingCalls += event.metadata.eventId.value
                listOf(
                    EventReaction(
                        EventReactionId("reaction-${event.metadata.eventId.value}-a"),
                        FakeTrigger("a-${event.metadata.eventId.value}"),
                    ),
                    EventReaction(
                        EventReactionId("reaction-${event.metadata.eventId.value}-b"),
                        FakeTrigger("b-${event.metadata.eventId.value}"),
                    ),
                )
            })
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        assertEquals(listOf("e-1", "e-2"), mappingCalls)
        coVerifySequence {
            executor.dispatch(EventReactionId("reaction-e-1-a"), FakeTrigger("a-e-1"), null)
            executor.dispatch(EventReactionId("reaction-e-1-b"), FakeTrigger("b-e-1"), null)
            executor.dispatch(EventReactionId("reaction-e-2-a"), FakeTrigger("a-e-2"), null)
            executor.dispatch(EventReactionId("reaction-e-2-b"), FakeTrigger("b-e-2"), null)
        }
    }

    @Test
    fun `tick advances offset per event after all that event's reactions are dispatched`() {
        givenEvents(persistedEvent(globalOffset = 10), persistedEvent(globalOffset = 11), persistedEvent(globalOffset = 12))

        val outbox =
            newOutbox(eventToReactions = { listOf(EventReaction(EventReactionId("t-${it.position.globalOffset}"), FakeTrigger("t"))) })
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        assertEquals(listOf(10L, 11L, 12L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `eventToReactions returning an empty list is valid and still advances offset`() {
        givenEvents(persistedEvent(globalOffset = 10))

        val outbox = newOutbox(eventToReactions = { emptyList() })
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        coVerify(exactly = 0) { executor.dispatch(any(), any(), any()) }
        assertEquals(listOf(10L), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `dispatch failure on the 2nd of 3 reactions for one event halts the batch`() {
        givenEvents(persistedEvent(globalOffset = 10, eventId = "e-1"), persistedEvent(globalOffset = 11, eventId = "e-2"))

        coEvery { executor.dispatch(EventReactionId("t-e-1-a"), any(), any()) } returns Unit
        coEvery { executor.dispatch(EventReactionId("t-e-1-b"), any(), any()) } throws RuntimeException("simulated")

        val outbox =
            newOutbox(eventToReactions = { event ->
                listOf(
                    EventReaction(EventReactionId("t-${event.metadata.eventId.value}-a"), FakeTrigger("a")),
                    EventReaction(EventReactionId("t-${event.metadata.eventId.value}-b"), FakeTrigger("b")),
                    EventReaction(EventReactionId("t-${event.metadata.eventId.value}-c"), FakeTrigger("c")),
                )
            })

        val caught = runCatching { kotlinx.coroutines.runBlocking { outbox.tickForTest() } }
        assertFalse(caught.isSuccess)

        // First reaction for e-1 dispatched; second threw; third and e-2 not attempted
        coVerify(exactly = 1) { executor.dispatch(EventReactionId("t-e-1-a"), any(), any()) }
        coVerify(exactly = 1) { executor.dispatch(EventReactionId("t-e-1-b"), any(), any()) }
        coVerify(exactly = 0) { executor.dispatch(EventReactionId("t-e-1-c"), any(), any()) }
        coVerify(exactly = 0) { executor.dispatch(EventReactionId("t-e-2-a"), any(), any()) }

        // Position never advanced (dispatch threw before the per-event savePosition call)
        assertEquals(emptyList(), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `eventToReactions exception halts the batch`() {
        givenEvents(persistedEvent(globalOffset = 10))

        val outbox = newOutbox(eventToReactions = { error("mapping exploded") })

        val caught = runCatching { kotlinx.coroutines.runBlocking { outbox.tickForTest() } }
        assertFalse(caught.isSuccess)

        coVerify(exactly = 0) { executor.dispatch(any(), any(), any()) }
        assertEquals(emptyList(), offsets.saved.map { it.globalOffset })
    }

    @Test
    fun `an ordered outbox stamps each reaction with the source aggregate, sequence and ordinal`() {
        every { executor.supportsOrdering } returns true
        givenEvents(persistedEvent(globalOffset = 10, aggregateId = "o-1", sequence = 4))
        val outbox =
            AggregateEventOutbox(
                backend = backend,
                executor = executor,
                eventToReactions = { listOf(EventReaction(EventReactionId("a"), FakeTrigger("a")), EventReaction(EventReactionId("b"), FakeTrigger("b"))) },
                getPosition = offsets::get,
                savePosition = offsets::save,
                isLeader = { true },
                ordering = ReactionOrdering.PerAggregate(OnGiveUp.BlockAggregate),
            )
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        coVerifyOrder {
            executor.dispatch(EventReactionId("a"), FakeTrigger("a"), DispatchOrdering("Order/o-1", 4, 0, OnGiveUp.BlockAggregate))
            executor.dispatch(EventReactionId("b"), FakeTrigger("b"), DispatchOrdering("Order/o-1", 4, 1, OnGiveUp.BlockAggregate))
        }
    }

    @Test
    fun `an ordered outbox on an executor that cannot order fails at construction`() {
        every { executor.supportsOrdering } returns false
        assertFailsWith<IllegalArgumentException> {
            AggregateEventOutbox(
                backend = backend,
                executor = executor,
                eventToReactions = { emptyList() },
                getPosition = offsets::get,
                savePosition = offsets::save,
                isLeader = { true },
                ordering = ReactionOrdering.PerAggregate(),
            )
        }
    }
}

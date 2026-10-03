package com.dreweaster.ddd.outbox

import com.dreweaster.ddd.DomainEventPollingBackend
import com.dreweaster.ddd.PersistedEvent
import com.dreweaster.ddd.event.reaction.EventReaction
import com.dreweaster.ddd.event.reaction.EventReactionExecutor
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.support.RecordingOffsets
import com.dreweaster.ddd.support.persistedEvent
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.coVerifySequence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.time.Duration

class AggregateEventOutboxTest {
    private data class FakeTrigger(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private val backend: DomainEventPollingBackend = mockk()
    private val executor: EventReactionExecutor<FakeTrigger, Any> = mockk(relaxed = true)
    private val offsets = RecordingOffsets(initial = 9L)

    private fun givenEvents(vararg events: PersistedEvent) {
        every { backend.readEventsAfter(9L, any()) } returns events.toList()
    }

    private fun newOutbox(
        eventToReactions: (PersistedEvent) -> List<EventReaction<FakeTrigger>>,
        isLeader: () -> Boolean = { true },
    ) = AggregateEventOutbox(
        backend = backend,
        executor = executor,
        eventToReactions = eventToReactions,
        getOffset = offsets::get,
        saveOffset = offsets::save,
        isLeader = isLeader,
    )

    @Test
    fun `tick is a no-op when isLeader returns false`() {
        val outbox = newOutbox(eventToReactions = { error("should not be invoked when not leader") }, isLeader = { false })
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        verify(exactly = 0) { backend.readEventsAfter(any(), any()) }
        coVerify(exactly = 0) { executor.dispatch(any(), any()) }
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
            executor.dispatch(EventReactionId("reaction-e-1-a"), FakeTrigger("a-e-1"))
            executor.dispatch(EventReactionId("reaction-e-1-b"), FakeTrigger("b-e-1"))
            executor.dispatch(EventReactionId("reaction-e-2-a"), FakeTrigger("a-e-2"))
            executor.dispatch(EventReactionId("reaction-e-2-b"), FakeTrigger("b-e-2"))
        }
    }

    @Test
    fun `tick advances offset per event after all that event's reactions are dispatched`() {
        givenEvents(persistedEvent(globalOffset = 10), persistedEvent(globalOffset = 11), persistedEvent(globalOffset = 12))

        val outbox =
            newOutbox(eventToReactions = { listOf(EventReaction(EventReactionId("t-${it.globalOffset}"), FakeTrigger("t"))) })
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        assertEquals(listOf(10L, 11L, 12L), offsets.saved)
    }

    @Test
    fun `eventToReactions returning an empty list is valid and still advances offset`() {
        givenEvents(persistedEvent(globalOffset = 10))

        val outbox = newOutbox(eventToReactions = { emptyList() })
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        coVerify(exactly = 0) { executor.dispatch(any(), any()) }
        assertEquals(listOf(10L), offsets.saved)
    }

    @Test
    fun `dispatch failure on the 2nd of 3 reactions for one event halts the batch`() {
        givenEvents(persistedEvent(globalOffset = 10, eventId = "e-1"), persistedEvent(globalOffset = 11, eventId = "e-2"))

        coEvery { executor.dispatch(EventReactionId("t-e-1-a"), any()) } returns Unit
        coEvery { executor.dispatch(EventReactionId("t-e-1-b"), any()) } throws RuntimeException("simulated")

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
        coVerify(exactly = 1) { executor.dispatch(EventReactionId("t-e-1-a"), any()) }
        coVerify(exactly = 1) { executor.dispatch(EventReactionId("t-e-1-b"), any()) }
        coVerify(exactly = 0) { executor.dispatch(EventReactionId("t-e-1-c"), any()) }
        coVerify(exactly = 0) { executor.dispatch(EventReactionId("t-e-2-a"), any()) }

        // Offset never advanced (dispatch threw before the per-event saveOffset call)
        assertEquals(emptyList(), offsets.saved)
    }

    @Test
    fun `eventToReactions exception halts the batch`() {
        givenEvents(persistedEvent(globalOffset = 10))

        val outbox = newOutbox(eventToReactions = { error("mapping exploded") })

        val caught = runCatching { kotlinx.coroutines.runBlocking { outbox.tickForTest() } }
        assertFalse(caught.isSuccess)

        coVerify(exactly = 0) { executor.dispatch(any(), any()) }
        assertEquals(emptyList(), offsets.saved)
    }
}

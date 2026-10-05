package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.OnGiveUp
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.TaskInstance
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration

class DbSchedulerTriggerSinkTest {
    private data class FakeTrigger(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private object FakeTriggerSerializer : EventReactionTriggerSerializer<FakeTrigger> {
        override suspend fun serialize(trigger: FakeTrigger): String {
            require(trigger.name != "unserializable") { "cannot serialize" }
            return trigger.name
        }

        override suspend fun deserialize(serializedTrigger: String) = FakeTrigger(serializedTrigger)
    }

    private val now = Instant.parse("2026-10-03T10:00:00Z")
    private val client: SchedulerClient = mockk()
    private val sink = DbSchedulerTriggerSink("billing-reactions", FakeTriggerSerializer, client, clock = { now })

    @Test
    fun `publish schedules the reaction if not already scheduled`() {
        val instance = slot<TaskInstance<String>>()
        every { client.scheduleIfNotExists(capture(instance), any<Instant>()) } returns true

        runBlocking { sink.publish(EventReactionId("charge-e-1"), FakeTrigger("charge"), null, null) }

        verify(exactly = 1) { client.scheduleIfNotExists(any<TaskInstance<String>>(), now) }
        assertEquals("billing-reactions", instance.captured.taskName)
        assertEquals("charge-e-1", instance.captured.id)
        assertEquals(ReactionTaskData(trigger = "charge", retryCount = 0), ReactionTaskData.decode(instance.captured.data))
    }

    @Test
    fun `publish of an already-scheduled reaction is not an error`() {
        every { client.scheduleIfNotExists(any<TaskInstance<String>>(), any<Instant>()) } returns false

        runBlocking { sink.publish(EventReactionId("charge-e-1"), FakeTrigger("charge"), null, null) }
    }

    @Test
    fun `serializer failure propagates and nothing is scheduled`() {
        assertFailsWith<IllegalArgumentException> {
            runBlocking { sink.publish(EventReactionId("charge-e-1"), FakeTrigger("unserializable"), null, null) }
        }
        verify(exactly = 0) { client.scheduleIfNotExists(any<TaskInstance<String>>(), any<Instant>()) }
    }
    @Test
    fun `ordered publish uses a sortable instance id and stamps the ordering`() {
        val orderedSink = DbSchedulerTriggerSink("billing-reactions", FakeTriggerSerializer, client, clock = { now }, supportsOrdering = true)
        val instance = slot<TaskInstance<String>>()
        every { client.scheduleIfNotExists(capture(instance), any<Instant>()) } returns true

        runBlocking {
            orderedSink.publish(EventReactionId("charge-e-1"), FakeTrigger("charge"), DispatchOrdering("Order/o-1", 3, 1, OnGiveUp.BlockAggregate), null)
        }

        assertEquals(orderedInstanceId("Order/o-1", 3, 1, "charge-e-1"), instance.captured.id)
        assertEquals(
            ReactionTaskData(
                trigger = "charge",
                retryCount = 0,
                ordering = OrderingStamp("Order/o-1", 3, 1, "BlockAggregate", "charge-e-1"),
            ),
            ReactionTaskData.decode(instance.captured.data),
        )
    }

    @Test
    fun `ordered publish is rejected when ordering is not supported`() {
        assertFailsWith<IllegalArgumentException> {
            runBlocking { sink.publish(EventReactionId("charge-e-1"), FakeTrigger("charge"), DispatchOrdering("Order/o-1", 3, 0, OnGiveUp.ContinueWithNext), null) }
        }
        verify(exactly = 0) { client.scheduleIfNotExists(any<TaskInstance<String>>(), any<Instant>()) }
    }

    @Test
    fun `a delayed reaction is scheduled at notBefore and carries it in its task data`() {
        val instance = slot<TaskInstance<String>>()
        val notBefore = kotlin.time.Instant.parse("2026-10-10T09:00:00.123456789Z")
        every { client.scheduleIfNotExists(capture(instance), any<Instant>()) } returns true

        runBlocking { sink.publish(EventReactionId("remind-e-1"), FakeTrigger("remind"), null, notBefore) }

        verify(exactly = 1) { client.scheduleIfNotExists(any<TaskInstance<String>>(), Instant.parse("2026-10-10T09:00:00.123456789Z")) }
        assertEquals(notBefore.toString(), ReactionTaskData.decode(instance.captured.data).notBefore)
    }
}

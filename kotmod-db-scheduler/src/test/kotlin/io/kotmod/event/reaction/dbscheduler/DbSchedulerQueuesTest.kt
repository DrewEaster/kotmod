package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.jdbc.JdbcContext
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration

class DbSchedulerQueuesTest {
    private data class FakeTrigger(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private object FakeTriggerSerializer : EventReactionTriggerSerializer<FakeTrigger> {
        override suspend fun serialize(trigger: FakeTrigger) = trigger.name

        override suspend fun deserialize(serializedTrigger: String) = FakeTrigger(serializedTrigger)
    }

    private val queues = DbSchedulerQueues(mockk<JdbcContext>())

    @Test
    fun `each queue is one db-scheduler task named after it`() {
        queues.channel("fraud-checks", FakeTriggerSerializer, ordered = true)
        queues.channel("DispatchDeadline-inputs", FakeTriggerSerializer, ordered = false)

        assertEquals(listOf("fraud-checks", "DispatchDeadline-inputs"), queues.tasks.map { it.name })
    }

    @Test
    fun `every queue supports ordering, so ordered work left in a queue that is no longer ordered can still run`() {
        assertTrue(queues.channel("ordered", FakeTriggerSerializer, ordered = true).sink.supportsOrdering)
        assertTrue(queues.channel("unordered", FakeTriggerSerializer, ordered = false).sink.supportsOrdering)
    }

    @Test
    fun `asking for the same queue twice is refused`() {
        queues.channel("fraud-checks", FakeTriggerSerializer, ordered = false)

        val error = assertFailsWith<IllegalArgumentException> { queues.channel("fraud-checks", FakeTriggerSerializer, ordered = false) }
        assertEquals(
            "DbSchedulerQueues already has a queue named fraud-checks: use-case names and process manager channels must be unique",
            error.message,
        )
    }

    @Test
    fun `asking for a queue after the tasks were read is refused`() {
        queues.channel("fraud-checks", FakeTriggerSerializer, ordered = false)
        queues.tasks

        val error = assertFailsWith<IllegalStateException> { queues.channel("late", FakeTriggerSerializer, ordered = false) }
        assertEquals(
            "Queue late was asked for after DbSchedulerQueues.tasks was read, so its task would never be registered: read tasks " +
                "after registering every use case and building every process manager",
            error.message,
        )
    }

    @Test
    fun `publishing before bind fails with a clear message`() =
        runBlocking<Unit> {
            val channel = queues.channel("fraud-checks", FakeTriggerSerializer, ordered = false)

            val error = assertFailsWith<IllegalStateException> { channel.sink.publish(EventReactionId("r"), FakeTrigger("x"), null, null) }
            assertEquals("Call bind(scheduler) on DbSchedulerQueues before starting", error.message)
        }

    @Test
    fun `operator helpers refuse a queue that doesn't exist`() {
        val error = assertFailsWith<IllegalArgumentException> { queues.blockedReactions(mockk(), "nope") }
        assertEquals("DbSchedulerQueues has no queue named nope", error.message)
    }
}

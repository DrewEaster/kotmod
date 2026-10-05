package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration

class DbSchedulerProcessManagerQueuesTest {
    private data class FakeTrigger(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private object FakeTriggerSerializer : EventReactionTriggerSerializer<FakeTrigger> {
        override suspend fun serialize(trigger: FakeTrigger) = trigger.name

        override suspend fun deserialize(serializedTrigger: String) = FakeTrigger(serializedTrigger)
    }

    @Test
    fun `each channel gets a task named after the queues and the channel`() {
        val queues = DbSchedulerProcessManagerQueues("windows")
        queues.channel("inputs", FakeTriggerSerializer, ordered = false)
        queues.channel("commands", FakeTriggerSerializer, ordered = false)

        assertEquals(listOf("windows-inputs", "windows-commands"), queues.tasks.map { it.name })
    }

    @Test
    fun `asking for the same channel twice is refused, as two process managers sharing the queues would`() {
        val queues = DbSchedulerProcessManagerQueues("windows")
        queues.channel("inputs", FakeTriggerSerializer, ordered = false)

        val error = assertFailsWith<IllegalArgumentException> { queues.channel("inputs", FakeTriggerSerializer, ordered = false) }
        assertEquals(
            "DbSchedulerProcessManagerQueues(windows) already has a channel named inputs; give each process manager its own queues",
            error.message,
        )
    }

    @Test
    fun `asking for a channel after the tasks were read is refused`() {
        val queues = DbSchedulerProcessManagerQueues("windows")
        queues.channel("inputs", FakeTriggerSerializer, ordered = false)
        queues.tasks

        val error = assertFailsWith<IllegalStateException> { queues.channel("contract-orders", FakeTriggerSerializer, ordered = false) }
        assertEquals(
            "Channel contract-orders was asked for after DbSchedulerProcessManagerQueues(windows).tasks was read, so its task " +
                "would never be registered: register tasks after building the process manager and all its subscribeTo calls",
            error.message,
        )
    }
}

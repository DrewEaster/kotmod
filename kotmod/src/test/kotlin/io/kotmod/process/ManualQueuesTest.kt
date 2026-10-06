package io.kotmod.process

import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOutcome
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class ManualQueuesTest {
    private data class Item(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private object ItemSerializer : EventReactionTriggerSerializer<Item> {
        override suspend fun serialize(trigger: Item) = trigger.name

        override suspend fun deserialize(serializedTrigger: String) = Item(serializedTrigger)
    }

    private fun stamp(
        key: String,
        sequence: Long,
    ) = DispatchOrdering(key, sequence, 0, OnGiveUp.ContinueWithNext)

    @Test
    fun `retries are counted per reaction and forgotten when it finishes`() =
        runBlocking {
            val queues = ManualQueues()
            val channel = queues.channel("q", ItemSerializer, ordered = false)
            val seen = mutableListOf<Int>()
            var outcome: ReactionOutcome = ReactionOutcome.Retry(1.seconds)
            channel.source.subscribe { _, _, _, retryCount, _ ->
                seen += retryCount
                outcome
            }
            channel.sink.publish(EventReactionId("r"), Item("r"), null, null)

            queues.deliver("q")
            queues.deliver("q")
            outcome = ReactionOutcome.Finished(gaveUp = false)
            queues.deliver("q")

            assertEquals(listOf(0, 1, 2), seen)
            assertEquals(0, queues.retries("q", EventReactionId("r")))
            assertTrue(queues.pending("q").isEmpty())
        }

    @Test
    fun `with ordering enforced, a reaction waits while an earlier one of its key is pending`() =
        runBlocking {
            val queues = ManualQueues(enforceOrdering = true)
            val channel = queues.channel("q", ItemSerializer, ordered = true)
            val ran = mutableListOf<String>()
            channel.source.subscribe { _, _, item, _, _ ->
                ran += item.name
                if (item.name == "a1") ReactionOutcome.Retry(1.seconds) else ReactionOutcome.Finished(gaveUp = false)
            }
            channel.sink.publish(EventReactionId("a2"), Item("a2"), stamp("Order/a", 2), null)
            channel.sink.publish(EventReactionId("a1"), Item("a1"), stamp("Order/a", 1), null)
            channel.sink.publish(EventReactionId("b1"), Item("b1"), stamp("Order/b", 1), null)

            queues.deliver("q")

            assertEquals(listOf("a1", "b1"), ran)
            assertEquals(listOf(EventReactionId("a2"), EventReactionId("a1")), queues.pending("q").map { it.id })
        }
}

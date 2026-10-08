package io.kotmod.event.reaction

import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskOutcome
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class ReactionQueueUnorderedTest {
    private var now = Instant.parse("2026-10-08T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val rows = InMemoryReactionRows { now }
    private val seen = mutableListOf<Delivery<String>>()

    private fun queue() = ReactionQueue("q", String.serializer(), scheduler.queue("q"), rows, 90.seconds) { now }

    private fun ReactionQueue<String>.startWith(handler: (Delivery<String>) -> ItemResult<String>) =
        start { delivery ->
            seen += delivery
            handler(delivery)
        }

    @Test
    fun `published work is delivered with attempt 0 and finishes`(): Unit =
        runBlocking {
            val queue = queue()
            queue.publish(Produced(EventReactionId("r-1"), "hello"))
            queue.startWith { ItemResult.Completed }
            val outcomes = scheduler.queue("q").deliverAll()
            assertEquals(listOf(Delivery(EventReactionId("r-1"), "hello", 0)), seen)
            assertEquals(listOf<TaskOutcome>(TaskOutcome.Done), outcomes)
            assertTrue(scheduler.queue("q").pending.isEmpty())
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `Retry runs it again after the delay with the attempt counted`(): Unit =
        runBlocking {
            val queue = queue()
            queue.publish(Produced(EventReactionId("r-1"), "hello"))
            queue.startWith { if (it.attempt == 0) ItemResult.Retry(5.seconds) else ItemResult.Completed }
            scheduler.queue("q").deliverNext()
            assertEquals(now + 5.seconds, scheduler.queue("q").pending.single().at)
            scheduler.queue("q").deliverAll()
            assertEquals(listOf(0, 1), seen.map { it.attempt })
        }

    @Test
    fun `work delivered before notBefore waits without running and without counting`(): Unit =
        runBlocking {
            val queue = queue()
            queue.publish(Produced(EventReactionId("r-1"), "hello", notBefore = now + 1.hours))
            queue.startWith { ItemResult.Completed }
            val outcome = scheduler.queue("q").deliverNext()
            assertEquals(TaskOutcome.RunAgain(at = now + 1.hours, payload = scheduler.queue("q").pending.single().payload), outcome)
            assertTrue(seen.isEmpty())
            now += 2.hours
            scheduler.queue("q").deliverNext()
            assertEquals(listOf(0), seen.map { it.attempt })
        }

    @Test
    fun `Blocked on unordered work finishes it, since it has no line to block`(): Unit =
        runBlocking {
            val queue = queue()
            queue.publish(Produced(EventReactionId("r-1"), "hello"))
            queue.startWith { ItemResult.Blocked }
            assertEquals(listOf<TaskOutcome>(TaskOutcome.Done), scheduler.queue("q").deliverAll())
            assertEquals(1, seen.size)
            assertTrue(scheduler.queue("q").pending.isEmpty())
        }

    @Test
    fun `publishing the same id while pending queues it once`(): Unit =
        runBlocking {
            val queue = queue()
            queue.publish(Produced(EventReactionId("r-1"), "hello"))
            queue.publish(Produced(EventReactionId("r-1"), "hello"))
            assertEquals(1, scheduler.queue("q").pending.size)
        }

    @Test
    fun `a throwing handler leaves the task for the backend to deliver again`(): Unit =
        runBlocking {
            val queue = queue()
            queue.publish(Produced(EventReactionId("r-1"), "hello"))
            var fail = true
            queue.startWith { if (fail) error("boom") else ItemResult.Completed }
            assertFailsWith<IllegalStateException> { scheduler.queue("q").deliverNext() }
            assertEquals(1, scheduler.queue("q").pending.size)
            fail = false
            scheduler.queue("q").deliverNext()
            assertEquals(listOf(0, 0), seen.map { it.attempt })
        }

    @Test
    fun `a stopped queue receives nothing`(): Unit =
        runBlocking {
            val queue = queue()
            queue.publish(Produced(EventReactionId("r-1"), "hello"))
            queue.startWith { ItemResult.Completed }
            queue.stop()
            val error = assertFailsWith<IllegalStateException> { scheduler.queue("q").deliverNext() }
            assertTrue(error.message.orEmpty().contains("Nothing subscribed"))
        }
}

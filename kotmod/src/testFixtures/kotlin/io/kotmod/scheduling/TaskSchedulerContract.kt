package io.kotmod.scheduling

import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * What every [TaskScheduler] backend must do. A backend's test extends this: [scheduler] returns the backend under
 * test (the same instance for the whole test); queues named [QUEUE_A] and [QUEUE_B] are created with it before
 * [start], which starts delivery; [stop] stops it.
 */
abstract class TaskSchedulerContract {
    protected abstract fun scheduler(): TaskScheduler

    protected abstract fun start()

    protected abstract fun stop()

    @AfterTest
    fun stopScheduler() = stop()

    private fun recording(queue: TaskQueue): MutableList<Pair<String, String>> {
        val seen = CopyOnWriteArrayList<Pair<String, String>>()
        queue.subscribe { name, payload ->
            seen += name to payload
            TaskOutcome.Done
        }
        return seen
    }

    @Test
    fun `a scheduled task is delivered once with its payload`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = recording(queue)
            queue.schedule("t-1", "payload-1", Clock.System.now())
            start()
            eventually { seen.isNotEmpty() }
            delay(1.seconds)
            assertEquals(listOf("t-1" to "payload-1"), seen)
        }

    @Test
    fun `scheduling a pending name again does nothing`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = recording(queue)
            val at = Clock.System.now() + 2.seconds
            queue.schedule("t-1", "first", at)
            queue.schedule("t-1", "second", at)
            start()
            eventually { seen.isNotEmpty() }
            delay(1.seconds)
            assertEquals(listOf("t-1" to "first"), seen)
        }

    @Test
    fun `a name can be scheduled again after its task finished`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = recording(queue)
            queue.schedule("t-1", "first", Clock.System.now())
            start()
            eventually { seen.isNotEmpty() }
            // kotmod's operator tools and repair sweep schedule a finished line front again under its usual name. The
            // backend may still hold the first task for a moment after the handler returns, and scheduling a pending
            // name does nothing, so schedule until the second delivery arrives.
            withTimeout(10.seconds) {
                while (seen.size < 2) {
                    queue.schedule("t-1", "second", Clock.System.now())
                    delay(200.milliseconds)
                }
            }
            delay(1.seconds)
            assertEquals(listOf("t-1" to "first", "t-1" to "second"), seen)
        }

    @Test
    fun `a task is not delivered before its time`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = recording(queue)
            val at = Clock.System.now() + 3.seconds
            queue.schedule("t-1", "later", at)
            start()
            delay(1.seconds)
            assertTrue(seen.isEmpty())
            eventually { seen.isNotEmpty() }
            assertTrue(Clock.System.now() >= at)
        }

    @Test
    fun `RunAgain delivers the task again later with the new payload`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = CopyOnWriteArrayList<String>()
            queue.subscribe { _, payload ->
                seen += payload
                if (payload == "first") TaskOutcome.RunAgain(Clock.System.now() + 1.seconds, "second") else TaskOutcome.Done
            }
            queue.schedule("t-1", "first", Clock.System.now())
            start()
            eventually { seen.size == 2 }
            assertEquals(listOf("first", "second"), seen)
        }

    @Test
    fun `a task whose handler throws is delivered again`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val attempts = CopyOnWriteArrayList<String>()
            queue.subscribe { name, _ ->
                attempts += name
                if (attempts.size == 1) error("transient") else TaskOutcome.Done
            }
            queue.schedule("t-1", "p", Clock.System.now())
            start()
            eventually(timeout = 60.seconds) { attempts.size == 2 }
        }

    @Test
    fun `queues are separate, and asking for a queue again reaches the same tasks`() =
        runBlocking<Unit> {
            val a = scheduler().queue(QUEUE_A)
            val b = scheduler().queue(QUEUE_B)
            val seenA = recording(a)
            val seenB = recording(b)
            scheduler().queue(QUEUE_A).schedule("t-1", "for-a", Clock.System.now())
            start()
            eventually { seenA.isNotEmpty() }
            delay(1.seconds)
            assertEquals(listOf("t-1" to "for-a"), seenA)
            assertTrue(seenB.isEmpty())
        }

    @Test
    fun `nothing is delivered to a cancelled subscription`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = CopyOnWriteArrayList<String>()
            val subscription =
                queue.subscribe { name, _ ->
                    seen += name
                    TaskOutcome.Done
                }
            subscription.cancel()
            queue.schedule("t-1", "p", Clock.System.now())
            start()
            delay(2.seconds)
            assertTrue(seen.isEmpty())
        }

    companion object {
        const val QUEUE_A = "contract-a"
        const val QUEUE_B = "contract-b"
    }
}

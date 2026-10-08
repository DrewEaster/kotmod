package io.kotmod.scheduling

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Instant

class ManualTaskSchedulerTest {
    private val t0 = Instant.parse("2026-10-08T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val queue = scheduler.queue("q")

    @Test
    fun `schedules once per pending name and delivers in schedule order`() =
        runBlocking<Unit> {
            val seen = mutableListOf<String>()
            queue.subscribe { name, payload ->
                seen += "$name:$payload"
                TaskOutcome.Done
            }
            queue.schedule("b", "1", t0)
            queue.schedule("a", "2", t0)
            queue.schedule("b", "3", t0)

            queue.deliverAll()

            assertEquals(listOf("b:1", "a:2"), seen)
            assertTrue(queue.pending.isEmpty())
        }

    @Test
    fun `RunAgain moves the task to the back with its new payload and time`() =
        runBlocking<Unit> {
            var first = true
            queue.subscribe { _, payload ->
                if (first) {
                    first = false
                    TaskOutcome.RunAgain(t0, "again:$payload")
                } else {
                    TaskOutcome.Done
                }
            }
            queue.schedule("a", "1", t0)
            queue.schedule("b", "2", t0)

            queue.deliverNext()

            assertEquals(listOf("b", "a"), queue.pending.map { it.name })
            assertEquals("again:1", queue.pending.last().payload)
        }

    @Test
    fun `a task being delivered is still pending, so scheduling its name again is ignored`() =
        runBlocking<Unit> {
            queue.subscribe { name, _ ->
                queue.schedule(name, "duplicate", t0)
                TaskOutcome.Done
            }
            queue.schedule("a", "1", t0)

            queue.deliverNext()

            assertTrue(queue.pending.isEmpty())
        }

    @Test
    fun `a throwing handler leaves the task pending and rethrows`() =
        runBlocking<Unit> {
            queue.subscribe { _, _ -> error("boom") }
            queue.schedule("a", "1", t0)

            assertFailsWith<IllegalStateException> { queue.deliverNext() }
            assertEquals(listOf("a"), queue.pending.map { it.name })
        }

    @Test
    fun `failNextSchedules makes schedule throw without recording the task`() =
        runBlocking<Unit> {
            queue.failNextSchedules = 1
            assertFailsWith<IllegalStateException> { queue.schedule("a", "1", t0) }
            queue.schedule("b", "1", t0)
            assertEquals(listOf("b"), queue.pending.map { it.name })
        }

    @Test
    fun `deliverDuplicate runs a pending task without taking it, and lose drops one`() =
        runBlocking<Unit> {
            val release = CompletableDeferred<Unit>()
            var calls = 0
            queue.subscribe { _, _ ->
                calls++
                release.await()
                TaskOutcome.Done
            }
            queue.schedule("a", "1", t0)
            val firstDelivery = async { queue.deliverNext() }
            val duplicate = async { queue.deliverDuplicate("a") }
            while (calls < 2) yield()
            release.complete(Unit)
            firstDelivery.await()
            duplicate.await()
            assertEquals(2, calls)

            queue.schedule("b", "1", t0)
            queue.lose("b")
            assertNull(queue.deliverNext())
        }

    @Test
    fun `delivering with nobody subscribed fails and leaves the task deliverable`() =
        runBlocking<Unit> {
            queue.schedule("a", "1", t0)
            assertFailsWith<IllegalStateException> { queue.deliverNext() }

            val seen = mutableListOf<String>()
            queue.subscribe { name, _ ->
                seen += name
                TaskOutcome.Done
            }
            queue.deliverNext()
            assertEquals(listOf("a"), seen)
        }
}

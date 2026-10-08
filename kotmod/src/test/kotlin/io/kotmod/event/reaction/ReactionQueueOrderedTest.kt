package io.kotmod.event.reaction

import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskOutcome
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class ReactionQueueOrderedTest {
    private val key = "Order/o-1"
    private var now = Instant.parse("2026-10-08T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val tasks = scheduler.queue("q")
    private val rows = InMemoryReactionRows { now }
    private val seen = mutableListOf<Delivery<String>>()

    /** Results to return, per item id, in turn; an item with none left completes. */
    private val results = mutableMapOf<String, ArrayDeque<ItemResult<String>>>()

    /** Behaviour that overrides [results] for an item id (suspending, throwing). */
    private val behaviour = mutableMapOf<String, suspend (Delivery<String>) -> ItemResult<String>>()

    private fun queue() = ReactionQueue("q", String.serializer(), tasks, rows, 90.seconds) { now }

    private fun item(
        id: String,
        seq: Long,
        ord: Int = 0,
        key: String = this.key,
    ) = OrderedItem(EventReactionId(id), key, seq, ord, id)

    private fun front(id: String) = ReactionTasks.frontName(key, id)

    private fun results(
        id: String,
        vararg items: ItemResult<String>,
    ) {
        results[id] = ArrayDeque(items.toList())
    }

    private fun handled() = seen.map { it.id.value }

    private fun row(id: String) = rows.rows("q").single { it.reactionId == id }

    private fun ReactionQueue<String>.started() =
        also {
            it.start { delivery ->
                seen += delivery
                val custom = behaviour[delivery.id.value]
                custom?.invoke(delivery) ?: (results[delivery.id.value]?.removeFirstOrNull() ?: ItemResult.Completed)
            }
        }

    @Test
    fun `a line runs one item at a time in sequence and ordinal order, scheduling only its front`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e6/0", 6), item("e5/1", 5, 1), item("e5/0", 5, 0)))
            assertEquals(listOf(front("e5/0")), tasks.pending.map { it.name })
            tasks.deliverAll()
            assertEquals(listOf("e5/0", "e5/1", "e6/0"), handled())
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `work added later with a lower place runs first`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e6", 6)))
            queue.publishOrdered(listOf(item("e5", 5)))
            assertEquals(listOf(front("e6"), front("e5")), tasks.pending.map { it.name })
            tasks.deliverAll()
            assertEquals(listOf("e5", "e6"), handled())
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `a replaced item's replacements take its place, ahead of later work, on a first-in-first-out scheduler`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("m5", 5), item("e6", 6)))
            results("m5", ItemResult.Replaced(listOf(Produced(EventReactionId("t5a"), "t5a"), Produced(EventReactionId("t5b"), "t5b"))))
            results("t5a", ItemResult.Retry(1.minutes), ItemResult.Completed)

            tasks.deliverNext()
            assertEquals(
                listOf(Triple("t5a", 5L, 0), Triple("t5b", 5L, 1), Triple("e6", 6L, 0)),
                rows.rows("q").map { Triple(it.reactionId, it.sequence, it.ordinal) },
            )
            assertEquals(listOf(front("t5a")), tasks.pending.map { it.name })

            // t5a retrying holds back t5b and e6: the replacements are in line, not queued beside it.
            tasks.deliverAll()
            assertEquals(listOf("m5", "t5a", "t5a", "t5b", "e6"), handled())
            assertEquals(listOf("m5", "t5a", "t5a", "t5b", "e6"), seen.map { it.item })
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `lines don't wait for each other`(): Unit =
        runBlocking {
            val queue = queue().started()
            behaviour["a1"] = { ItemResult.Retry(1.minutes) }
            queue.publishOrdered(listOf(item("a1", 1, key = "Order/A"), item("b1", 1, key = "Order/B")))
            repeat(3) { tasks.deliverNext() }
            assertEquals(listOf("a1", "b1", "a1"), handled())
            assertEquals(listOf(ReactionTasks.frontName("Order/A", "a1")), tasks.pending.map { it.name })
            assertEquals(listOf("a1"), rows.rows("q").map { it.reactionId })
        }

    @Test
    fun `Retry runs the same item again later, counting the attempt at the start`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5)))
            results("e5", ItemResult.Retry(10.seconds), ItemResult.Retry(20.seconds), ItemResult.Completed)

            tasks.deliverNext()
            assertEquals(now + 10.seconds, tasks.pending.single().at)
            assertEquals(1, row("e5").attempts)
            assertNull(row("e5").leaseUntil)

            tasks.deliverNext()
            assertEquals(now + 20.seconds, tasks.pending.single().at)
            assertEquals(2, row("e5").attempts)
            assertNull(row("e5").leaseUntil)

            tasks.deliverNext()
            assertEquals(listOf(0, 1, 2), seen.map { it.attempt })
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `Blocked holds back the line and schedules nothing`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5), item("e6", 6)))
            results("e5", ItemResult.Blocked)
            tasks.deliverAll()
            assertEquals(listOf("e5"), handled())
            assertTrue(tasks.pending.isEmpty())
            assertTrue(row("e5").blocked)
            assertNull(row("e5").leaseUntil)
            assertEquals(listOf("e5", "e6"), rows.rows("q").map { it.reactionId })
        }

    @Test
    fun `a delivered task for an item that isn't the front schedules the real front without running anything`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5), item("e6", 6)))
            tasks.schedule(front("e6"), ReactionTasks.encode(TaskPayload.Front(key, "e6")), now)
            tasks.lose(front("e5"))
            assertEquals(listOf(front("e6")), tasks.pending.map { it.name })
            assertEquals(TaskOutcome.Done, tasks.deliver(front("e6")))
            assertTrue(seen.isEmpty())
            assertEquals(listOf(front("e5")), tasks.pending.map { it.name })
            assertEquals(0, row("e5").attempts)
        }

    @Test
    fun `a duplicate delivery while the item runs waits for the lease`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5)))
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            behaviour["e5"] = {
                started.complete(Unit)
                release.await()
                ItemResult.Completed
            }
            val first = async { tasks.deliverNext() }
            started.await()

            val duplicate = tasks.deliverDuplicate(front("e5"))
            assertEquals(TaskOutcome.RunAgain(now + 90.seconds, ReactionTasks.encode(TaskPayload.Front(key, "e5"))), duplicate)
            assertEquals(1, seen.size)

            release.complete(Unit)
            assertEquals(TaskOutcome.Done, first.await())
            assertEquals(listOf("e5"), handled())
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `a crash after finishing an item but before scheduling the next is repaired by the redelivery`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5), item("e6", 6)))
            tasks.failNextSchedules = 1
            assertFailsWith<IllegalStateException> { tasks.deliverNext() }
            assertEquals(listOf("e6"), rows.rows("q").map { it.reactionId })
            assertEquals(listOf(front("e5")), tasks.pending.map { it.name })

            tasks.deliverAll()
            assertEquals(listOf("e5", "e6"), handled())
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `a crash mid-run counts the attempt, and the item runs again once its lease ends`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5)))
            behaviour["e5"] = { error("database down") }
            assertFailsWith<IllegalStateException> { tasks.deliverNext() }
            assertEquals(1, row("e5").attempts)
            assertEquals(now + 90.seconds, row("e5").leaseUntil)

            behaviour.remove("e5")
            assertEquals(TaskOutcome.RunAgain(now + 90.seconds, ReactionTasks.encode(TaskPayload.Front(key, "e5"))), tasks.deliverNext())
            assertEquals(1, seen.size)

            now += 2.minutes
            assertEquals(TaskOutcome.Done, tasks.deliverNext())
            assertEquals(listOf(0, 1), seen.map { it.attempt })
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `an attempt whose lease ran out and was overtaken by a later attempt leaves the row to that attempt`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5), item("e6", 6)))
            val started = List(2) { CompletableDeferred<Unit>() }
            val release = List(2) { CompletableDeferred<Unit>() }
            behaviour["e5"] = { delivery ->
                started[delivery.attempt].complete(Unit)
                release[delivery.attempt].await()
                ItemResult.Completed
            }

            // Attempt 1 starts, then hangs past its lease; a duplicate delivery starts attempt 2.
            val first = async { tasks.deliverNext() }
            started[0].await()
            now += 2.minutes
            val second = async { tasks.deliverDuplicate(front("e5")) }
            started[1].await()
            assertEquals(2, row("e5").attempts)

            // Attempt 1 finishes late: the row belongs to attempt 2, so it is left alone and the line doesn't move.
            release[0].complete(Unit)
            assertEquals(TaskOutcome.Done, first.await())
            assertEquals(listOf("e5", "e6"), rows.rows("q").map { it.reactionId })
            assertEquals(2, row("e5").attempts)
            assertTrue(tasks.pending.isEmpty())

            // Attempt 2's completion advances the line.
            release[1].complete(Unit)
            assertEquals(TaskOutcome.Done, second.await())
            assertEquals(listOf("e6"), rows.rows("q").map { it.reactionId })
            assertEquals(listOf(front("e6")), tasks.pending.map { it.name })
            tasks.deliverAll()
            assertEquals(listOf("e5", "e5", "e6"), handled())
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `a shutdown mid-run gives the attempt back`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5)))
            val started = CompletableDeferred<Unit>()
            behaviour["e5"] = {
                started.complete(Unit)
                awaitCancellation()
            }
            val job = launch { tasks.deliverNext() }
            started.await()
            job.cancelAndJoin()
            assertEquals(0, row("e5").attempts)
            assertNull(row("e5").leaseUntil)

            behaviour.remove("e5")
            assertEquals(TaskOutcome.Done, tasks.deliverNext())
            assertEquals(listOf(0, 0), seen.map { it.attempt })
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `the reactor adding work while the front finishes never strands the line (front finishes first)`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5)))
            val scheduled = mutableListOf<String>()
            tasks.beforeSchedule = { scheduled += it }

            // e5 finishes first: its locked step deletes it and finds no front, so it schedules nothing.
            assertEquals(TaskOutcome.Done, tasks.deliverNext())
            assertEquals(emptyList(), scheduled)
            assertTrue(tasks.pending.isEmpty())

            // The reactor then adds e6: its locked step sees e6 as the front and schedules it.
            queue.publishOrdered(listOf(item("e6", 6)))
            assertEquals(listOf(front("e6")), scheduled)

            tasks.deliverAll()
            assertEquals(listOf("e5", "e6"), handled())
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `the reactor adding work while the front finishes never strands the line (reactor first)`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5)))
            val paused = CompletableDeferred<Unit>()
            val gate = CompletableDeferred<Unit>()
            var armed = true
            tasks.beforeSchedule = { name ->
                if (armed && name == front("e5")) {
                    armed = false
                    paused.complete(Unit)
                    gate.await()
                }
            }

            // The reactor's locked step adds e6 and sees e5 as the front, then pauses before scheduling it.
            val reactor = async { queue.publishOrdered(listOf(item("e6", 6))) }
            paused.await()
            assertEquals(listOf("e5", "e6"), rows.rows("q").map { it.reactionId })

            // Meanwhile e5 runs and finishes; its locked step sees e6 and schedules it.
            assertEquals(TaskOutcome.Done, tasks.deliverNext())
            assertEquals(listOf(front("e6")), tasks.pending.map { it.name })

            // The reactor resumes and schedules e5's name again: its task is gone, so a stale one is added.
            gate.complete(Unit)
            reactor.await()
            assertEquals(listOf(front("e6"), front("e5")), tasks.pending.map { it.name })

            tasks.deliverAll()
            assertEquals(listOf("e5", "e6"), handled())
            assertTrue(tasks.pending.isEmpty())
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `the reactor adding work while the front runs never strands the line`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("e5", 5)))
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            behaviour["e5"] = {
                started.complete(Unit)
                release.await()
                ItemResult.Completed
            }
            val scheduled = mutableListOf<String>()
            tasks.beforeSchedule = { scheduled += it }
            val delivery = async { tasks.deliverNext() }
            started.await()

            // The reactor adds e6 while e5 runs: it sees e5 as the front, and its schedule of e5's name is ignored
            // because e5's task is still pending.
            queue.publishOrdered(listOf(item("e6", 6)))
            assertEquals(listOf(front("e5")), scheduled)
            assertEquals(listOf(front("e5")), tasks.pending.map { it.name })

            // e5 finishes: its locked step must find e6 and schedule it, or nothing ever would.
            release.complete(Unit)
            assertEquals(TaskOutcome.Done, delivery.await())
            assertEquals(listOf(front("e6")), tasks.pending.map { it.name })

            tasks.deliverAll()
            assertEquals(listOf("e5", "e6"), handled())
            assertTrue(tasks.pending.isEmpty())
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `a line's front task still runs after its policy stops being ordered`(): Unit =
        runBlocking {
            // Guards against a queue only handling front tasks while its policy is ordered, stranding rows left behind.
            queue().publishOrdered(listOf(item("e5", 5)))
            assertEquals(listOf(front("e5")), tasks.pending.map { it.name })

            // Redeployed: a new queue on the same tasks and rows now publishes only unordered work.
            val redeployed = queue().started()
            redeployed.publish(Produced(EventReactionId("u1"), "u1"))
            tasks.deliverAll()
            assertEquals(listOf("e5", "u1"), handled())
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `a replaced item whose row vanished while it ran inserts no replacements, and the line moves on`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(item("m5", 5), item("e6", 6)))
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            behaviour["m5"] = {
                started.complete(Unit)
                release.await()
                ItemResult.Replaced(listOf(Produced(EventReactionId("t5a"), "t5a")))
            }
            val delivery = async { tasks.deliverNext() }
            started.await()

            // An operator skips m5 while it runs.
            rows.inLine("q", key) { delete("m5") }
            release.complete(Unit)

            assertEquals(TaskOutcome.Done, delivery.await())
            assertEquals(listOf("e6"), rows.rows("q").map { it.reactionId })
            assertEquals(listOf(front("e6")), tasks.pending.map { it.name })
            tasks.deliverAll()
            assertEquals(listOf("m5", "e6"), handled())
            assertTrue(rows.rows("q").isEmpty())
        }
}

package io.kotmod.event.reaction

import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskOutcome
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class ReactionQueueKeptAndSweepTest {
    private val key = "Order/o-1"
    private var now = Instant.parse("2026-10-08T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val tasks = scheduler.queue("q")
    private val rows = InMemoryReactionRows { now }
    private val seen = mutableListOf<Delivery<String>>()
    private val results = ArrayDeque<ItemResult<String>>()

    private fun queue() = ReactionQueue("q", String.serializer(), tasks, rows, 90.seconds) { now }

    private fun ReactionQueue<String>.started() =
        also {
            it.start { delivery ->
                seen += delivery
                results.removeFirstOrNull() ?: ItemResult.Completed
            }
        }

    private fun orderedRow(
        id: String,
        line: String,
        seq: Long,
    ) = ReactionRow("q", id, RowKind.ORDERED, line, seq, 0, id)

    @Test
    fun `a kept item is stored and scheduled, and finishing it deletes it`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.keep(EventReactionId("k1"), "x")
            assertEquals(listOf(RowKind.KEPT), rows.rows("q").map { it.kind })
            assertEquals(listOf("k1"), tasks.pending.map { it.name })
            val outcome = tasks.deliver("k1")
            assertEquals(TaskOutcome.Done, outcome)
            assertEquals(listOf(Delivery(EventReactionId("k1"), "x", 0)), seen)
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `Blocked on a kept item finishes and deletes it, since it has no line to block`(): Unit =
        runBlocking {
            val queue = queue().started()
            results += ItemResult.Blocked
            queue.keep(EventReactionId("k1"), "x")
            assertEquals(TaskOutcome.Done, tasks.deliver("k1"))
            assertEquals(1, seen.size)
            assertTrue(rows.rows("q").isEmpty())
            assertTrue(tasks.pending.isEmpty())
        }

    @Test
    fun `a kept item's failures are counted in its row`(): Unit =
        runBlocking {
            val queue = queue().started()
            results += listOf(ItemResult.Retry(5.seconds), ItemResult.Retry(5.seconds), ItemResult.Completed)
            queue.keep(EventReactionId("k1"), "x")
            tasks.deliverAll()
            assertEquals(listOf(0, 1, 2), seen.map { it.attempt })
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `a replaced kept item queues its replacements as unordered work, then disappears`(): Unit =
        runBlocking {
            val queue = queue().started()
            results +=
                ItemResult.Replaced(
                    listOf(
                        Produced(EventReactionId("t1"), "a"),
                        Produced(EventReactionId("t2"), "b", notBefore = now + 1.hours),
                    ),
                )
            queue.keep(EventReactionId("k1"), "x")
            tasks.deliverNext()
            assertTrue(rows.rows("q").isEmpty())
            val pending = tasks.pending.associateBy { it.name }
            assertEquals(setOf("t1", "t2"), pending.keys)
            assertEquals(
                TaskPayload.Unordered("t1", "\"a\"", 0, null),
                ReactionTasks.decode(pending.getValue("t1").payload),
            )
            assertEquals(
                TaskPayload.Unordered("t2", "\"b\"", 0, (now + 1.hours).toString()),
                ReactionTasks.decode(pending.getValue("t2").payload),
            )
            assertEquals(now + 1.hours, pending.getValue("t2").at)
        }

    @Test
    fun `a kept task whose row is gone finishes without running`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.keep(EventReactionId("k1"), "x")
            rows.inQueue("q") { delete("k1") }
            assertEquals(TaskOutcome.Done, tasks.deliver("k1"))
            assertTrue(seen.isEmpty())
        }

    @Test
    fun `the sweep reschedules a lost front and a lost kept task`(): Unit =
        runBlocking {
            val queue = queue().started()
            queue.publishOrdered(listOf(OrderedItem(EventReactionId("e5"), key, 5, 0, "e5")))
            queue.keep(EventReactionId("k1"), "x")
            tasks.lose(ReactionTasks.frontName(key, "e5"))
            tasks.lose("k1")
            assertTrue(tasks.pending.isEmpty())
            now += 31.minutes
            queue.sweep(30.minutes)
            assertEquals(setOf(ReactionTasks.frontName(key, "e5"), "k1"), tasks.pending.map { it.name }.toSet())
            tasks.deliverAll()
            assertEquals(setOf("e5", "k1"), seen.map { it.id.value }.toSet())
            assertEquals(2, seen.size)
            assertTrue(rows.rows("q").isEmpty())
        }

    @Test
    fun `the sweep leaves blocked, leased, recent and non-front rows alone`(): Unit =
        runBlocking {
            val queue = queue().started()
            // All of these are old by the time of the sweep, except line C's front.
            rows.inLine("q", "A") { insert(orderedRow("a1", "A", 1)) }
            rows.inLine("q", "A") { update(front()!!.copy(blocked = true)) }
            rows.inLine("q", "B") { insert(orderedRow("b1", "B", 1)) }
            rows.inLine("q", "B") { update(front()!!.copy(leaseUntil = now + 10.minutes + 1.hours)) }
            rows.inLine("q", "D") {
                insert(orderedRow("d1", "D", 1))
                insert(orderedRow("d2", "D", 2))
            }
            now += 1.hours
            rows.inLine("q", "C") { insert(orderedRow("c1", "C", 1)) }
            now += 1.minutes
            queue.sweep(30.minutes)
            assertEquals(listOf(ReactionTasks.frontName("D", "d1")), tasks.pending.map { it.name })
        }
}

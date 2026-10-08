package io.kotmod.postgres

import io.kotmod.event.reaction.ReactionRow
import io.kotmod.event.reaction.RowKind
import io.kotmod.postgres.support.IntegrationTest
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class PostgresReactionRowsIntegrationTest : IntegrationTest() {
    private val now = Instant.parse("2026-10-08T10:00:00Z")
    private val rows = PostgresReactionRows(jdbc) { now }

    private fun ordered(
        id: String,
        key: String = "Order/o-1",
        seq: Long,
        ord: Int = 0,
        queue: String = "q",
    ) = ReactionRow(queue, id, RowKind.ORDERED, key, seq, ord, """{"n":"$id"}""")

    private fun kept(id: String) = ReactionRow("q", id, RowKind.KEPT, null, null, null, """{"n":"$id"}""")

    @Test
    fun `front is the lowest sequence then ordinal, whatever the insert order`() {
        rows.inLine("q", "Order/o-1") {
            insert(ordered("c", seq = 6, ord = 0))
            insert(ordered("b", seq = 5, ord = 1))
            insert(ordered("a", seq = 5, ord = 0))
        }
        assertEquals("a", rows.inLine("q", "Order/o-1") { front() }?.reactionId)
    }

    @Test
    fun `insert ignores a taken reaction id or a taken place in line`() {
        rows.inLine("q", "Order/o-1") {
            assertTrue(insert(ordered("a", seq = 5)))
            assertFalse(insert(ordered("a", seq = 9)))
            assertFalse(insert(ordered("b", seq = 5)))
        }
        assertEquals(listOf("a"), rows.list("q").map { it.reactionId })
    }

    @Test
    fun `kept rows have no line and never conflict on place`() {
        rows.inQueue("q") {
            assertTrue(insert(kept("k1")))
            assertTrue(insert(kept("k2")))
        }
    }

    @Test
    fun `update saves attempts, blocked and lease, and delete removes`() {
        val lease = now + 1.minutes
        rows.inQueue("q") { insert(ordered("a", seq = 1)) }
        rows.inQueue("q") {
            update(checkNotNull(get("a")).copy(attempts = 3, blocked = true, leaseUntil = lease))
        }
        val saved = checkNotNull(rows.inQueue("q") { get("a") })
        assertEquals(3, saved.attempts)
        assertTrue(saved.blocked)
        assertEquals(lease, saved.leaseUntil)
        rows.inQueue("q") { delete("a") }
        assertNull(rows.inQueue("q") { get("a") })
    }

    @Test
    fun `lines and queues are separate`() {
        rows.inLine("q", "Order/o-1") { insert(ordered("a", seq = 5)) }
        rows.inLine("q2", "Order/o-1") { insert(ordered("z", seq = 1, queue = "q2")) }
        rows.inLine("q", "Order/o-2") { insert(ordered("y", key = "Order/o-2", seq = 1)) }
        assertEquals("a", rows.inLine("q", "Order/o-1") { front() }?.reactionId)
        assertEquals("z", rows.inLine("q2", "Order/o-1") { front() }?.reactionId)
    }

    @Test
    fun `stale returns unblocked, unleased fronts and kept rows changed before the cutoff`() {
        rows.inQueue("q") {
            insert(ordered("a1", key = "A", seq = 1))
            insert(ordered("a2", key = "A", seq = 2))
            insert(ordered("b1", key = "B", seq = 1).copy(blocked = true))
            insert(ordered("c1", key = "C", seq = 1).copy(leaseUntil = now + 1.minutes))
            insert(kept("k"))
        }
        val stale = rows.stale("q", now, now + 1.seconds).map { it.reactionId }
        assertEquals(setOf("a1", "k"), stale.toSet())
        assertEquals(2, stale.size)
        assertTrue(rows.stale("q", now, now - 1.seconds).isEmpty())
    }

    @Test
    fun `the line lock serialises two transactions on the same line`() {
        val entered = CountDownLatch(1)
        val first =
            thread {
                rows.inLine("q", "k") {
                    entered.countDown()
                    Thread.sleep(500)
                    insert(ordered("x", key = "k", seq = 1))
                }
            }
        assertTrue(entered.await(5, TimeUnit.SECONDS))

        var seen: String? = null
        var tookMs = 0L
        val second =
            thread {
                val start = System.nanoTime()
                seen = rows.inLine("q", "k") { front() }?.reactionId
                tookMs = (System.nanoTime() - start) / 1_000_000
            }
        val otherStart = System.nanoTime()
        rows.inLine("q", "k2") { front() }
        val otherMs = (System.nanoTime() - otherStart) / 1_000_000

        first.join()
        second.join()
        assertEquals("x", seen)
        assertTrue(tookMs >= 400, "second transaction waited only $tookMs ms")
        assertTrue(otherMs < 300, "other line took $otherMs ms")
    }

    @Test
    fun `a 300-character aggregate id works as a line key`() {
        val key = "Order/" + "x".repeat(300)
        rows.inLine("q", key) { insert(ordered("a", key = key, seq = 1)) }
        assertEquals("a", rows.inLine("q", key) { front() }?.reactionId)
    }
}

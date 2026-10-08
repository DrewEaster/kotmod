package io.kotmod.event.reaction

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Instant

/**
 * In-memory [ReactionRows] for unit tests. One lock guards every call (stronger than per-line locks), and a block that
 * throws leaves the rows untouched. [clock] stamps each row's update time for [stale].
 */
internal class InMemoryReactionRows(
    private val clock: () -> Instant,
) : ReactionRows {
    private class Stored(
        val row: ReactionRow,
        val updatedAt: Instant,
    )

    private val lock = ReentrantLock()
    private var stored: List<Stored> = emptyList()

    fun rows(queue: String): List<ReactionRow> = lock.withLock { ordered(stored.filter { it.row.queue == queue }).map { it.row } }

    override fun <R> inLine(
        queue: String,
        key: String,
        block: LineTx.() -> R,
    ): R = transact { Tx(queue, key, it).block() }

    override fun <R> inQueue(
        queue: String,
        block: RowTx.() -> R,
    ): R = transact { Tx(queue, null, it).block() }

    override fun list(queue: String): List<ReactionRow> = rows(queue)

    override fun stale(
        queue: String,
        now: Instant,
        before: Instant,
    ): List<ReactionRow> =
        lock.withLock {
            val inQueue = stored.filter { it.row.queue == queue }
            ordered(
                inQueue.filter { s ->
                    val r = s.row
                    !r.blocked &&
                        (r.leaseUntil == null || r.leaseUntil <= now) &&
                        s.updatedAt < before &&
                        (r.kind == RowKind.KEPT || inQueue.none { e -> e.row.key == r.key && earlier(e.row, r) })
                },
            ).map { it.row }
        }

    private fun earlier(
        a: ReactionRow,
        b: ReactionRow,
    ): Boolean {
        val seq = (a.sequence ?: 0L).compareTo(b.sequence ?: 0L)
        return seq < 0 || (seq == 0 && (a.ordinal ?: 0) < (b.ordinal ?: 0))
    }

    private fun ordered(list: List<Stored>): List<Stored> =
        list.sortedWith(
            compareBy<Stored, String?>(nullsLast()) { it.row.key }
                .thenBy { it.row.sequence ?: 0L }
                .thenBy { it.row.ordinal ?: 0 }
                .thenBy { it.row.reactionId },
        )

    private fun <R> transact(block: (Work) -> R): R =
        lock.withLock {
            val work = Work(stored.toMutableList())
            val result = block(work)
            stored = work.list
            result
        }

    private class Work(
        val list: MutableList<Stored>,
    )

    private inner class Tx(
        private val queue: String,
        private val key: String?,
        private val work: Work,
    ) : LineTx {
        override fun get(reactionId: String): ReactionRow? = find(reactionId)?.row

        private fun find(reactionId: String) = work.list.firstOrNull { it.row.queue == queue && it.row.reactionId == reactionId }

        override fun insert(row: ReactionRow): Boolean {
            val taken =
                work.list.any { s ->
                    s.row.queue == queue &&
                        (
                            s.row.reactionId == row.reactionId ||
                                (
                                    row.kind == RowKind.ORDERED && s.row.key == row.key &&
                                        s.row.sequence == row.sequence && s.row.ordinal == row.ordinal
                                )
                        )
                }
            if (taken) return false
            work.list.add(Stored(row.copy(queue = queue), clock()))
            return true
        }

        override fun delete(reactionId: String) {
            work.list.removeAll { it.row.queue == queue && it.row.reactionId == reactionId }
        }

        override fun update(row: ReactionRow) {
            val existing = find(row.reactionId) ?: return
            val updated = existing.row.copy(attempts = row.attempts, blocked = row.blocked, leaseUntil = row.leaseUntil)
            work.list[work.list.indexOf(existing)] = Stored(updated, clock())
        }

        override fun front(): ReactionRow? =
            work.list
                .filter { it.row.queue == queue && it.row.key == key }
                .minWithOrNull(compareBy({ it.row.sequence }, { it.row.ordinal }))
                ?.row
    }
}

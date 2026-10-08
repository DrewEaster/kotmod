package io.kotmod.event.reaction

import kotlin.time.Instant

/** An ordered item in its aggregate's line, or a kept item (an unordered parked mapping) outside any line. */
internal enum class RowKind { ORDERED, KEPT }

/**
 * One row of `ddd_reaction_row`. Ordered rows have a [key] (the line: `<aggregateType>/<aggregateId>`) and a place in
 * it ([sequence], [ordinal]); kept rows have none. [item] is the queue's own JSON. [attempts] counts attempts started
 * (ordered) or failed (kept).
 */
internal data class ReactionRow(
    val queue: String,
    val reactionId: String,
    val kind: RowKind,
    val key: String?,
    val sequence: Long?,
    val ordinal: Int?,
    val item: String,
    val attempts: Int = 0,
    val blocked: Boolean = false,
    val leaseUntil: Instant? = null,
)

/** Reads and writes one queue's rows inside a transaction. */
internal interface RowTx {
    fun get(reactionId: String): ReactionRow?

    /** Inserts [row] unless its reaction id, or (ordered) its place in line, is taken; returns whether it did. */
    fun insert(row: ReactionRow): Boolean

    fun delete(reactionId: String)

    /** Saves [row]'s attempts, blocked flag and lease, and touches its update time. */
    fun update(row: ReactionRow)
}

/** A transaction holding one line's lock. */
internal interface LineTx : RowTx {
    /** The line's first row by (sequence, ordinal), blocked or not; `null` if the line is empty. */
    fun front(): ReactionRow?
}

/**
 * Where kotmod keeps ordered work and kept items. Calls block the thread (JDBC).
 *
 * Don't call it from inside another transaction: a line's lock is held until the transaction ends.
 */
internal interface ReactionRows {
    /** Runs [block] in one transaction that holds the lock on line ([queue], [key]). */
    fun <R> inLine(
        queue: String,
        key: String,
        block: LineTx.() -> R,
    ): R

    /** Runs [block] in one transaction, without a line lock (for kept rows). */
    fun <R> inQueue(
        queue: String,
        block: RowTx.() -> R,
    ): R

    /** Every row of [queue], ordered by line and place in line (kept rows last, by reaction id). */
    fun list(queue: String): List<ReactionRow>

    /**
     * The rows of [queue] that should be running but may have lost their task: unblocked line fronts and kept rows,
     * not leased at [now], last changed before [before].
     */
    fun stale(
        queue: String,
        now: Instant,
        before: Instant,
    ): List<ReactionRow>
}

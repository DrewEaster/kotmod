package io.kotmod.postgres

import io.kotmod.event.reaction.LineTx
import io.kotmod.event.reaction.ReactionRow
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.RowKind
import io.kotmod.event.reaction.RowTx
import io.kotmod.jdbc.JdbcContext
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import kotlin.time.toKotlinInstant

/** [ReactionRows] on `ddd_reaction_row`; a line's lock is a transaction-scoped advisory lock on its queue and key. */
internal class PostgresReactionRows(
    private val jdbc: JdbcContext,
    private val clock: () -> Instant = { Clock.System.now() },
) : ReactionRows {
    override fun <R> inLine(
        queue: String,
        key: String,
        block: LineTx.() -> R,
    ): R =
        jdbc.inTransaction {
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))").use { ps ->
                    ps.setString(1, "ddd_reaction_row\u001f$queue\u001f$key")
                    ps.executeQuery().close()
                }
                Line(conn, queue, key).block()
            }
        }

    override fun <R> inQueue(
        queue: String,
        block: RowTx.() -> R,
    ): R = jdbc.inTransaction { jdbc.withConnection { conn -> Rows(conn, queue).block() } }

    override fun list(queue: String): List<ReactionRow> =
        jdbc.withConnection { conn ->
            conn.query(
                "SELECT * FROM ddd_reaction_row WHERE queue_name = ? " +
                    "ORDER BY line_key NULLS LAST, line_sequence, line_ordinal, reaction_id",
                queue,
            )
        }

    override fun stale(
        queue: String,
        now: Instant,
        before: Instant,
    ): List<ReactionRow> =
        jdbc.withConnection { conn ->
            conn.query(
                """
                SELECT * FROM ddd_reaction_row r
                WHERE r.queue_name = ? AND NOT r.blocked
                  AND (r.lease_until IS NULL OR r.lease_until <= ?)
                  AND r.updated_at < ?
                  AND (r.kind = 'KEPT' OR NOT EXISTS (
                      SELECT 1 FROM ddd_reaction_row e
                      WHERE e.queue_name = r.queue_name AND e.line_key = r.line_key
                        AND (e.line_sequence, e.line_ordinal) < (r.line_sequence, r.line_ordinal)))
                ORDER BY r.line_key NULLS LAST, r.reaction_id
                """.trimIndent(),
                queue,
                Timestamp.from(now.toJavaInstant()),
                Timestamp.from(before.toJavaInstant()),
            )
        }

    private open inner class Rows(
        protected val conn: Connection,
        protected val queue: String,
    ) : RowTx {
        override fun get(reactionId: String): ReactionRow? =
            conn.query("SELECT * FROM ddd_reaction_row WHERE queue_name = ? AND reaction_id = ?", queue, reactionId).firstOrNull()

        override fun insert(row: ReactionRow): Boolean =
            conn
                .prepareStatement(
                    "INSERT INTO ddd_reaction_row (queue_name, reaction_id, kind, line_key, line_sequence, line_ordinal, " +
                        "item, attempts, blocked, lease_until, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT DO NOTHING",
                ).use { ps ->
                    ps.setString(1, queue)
                    ps.setString(2, row.reactionId)
                    ps.setString(3, row.kind.name)
                    ps.setString(4, row.key)
                    ps.setObject(5, row.sequence)
                    ps.setObject(6, row.ordinal)
                    ps.setString(7, row.item)
                    ps.setInt(8, row.attempts)
                    ps.setBoolean(9, row.blocked)
                    ps.setTimestamp(10, row.leaseUntil?.let { Timestamp.from(it.toJavaInstant()) })
                    ps.setTimestamp(11, Timestamp.from(clock().toJavaInstant()))
                    ps.executeUpdate() == 1
                }

        override fun delete(reactionId: String) {
            conn.prepareStatement("DELETE FROM ddd_reaction_row WHERE queue_name = ? AND reaction_id = ?").use { ps ->
                ps.setString(1, queue)
                ps.setString(2, reactionId)
                ps.executeUpdate()
            }
        }

        override fun update(row: ReactionRow) {
            conn
                .prepareStatement(
                    "UPDATE ddd_reaction_row SET attempts = ?, blocked = ?, lease_until = ?, updated_at = ? " +
                        "WHERE queue_name = ? AND reaction_id = ?",
                ).use { ps ->
                    ps.setInt(1, row.attempts)
                    ps.setBoolean(2, row.blocked)
                    ps.setTimestamp(3, row.leaseUntil?.let { Timestamp.from(it.toJavaInstant()) })
                    ps.setTimestamp(4, Timestamp.from(clock().toJavaInstant()))
                    ps.setString(5, queue)
                    ps.setString(6, row.reactionId)
                    ps.executeUpdate()
                }
        }
    }

    private inner class Line(
        conn: Connection,
        queue: String,
        private val key: String,
    ) : Rows(conn, queue),
        LineTx {
        override fun front(): ReactionRow? =
            conn
                .query(
                    "SELECT * FROM ddd_reaction_row WHERE queue_name = ? AND line_key = ? " +
                        "ORDER BY line_sequence, line_ordinal LIMIT 1",
                    queue,
                    key,
                ).firstOrNull()
    }
}

private fun Connection.query(
    sql: String,
    vararg params: Any,
): List<ReactionRow> =
    prepareStatement(sql).use { ps ->
        params.forEachIndexed { i, p -> ps.setObject(i + 1, p) }
        ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.toRow()) } }
    }

private fun ResultSet.toRow(): ReactionRow =
    ReactionRow(
        queue = getString("queue_name"),
        reactionId = getString("reaction_id"),
        kind = RowKind.valueOf(getString("kind")),
        key = getString("line_key"),
        sequence = getObject("line_sequence") as Long?,
        ordinal = getObject("line_ordinal") as Int?,
        item = getString("item"),
        attempts = getInt("attempts"),
        blocked = getBoolean("blocked"),
        leaseUntil = getTimestamp("lease_until")?.toInstant()?.toKotlinInstant(),
    )

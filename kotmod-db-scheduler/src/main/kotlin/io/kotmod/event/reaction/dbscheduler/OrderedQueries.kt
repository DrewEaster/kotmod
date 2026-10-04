package io.kotmod.event.reaction.dbscheduler

import io.kotmod.jdbc.JdbcContext

/**
 * SQL against db-scheduler's own table for ordered reactions. A key's rows are selected as the "C"-collated range
 * [orderedKeyPrefix, orderedKeyUpperBound), which an index on `(task_name, task_instance COLLATE "C")` can serve.
 */
internal class OrderedQueries(
    private val jdbc: JdbcContext,
    private val tableName: String,
) {
    /** Whether a reaction of [key] that sorts before [ownInstanceId] (which starts with [key]'s prefix) is still pending for [taskName]. */
    fun earlierPending(
        taskName: String,
        key: String,
        ownInstanceId: String,
    ): Boolean =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT EXISTS (SELECT 1 FROM $tableName WHERE task_name = ? " +
                        "AND task_instance COLLATE \"C\" >= ? AND task_instance COLLATE \"C\" < ?)",
                ).use { ps ->
                    ps.setString(1, taskName)
                    ps.setString(2, orderedKeyPrefix(key))
                    ps.setString(3, ownInstanceId)
                    ps.executeQuery().use { rs ->
                        rs.next()
                        rs.getBoolean(1)
                    }
                }
        }

    /** The next pending, not-parked reaction of [key], if any. */
    fun nextPending(
        taskName: String,
        key: String,
    ): String? =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT task_instance FROM $tableName WHERE task_name = ? " +
                        "AND task_instance COLLATE \"C\" >= ? AND task_instance COLLATE \"C\" < ? " +
                        "AND NOT picked AND execution_time < now() + interval '50 years' " +
                        "ORDER BY task_instance COLLATE \"C\" LIMIT 1",
                ).use { ps ->
                    ps.setString(1, taskName)
                    ps.setString(2, orderedKeyPrefix(key))
                    ps.setString(3, orderedKeyUpperBound(key))
                    ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
                }
        }
}

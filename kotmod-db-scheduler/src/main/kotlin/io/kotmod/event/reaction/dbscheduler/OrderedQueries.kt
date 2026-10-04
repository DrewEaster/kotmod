package io.kotmod.event.reaction.dbscheduler

import io.kotmod.jdbc.JdbcContext

/** SQL against db-scheduler's own table for ordered reactions. Comparisons use the "C" collation. */
internal class OrderedQueries(
    private val jdbc: JdbcContext,
    private val tableName: String,
) {
    /** Whether a reaction of [key] that sorts before [ownInstanceId] is still pending for [taskName]. */
    fun earlierPending(
        taskName: String,
        key: String,
        ownInstanceId: String,
    ): Boolean =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT EXISTS (SELECT 1 FROM $tableName WHERE task_name = ? AND starts_with(task_instance, ?) " +
                        "AND task_instance COLLATE \"C\" < ? COLLATE \"C\")",
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
                    "SELECT task_instance FROM $tableName WHERE task_name = ? AND starts_with(task_instance, ?) " +
                        "AND NOT picked AND execution_time < now() + interval '50 years' " +
                        "ORDER BY task_instance COLLATE \"C\" LIMIT 1",
                ).use { ps ->
                    ps.setString(1, taskName)
                    ps.setString(2, orderedKeyPrefix(key))
                    ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
                }
        }
}

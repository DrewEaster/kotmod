package io.kotmod.postgres

import io.kotmod.jdbc.JdbcContext

/**
 * Remembers how far each event consumer has read through the event log, in `ddd_consumer_offset`.
 *
 * Each poller uses its own consumer name:
 * ```
 * getOffset = { offsets.getOffset("orders-outbox") },
 * saveOffset = { offsets.saveOffset("orders-outbox", it) },
 * ```
 */
class PostgresOffsetManager(
    private val jdbc: JdbcContext,
) {
    /** Returns the last offset saved for [consumerName], or [INITIAL_OFFSET] if it has never saved one. */
    fun getOffset(consumerName: String): Long =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT last_offset FROM ddd_consumer_offset WHERE consumer_name = ?",
                ).use { ps ->
                    ps.setString(1, consumerName)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) rs.getLong("last_offset") else INITIAL_OFFSET
                    }
                }
        }

    /** Saves [offset] as the last offset processed by [consumerName], replacing any previous value. */
    fun saveOffset(
        consumerName: String,
        offset: Long,
    ) {
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO ddd_consumer_offset (consumer_name, last_offset, updated_at) " +
                        "VALUES (?, ?, now()) " +
                        "ON CONFLICT (consumer_name) DO UPDATE " +
                        "SET last_offset = EXCLUDED.last_offset, updated_at = EXCLUDED.updated_at",
                ).use { ps ->
                    ps.setString(1, consumerName)
                    ps.setLong(2, offset)
                    ps.executeUpdate()
                }
        }
    }


    companion object {
        /** Offset for a consumer that has not processed anything yet; lower than any real `global_offset`. */
        const val INITIAL_OFFSET: Long = -1L
    }
}

package com.dreweaster.ddd.postgres

import app.cash.sqldelight.driver.jdbc.JdbcDriver
import java.sql.Connection

/**
 * Persists the last processed `global_offset` for named event consumers (e.g. an
 * [com.dreweaster.ddd.outbox.AggregateEventOutbox] or a
 * [com.dreweaster.ddd.contract.PublicEventContract]) in the `ddd_consumer_offset` table.
 *
 * Wire into a poller with:
 * ```
 * getOffset = { offsets.getOffset("orders-outbox") },
 * saveOffset = { offsets.saveOffset("orders-outbox", it) },
 * ```
 */
class PostgresOffsetManager(
    private val driver: JdbcDriver,
) {
    /** Returns the last saved offset for [consumerName], or [INITIAL_OFFSET] if none has been saved. */
    fun getOffset(consumerName: String): Long =
        useConnection { conn ->
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

    fun saveOffset(
        consumerName: String,
        offset: Long,
    ) {
        useConnection { conn ->
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

    private inline fun <R> useConnection(block: (Connection) -> R): R {
        val (conn, close) = driver.connectionAndClose()
        try {
            return block(conn)
        } finally {
            close()
        }
    }

    companion object {
        /** Offset returned for a consumer with no saved offset; `global_offset` starts at 1. */
        const val INITIAL_OFFSET: Long = -1L
    }
}

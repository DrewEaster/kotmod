package io.kotmod.postgres

import io.kotmod.EventLogPosition
import io.kotmod.jdbc.JdbcContext

/**
 * Remembers how far each event consumer has read through the event log, in `ddd_consumer_offset`.
 *
 * Each poller uses its own consumer name:
 * ```
 * getPosition = { offsets.getPosition("orders-outbox") },
 * savePosition = { offsets.savePosition("orders-outbox", it) },
 * ```
 */
class PostgresOffsetManager(
    private val jdbc: JdbcContext,
) {
    /** Returns the position last saved for [consumerName], or [EventLogPosition.START] if it has never saved one. */
    fun getPosition(consumerName: String): EventLogPosition =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT last_transaction_id, last_offset FROM ddd_consumer_offset WHERE consumer_name = ?",
                ).use { ps ->
                    ps.setString(1, consumerName)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) EventLogPosition(rs.getLong(1), rs.getLong(2)) else EventLogPosition.START
                    }
                }
        }

    /** Saves [position] as the last position processed by [consumerName], replacing any previous value. */
    fun savePosition(
        consumerName: String,
        position: EventLogPosition,
    ) {
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO ddd_consumer_offset (consumer_name, last_transaction_id, last_offset, updated_at) " +
                        "VALUES (?, ?, ?, now()) " +
                        "ON CONFLICT (consumer_name) DO UPDATE SET last_transaction_id = EXCLUDED.last_transaction_id, " +
                        "last_offset = EXCLUDED.last_offset, updated_at = EXCLUDED.updated_at",
                ).use { ps ->
                    ps.setString(1, consumerName)
                    ps.setLong(2, position.transactionId)
                    ps.setLong(3, position.globalOffset)
                    ps.executeUpdate()
                }
        }
    }
}

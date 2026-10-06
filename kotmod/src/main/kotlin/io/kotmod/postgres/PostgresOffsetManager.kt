package io.kotmod.postgres

import io.kotmod.EventLogPosition
import io.kotmod.jdbc.JdbcContext
import java.sql.Connection

/** Where a consumer with no saved position starts reading the event log. */
enum class StartFrom {
    /**
     * The head of the log: only events written after the consumer first asks for its position. Events from a
     * transaction still in progress at that moment are included, so nothing is lost at start-up.
     */
    Latest,

    /** The very beginning of the log, so the consumer sees every event ever written. */
    Beginning,
}

/**
 * Remembers how far each event consumer has read through the event log, in `ddd_consumer_offset`.
 *
 * Each poller uses its own consumer name. A consumer that has never saved a position starts at the head of the log
 * by default; pass [StartFrom.Beginning] for one that needs history, such as a new projection or a process manager
 * that should cover work already in flight:
 * ```
 * getPosition = { offsets.getPosition("order-emails") },
 * savePosition = { offsets.savePosition("order-emails", it) },
 *
 * getPosition = { offsets.getPosition("order-report", startFrom = StartFrom.Beginning) },
 * ```
 */
class PostgresOffsetManager(
    private val jdbc: JdbcContext,
) {
    /**
     * Returns the position last saved for [consumerName]. If it has never saved one, the position chosen by
     * [startFrom] is saved straight away and returned, so every node and every restart agrees on it. A saved
     * position always wins over [startFrom].
     *
     * The first call fixes the starting position, so calling this just to observe a consumer from outside can set
     * where that consumer starts. To observe without that risk, pass the same [startFrom] the consumer uses.
     */
    fun getPosition(
        consumerName: String,
        startFrom: StartFrom = StartFrom.Latest,
    ): EventLogPosition =
        jdbc.withConnection { conn ->
            readSaved(conn, consumerName) ?: run {
                saveInitial(conn, consumerName, startFrom)
                checkNotNull(readSaved(conn, consumerName)) { "No position saved for consumer $consumerName" }
            }
        }

    private fun readSaved(
        conn: Connection,
        consumerName: String,
    ): EventLogPosition? =
        conn
            .prepareStatement(
                "SELECT last_transaction_id, last_offset FROM ddd_consumer_offset WHERE consumer_name = ?",
            ).use { ps ->
                ps.setString(1, consumerName)
                ps.executeQuery().use { rs ->
                    if (rs.next()) EventLogPosition(rs.getLong(1), rs.getLong(2)) else null
                }
            }

    private fun saveInitial(
        conn: Connection,
        consumerName: String,
        startFrom: StartFrom,
    ) {
        // The head is just below the oldest transaction still in progress: everything below it is committed (and
        // so past), everything at or above it is still to be delivered.
        val position =
            when (startFrom) {
                StartFrom.Beginning -> EventLogPosition.START
                StartFrom.Latest ->
                    conn.createStatement().use { st ->
                        st.executeQuery("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint").use { rs ->
                            rs.next()
                            EventLogPosition(rs.getLong(1) - 1, Long.MAX_VALUE)
                        }
                    }
            }
        conn
            .prepareStatement(
                "INSERT INTO ddd_consumer_offset (consumer_name, last_transaction_id, last_offset, updated_at) " +
                    "VALUES (?, ?, ?, now()) ON CONFLICT (consumer_name) DO NOTHING",
            ).use { ps ->
                ps.setString(1, consumerName)
                ps.setLong(2, position.transactionId)
                ps.setLong(3, position.globalOffset)
                ps.executeUpdate()
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

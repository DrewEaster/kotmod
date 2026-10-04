package io.kotmod.postgres.support

import io.kotmod.jdbc.JdbcContext
import java.sql.Connection

/**
 * A [JdbcContext] bound to one open, non-auto-commit [connection], so a test can interleave several
 * transactions on one thread. The test commits or rolls back the connection itself.
 */
class ConnectionJdbcContext(
    private val connection: Connection,
) : JdbcContext {
    override fun <R> withConnection(block: (Connection) -> R): R = block(connection)

    override fun <R> inTransaction(block: () -> R): R = block()

    override fun isInTransaction(): Boolean = true
}

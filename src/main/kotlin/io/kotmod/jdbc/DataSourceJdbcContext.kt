package io.kotmod.jdbc

import java.sql.Connection
import javax.sql.DataSource

/**
 * A [JdbcContext] over a plain [DataSource]. The outermost [inTransaction] borrows a connection, turns
 * auto-commit off and binds the connection to the current thread until it commits or rolls back; nested
 * calls on that thread reuse it. Uses the database's default isolation level.
 */
class DataSourceJdbcContext(
    private val dataSource: DataSource,
) : JdbcContext {
    private val transactionConnection = ThreadLocal<Connection?>()

    override fun <R> withConnection(block: (Connection) -> R): R {
        KotmodTransaction.requireCompatible(this)
        val open = transactionConnection.get()
        return if (open != null) block(open) else dataSource.connection.use(block)
    }

    override fun <R> inTransaction(block: () -> R): R {
        KotmodTransaction.requireCompatible(this)
        if (transactionConnection.get() != null) return block()

        val connection = dataSource.connection
        val previousAutoCommit = connection.autoCommit
        try {
            connection.autoCommit = false
            transactionConnection.set(connection)
            val result =
                try {
                    block()
                } catch (failure: Throwable) {
                    try {
                        connection.rollback()
                    } catch (rollbackFailure: Throwable) {
                        failure.addSuppressed(rollbackFailure)
                    }
                    throw failure
                }
            connection.commit()
            return result
        } finally {
            transactionConnection.remove()
            try {
                connection.autoCommit = previousAutoCommit
            } finally {
                connection.close()
            }
        }
    }

    override fun isInTransaction(): Boolean = transactionConnection.get() != null
}

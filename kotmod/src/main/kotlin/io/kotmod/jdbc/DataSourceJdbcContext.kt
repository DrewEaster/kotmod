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
    private class OpenTransaction(
        val connection: Connection,
    ) {
        var rollbackOnly = false
    }

    private val openTransaction = ThreadLocal<OpenTransaction?>()

    override fun <R> withConnection(block: (Connection) -> R): R {
        KotmodTransaction.requireCompatible(this)
        val open = openTransaction.get()
        return if (open != null) block(open.connection) else dataSource.connection.use(block)
    }

    override fun <R> inTransaction(block: () -> R): R {
        KotmodTransaction.requireCompatible(this)
        val enclosing = openTransaction.get()
        if (enclosing != null) {
            try {
                return block()
            } catch (failure: Throwable) {
                enclosing.rollbackOnly = true
                throw failure
            }
        }

        val connection = dataSource.connection
        val previousAutoCommit = connection.autoCommit
        try {
            connection.autoCommit = false
            val transaction = OpenTransaction(connection)
            openTransaction.set(transaction)
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
            if (transaction.rollbackOnly) {
                connection.rollback()
                throw TransactionRolledBackException(ROLLBACK_ONLY_MESSAGE)
            }
            connection.commit()
            return result
        } finally {
            openTransaction.remove()
            try {
                connection.autoCommit = previousAutoCommit
            } finally {
                connection.close()
            }
        }
    }

    override fun isInTransaction(): Boolean = openTransaction.get() != null
}

package io.kotmod.sqldelight

import app.cash.sqldelight.TransacterImpl
import app.cash.sqldelight.driver.jdbc.JdbcDriver
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.KotmodTransaction
import io.kotmod.jdbc.ROLLBACK_ONLY_MESSAGE
import io.kotmod.jdbc.TransactionRolledBackException
import java.sql.Connection

/**
 * A [JdbcContext] for apps that use SQLDelight. kotmod borrows connections from [driver] and runs its
 * transactions through SQLDelight's, so kotmod's writes and your SQLDelight queries share one transaction
 * whenever they run on the same thread — whether you open it with SQLDelight (`database.transaction { }`)
 * or with kotmod (`jdbc.transaction { }`).
 *
 * Two instances over the same [driver] are equal: they share the driver's transactions.
 */
class SqlDelightJdbcContext(
    private val driver: JdbcDriver,
) : JdbcContext {
    private val transacter = object : TransacterImpl(driver) {}

    private class KotmodOwnedTransaction {
        var rollbackOnly = false
    }

    // Set only while kotmod itself opened the outermost SQLDelight transaction on this thread.
    private val kotmodOwned = ThreadLocal<KotmodOwnedTransaction?>()

    override fun <R> withConnection(block: (Connection) -> R): R {
        KotmodTransaction.requireCompatible(this)
        val (connection, close) = driver.connectionAndClose()
        try {
            return block(connection)
        } finally {
            close()
        }
    }

    override fun <R> inTransaction(block: () -> R): R {
        KotmodTransaction.requireCompatible(this)
        if (driver.currentTransaction() != null) {
            // Joining: SQLDelight marks the enclosing transaction as failed if this throws.
            try {
                return transacter.transactionWithResult { block() }
            } catch (failure: Throwable) {
                kotmodOwned.get()?.rollbackOnly = true
                throw failure
            }
        }
        val owned = KotmodOwnedTransaction()
        kotmodOwned.set(owned)
        try {
            val result = transacter.transactionWithResult { block() }
            // SQLDelight has already rolled back silently because a nested transaction failed; say so.
            if (owned.rollbackOnly) throw TransactionRolledBackException(ROLLBACK_ONLY_MESSAGE)
            return result
        } finally {
            kotmodOwned.remove()
        }
    }

    override fun isInTransaction(): Boolean = driver.currentTransaction() != null

    override fun equals(other: Any?): Boolean = other is SqlDelightJdbcContext && other.driver === driver

    override fun hashCode(): Int = System.identityHashCode(driver)
}

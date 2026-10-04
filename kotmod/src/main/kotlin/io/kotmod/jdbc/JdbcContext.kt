package io.kotmod.jdbc

import java.sql.Connection

/**
 * How kotmod reaches the database: borrowing a connection and running work in a transaction.
 *
 * Transactions are bound to the thread that opened them. Pass the same `JdbcContext` to every kotmod
 * class and to your own repositories so they all share one transaction. Implementations:
 * [DataSourceJdbcContext] for a plain `DataSource`, and `SqlDelightJdbcContext` (module
 * `kotmod-sqldelight`) for apps using SQLDelight.
 */
interface JdbcContext {
    /**
     * Runs [block] with a connection. Inside a transaction on this thread it is the transaction's
     * connection — don't close, commit or roll it back. Outside one it is a fresh auto-commit connection,
     * released when [block] returns.
     */
    fun <R> withConnection(block: (Connection) -> R): R

    /**
     * Runs [block] in a transaction. If one is already open on this thread, [block] joins it; otherwise a
     * new one is committed when [block] returns, or rolled back if it throws (the exception is rethrown).
     */
    fun <R> inTransaction(block: () -> R): R

    /** Returns whether a transaction is open on the current thread. */
    fun isInTransaction(): Boolean
}

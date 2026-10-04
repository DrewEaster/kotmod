package io.kotmod.jdbc

/**
 * Thrown when a transaction's block returned normally but the transaction had already been marked
 * rollback-only — because an exception escaped a nested [JdbcContext.inTransaction] (for example a kotmod
 * command that failed) and the caller caught it and carried on. The transaction is rolled back rather than
 * committing a partial result.
 */
class TransactionRolledBackException(
    message: String,
) : IllegalStateException(message)

/** Message used by kotmod's [JdbcContext] implementations when a rollback-only transaction's block returns. */
const val ROLLBACK_ONLY_MESSAGE: String =
    "Transaction rolled back: an operation inside it failed and the failure was caught, so committing would " +
        "leave a partial result. Let the exception propagate out of the transaction instead."

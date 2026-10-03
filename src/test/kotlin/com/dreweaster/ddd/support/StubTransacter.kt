package com.dreweaster.ddd.support

import app.cash.sqldelight.Transacter
import app.cash.sqldelight.TransactionWithReturn
import app.cash.sqldelight.TransactionWithoutReturn

class StubTransacter : Transacter {
    var transactionDepth = 0
        private set

    override fun transaction(
        noEnclosing: Boolean,
        body: TransactionWithoutReturn.() -> Unit,
    ) {
        check(!(noEnclosing && transactionDepth > 0)) { "noEnclosing transaction called inside an existing transaction" }
        transactionDepth++
        try {
            val tx =
                object : TransactionWithoutReturn {
                    override fun rollback(): Nothing = throw RollbackException(null)

                    override fun transaction(body: TransactionWithoutReturn.() -> Unit) = this.body()

                    override fun afterCommit(function: () -> Unit) = Unit

                    override fun afterRollback(function: () -> Unit) = Unit
                }
            try {
                tx.body()
            } catch (_: RollbackException) {
                // rolled back — swallow
            }
        } finally {
            transactionDepth--
        }
    }

    override fun <R> transactionWithResult(
        noEnclosing: Boolean,
        bodyWithReturn: TransactionWithReturn<R>.() -> R,
    ): R {
        check(!(noEnclosing && transactionDepth > 0)) { "noEnclosing transaction called inside an existing transaction" }
        transactionDepth++
        try {
            val tx =
                object : TransactionWithReturn<R> {
                    override fun rollback(returnValue: R): Nothing = throw RollbackException(returnValue)

                    override fun <R> transaction(body: TransactionWithReturn<R>.() -> R): R {
                        val inner =
                            object : TransactionWithReturn<R> {
                                override fun rollback(returnValue: R): Nothing = throw RollbackException(returnValue)

                                override fun <R> transaction(body: TransactionWithReturn<R>.() -> R): R = error("nested not supported")

                                override fun afterCommit(function: () -> Unit) = Unit

                                override fun afterRollback(function: () -> Unit) = Unit
                            }
                        return inner.body()
                    }

                    override fun afterCommit(function: () -> Unit) = Unit

                    override fun afterRollback(function: () -> Unit) = Unit
                }
            return try {
                tx.bodyWithReturn()
            } catch (e: RollbackException) {
                @Suppress("UNCHECKED_CAST")
                e.returnValue as R
            }
        } finally {
            transactionDepth--
        }
    }

    private class RollbackException(
        val returnValue: Any?,
    ) : Throwable()
}

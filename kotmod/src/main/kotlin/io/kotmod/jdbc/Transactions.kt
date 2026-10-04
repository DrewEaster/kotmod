package io.kotmod.jdbc

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext

/**
 * Marks a coroutine as running inside an outer kotmod transaction opened by [transaction], recording the
 * [JdbcContext] it belongs to and the [thread] that owns it.
 */
class KotmodTransaction internal constructor(
    val jdbc: JdbcContext,
    val thread: Thread,
) : AbstractCoroutineContextElement(KotmodTransaction) {
    companion object Key : CoroutineContext.Key<KotmodTransaction> {
        internal val current = ThreadLocal<KotmodTransaction?>()

        /**
         * Throws [IllegalStateException] if an outer transaction for a different [JdbcContext] is open on
         * this thread. Called by [JdbcContext] implementations before touching the database, so a mismatched
         * context fails loudly instead of silently opening a second transaction.
         */
        fun requireCompatible(jdbc: JdbcContext) {
            val open = current.get() ?: return
            open.requireOwningThread()
            check(open.jdbc == jdbc) {
                "A kotmod transaction is open on this thread for a different JdbcContext; " +
                    "use the same JdbcContext for everything inside jdbc.transaction { }"
            }
        }
    }

    /** Throws [IllegalStateException] unless called on the thread that owns this transaction. */
    fun requireOwningThread() {
        check(Thread.currentThread() === thread) {
            "kotmod transaction used from thread '${Thread.currentThread().name}' but it belongs to " +
                "'${thread.name}'; don't switch threads (e.g. withContext) inside jdbc.transaction { }"
        }
    }
}

/**
 * Runs [block] so that every kotmod command inside it — on any number of aggregates — commits or rolls
 * back together:
 * ```
 * jdbc.transaction {
 *     orders.execute<PendingOrder>(orderId) { … }
 *     invoices.create(invoiceId) { … }
 * }
 * ```
 * Outside a transaction, it switches to an IO thread, opens a transaction there and runs [block] confined
 * to that thread. If a transaction is already open on the current thread (for example the app's own
 * SQLDelight transaction, or an enclosing `transaction { }`), [block] joins it. The transaction commits when
 * [block] returns and rolls back if it throws or is cancelled.
 *
 * Run commands one after another inside [block], never in parallel, and don't switch threads (e.g. with
 * `withContext`) — doing so throws [IllegalStateException]. [block] keeps the caller's coroutine context.
 * If something inside fails and you catch it and carry on, the transaction is rolled back anyway and
 * [TransactionRolledBackException] is thrown. Keep [block] short: it holds a database transaction open.
 */
suspend fun <R> JdbcContext.transaction(block: suspend () -> R): R {
    val open = currentCoroutineContext()[KotmodTransaction]
    if (open != null) {
        check(open.jdbc == this) {
            "A kotmod transaction is open for a different JdbcContext; use the same JdbcContext for everything inside jdbc.transaction { }"
        }
        open.requireOwningThread()
        return failuresMarkRollbackOnly { block() }
    }

    if (isInTransaction()) {
        val joined = KotmodTransaction(this, Thread.currentThread())
        return withContext(joined + KotmodTransaction.current.asContextElement(joined)) {
            failuresMarkRollbackOnly { block() }
        }
    }

    return withContext(Dispatchers.IO) {
        // Keep the caller's context (job, name, tracing/MDC elements) but run on this thread, not a dispatcher.
        val callerContext = currentCoroutineContext().minusKey(ContinuationInterceptor)
        inTransaction {
            val opened = KotmodTransaction(this@transaction, Thread.currentThread())
            runBlocking(callerContext + opened + KotmodTransaction.current.asContextElement(opened)) { block() }
        }
    }
}

/**
 * Runs [block] inside an already-open transaction. If it throws, the enclosing transaction is marked
 * rollback-only (by letting the failure escape a nested [JdbcContext.inTransaction]), so catching the
 * failure further out can't lead to a partial commit.
 */
private suspend fun <R> JdbcContext.failuresMarkRollbackOnly(block: suspend () -> R): R =
    try {
        block()
    } catch (failure: Throwable) {
        inTransaction { throw failure }
    }

/**
 * Runs kotmod's blocking database work for a command. Inside an outer [transaction] it runs on the owning
 * thread; if [inTransactionOnThisThread] reports a transaction already open on the calling thread (for
 * example the app's own SQLDelight transaction, called from blocking code) it joins it there; otherwise it
 * switches to [Dispatchers.IO].
 */
internal suspend fun <R> databaseWork(
    inTransactionOnThisThread: () -> Boolean,
    block: () -> R,
): R {
    val open = currentCoroutineContext()[KotmodTransaction]
    return when {
        open != null -> {
            open.requireOwningThread()
            block()
        }
        inTransactionOnThisThread() -> block()
        else -> withContext(Dispatchers.IO) { block() }
    }
}

package io.kotmod.jdbc

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
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
 * `withContext`) — doing so throws [IllegalStateException]. Keep [block] short: it holds a database
 * transaction open.
 */
suspend fun <R> JdbcContext.transaction(block: suspend () -> R): R {
    val open = currentCoroutineContext()[KotmodTransaction]
    if (open != null) {
        check(open.jdbc == this) {
            "A kotmod transaction is open for a different JdbcContext; use the same JdbcContext for everything inside jdbc.transaction { }"
        }
        open.requireOwningThread()
        return block()
    }

    if (isInTransaction()) {
        val joined = KotmodTransaction(this, Thread.currentThread())
        return withContext(joined + KotmodTransaction.current.asContextElement(joined)) { block() }
    }

    return withContext(Dispatchers.IO) {
        val parentJob = currentCoroutineContext()[Job]
        inTransaction {
            val opened = KotmodTransaction(this@transaction, Thread.currentThread())
            val context = opened + KotmodTransaction.current.asContextElement(opened)
            runBlocking(if (parentJob != null) context + parentJob else context) { block() }
        }
    }
}

/**
 * Runs kotmod's blocking database work for a command: on the owning thread inside an outer
 * [transaction], otherwise on [Dispatchers.IO].
 */
internal suspend fun <R> databaseWork(block: () -> R): R {
    val open = currentCoroutineContext()[KotmodTransaction]
    return if (open == null) {
        withContext(Dispatchers.IO) { block() }
    } else {
        open.requireOwningThread()
        block()
    }
}

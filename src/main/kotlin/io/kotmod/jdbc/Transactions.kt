package io.kotmod.jdbc

import kotlinx.coroutines.Dispatchers
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
 * Runs kotmod's blocking database work for a command. Outside an outer transaction it switches to
 * [kotlinx.coroutines.Dispatchers.IO].
 */
internal suspend fun <R> databaseWork(block: () -> R): R = withContext(Dispatchers.IO) { block() }

package io.kotmod

/**
 * Stores the current state of one aggregate type, keyed by [AggregateId].
 *
 * The library does not prescribe how state is stored; the app implements this against its own
 * tables. [AggregateManager] calls [save] inside the same transaction that records the aggregate's
 * version, events and handled command, so implementations must use the same
 * [io.kotmod.jdbc.JdbcContext] (or SQLDelight driver) as the backend, e.g. via
 * [io.kotmod.jdbc.JdbcContext.withConnection].
 *
 * @param S the aggregate's state type.
 */
interface Repository<S : Any> {
    /** Loads the current state of aggregate [id], or `null` if it does not exist. */
    fun get(id: AggregateId): S?

    /** Inserts or replaces the state of aggregate [id] with [state]. */
    fun save(
        id: AggregateId,
        state: S,
    )
}

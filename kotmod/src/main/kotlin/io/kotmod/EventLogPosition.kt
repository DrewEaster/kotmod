package io.kotmod

/**
 * Where an event sits in the event log: the id of the transaction that wrote it, then its global offset.
 * Consumers save the position of the last event they handled and resume after it. Ordering by transaction
 * first is what lets a consumer never skip an event committed by a slower, concurrent transaction.
 */
data class EventLogPosition(
    val transactionId: Long,
    val globalOffset: Long,
) : Comparable<EventLogPosition> {
    override fun compareTo(other: EventLogPosition): Int =
        compareValuesBy(this, other, EventLogPosition::transactionId, EventLogPosition::globalOffset)

    companion object {
        /** The position before every event; a consumer that has saved nothing starts here. */
        val START = EventLogPosition(0, 0)
    }
}

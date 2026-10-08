package io.kotmod.event.reaction

import io.kotmod.EventMetadata

/** Whether an event policy's reactions run in their aggregate's history order. */
sealed interface ReactionOrdering {
    /** Reactions may run in any order and in parallel (the default). */
    data object Unordered : ReactionOrdering

    /**
     * Reactions for the same source aggregate run one at a time, in the order of the events that caused them.
     * [onGiveUp] decides what happens to later reactions when one gives up for good.
     */
    data class PerAggregate(
        val onGiveUp: OnGiveUp = OnGiveUp.ContinueWithNext,
    ) : ReactionOrdering
}

/** What an ordered event policy does when a reaction gives up for good. */
enum class OnGiveUp {
    /** Complete it as failed and move on to the aggregate's next reaction. */
    ContinueWithNext,

    /** Hold back the aggregate's later reactions until the failed one is retried or skipped by an operator. */
    BlockAggregate,
}

/** The line an ordered reaction to the event described by [metadata] joins: its aggregate's. */
internal fun lineKey(metadata: EventMetadata): String = "${metadata.aggregateType.value}/${metadata.aggregateId.value}"

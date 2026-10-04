package io.kotmod.event.reaction

import io.kotmod.EventMetadata
import kotlin.time.Duration

/** Whether a subscription's reactions run in their aggregate's history order. */
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

/** What an ordered subscription does when a reaction gives up for good. */
enum class OnGiveUp {
    /** Complete it as failed and move on to the aggregate's next reaction. */
    ContinueWithNext,

    /** Hold back the aggregate's later reactions until the failed one is retried or skipped by an operator. */
    BlockAggregate,
}

/** The ordering stamp kotmod attaches to a reaction dispatched by an ordered subscription. */
data class DispatchOrdering(
    val key: String,
    val sequence: Long,
    val ordinal: Int,
    val onGiveUp: OnGiveUp,
)

/** What a source learns after one attempt at a reaction. */
sealed interface ReactionOutcome {
    /** Run the reaction again after [delay]. */
    data class Retry(
        val delay: Duration,
    ) : ReactionOutcome

    /** The reaction is done; [gaveUp] is true when it ended as a failure. */
    data class Finished(
        val gaveUp: Boolean,
    ) : ReactionOutcome
}

internal fun ReactionOrdering.stampFor(
    metadata: EventMetadata,
    ordinal: Int,
): DispatchOrdering? =
    when (this) {
        ReactionOrdering.Unordered -> null
        is ReactionOrdering.PerAggregate ->
            DispatchOrdering("${metadata.aggregateType.value}/${metadata.aggregateId.value}", metadata.sequence, ordinal, onGiveUp)
    }

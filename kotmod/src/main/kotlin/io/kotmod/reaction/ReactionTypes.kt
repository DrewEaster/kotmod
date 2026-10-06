package io.kotmod.reaction

import kotlin.time.Duration

/**
 * What [EventPolicy.handle] knows about the reaction it runs. [reactionId] is deterministic and the same on every retry and
 * redelivery, so pass it as the idempotency key of external calls; [attempt] counts retries, starting at 0.
 */
data class ReactionContext(
    val reactionId: String,
    val attempt: Int,
)

/** What [EventPolicy.onFailure] decides after a failed attempt: [Retry] or [GiveUp]. */
sealed interface FailureDecision

/** Run the reaction again after [delay]. */
data class Retry(
    val delay: Duration,
) : FailureDecision

/** Stop retrying; [EventPolicy.onCompletion] is told with [ReactionResult.GaveUp]. */
data object GiveUp : FailureDecision

/** How a reaction finally ended, passed to [EventPolicy.onCompletion]. */
sealed interface ReactionResult {
    /** [EventPolicy.handle] returned normally. */
    data object Completed : ReactionResult

    /** [EventPolicy.onFailure] gave up after [error]. */
    data class GaveUp(
        val error: Throwable,
    ) : ReactionResult
}

/** The failure passed to [EventPolicy.onFailure] when [EventPolicy.handle] runs longer than the event policy's [timeout]. */
class ReactionTimeoutException(
    val timeout: Duration,
) : RuntimeException("The reaction did not finish within $timeout")

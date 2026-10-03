package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.RetrySignal
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.toJavaDuration

/**
 * Longest delay a retry is rescheduled by. Larger delays (including [Duration.INFINITE]) cannot be represented
 * as an [Instant] or stored by db-scheduler, so they are capped rather than failing an already-executed reaction.
 */
internal val MAX_RETRY_DELAY: Duration = 3650.days

/** What the db-scheduler task should do with its row once an execution has finished. */
internal sealed interface ReactionOutcome {
    data object Remove : ReactionOutcome

    data class Reschedule(
        val at: Instant,
        val taskData: String,
    ) : ReactionOutcome
}

internal fun outcomeAfterExecution(
    result: RetrySignal.Retry?,
    data: ReactionTaskData,
    now: Instant,
): ReactionOutcome =
    when (result) {
        null -> ReactionOutcome.Remove
        else ->
            ReactionOutcome.Reschedule(
                at = now.plus(result.delay.coerceAtMost(MAX_RETRY_DELAY).toJavaDuration()),
                taskData = data.copy(retryCount = data.retryCount + 1).encode(),
            )
    }

internal fun outcomeWhenUnsubscribed(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): ReactionOutcome = ReactionOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)

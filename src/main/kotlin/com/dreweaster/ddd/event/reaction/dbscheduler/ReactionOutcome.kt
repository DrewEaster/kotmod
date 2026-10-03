package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.RetrySignal
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.toJavaDuration

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
                at = now.plus(result.delay.toJavaDuration()),
                taskData = data.copy(retryCount = data.retryCount + 1).encode(),
            )
    }

internal fun outcomeWhenUnsubscribed(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): ReactionOutcome = ReactionOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)

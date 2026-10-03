package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.RetrySignal
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.toJavaDuration

/** Longest retry delay the task will reschedule by; larger delays (including infinite ones) are capped to it. */
internal val MAX_RETRY_DELAY: Duration = 3650.days

/** What the db-scheduler task does with its row after an execution: remove it, or reschedule it with new data. */
internal sealed interface ReactionOutcome {
    data object Remove : ReactionOutcome

    data class Reschedule(
        val at: Instant,
        val taskData: String,
    ) : ReactionOutcome
}

/** Removes the row if the reaction finished, or reschedules it after the requested delay with the retry count incremented. */
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

/** Reschedules the row after [delay] with its data unchanged, for executions that arrive before an executor has subscribed. */
internal fun outcomeWhenUnsubscribed(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): ReactionOutcome = ReactionOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)

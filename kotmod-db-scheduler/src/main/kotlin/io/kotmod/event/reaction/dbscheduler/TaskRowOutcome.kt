package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOutcome
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.toJavaDuration

/** Longest retry delay the task will reschedule by; larger delays (including infinite ones) are capped to it. */
internal val MAX_RETRY_DELAY: Duration = 3650.days

/** How far ahead a blocked ordered reaction is parked; it only runs again when retried by an operator. */
internal val PARKED_DELAY: Duration = 36500.days

/** What the db-scheduler task does with its row after an execution: remove it, or reschedule it with new data. */
internal sealed interface TaskRowOutcome {
    /** Remove the row; for ordered reactions, nudge the next pending reaction of [nudgeKey]. */
    data class Remove(
        val nudgeKey: String? = null,
    ) : TaskRowOutcome

    data class Reschedule(
        val at: Instant,
        val taskData: String,
    ) : TaskRowOutcome
}

/**
 * Removes the row if the reaction finished, or reschedules it after the requested delay with the retry count
 * incremented. An ordered reaction that gave up with [OnGiveUp.BlockAggregate] is instead parked and flagged blocked.
 */
internal fun outcomeAfterExecution(
    result: ReactionOutcome,
    data: ReactionTaskData,
    now: Instant,
): TaskRowOutcome =
    when (result) {
        is ReactionOutcome.Retry ->
            TaskRowOutcome.Reschedule(
                at = now.plus(result.delay.coerceAtMost(MAX_RETRY_DELAY).toJavaDuration()),
                taskData = data.copy(retryCount = data.retryCount + 1).encode(),
            )
        is ReactionOutcome.Finished -> {
            val ordering = data.ordering
            if (ordering != null && result.gaveUp && ordering.onGiveUp == OnGiveUp.BlockAggregate.name) {
                TaskRowOutcome.Reschedule(now.plus(PARKED_DELAY.toJavaDuration()), data.copy(blocked = true).encode())
            } else {
                TaskRowOutcome.Remove(nudgeKey = ordering?.key)
            }
        }
    }

/** Reschedules the row after [delay] with its data unchanged, for executions that arrive before an executor has subscribed. */
internal fun outcomeWhenUnsubscribed(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): TaskRowOutcome = TaskRowOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)

/** Reschedules an ordered reaction after [delay] with its data (and retry count) unchanged, while it waits its turn. */
internal fun outcomeWhenWaiting(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): TaskRowOutcome = TaskRowOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)

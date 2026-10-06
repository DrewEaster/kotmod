package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOutcome
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

/** Longest retry delay the task will reschedule by; larger delays (including infinite ones) are capped to it. */
internal val MAX_RETRY_DELAY: Duration = 3650.days

/** How far ahead a blocked ordered reaction is parked; it only runs again when retried by an operator. */
internal val PARKED_DELAY: Duration = 36500.days

/** Longest delay between rechecks of an ordered reaction waiting for an earlier one. */
internal val MAX_ORDERED_WAIT_DELAY: Duration = 1.minutes

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
 * Removes the row if the reaction finished, reschedules it after the requested delay with the retry count
 * incremented for a retry, or unchanged for a wait. An ordered reaction that gave up with
 * [OnGiveUp.BlockAggregate] is instead parked and flagged blocked. After a retry or a finish the reaction ran,
 * so its wait count is reset; after a wait it is left unchanged.
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
                taskData = data.copy(retryCount = data.retryCount + 1, waits = 0).encode(),
            )
        is ReactionOutcome.Wait ->
            TaskRowOutcome.Reschedule(
                at = now.plus(result.delay.coerceAtMost(MAX_RETRY_DELAY).toJavaDuration()),
                taskData = data.encode(),
            )
        is ReactionOutcome.Finished -> {
            val ordering = data.ordering
            if (ordering != null && result.gaveUp && ordering.onGiveUp == OnGiveUp.BlockAggregate.name) {
                TaskRowOutcome.Reschedule(now.plus(PARKED_DELAY.toJavaDuration()), data.copy(blocked = true, waits = 0).encode())
            } else {
                TaskRowOutcome.Remove(nudgeKey = ordering?.key)
            }
        }
    }

/** Reschedules the row after [delay] with its data unchanged, for executions that arrive before anything has subscribed. */
internal fun outcomeWhenUnsubscribed(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): TaskRowOutcome = TaskRowOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)

/** Reschedules the row after [delay] with its data unchanged (used to keep a blocked reaction parked). */
internal fun outcomeWhenParked(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): TaskRowOutcome = TaskRowOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)

/** The delay before rechecking an ordered reaction that has already waited [waits] times: [base] doubled per wait, capped. */
internal fun orderedWaitDelay(
    base: Duration,
    waits: Int,
): Duration = if (waits >= 30) MAX_ORDERED_WAIT_DELAY else minOf(base * (1 shl waits.coerceAtLeast(0)), MAX_ORDERED_WAIT_DELAY)

/**
 * Reschedules an ordered reaction that must wait for an earlier one, backing off by [orderedWaitDelay] and counting the
 * wait. Its retry count is unchanged; a nudge from the reaction ahead of it usually runs it sooner.
 */
internal fun outcomeWhenWaitingForEarlier(
    data: ReactionTaskData,
    now: Instant,
    base: Duration,
): TaskRowOutcome =
    TaskRowOutcome.Reschedule(
        at = now.plus(orderedWaitDelay(base, data.waits).toJavaDuration()),
        taskData = data.copy(waits = data.waits + 1).encode(),
    )

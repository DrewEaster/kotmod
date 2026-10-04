package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.ReactionOutcome
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.seconds

class TaskRowOutcomeTest {
    private val now = Instant.parse("2026-10-03T10:00:00Z")
    private val data = ReactionTaskData(trigger = "t", retryCount = 2)

    @Test
    fun `a finished reaction removes the row`() {
        assertEquals(TaskRowOutcome.Remove(nudgeKey = null), outcomeAfterExecution(ReactionOutcome.Finished(false), data, now))
    }

    @Test
    fun `retry reschedules after the delay with retryCount incremented`() {
        assertEquals(
            TaskRowOutcome.Reschedule(
                at = Instant.parse("2026-10-03T10:00:30Z"),
                taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode(),
            ),
            outcomeAfterExecution(ReactionOutcome.Retry(30.seconds), data, now),
        )
    }

    @Test
    fun `zero-delay retry reschedules immediately and still increments retryCount`() {
        assertEquals(
            TaskRowOutcome.Reschedule(at = now, taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode()),
            outcomeAfterExecution(ReactionOutcome.Retry(Duration.ZERO), data, now),
        )
    }

    @Test
    fun `unsubscribed reschedules after the delay with task data unchanged`() {
        assertEquals(
            TaskRowOutcome.Reschedule(at = Instant.parse("2026-10-03T10:00:05Z"), taskData = "raw, possibly undecodable"),
            outcomeWhenUnsubscribed("raw, possibly undecodable", now, 5.seconds),
        )
    }

    @Test
    fun `infinite or oversized retry delays are capped at MAX_RETRY_DELAY instead of failing`() {
        val capped = TaskRowOutcome.Reschedule(
            at = now.plusSeconds(MAX_RETRY_DELAY.inWholeSeconds),
            taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode(),
        )
        assertEquals(capped, outcomeAfterExecution(ReactionOutcome.Retry(Duration.INFINITE), data, now))
        assertEquals(capped, outcomeAfterExecution(ReactionOutcome.Retry(1_000_000.days), data, now))
    }
    @Test
    fun `an ordered reaction that gives up with BlockAggregate is parked and flagged`() {
        val ordered = data.copy(ordering = OrderingStamp("Order/o-1", 3, 0, "BlockAggregate", "r-1"))
        val outcome = outcomeAfterExecution(ReactionOutcome.Finished(gaveUp = true), ordered, now)
        outcome as TaskRowOutcome.Reschedule
        assertTrue(outcome.at.isAfter(now.plusSeconds(PARKED_DELAY.inWholeSeconds - 1)))
        assertEquals(true, ReactionTaskData.decode(outcome.taskData).blocked)
    }

    @Test
    fun `an ordered reaction that gives up with ContinueWithNext is removed and nudges the next`() {
        val ordered = data.copy(ordering = OrderingStamp("Order/o-1", 3, 0, "ContinueWithNext", "r-1"))
        assertEquals(TaskRowOutcome.Remove(nudgeKey = "Order/o-1"), outcomeAfterExecution(ReactionOutcome.Finished(true), ordered, now))
    }
}

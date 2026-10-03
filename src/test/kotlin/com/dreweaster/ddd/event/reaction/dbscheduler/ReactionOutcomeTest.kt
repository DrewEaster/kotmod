package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.RetrySignal
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class ReactionOutcomeTest {
    private val now = Instant.parse("2026-10-03T10:00:00Z")
    private val data = ReactionTaskData(trigger = "t", retryCount = 2)

    @Test
    fun `null result removes the row`() {
        assertEquals(ReactionOutcome.Remove, outcomeAfterExecution(null, data, now))
    }

    @Test
    fun `retry reschedules after the delay with retryCount incremented`() {
        assertEquals(
            ReactionOutcome.Reschedule(
                at = Instant.parse("2026-10-03T10:00:30Z"),
                taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode(),
            ),
            outcomeAfterExecution(RetrySignal.Retry(30.seconds), data, now),
        )
    }

    @Test
    fun `zero-delay retry reschedules immediately and still increments retryCount`() {
        assertEquals(
            ReactionOutcome.Reschedule(at = now, taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode()),
            outcomeAfterExecution(RetrySignal.Retry(Duration.ZERO), data, now),
        )
    }

    @Test
    fun `unsubscribed reschedules after the delay with task data unchanged`() {
        assertEquals(
            ReactionOutcome.Reschedule(at = Instant.parse("2026-10-03T10:00:05Z"), taskData = "raw, possibly undecodable"),
            outcomeWhenUnsubscribed("raw, possibly undecodable", now, 5.seconds),
        )
    }
}

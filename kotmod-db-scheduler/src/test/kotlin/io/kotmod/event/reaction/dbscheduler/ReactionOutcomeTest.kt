package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.RetrySignal
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
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

    @Test
    fun `infinite or oversized retry delays are capped at MAX_RETRY_DELAY instead of failing`() {
        val capped = ReactionOutcome.Reschedule(
            at = now.plusSeconds(MAX_RETRY_DELAY.inWholeSeconds),
            taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode(),
        )
        assertEquals(capped, outcomeAfterExecution(RetrySignal.Retry(Duration.INFINITE), data, now))
        assertEquals(capped, outcomeAfterExecution(RetrySignal.Retry(1_000_000.days), data, now))
    }
}

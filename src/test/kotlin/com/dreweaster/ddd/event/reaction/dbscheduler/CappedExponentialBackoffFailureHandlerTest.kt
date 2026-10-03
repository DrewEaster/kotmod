package com.dreweaster.ddd.event.reaction.dbscheduler

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

class CappedExponentialBackoffFailureHandlerTest {
    private val handler = CappedExponentialBackoffFailureHandler(initialDelay = 10.seconds, maximumDelay = 1.hours)

    @Test
    fun `doubles from the initial delay`() {
        assertEquals(10.seconds, handler.delayFor(0))
        assertEquals(20.seconds, handler.delayFor(1))
        assertEquals(2560.seconds, handler.delayFor(8))
    }

    @Test
    fun `caps at the maximum delay`() {
        assertEquals(1.hours, handler.delayFor(9))
        assertEquals(1.hours, handler.delayFor(1_000))
        assertEquals(1.hours, handler.delayFor(Int.MAX_VALUE))
    }
}

package io.kotmod.event.reaction

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class BackoffStrategyTest {
    private val strategy = BackoffStrategy(maximumDuration = 600.seconds)

    @Test
    fun `doubles from one second`() {
        assertEquals(1.seconds, strategy.calculateBackoff(0))
        assertEquals(2.seconds, strategy.calculateBackoff(1))
        assertEquals(512.seconds, strategy.calculateBackoff(9))
    }

    @Test
    fun `caps at maximumDuration`() {
        assertEquals(600.seconds, strategy.calculateBackoff(10))
        assertEquals(600.seconds, strategy.calculateBackoff(100))
        assertEquals(600.seconds, strategy.calculateBackoff(Int.MAX_VALUE))
    }

    @Test
    fun `negative retry count is treated as zero`() {
        assertEquals(1.seconds, strategy.calculateBackoff(-1))
    }
}

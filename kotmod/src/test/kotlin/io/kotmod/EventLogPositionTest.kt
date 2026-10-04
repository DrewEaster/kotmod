package io.kotmod

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EventLogPositionTest {
    @Test
    fun `positions order by transaction id first, then offset`() {
        assertTrue(EventLogPosition(1, 99) < EventLogPosition(2, 1))
        assertTrue(EventLogPosition(2, 1) < EventLogPosition(2, 2))
        assertEquals(0, EventLogPosition(3, 4).compareTo(EventLogPosition(3, 4)))
    }

    @Test
    fun `START is before every real position`() {
        assertEquals(EventLogPosition(0, 0), EventLogPosition.START)
        assertTrue(EventLogPosition.START < EventLogPosition(1, 1))
    }
}

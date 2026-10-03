package com.dreweaster.ddd.event.reaction.dbscheduler

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ReactionTaskDataTest {
    @Test
    fun `encodes to the documented JSON shape`() {
        assertEquals("""{"trigger":"a","retryCount":0}""", ReactionTaskData("a", 0).encode())
    }

    @Test
    fun `round-trips triggers containing quotes, newlines and unicode`() {
        val data = ReactionTaskData(trigger = "{\"name\":\"x\"}\nline two ☃", retryCount = 7)
        assertEquals(data, ReactionTaskData.decode(data.encode()))
    }

    @Test
    fun `decode rejects garbage`() {
        assertFailsWith<IllegalArgumentException> { ReactionTaskData.decode("not json") }
        assertFailsWith<IllegalArgumentException> { ReactionTaskData.decode("""{"trigger":"a"}""") }
    }
}

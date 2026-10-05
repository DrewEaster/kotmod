package io.kotmod.event.reaction.dbscheduler

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

    @Test
    fun `task data written before notBefore existed decodes as not delayed`() {
        assertEquals(ReactionTaskData(trigger = "t", retryCount = 0), ReactionTaskData.decode("""{"trigger":"t","retryCount":0}"""))
    }

    @Test
    fun `notBefore round-trips`() {
        val data = ReactionTaskData(trigger = "t", retryCount = 0, notBeforeEpochMillis = 1_760_000_000_000)
        assertEquals(data, ReactionTaskData.decode(data.encode()))
    }
}

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
    fun `notBefore round-trips with sub-millisecond precision`() {
        val notBefore = kotlin.time.Instant.fromEpochSeconds(1_760_000_000, 123_456_789)
        val data = ReactionTaskData(trigger = "t", retryCount = 0, notBefore = notBefore.toString())
        val decoded = ReactionTaskData.decode(data.encode())
        assertEquals(notBefore, kotlin.time.Instant.parse(decoded.notBefore!!))
    }

    @Test
    fun `notBefore string round-trips`() {
        val data = ReactionTaskData(trigger = "t", retryCount = 0, notBefore = "2026-10-10T09:00:00.123456789Z")
        assertEquals(data, ReactionTaskData.decode(data.encode()))
    }
}

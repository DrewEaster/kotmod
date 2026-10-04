package io.kotmod.event.reaction.dbscheduler

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OrderedIdsTest {
    @Test
    fun `ids sort by sequence then ordinal within a key`() {
        val ids =
            listOf(
                orderedInstanceId("Order/o-1", 10, 0, "r-c"),
                orderedInstanceId("Order/o-1", 2, 1, "r-b"),
                orderedInstanceId("Order/o-1", 2, 0, "r-a"),
            )
        assertEquals(listOf("r-a", "r-b", "r-c"), ids.sorted().map { it.substringAfterLast('#') })
    }

    @Test
    fun `key prefixes never overlap even when ids contain separators`() {
        val a = orderedKeyPrefix("Order/a")
        val tricky = orderedInstanceId("Order/a#0", 1, 0, "r")
        assertTrue(!tricky.startsWith(a), "different keys must never share a prefix")
    }

    @Test
    fun `a key's range holds its own ids and never another key's`() {
        val key = "Order/a"
        val lower = orderedKeyPrefix(key)
        val upper = orderedKeyUpperBound(key)
        fun inRange(id: String) = id >= lower && id < upper

        assertTrue(inRange(orderedInstanceId(key, 0, 0, "r")))
        assertTrue(inRange(orderedInstanceId(key, Long.MAX_VALUE, 9999, "r~~~\uFFFF")))
        val others =
            listOf("Order/a#0", "Order/b", "Order/", "Order/a ", "Order/a$", "Order/a\u0000", "Order/aa", "7:Order/a", "")
        others.forEach { other ->
            assertTrue(!inRange(orderedInstanceId(other, 1, 0, "r")), "id of key '$other' must fall outside '$key's range")
        }
    }
}

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
}

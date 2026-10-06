package io.kotmod

import io.kotmod.support.OrderPlaced
import io.kotmod.support.testOrders
import kotlin.test.Test
import kotlin.test.assertEquals

class AggregateKindTest {
    @Test
    fun `a kind carries its event serialization`() {
        val serialized = testOrders.eventSerialization.serialize(OrderPlaced("book"))

        assertEquals(OrderPlaced("book"), testOrders.eventSerialization.deserialize(serialized))
    }
}

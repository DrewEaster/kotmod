package io.kotmod.readme

import io.kotmod.SerializedEvent
import kotlin.test.Test
import kotlin.test.assertEquals

class ReadmeExamplesTest {
    @Test
    fun `migration example reads events stored under the old class name`() {
        val old = SerializedEvent(type = "com.example.orders.OrderDispatched", version = 1, payload = """{"item":"book"}""")
        assertEquals(OrderShipped("book"), orderEventSerialization.deserialize(old))
    }

    @Test
    fun `migration example fills in the reason for old cancellations`() {
        val old = SerializedEvent(type = OrderCancelled::class.qualifiedName!!, version = 1, payload = """{"item":"book"}""")
        assertEquals(OrderCancelled("book", "not recorded"), orderEventSerialization.deserialize(old))
    }

    @Test
    fun `migration example writes new events at the latest version`() {
        val serialized = orderEventSerialization.serialize(OrderCancelled("book", "changed mind"))
        assertEquals(2, serialized.version)
    }
}

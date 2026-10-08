package io.kotmod.reaction

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

/** Queued items outlive releases: what 0.3.0 stored must still decode, and encode the same way. */
class PolicyItemSerializationTest {
    private val trigger030 = """{"type":"trigger","trigger":"{\"type\":\"io.kotmod.reaction.Confirm\",\"orderId\":\"o-1\"}"}"""
    private val parked030 = """{"type":"parked","eventId":"e-1","aggregateType":"Order","aggregateId":"o-1","source":"aggregate kind Order"}"""

    @Test
    fun `a trigger item queued by 0_3_0 decodes and encodes unchanged`() {
        val item = Json.decodeFromString(PolicyItem.serializer(), trigger030)

        assertEquals(TriggerItem("""{"type":"io.kotmod.reaction.Confirm","orderId":"o-1"}"""), item)
        assertEquals(trigger030, Json.encodeToString(PolicyItem.serializer(), item))
    }

    @Test
    fun `a parked mapping queued by 0_3_0 decodes and encodes unchanged`() {
        val item = Json.decodeFromString(PolicyItem.serializer(), parked030)

        assertEquals(ParkedItem("e-1", "Order", "o-1", "aggregate kind Order"), item)
        assertEquals(parked030, Json.encodeToString(PolicyItem.serializer(), item))
    }
}

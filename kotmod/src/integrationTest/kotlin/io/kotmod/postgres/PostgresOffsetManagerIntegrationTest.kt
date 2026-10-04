package io.kotmod.postgres

import io.kotmod.EventLogPosition
import io.kotmod.postgres.support.IntegrationTest
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals

class PostgresOffsetManagerIntegrationTest : IntegrationTest() {
    private lateinit var offsets: PostgresOffsetManager

    @BeforeEach
    fun createOffsetManager() {
        offsets = PostgresOffsetManager(jdbc)
    }

    @Test
    fun `getPosition returns START for an unknown consumer`() {
        assertEquals(EventLogPosition.START, offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `savePosition then getPosition round-trips`() {
        offsets.savePosition("orders-outbox", EventLogPosition(7, 10))
        assertEquals(EventLogPosition(7, 10), offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `savePosition overwrites the previous position`() {
        offsets.savePosition("orders-outbox", EventLogPosition(7, 10))
        offsets.savePosition("orders-outbox", EventLogPosition(9, 42))
        assertEquals(EventLogPosition(9, 42), offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `positions are independent per consumer`() {
        offsets.savePosition("orders-outbox", EventLogPosition(9, 42))
        offsets.savePosition("public-contract", EventLogPosition(3, 3))

        assertEquals(EventLogPosition(9, 42), offsets.getPosition("orders-outbox"))
        assertEquals(EventLogPosition(3, 3), offsets.getPosition("public-contract"))
        assertEquals(EventLogPosition.START, offsets.getPosition("someone-else"))
    }
}

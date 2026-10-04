package io.kotmod.postgres

import io.kotmod.postgres.support.IntegrationTest
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals

class PostgresOffsetManagerIntegrationTest : IntegrationTest() {
    private lateinit var offsets: PostgresOffsetManager

    @BeforeEach
    fun createOffsetManager() {
        offsets = PostgresOffsetManager(driver)
    }

    @Test
    fun `getOffset returns INITIAL_OFFSET for an unknown consumer`() {
        assertEquals(PostgresOffsetManager.INITIAL_OFFSET, offsets.getOffset("orders-outbox"))
    }

    @Test
    fun `saveOffset then getOffset round-trips`() {
        offsets.saveOffset("orders-outbox", 10)
        assertEquals(10L, offsets.getOffset("orders-outbox"))
    }

    @Test
    fun `saveOffset overwrites the previous offset`() {
        offsets.saveOffset("orders-outbox", 10)
        offsets.saveOffset("orders-outbox", 42)
        assertEquals(42L, offsets.getOffset("orders-outbox"))
    }

    @Test
    fun `offsets are independent per consumer`() {
        offsets.saveOffset("orders-outbox", 42)
        offsets.saveOffset("public-contract", 3)

        assertEquals(42L, offsets.getOffset("orders-outbox"))
        assertEquals(3L, offsets.getOffset("public-contract"))
        assertEquals(PostgresOffsetManager.INITIAL_OFFSET, offsets.getOffset("someone-else"))
    }
}

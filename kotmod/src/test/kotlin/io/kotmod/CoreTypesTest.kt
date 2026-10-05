package io.kotmod

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class CoreTypesTest {
    @Test
    fun `aggregate ids with the same value are equal`() {
        assertEquals(AggregateId("abc"), AggregateId("abc"))
        assertNotEquals(AggregateId("abc"), AggregateId("xyz"))
    }

    @Test
    fun `aggregate types with the same value are equal`() {
        assertEquals(AggregateType("Order"), AggregateType("Order"))
        assertNotEquals(AggregateType("Order"), AggregateType("User"))
    }

    @Test
    fun `command ids with the same value are equal`() {
        assertEquals(CommandId("abc"), CommandId("abc"))
        assertNotEquals(CommandId("abc"), CommandId("xyz"))
    }

    @Test
    fun `correlation ids with the same value are equal`() {
        assertEquals(CorrelationId("abc"), CorrelationId("abc"))
        assertNotEquals(CorrelationId("abc"), CorrelationId("xyz"))
    }

    @Test
    fun `event ids with the same value are equal`() {
        assertEquals(EventId("abc"), EventId("abc"))
        assertNotEquals(EventId("abc"), EventId("xyz"))
    }

    @Test
    fun `AggregateNotFoundException carries type and id`() {
        val ex =
            AggregateNotFoundException(
                aggregateType = AggregateType("Order"),
                aggregateId = AggregateId("o-1"),
            )
        assertEquals(AggregateType("Order"), ex.aggregateType)
        assertEquals(AggregateId("o-1"), ex.aggregateId)
    }

    @Test
    fun `AggregateAlreadyExistsException carries type and id`() {
        val ex =
            AggregateAlreadyExistsException(
                aggregateType = AggregateType("Order"),
                aggregateId = AggregateId("o-1"),
            )
        assertEquals(AggregateType("Order"), ex.aggregateType)
        assertEquals(AggregateId("o-1"), ex.aggregateId)
    }

    @Test
    fun `OptimisticConcurrencyException carries the expected version`() {
        val ex =
            OptimisticConcurrencyException(
                AggregateType("Order"),
                AggregateId("o-1"),
                expectedVersion = 3,
            )
        assertEquals(3, ex.expectedVersion)
    }

    @Test
    fun `value classes reject blank values`() {
        assertFailsWith<IllegalArgumentException> { AggregateId("") }
        assertFailsWith<IllegalArgumentException> { AggregateId("   ") }
        assertFailsWith<IllegalArgumentException> { AggregateType("") }
        assertFailsWith<IllegalArgumentException> { AggregateType("   ") }
        assertFailsWith<IllegalArgumentException> { CommandId("") }
        assertFailsWith<IllegalArgumentException> { CommandId("   ") }
        assertFailsWith<IllegalArgumentException> { CorrelationId("") }
        assertFailsWith<IllegalArgumentException> { CorrelationId("   ") }
        assertFailsWith<IllegalArgumentException> { EventId("") }
        assertFailsWith<IllegalArgumentException> { EventId("   ") }
    }

    @Test
    fun `DomainEvent is a usable empty marker`() {
        val e: DomainEvent = object : DomainEvent {}
        assertEquals(e, e)
    }
}

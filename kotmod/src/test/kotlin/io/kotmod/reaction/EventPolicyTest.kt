package io.kotmod.reaction

import io.kotmod.AggregateType
import io.kotmod.EventMetadata
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.testOrderKind
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class EventPolicyTest {
    private val payments = paymentContract(InMemoryLog())

    private class TwoKinds : EventPolicy<Notice>("two-kinds", Notice.serializer()) {
        init {
            on(testOrders) { _, _ -> }
            on(testOrderKind("Order")) { _, _ -> }
        }

        override suspend fun handle(
            trigger: Notice,
            context: ReactionContext,
        ) = Unit
    }

    private class Defaults : EventPolicy<Notice>("defaults", Notice.serializer()) {
        override suspend fun handle(
            trigger: Notice,
            context: ReactionContext,
        ) = Unit
    }

    @Test
    fun `an aggregate kind source delivers its typed events and their metadata to the block`() {
        val seen = mutableListOf<Pair<OrderEvent, EventMetadata>>()
        val policy =
            RecordingPolicy(mapping = { event, metadata ->
                seen += event to metadata
                trigger(Confirm(metadata.aggregateId.value))
            })
        val event = orderEvent(OrderPlaced("book"), eventId = "e-7", orderId = "o-7")

        val produced = checkNotNull(policy.sourceFor(AggregateType("Order"))).map(event)

        assertEquals(listOf<Pair<OrderEvent, EventMetadata>>(OrderPlaced("book") to event.metadata), seen)
        assertEquals(listOf(ProducedTrigger<Notice>(Confirm("o-7"), null)), produced)
    }

    @Test
    fun `a trigger can be delayed with notBefore`() {
        val at = Instant.parse("2026-10-13T10:00:00Z")
        val policy = RecordingPolicy(mapping = { _, metadata -> trigger(Confirm(metadata.aggregateId.value), notBefore = at) })

        val produced = checkNotNull(policy.sourceFor(AggregateType("Order"))).map(orderEvent(OrderPlaced("book")))

        assertEquals(listOf(ProducedTrigger<Notice>(Confirm("o-1"), at)), produced)
    }

    @Test
    fun `a block that throws produces nothing, even after triggering`() {
        val policy =
            RecordingPolicy(mapping = { _, metadata ->
                trigger(Confirm(metadata.aggregateId.value))
                error("broken mapping")
            })

        assertFailsWith<IllegalStateException> {
            checkNotNull(policy.sourceFor(AggregateType("Order"))).map(orderEvent(OrderPlaced("book")))
        }
    }

    @Test
    fun `a contract source delivers typed public events, and nothing for events the contract keeps private`() {
        val policy = RecordingPolicy(kind = null)
        policy.listenTo(payments) { event, metadata -> trigger(Flag("${event.customerId}@${metadata.aggregateId.value}")) }
        val source = checkNotNull(policy.sourceFor(AggregateType("Payment")))

        assertEquals(listOf(ProducedTrigger<Notice>(Flag("c-1@c-1"), null)), source.map(paymentEvent("c-1", declined = true)))
        assertEquals(emptyList(), source.map(paymentEvent("c-2", declined = false)))
    }

    @Test
    fun `local routing only uses aggregate kind sources`() {
        val policy = RecordingPolicy()
        policy.listenTo(payments) { _, _ -> }

        assertIs<KindSource<*, *>>(policy.kindSourceFor(AggregateType("Order")))
        assertNull(policy.kindSourceFor(AggregateType("Payment")))
        assertIs<ContractSource<*, *>>(policy.sourceFor(AggregateType("Payment")))
    }

    @Test
    fun `the same aggregate type through two aggregate kinds is refused`() {
        val error = assertFailsWith<IllegalArgumentException> { TwoKinds() }

        assertEquals(
            "Event policy two-kinds listens to the same aggregate type through aggregate kind Order and aggregate kind Order; " +
                "an aggregate type can reach a policy through only one source",
            error.message,
        )
    }

    @Test
    fun `a contract over an aggregate type a kind already covers is refused`() {
        val policy = RecordingPolicy()
        val orderContract = paymentContract(InMemoryLog(), aggregateTypes = setOf(AggregateType("Order")))

        assertFailsWith<IllegalArgumentException> { policy.listenTo(orderContract) { _, _ -> } }
    }

    @Test
    fun `a contract without an aggregate type filter covers every type, so it can't be combined with another source`() {
        val policy = RecordingPolicy()

        assertFailsWith<IllegalArgumentException> { policy.listenTo(paymentContract(InMemoryLog(), aggregateTypes = null)) { _, _ -> } }
    }

    @Test
    fun `a source declared after registration is refused`() {
        val policy = RecordingPolicy()
        policy.registered = true

        assertFailsWith<IllegalStateException> { policy.listenTo(payments) { _, _ -> } }
    }

    @Test
    fun `a blank name is refused`() {
        assertFailsWith<IllegalArgumentException> { RecordingPolicy(name = " ") }
    }

    @Test
    fun `defaults are unordered, a 60 second timeout, capped exponential retries that never give up, and a no-op completion`() =
        runBlocking {
            val policy = Defaults()

            assertEquals(ReactionOrdering.Unordered, policy.ordering)
            assertEquals(60.seconds, policy.timeout)
            assertEquals(Retry(1.seconds), policy.onFailure(Confirm("o-1"), 0, RuntimeException()))
            assertEquals(Retry(8.seconds), policy.onFailure(Confirm("o-1"), 3, RuntimeException()))
            assertEquals(Retry(600.seconds), policy.onFailure(Confirm("o-1"), 40, RuntimeException()))
            policy.onCompletion(Confirm("o-1"), ReactionResult.Completed)
        }
}

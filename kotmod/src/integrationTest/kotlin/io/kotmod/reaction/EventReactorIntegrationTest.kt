package io.kotmod.reaction

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.PostgresReactionRows
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.support.OrderPlaced
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Instant

class EventReactorIntegrationTest : IntegrationTest() {
    private class Confirmations : EventPolicy<String>("confirmations", String.serializer()) {
        init {
            on(testOrders) { event, metadata -> if (event is OrderPlaced) trigger(metadata.aggregateId.value) }
        }

        override suspend fun handle(
            trigger: String,
            context: ReactionContext,
        ) = Unit
    }

    /** Records each order it confirms, in the order it confirms them. */
    private class OrderedConfirmations : EventPolicy<String>("ordered-confirmations", String.serializer()) {
        val confirmed = mutableListOf<String>()

        override val ordering = ReactionOrdering.PerAggregate()

        init {
            on(testOrders) { _, metadata -> trigger("${metadata.aggregateId.value}#${metadata.sequence}") }
        }

        override suspend fun handle(
            trigger: String,
            context: ReactionContext,
        ) {
            confirmed += trigger
        }
    }

    private fun appendOrderPlaced(
        eventId: String,
        orderId: String,
        sequence: Long = 1,
    ) {
        PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()).appendEvents(
            listOf(
                PendingEvent(
                    EventMetadata(EventId(eventId), AggregateType("Order"), AggregateId(orderId), CommandId("cmd-$eventId"), null, Instant.parse("2026-10-06T10:00:00Z"), sequence),
                    OrderPlaced("book"),
                ),
            ),
        )
    }

    @Test
    fun `a new reactor starts at the head of the log as of start, and saves its position under its name`() =
        runBlocking {
            appendOrderPlaced("e-1", "o-1")
            val first = EventReactor(jdbc, ManualTaskScheduler(), isLeader = { false })
            first.register(Confirmations())
            first.start()
            first.stop()
            appendOrderPlaced("e-2", "o-2")

            val scheduler = ManualTaskScheduler()
            val reactor = EventReactor(jdbc, scheduler, isLeader = { true })
            reactor.register(Confirmations())
            reactor.tickForTest()

            assertEquals(listOf("confirmations/e-2/0"), scheduler.queue("confirmations").pending.map { it.name })
            assertEquals(2L, PostgresOffsetManager(jdbc).getPosition("reactor").globalOffset)
        }

    @Test
    fun `an ordered event policy keeps its aggregate's line in the reaction table and runs it in order`() =
        runBlocking {
            val scheduler = ManualTaskScheduler()
            val reactor = EventReactor(jdbc, scheduler, isLeader = { true })
            val policy = OrderedConfirmations()
            reactor.register(policy)
            reactor.start()
            reactor.stop() // fixes the starting position; the test drives the reader and the scheduler itself
            reactor.startPoliciesForTest()
            appendOrderPlaced("e-1", "o-1", sequence = 1)
            appendOrderPlaced("e-2", "o-1", sequence = 2)

            reactor.tickForTest()

            val line = PostgresReactionRows(jdbc).list("ordered-confirmations")
            assertEquals(listOf("ordered-confirmations/e-1/0", "ordered-confirmations/e-2/0"), line.map { it.reactionId })
            assertEquals(listOf("Order/o-1", "Order/o-1"), line.map { it.key })
            scheduler.queue("ordered-confirmations").deliverAll()
            assertEquals(listOf("o-1#1", "o-1#2"), policy.confirmed)
            assertEquals(emptyList(), PostgresReactionRows(jdbc).list("ordered-confirmations"))
        }
}

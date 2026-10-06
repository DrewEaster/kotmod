package io.kotmod.reaction

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.process.ManualQueues
import io.kotmod.support.OrderPlaced
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Instant

class EventReactorIntegrationTest : IntegrationTest() {
    private class Confirmations : Reactions<String>("confirmations", String.serializer()) {
        init {
            on(testOrders) { event, metadata -> if (event is OrderPlaced) trigger(metadata.aggregateId.value) }
        }

        override suspend fun handle(
            trigger: String,
            context: ReactionContext,
        ) = Unit
    }

    private fun appendOrderPlaced(
        eventId: String,
        orderId: String,
    ) {
        PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()).appendEvents(
            listOf(
                PendingEvent(
                    EventMetadata(EventId(eventId), AggregateType("Order"), AggregateId(orderId), CommandId("cmd-$eventId"), null, Instant.parse("2026-10-06T10:00:00Z"), 1),
                    OrderPlaced("book"),
                ),
            ),
        )
    }

    @Test
    fun `a new reactor starts at the head of the log as of start, and saves its position under its name`() =
        runBlocking {
            appendOrderPlaced("e-1", "o-1")
            val first = EventReactor(jdbc, ManualQueues(), isLeader = { false })
            first.register(Confirmations())
            first.start()
            first.stop()
            appendOrderPlaced("e-2", "o-2")

            val queues = ManualQueues()
            val reactor = EventReactor(jdbc, queues, isLeader = { true })
            reactor.register(Confirmations())
            reactor.tickForTest()

            assertEquals(listOf("confirmations/e-2/0"), queues.pending("confirmations").map { it.id.value })
            assertEquals(2L, PostgresOffsetManager(jdbc).getPosition("reactor").globalOffset)
        }
}

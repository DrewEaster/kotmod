package io.kotmod.event.reaction

import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.outbox.AggregateEventOutbox
import io.kotmod.support.persistedEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class DelayedReactionsTest {
    private data class FakeTrigger(
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private data class Published(
        val id: EventReactionId,
        val ordering: DispatchOrdering?,
        val notBefore: Instant?,
    )

    private val now = Instant.parse("2026-10-05T10:00:00Z")
    private val published = mutableListOf<Published>()
    private val contextsCreated = AtomicInteger()
    private val executions = AtomicInteger()
    private lateinit var deliver: suspend (EventReactionId, EventReactionExecutionId, FakeTrigger, RetryCount, Instant?) -> ReactionOutcome

    private fun executor(supportsOrdering: Boolean = false) =
        EventReactionExecutor<FakeTrigger, Unit>(
            sink =
                object : EventReactionTriggerSink<FakeTrigger> {
                    override val supportsOrdering = supportsOrdering

                    override suspend fun publish(
                        id: EventReactionId,
                        trigger: FakeTrigger,
                        ordering: DispatchOrdering?,
                        notBefore: Instant?,
                    ) {
                        published += Published(id, ordering, notBefore)
                    }
                },
            source =
                object : EventReactionTriggerSource<FakeTrigger> {
                    override fun subscribe(
                        block: suspend (EventReactionId, EventReactionExecutionId, FakeTrigger, RetryCount, Instant?) -> ReactionOutcome,
                    ): Cancellable {
                        deliver = block
                        return object : Cancellable {
                            override fun cancel() {}
                        }
                    }
                },
            createExecutionContext = { _, _ -> contextsCreated.incrementAndGet() },
            execute = { _, _, _, _, _ ->
                executions.incrementAndGet()
                EventReactionExecutionResult.EventReactionExecutionCompleted
            },
            failureRetryHandler = { _, _, _, _, _, _ -> RetrySignal.Retry(1.seconds) },
            timeoutRetryHandler = { _, _, _, _, _ -> RetrySignal.Retry(1.seconds) },
            onCompletion = { _, _, _, _, _, _ -> },
            clock = { now },
        )

    private class OneEventBackend(
        private val event: PersistedEvent,
    ) : DomainEventPollingBackend {
        override fun readEventsAfter(
            position: EventLogPosition,
            limit: Int,
        ): List<PersistedEvent> = if (position == EventLogPosition.START) listOf(event) else emptyList()
    }

    @Test
    fun `dispatch hands notBefore to the sink`() =
        runBlocking {
            executor().dispatch(EventReactionId("r-1"), FakeTrigger(), notBefore = now + 60.seconds)

            assertEquals(listOf(Published(EventReactionId("r-1"), null, now + 60.seconds)), published)
        }

    @Test
    fun `dispatch refuses a reaction that is both ordered and delayed`() =
        runBlocking {
            val failure =
                assertFailsWith<IllegalArgumentException> {
                    executor(supportsOrdering = true).dispatch(
                        EventReactionId("r-1"),
                        FakeTrigger(),
                        ordering = DispatchOrdering("Order/o-1", 1, 0, OnGiveUp.ContinueWithNext),
                        notBefore = now + 60.seconds,
                    )
                }

            assertEquals(true, failure.message!!.contains("r-1"))
            assertEquals(emptyList(), published)
        }

    @Test
    fun `a delivery before notBefore waits without running and without counting a retry`() =
        runBlocking {
            executor().start()

            val outcome = deliver(EventReactionId("r-1"), EventReactionExecutionId("x-1"), FakeTrigger(), 0, now + 90.seconds)

            assertEquals(ReactionOutcome.Wait(90.seconds), outcome)
            assertEquals(0, contextsCreated.get())
            assertEquals(0, executions.get())
        }

    @Test
    fun `a delivery at or after notBefore runs normally`() =
        runBlocking {
            executor().start()

            assertEquals(
                ReactionOutcome.Finished(gaveUp = false),
                deliver(EventReactionId("r-1"), EventReactionExecutionId("x-1"), FakeTrigger(), 0, now),
            )
            assertEquals(1, executions.get())
        }

    @Test
    fun `an outbox passes a reaction's notBefore to dispatch`() =
        runBlocking {
            val outbox =
                AggregateEventOutbox(
                    backend = OneEventBackend(persistedEvent(globalOffset = 1)),
                    executor = executor(),
                    eventToReactions = { listOf(EventReaction(EventReactionId("remind"), FakeTrigger(), notBefore = now + 7.seconds)) },
                    getPosition = { EventLogPosition.START },
                    savePosition = {},
                    isLeader = { true },
                )

            outbox.tickForTest()

            assertEquals(listOf(Published(EventReactionId("remind"), null, now + 7.seconds)), published)
        }

    @Test
    fun `an ordered outbox fails loudly on a delayed reaction`() =
        runBlocking {
            val outbox =
                AggregateEventOutbox(
                    backend = OneEventBackend(persistedEvent(globalOffset = 1)),
                    executor = executor(supportsOrdering = true),
                    eventToReactions = { listOf(EventReaction(EventReactionId("remind"), FakeTrigger(), notBefore = now + 7.seconds)) },
                    getPosition = { EventLogPosition.START },
                    savePosition = {},
                    isLeader = { true },
                    ordering = ReactionOrdering.PerAggregate(),
                )

            assertFailsWith<IllegalArgumentException> { outbox.tickForTest() }
            assertEquals(emptyList(), published)
        }

    private data class Internal(
        val id: String,
    ) : DomainEvent

    private data class Public(
        val id: String,
    ) : PublicDomainEvent

    @Test
    fun `a contract subscription passes a reaction's notBefore to dispatch`() =
        runBlocking {
            val contract =
                PublicEventContract<Internal, Public>(
                    backend = OneEventBackend(persistedEvent(globalOffset = 1)),
                    serialization =
                        object : DataSerializationContext<Internal> {
                            override fun serialize(event: Internal) = SerializedEvent("Internal", 1, event.id)

                            override fun deserialize(serialized: SerializedEvent) = Internal(serialized.payload)
                        },
                    internalToPublic = { Public(it.id) },
                    getPosition = { EventLogPosition.START },
                    savePosition = {},
                    isLeader = { true },
                )
            contract.subscribe(executor()) { listOf(EventReaction(EventReactionId("later"), FakeTrigger(), notBefore = now + 3.seconds)) }

            contract.tickForTest()

            assertEquals(listOf(Published(EventReactionId("later"), null, now + 3.seconds)), published)
        }
}

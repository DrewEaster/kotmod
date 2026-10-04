package io.kotmod.event.reaction

import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventLogPosition
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.outbox.AggregateEventOutbox
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration

class OrderedSourceGuardTest {
    private data class FakeTrigger(
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private data class Internal(
        val id: String,
    ) : DomainEvent

    private data class Public(
        val id: String,
    ) : PublicDomainEvent

    private val backend: DomainEventPollingBackend = mockk()

    private fun orderedExecutor() =
        EventReactionExecutor<FakeTrigger, Unit>(
            sink =
                object : EventReactionTriggerSink<FakeTrigger> {
                    override val supportsOrdering = true

                    override suspend fun publish(
                        id: EventReactionId,
                        trigger: FakeTrigger,
                        ordering: DispatchOrdering?,
                    ) {}
                },
            source = mockk(),
            createExecutionContext = { _, _ -> },
            execute = { _, _, _, _, _ -> EventReactionExecutionResult.EventReactionExecutionCompleted },
            failureRetryHandler = { _, _, _, _, _, _ -> RetrySignal.Retry(Duration.ZERO) },
            timeoutRetryHandler = { _, _, _, _, _ -> RetrySignal.Retry(Duration.ZERO) },
            onCompletion = { _, _, _, _, _, _ -> },
        )

    private fun outbox(
        executor: EventReactionExecutor<FakeTrigger, Unit>,
        ordering: ReactionOrdering = ReactionOrdering.PerAggregate(),
    ) = AggregateEventOutbox(
        backend = backend,
        executor = executor,
        eventToReactions = { emptyList() },
        getPosition = { EventLogPosition.START },
        savePosition = {},
        isLeader = { true },
        ordering = ordering,
    )

    private fun contract() =
        PublicEventContract<Internal, Public>(
            backend = backend,
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

    @Test
    fun `a second ordered outbox on the same executor fails at construction`() {
        val executor = orderedExecutor()
        outbox(executor)

        assertFailsWith<IllegalArgumentException> { outbox(executor) }
    }

    @Test
    fun `an ordered outbox and an ordered contract cannot share an executor`() {
        val executor = orderedExecutor()
        outbox(executor)

        assertFailsWith<IllegalArgumentException> {
            contract().subscribe(executor, ordering = ReactionOrdering.PerAggregate()) { emptyList() }
        }
    }

    @Test
    fun `one contract may have several ordered subscriptions on one executor`() {
        val executor = orderedExecutor()
        val contract = contract()

        contract.subscribe(executor, ordering = ReactionOrdering.PerAggregate()) { emptyList() }
        contract.subscribe(executor, ordering = ReactionOrdering.PerAggregate()) { emptyList() }
    }

    @Test
    fun `unordered sources do not claim the executor`() {
        val executor = orderedExecutor()
        outbox(executor, ordering = ReactionOrdering.Unordered)
        contract().subscribe(executor) { emptyList() }

        outbox(executor)
    }
}

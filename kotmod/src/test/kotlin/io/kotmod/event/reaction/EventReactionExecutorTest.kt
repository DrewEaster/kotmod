package io.kotmod.event.reaction

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class EventReactionExecutorTest {
    private data class FakeTrigger(
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private lateinit var subscribed: suspend (EventReactionId, EventReactionExecutionId, FakeTrigger, RetryCount, Instant?) -> ReactionOutcome
    private val failureRetryCalls = AtomicInteger()
    private val timeoutRetryCalls = AtomicInteger()

    private val source =
        object : EventReactionTriggerSource<FakeTrigger> {
            override fun subscribe(
                block: suspend (EventReactionId, EventReactionExecutionId, FakeTrigger, RetryCount, Instant?) -> ReactionOutcome,
            ): Cancellable {
                subscribed = block
                return object : Cancellable {
                    override fun cancel() {}
                }
            }
        }

    private val sink =
        object : EventReactionTriggerSink<FakeTrigger> {
            override suspend fun publish(
                id: EventReactionId,
                trigger: FakeTrigger,
                ordering: DispatchOrdering?,
                notBefore: Instant?,
            ) {}
        }

    private fun startExecutor(execute: suspend () -> EventReactionExecutionResult) =
        EventReactionExecutor<FakeTrigger, Unit>(
            sink = sink,
            source = source,
            createExecutionContext = { _, _ -> },
            execute = { _, _, _, _, _ -> execute() },
            failureRetryHandler = { _, _, _, _, _, _ ->
                failureRetryCalls.incrementAndGet()
                RetrySignal.DoNotRetry(EventReactionCompletionResult.EventReactionFailed("failed", allowManualRetry = false))
            },
            timeoutRetryHandler = { _, _, _, _, _ ->
                timeoutRetryCalls.incrementAndGet()
                RetrySignal.Retry(1.seconds)
            },
            onCompletion = { _, _, _, _, _, _ -> },
        ).also { it.start() }

    private suspend fun runReaction(trigger: FakeTrigger = FakeTrigger()) =
        subscribed(EventReactionId("r-1"), EventReactionExecutionId("x-1"), trigger, 0, null)

    @Test
    fun `a completed reaction finishes without giving up`() =
        runBlocking {
            startExecutor { EventReactionExecutionResult.EventReactionExecutionCompleted }
            assertEquals(ReactionOutcome.Finished(gaveUp = false), runReaction())
        }

    @Test
    fun `a failed reaction that is not retried finishes as given up`() =
        runBlocking {
            startExecutor { EventReactionExecutionResult.EventReactionFailed(RuntimeException("boom")) }
            assertEquals(ReactionOutcome.Finished(gaveUp = true), runReaction())
        }

    @Test
    fun `cancelling a running reaction propagates instead of being reported as a failure`() =
        runBlocking {
            val started = CompletableDeferred<Unit>()
            startExecutor {
                started.complete(Unit)
                awaitCancellation()
            }

            val reaction = launch { runReaction() }
            started.await()
            reaction.cancelAndJoin()

            assertEquals(0, failureRetryCalls.get())
        }

    @Test
    fun `a reaction exceeding its timeout still goes to the timeout retry handler`() =
        runBlocking {
            startExecutor {
                delay(10.seconds)
                EventReactionExecutionResult.EventReactionExecutionCompleted
            }

            val result = runReaction(FakeTrigger(timeout = 50.milliseconds))

            assertEquals(ReactionOutcome.Retry(1.seconds), result)
            assertEquals(1, timeoutRetryCalls.get())
            assertEquals(0, failureRetryCalls.get())
        }
}

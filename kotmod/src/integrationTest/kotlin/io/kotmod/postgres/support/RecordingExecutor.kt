package io.kotmod.postgres.support

import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.event.reaction.EventReactionCompletionResult
import io.kotmod.event.reaction.EventReactionExecutionId
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.RetryCount
import io.kotmod.event.reaction.RetrySignal

/**
 * Builds a minimal [EventReactionExecutor] whose sink hands every dispatch to [onDispatch].
 *
 * We avoid MockK for [EventReactionExecutor.dispatch] because [EventReactionId] is a
 * `@JvmInline value class` — at the JVM level it erases to [String], which trips up
 * MockK argument capture. A plain sink lambda sidesteps that entirely.
 */
fun <T : EventReactionTrigger> recordingExecutor(onDispatch: suspend (EventReactionId, T) -> Unit): EventReactionExecutor<T, Unit> {
    val sink =
        object : EventReactionTriggerSink<T> {
            override suspend fun publish(
                id: EventReactionId,
                trigger: T,
                ordering: DispatchOrdering?,
            ) = onDispatch(id, trigger)
        }
    val source =
        object : EventReactionTriggerSource<T> {
            override fun subscribe(block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> ReactionOutcome) =
                object : Cancellable {
                    override fun cancel() {}
                }
        }
    val doNotRetry = RetrySignal.DoNotRetry(EventReactionCompletionResult.EventReactionCompleted)
    return EventReactionExecutor(
        sink = sink,
        source = source,
        createExecutionContext = { _, _ -> },
        execute = { _, _, _, _, _ -> EventReactionExecutionResult.EventReactionExecutionCompleted },
        failureRetryHandler = { _, _, _, _, _, _ -> doNotRetry },
        timeoutRetryHandler = { _, _, _, _, _ -> doNotRetry },
        onCompletion = { _, _, _, _, _, _ -> },
    )
}

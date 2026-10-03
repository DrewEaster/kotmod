package com.dreweaster.ddd.event.reaction

import com.dreweaster.ddd.event.reaction.EventReactionExecutionResult.EventReactionCancelled
import com.dreweaster.ddd.event.reaction.EventReactionExecutionResult.EventReactionExecutionCompleted
import com.dreweaster.ddd.event.reaction.EventReactionExecutionResult.EventReactionFailed
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

typealias RetryCount = Int

interface EventReactionTrigger {
    val timeout: Duration?
}

/**
 * A reaction to dispatch for an event. [id] must be deterministic for a given (event, reaction kind) —
 * typically built from the event's `eventId` plus a label, e.g. `EventReactionId("charge-${eventId}")` —
 * so that re-dispatching the same event after a crash is recognised as a duplicate. A random id would
 * create a second reaction.
 */
data class EventReaction<T : EventReactionTrigger>(
    val id: EventReactionId,
    val trigger: T,
)

@JvmInline
value class EventReactionId(
    val value: String,
)

@JvmInline
value class EventReactionExecutionId(
    val value: String,
)

interface EventReactionTriggerSerializer<T : EventReactionTrigger> {
    suspend fun serialize(trigger: T): String

    suspend fun deserialize(serializedTrigger: String): T
}

sealed interface EventReactionExecutionResult {
    data object EventReactionExecutionCompleted : EventReactionExecutionResult

    data object EventReactionCancelled : EventReactionExecutionResult

    data class EventReactionFailed(
        val ex: Throwable,
    ) : EventReactionExecutionResult

    data object EventReactionTimedOut : EventReactionExecutionResult
}

sealed interface EventReactionCompletionResult {
    data object EventReactionCompleted : EventReactionCompletionResult

    data class EventReactionFailed(
        val errorMessage: String,
        val allowManualRetry: Boolean,
    ) : EventReactionCompletionResult

    data object EventReactionCancelled : EventReactionCompletionResult
}

sealed interface RetrySignal {
    data class Retry(
        val delay: Duration,
    ) : RetrySignal

    data class DoNotRetry(
        val completionResult: EventReactionCompletionResult,
    ) : RetrySignal
}

interface EventReactionTriggerSink<T : EventReactionTrigger> {
    suspend fun publish(
        id: EventReactionId,
        trigger: T,
    )
}

interface Cancellable {
    fun cancel()
}

interface EventReactionTriggerSource<T : EventReactionTrigger> {
    fun subscribe(block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> RetrySignal.Retry?): Cancellable
}

class BackoffStrategy(
    private val maximumDuration: Duration = 600.seconds,
) {
    fun calculateBackoff(retryCount: RetryCount): Duration {
        // 2^30 seconds is decades — far beyond any sensible cap — and keeps the shift from overflowing.
        val exponent = retryCount.coerceIn(0, 30)
        return minOf((1L shl exponent).seconds, maximumDuration)
    }
}

class EventReactionExecutor<T : EventReactionTrigger, ExecutionContext>(
    private val sink: EventReactionTriggerSink<T>,
    private val source: EventReactionTriggerSource<T>,
    private val createExecutionContext: suspend (EventReactionId, T) -> ExecutionContext,
    private val execute: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, ExecutionContext) -> EventReactionExecutionResult,
    private val failureRetryHandler: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, ExecutionContext, Throwable) -> RetrySignal,
    private val timeoutRetryHandler: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, ExecutionContext) -> RetrySignal,
    private val onCompletion: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, ExecutionContext, EventReactionCompletionResult) -> Unit,
    private val defaultTimeout: Duration = 60.seconds,
    private val defaultBackoffStrategy: BackoffStrategy = BackoffStrategy()
) {
    private val log = LoggerFactory.getLogger(EventReactionExecutor::class.java)

    private var subscribeJob: Cancellable? = null

    suspend fun dispatch(id: EventReactionId, trigger: T) {
        sink.publish(id, trigger)
    }

    fun start() {
        log.info("Starting event reaction executor")
        subscribeJob =
            source.subscribe { id, executionId, trigger, retryCount ->
                val executionContext = createExecutionContext(id, trigger)

                runCatching {
                    withTimeout(trigger.timeout ?: defaultTimeout) {
                        execute(id, executionId, trigger, retryCount, executionContext)
                    }
                }.getOrElse { ex ->
                    when (ex) {
                        is TimeoutCancellationException -> EventReactionExecutionResult.EventReactionTimedOut
                        // The reaction itself was cancelled (e.g. the scheduler interrupted it on shutdown):
                        // not an outcome to report — let the source deal with the interrupted execution.
                        is CancellationException -> throw ex
                        else -> EventReactionFailed(ex)
                    }
                }.let { result ->
                    when (result) {
                        is EventReactionExecutionCompleted -> {
                            runCatching {
                                onCompletion(id, executionId, trigger, retryCount, executionContext,
                                    EventReactionCompletionResult.EventReactionCompleted)
                                null
                            }.getOrElse { ex ->
                                log.error(
                                    "Exception when executing completion handler for event reaction ${id.value} [ totalRetries=$retryCount ]",
                                    ex,
                                )
                                RetrySignal.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                            }
                        }
                        is EventReactionCancelled -> {
                            runCatching {
                                onCompletion(id, executionId, trigger, retryCount, executionContext,
                                    EventReactionCompletionResult.EventReactionCancelled)
                            }.onFailure { ex ->
                                log.error(
                                    "Exception when executing cancellation completion handler for event reaction ${id.value} — not retrying",
                                    ex,
                                )
                            }
                            null
                        }
                        is EventReactionFailed -> {
                            log.error("Event reaction ${id.value} failed [ totalRetries=$retryCount ]", result.ex)
                            runCatching { failureRetryHandler(id, executionId, trigger, retryCount, executionContext, result.ex) }
                                .map { retryHandlingResult ->
                                    when (retryHandlingResult) {
                                        is RetrySignal.Retry -> {
                                            log.warn("Event reaction ${id.value} will be retried after failure [ totalRetries=$retryCount ]")
                                            retryHandlingResult
                                        }
                                        is RetrySignal.DoNotRetry -> {
                                            log.warn("Event reaction ${id.value} will not be retried after failure [ totalRetries=$retryCount ]")
                                            onCompletion(
                                                id,
                                                executionId,
                                                trigger,
                                                retryCount,
                                                executionContext,
                                                retryHandlingResult.completionResult,
                                            )
                                            null
                                        }
                                    }
                                }.getOrElse { ex ->
                                    log.error(
                                        "Exception when applying event reaction ${id.value} retry handling logic. Event reaction will be retried [ totalRetries=$retryCount ]",
                                        ex,
                                    )
                                    RetrySignal.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                                }
                        }
                        is EventReactionExecutionResult.EventReactionTimedOut -> {
                            log.error("Event reaction ${id.value} timed out")
                            runCatching { timeoutRetryHandler(id, executionId, trigger, retryCount, executionContext) }
                                .map { retryHandlingResult ->
                                    when (retryHandlingResult) {
                                        is RetrySignal.Retry -> {
                                            log.warn("Event reaction ${id.value} will be retried after timeout [ totalRetries=$retryCount ]")
                                            retryHandlingResult
                                        }
                                        is RetrySignal.DoNotRetry -> {
                                            log.warn("Event reaction ${id.value} will not be retried after timeout [ totalRetries=$retryCount ]")
                                            onCompletion(
                                                id,
                                                executionId,
                                                trigger,
                                                retryCount,
                                                executionContext,
                                                retryHandlingResult.completionResult,
                                            )
                                            null
                                        }
                                    }
                                }.getOrElse { ex ->
                                    log.error(
                                        "Exception when applying event reaction ${id.value} timeout retry handling logic. Event reaction will be retried [ totalRetries=$retryCount ]",
                                        ex,
                                    )
                                    RetrySignal.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                                }
                        }
                    }
                }
            }
    }

    fun stop() {
        log.info("Stopping event reaction executor")
        subscribeJob?.cancel()
        subscribeJob = null
    }
}

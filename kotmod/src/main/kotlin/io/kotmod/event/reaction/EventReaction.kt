package io.kotmod.event.reaction

import io.kotmod.event.reaction.EventReactionExecutionResult.EventReactionCancelled
import io.kotmod.event.reaction.EventReactionExecutionResult.EventReactionExecutionCompleted
import io.kotmod.event.reaction.EventReactionExecutionResult.EventReactionFailed
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** How many times an event reaction has already been retried; 0 on the first attempt. */
typealias RetryCount = Int

/**
 * The input to an event reaction: what the reaction should do, as data. Implementations are app-defined
 * and must be serializable with an [EventReactionTriggerSerializer].
 *
 * @property timeout how long one execution may run; `null` uses the executor's default.
 */
interface EventReactionTrigger {
    val timeout: Duration?
}

/**
 * A reaction to dispatch for an event. [id] must be deterministic for a given (event, reaction kind) —
 * typically the event's id plus a label, e.g. `EventReactionId("charge-${eventId}")` — so that
 * re-dispatching the same event after a crash is recognised as a duplicate. A random id would create a
 * second reaction.
 */
data class EventReaction<T : EventReactionTrigger>(
    val id: EventReactionId,
    val trigger: T,
)

/** Identifies one event reaction across all of its retries. */
@JvmInline
value class EventReactionId(
    val value: String,
)

/** Identifies one attempt at running an event reaction; different on every retry. */
@JvmInline
value class EventReactionExecutionId(
    val value: String,
)

/** Converts triggers to and from the string form stored by an [EventReactionTriggerSink]. */
interface EventReactionTriggerSerializer<T : EventReactionTrigger> {
    /** Converts [trigger] to its stored string form. */
    suspend fun serialize(trigger: T): String

    /** Rebuilds a trigger from the string produced by [serialize]. */
    suspend fun deserialize(serializedTrigger: String): T
}

/** What the app's `execute` function reports after one attempt at a reaction. */
sealed interface EventReactionExecutionResult {
    /** The reaction did its work. */
    data object EventReactionExecutionCompleted : EventReactionExecutionResult

    /** The reaction decided it no longer needs to run; it is completed as cancelled and not retried. */
    data object EventReactionCancelled : EventReactionExecutionResult

    /** The attempt failed with [ex]; the executor's failure retry handler decides whether to retry. */
    data class EventReactionFailed(
        val ex: Throwable,
    ) : EventReactionExecutionResult

    /** The attempt exceeded its timeout; the executor's timeout retry handler decides whether to retry. */
    data object EventReactionTimedOut : EventReactionExecutionResult
}

/** How an event reaction finally ended, passed to the executor's `onCompletion` handler. */
sealed interface EventReactionCompletionResult {
    /** The reaction succeeded. */
    data object EventReactionCompleted : EventReactionCompletionResult

    /** The reaction failed for good. [allowManualRetry] tells the app whether a person may sensibly retry it. */
    data class EventReactionFailed(
        val errorMessage: String,
        val allowManualRetry: Boolean,
    ) : EventReactionCompletionResult

    /** The reaction was cancelled. */
    data object EventReactionCancelled : EventReactionCompletionResult
}

/** A retry handler's decision after a failed or timed-out attempt. */
sealed interface RetrySignal {
    /** Run the reaction again after [delay], with the retry count incremented. */
    data class Retry(
        val delay: Duration,
    ) : RetrySignal

    /** Stop retrying and complete the reaction with [completionResult]. */
    data class DoNotRetry(
        val completionResult: EventReactionCompletionResult,
    ) : RetrySignal
}

/** Accepts dispatched reactions for later execution, e.g. by storing them in a durable queue. */
interface EventReactionTriggerSink<T : EventReactionTrigger> {
    /** Whether this sink can run reactions in order; only such sinks are passed an [ordering] stamp. */
    val supportsOrdering: Boolean get() = false

    /**
     * Queues reaction [id] with [trigger]. Publishing an id that is already queued must not queue it twice.
     * An ordering stamp is only passed to sinks that support ordering; such sinks must run reactions with the
     * same key one at a time, in (sequence, ordinal) order.
     */
    suspend fun publish(
        id: EventReactionId,
        trigger: T,
        ordering: DispatchOrdering?,
    )
}

/** A handle for undoing a subscription. */
interface Cancellable {
    /** Undoes the subscription. Calling it more than once has no further effect. */
    fun cancel()
}

/** Delivers queued reactions to the [EventReactionExecutor] that subscribed to it. */
interface EventReactionTriggerSource<T : EventReactionTrigger> {
    /**
     * Starts delivering reactions to [block], which runs one attempt and returns a [ReactionOutcome]:
     * [ReactionOutcome.Retry] if the reaction should run again, or [ReactionOutcome.Finished] once it is done.
     * Returns a handle that stops delivery.
     */
    fun subscribe(block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> ReactionOutcome): Cancellable
}

/** Exponential backoff for retries the executor schedules itself: 1s, 2s, 4s… capped at [maximumDuration]. */
class BackoffStrategy(
    private val maximumDuration: Duration = 600.seconds,
) {
    /** Returns the delay before retry number [retryCount] + 1. */
    fun calculateBackoff(retryCount: RetryCount): Duration {
        // 2^30 seconds is decades — far beyond any sensible cap — and keeps the shift from overflowing.
        val exponent = retryCount.coerceIn(0, 30)
        return minOf((1L shl exponent).seconds, maximumDuration)
    }
}

/**
 * Runs event reactions with timeouts, retries and a final completion callback, on top of a queue made of
 * a [sink] (where reactions are dispatched) and a [source] (which delivers them back for execution).
 *
 * For each delivered reaction it creates an execution context, runs [execute] within the trigger's
 * timeout (or [defaultTimeout]), then:
 * - **completed or cancelled:** calls [onCompletion] with the result;
 * - **failed or timed out:** asks [failureRetryHandler] or [timeoutRetryHandler] whether to retry, and
 *   calls [onCompletion] if it says not to.
 *
 * If a retry handler throws, or [onCompletion] throws for a completed or failed reaction, the reaction
 * is retried after a [defaultBackoffStrategy] delay ([onCompletion] failures for cancelled reactions are
 * only logged). If [createExecutionContext] throws, or the attempt itself is cancelled (e.g. the
 * scheduler is shutting down), no handler is called and the exception is passed back to the [source].
 * Delivery is at-least-once, so [execute] and [onCompletion] must be idempotent.
 *
 * @param ExecutionContext app-defined per-attempt context, created by [createExecutionContext].
 */
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

    /** Whether this executor's sink can run reactions in order. */
    val supportsOrdering: Boolean get() = sink.supportsOrdering

    /** Queues reaction [id] with [trigger] for execution, stamped with [ordering] if given. */
    suspend fun dispatch(id: EventReactionId, trigger: T, ordering: DispatchOrdering? = null) {
        require(ordering == null || sink.supportsOrdering) { "This executor's sink does not support ordering" }
        sink.publish(id, trigger, ordering)
    }

    /** Subscribes to the [source] so reactions start executing. */
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
                                ReactionOutcome.Finished(gaveUp = false)
                            }.getOrElse { ex ->
                                log.error(
                                    "Exception when executing completion handler for event reaction ${id.value} [ totalRetries=$retryCount ]",
                                    ex,
                                )
                                ReactionOutcome.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
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
                            ReactionOutcome.Finished(gaveUp = false)
                        }
                        is EventReactionFailed -> {
                            log.error("Event reaction ${id.value} failed [ totalRetries=$retryCount ]", result.ex)
                            runCatching { failureRetryHandler(id, executionId, trigger, retryCount, executionContext, result.ex) }
                                .map { retryHandlingResult ->
                                    when (retryHandlingResult) {
                                        is RetrySignal.Retry -> {
                                            log.warn("Event reaction ${id.value} will be retried after failure [ totalRetries=$retryCount ]")
                                            ReactionOutcome.Retry(retryHandlingResult.delay)
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
                                            ReactionOutcome.Finished(
                                                gaveUp = retryHandlingResult.completionResult is EventReactionCompletionResult.EventReactionFailed,
                                            )
                                        }
                                    }
                                }.getOrElse { ex ->
                                    log.error(
                                        "Exception when applying event reaction ${id.value} retry handling logic. Event reaction will be retried [ totalRetries=$retryCount ]",
                                        ex,
                                    )
                                    ReactionOutcome.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                                }
                        }
                        is EventReactionExecutionResult.EventReactionTimedOut -> {
                            log.error("Event reaction ${id.value} timed out")
                            runCatching { timeoutRetryHandler(id, executionId, trigger, retryCount, executionContext) }
                                .map { retryHandlingResult ->
                                    when (retryHandlingResult) {
                                        is RetrySignal.Retry -> {
                                            log.warn("Event reaction ${id.value} will be retried after timeout [ totalRetries=$retryCount ]")
                                            ReactionOutcome.Retry(retryHandlingResult.delay)
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
                                            ReactionOutcome.Finished(
                                                gaveUp = retryHandlingResult.completionResult is EventReactionCompletionResult.EventReactionFailed,
                                            )
                                        }
                                    }
                                }.getOrElse { ex ->
                                    log.error(
                                        "Exception when applying event reaction ${id.value} timeout retry handling logic. Event reaction will be retried [ totalRetries=$retryCount ]",
                                        ex,
                                    )
                                    ReactionOutcome.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                                }
                        }
                    }
                }
            }
    }

    /** Unsubscribes from the [source]; reactions delivered afterwards are not executed by this executor. */
    fun stop() {
        log.info("Stopping event reaction executor")
        subscribeJob?.cancel()
        subscribeJob = null
    }
}

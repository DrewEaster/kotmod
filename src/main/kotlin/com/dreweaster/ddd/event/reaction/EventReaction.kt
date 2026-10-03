package com.dreweaster.ddd.infrastructure.task

import com.dreweaster.ddd.infrastructure.task.TaskExecutionResult.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

typealias RetryCount = Int

@JvmInline
value class DomainEventReactionId(
    val value: String,
)

@JvmInline
value class DomainEventReactionExecutionId(
    val value: String,
)

interface TaskTrigger {
    val timeout: Duration?
}

interface TaskTriggerSerializer<T : TaskTrigger> {
    suspend fun serialize(trigger: T): String

    suspend fun deserialize(serializedTrigger: String): T
}

sealed interface TaskExecutionResult {
    data object TaskCompleted : TaskExecutionResult

    data object TaskCancelled : TaskExecutionResult

    data class TaskFailed(
        val ex: Throwable,
    ) : TaskExecutionResult

    data object TaskTimedOut : TaskExecutionResult

    data class TaskDiscarded(
        val reason: String,
    ) : TaskExecutionResult
}

sealed interface TaskCompletionResult {
    data object TaskCompleted : TaskCompletionResult

    data class TaskFailed(
        val errorMessage: String,
        val allowManualRetry: Boolean,
    ) : TaskCompletionResult

    data object TaskCancelled : TaskCompletionResult
}

sealed interface RetrySignal {
    data class Retry(
        val delay: Duration,
    ) : RetrySignal

    data class DoNotRetry(
        val completionResult: TaskCompletionResult,
    ) : RetrySignal
}

interface TaskTriggerSink<T : TaskTrigger> {
    suspend fun publish(
        id: TaskId,
        trigger: T,
    )
}

interface Cancellable {
    fun cancel()
}

interface TaskTriggerSource<T : TaskTrigger> {
    fun subscribe(block: suspend (TaskId, TaskExecutionId, T, RetryCount) -> RetrySignal.Retry?): Cancellable
}

class BackoffStrategy(
    private val maximumDuration: Duration = 600.seconds,
) {
    fun calculateBackoff(retryCount: RetryCount): Duration {
        val backoffSeconds = (1L shl retryCount.coerceAtMost(maximumDuration.inWholeSeconds.toInt()))
        return backoffSeconds.seconds
    }
}

/**
 * Configuration for the optional dispatch deduplication feature of
 * [AsyncTaskRunner]. When [Enabled], `dispatch` consults the supplied
 * [DispatchLog] before publishing — if the TaskId has already been
 * dispatched, the publish is skipped. The runner also launches a background
 * cleanup coroutine, gated by [Enabled.isLeader], that periodically deletes
 * log entries older than [Enabled.retention].
 *
 * **Best-effort, not strict single-dispatch.** The
 * `hasBeenDispatched` → `sink.publish` → `recordDispatched` sequence is not
 * atomic: two concurrent dispatches of the same `TaskId` can both observe
 * `hasBeenDispatched = false`, both publish to the sink, and then both
 * attempt to record (the second insert is absorbed by the log's
 * `ON CONFLICT DO NOTHING`, but the sink has already been hit twice).
 * Dedup reliably collapses the common cases — retries and
 * replay-after-restart — but does not guarantee the sink is hit at most
 * once under concurrent callers. Downstream task handlers must remain
 * idempotent regardless.
 *
 * Default is [Disabled] — preserves the existing behavior of all pre-existing
 * `AsyncTaskRunner(...)` construction sites.
 */
sealed interface DispatchDedupConfig {
    data object Disabled : DispatchDedupConfig

    data class Enabled(
        val log: DispatchLog,
        val retention: Duration = 7.days,
        val cleanupInterval: Duration = 1.hours,
        val isLeader: () -> Boolean = { true },
    ) : DispatchDedupConfig
}

class AsyncTaskRunner<T : TaskTrigger, ExecutionContext>(
    private val sink: TaskTriggerSink<T>,
    private val source: TaskTriggerSource<T>,
    private val createExecutionContext: suspend (TaskId, T) -> ExecutionContext,
    private val execute: suspend (TaskId, TaskExecutionId, T, RetryCount, ExecutionContext) -> TaskExecutionResult,
    private val failureRetryHandler: suspend (TaskId, TaskExecutionId, T, RetryCount, ExecutionContext, Throwable) -> RetrySignal,
    private val timeoutRetryHandler: suspend (TaskId, TaskExecutionId, T, RetryCount, ExecutionContext) -> RetrySignal,
    private val onCompletion: suspend (TaskId, TaskExecutionId, T, RetryCount, ExecutionContext, TaskCompletionResult) -> Unit,
    private val defaultTimeout: Duration = 60.seconds,
    private val defaultBackoffStrategy: BackoffStrategy = BackoffStrategy(),
    private val dispatchDedup: DispatchDedupConfig = DispatchDedupConfig.Disabled,
) {
    private val log = LoggerFactory.getLogger(AsyncTaskRunner::class.java)

    private var subscribeJob: Cancellable? = null
    private var cleanupJob: Job? = null
    private val cleanupScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    suspend fun dispatch(
        id: TaskId,
        trigger: T,
    ) {
        when (val dedup = dispatchDedup) {
            is DispatchDedupConfig.Disabled -> sink.publish(id, trigger)
            is DispatchDedupConfig.Enabled -> {
                if (dedup.log.hasBeenDispatched(id)) {
                    log.debug("Skipping already-dispatched task {}", id.value)
                    return
                }
                sink.publish(id, trigger)
                dedup.log.recordDispatched(id)
            }
        }
    }

    fun start() {
        log.info("Starting async task runner")
        subscribeJob =
            source.subscribe { id, executionId, trigger, retryCount ->
                val executionContext = createExecutionContext(id, trigger)

                runCatching {
                    withTimeout(trigger.timeout ?: defaultTimeout) {
                        execute(id, executionId, trigger, retryCount, executionContext)
                    }
                }.getOrElse { ex ->
                    when (ex) {
                        is TimeoutCancellationException -> TaskTimedOut
                        else -> TaskFailed(ex)
                    }
                }.let { result ->
                    when (result) {
                        is TaskCompleted -> {
                            runCatching {
                                onCompletion(id, executionId, trigger, retryCount, executionContext, TaskCompletionResult.TaskCompleted)
                                null
                            }.getOrElse { ex ->
                                log.error(
                                    "Exception when executing completion handler for task ${id.value} [ totalRetries=$retryCount ]",
                                    ex,
                                )
                                RetrySignal.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                            }
                        }
                        is TaskCancelled -> {
                            runCatching {
                                onCompletion(id, executionId, trigger, retryCount, executionContext, TaskCompletionResult.TaskCancelled)
                            }.onFailure { ex ->
                                log.error(
                                    "Exception when executing cancellation completion handler for task ${id.value} — not retrying",
                                    ex,
                                )
                            }
                            null
                        }
                        is TaskDiscarded -> {
                            log.warn("Task ${id.value} discarded [ reason=${result.reason} ]")
                            null
                        }
                        is TaskFailed -> {
                            log.error("Task ${id.value} failed [ totalRetries=$retryCount ]", result.ex)
                            runCatching { failureRetryHandler(id, executionId, trigger, retryCount, executionContext, result.ex) }
                                .map { retryHandlingResult ->
                                    when (retryHandlingResult) {
                                        is RetrySignal.Retry -> {
                                            log.warn("Task ${id.value} will be retried after failure [ totalRetries=$retryCount ]")
                                            retryHandlingResult
                                        }
                                        is RetrySignal.DoNotRetry -> {
                                            log.warn("Task ${id.value} will not be retried after failure [ totalRetries=$retryCount ]")
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
                                        "Exception when applying task ${id.value} retry handling logic. Task will be retried [ totalRetries=$retryCount ]",
                                        ex,
                                    )
                                    RetrySignal.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                                }
                        }
                        is TaskTimedOut -> {
                            log.error("Task ${id.value} timed out")
                            runCatching { timeoutRetryHandler(id, executionId, trigger, retryCount, executionContext) }
                                .map { retryHandlingResult ->
                                    when (retryHandlingResult) {
                                        is RetrySignal.Retry -> {
                                            log.warn("Task ${id.value} will be retried after timeout [ totalRetries=$retryCount ]")
                                            retryHandlingResult
                                        }
                                        is RetrySignal.DoNotRetry -> {
                                            log.warn("Task ${id.value} will not be retried after timeout [ totalRetries=$retryCount ]")
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
                                        "Exception when applying task ${id.value} timeout retry handling logic. Task will be retried [ totalRetries=$retryCount ]",
                                        ex,
                                    )
                                    RetrySignal.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))
                                }
                        }
                    }
                }
            }

        if (dispatchDedup is DispatchDedupConfig.Enabled) {
            launchCleanup(dispatchDedup)
        }
    }

    fun stop() {
        log.info("Stopping task runner")
        subscribeJob?.cancel()
        subscribeJob = null
        cleanupJob?.cancel()
        cleanupJob = null
    }

    private fun launchCleanup(dedup: DispatchDedupConfig.Enabled) {
        cleanupJob =
            cleanupScope.launch {
                while (isActive) {
                    try {
                        if (dedup.isLeader()) {
                            dedup.log.cleanupOlderThan(dedup.retention)
                        }
                    } catch (ex: CancellationException) {
                        throw ex
                    } catch (ex: Exception) {
                        log.error("Dispatch log cleanup failed", ex)
                    }
                    delay(dedup.cleanupInterval)
                }
            }
    }
}

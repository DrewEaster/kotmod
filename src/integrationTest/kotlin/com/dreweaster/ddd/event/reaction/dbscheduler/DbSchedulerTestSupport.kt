package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionCompletionResult
import com.dreweaster.ddd.event.reaction.EventReactionExecutionId
import com.dreweaster.ddd.event.reaction.EventReactionExecutionResult
import com.dreweaster.ddd.event.reaction.EventReactionExecutor
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.event.reaction.RetrySignal
import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

data class TestTrigger(
    val name: String,
    override val timeout: Duration? = null,
) : EventReactionTrigger

/** Serializes a trigger as its name. Names starting with "poison" are rejected on deserialize. */
object TestTriggerSerializer : EventReactionTriggerSerializer<TestTrigger> {
    override suspend fun serialize(trigger: TestTrigger): String = trigger.name

    override suspend fun deserialize(serializedTrigger: String): TestTrigger {
        require(!serializedTrigger.startsWith("poison")) { "cannot deserialize $serializedTrigger" }
        return TestTrigger(serializedTrigger)
    }
}

data class Attempt(
    val id: EventReactionId,
    val executionId: EventReactionExecutionId,
    val trigger: TestTrigger,
    val retryCount: Int,
)

/** Thread-safe record of what an executor saw. */
class ReactionRecorder {
    val attempts = CopyOnWriteArrayList<Attempt>()
    val completions = CopyOnWriteArrayList<Pair<EventReactionId, EventReactionCompletionResult>>()
}

fun testScheduler(
    dataSource: DataSource,
    vararg tasks: Task<*>,
): Scheduler =
    Scheduler
        .create(dataSource, *tasks)
        .pollingInterval(java.time.Duration.ofMillis(100))
        .enableImmediateExecution()
        .threads(4)
        .shutdownMaxWait(java.time.Duration.ofSeconds(2))
        .build()

fun testExecutor(
    reactions: DbSchedulerEventReactions<TestTrigger>,
    client: SchedulerClient,
    recorder: ReactionRecorder,
    execute: (Attempt) -> EventReactionExecutionResult = { EventReactionExecutionResult.EventReactionExecutionCompleted },
    failureRetryHandler: (Attempt, Throwable) -> RetrySignal = { _, _ -> RetrySignal.Retry(100.milliseconds) },
    createExecutionContext: (EventReactionId, TestTrigger) -> Unit = { _, _ -> },
): EventReactionExecutor<TestTrigger, Unit> =
    EventReactionExecutor(
        sink = reactions.sink(client),
        source = reactions.source,
        createExecutionContext = { id, trigger -> createExecutionContext(id, trigger) },
        execute = { id, executionId, trigger, retryCount, _ ->
            val attempt = Attempt(id, executionId, trigger, retryCount)
            recorder.attempts += attempt
            execute(attempt)
        },
        failureRetryHandler = { id, executionId, trigger, retryCount, _, ex ->
            failureRetryHandler(Attempt(id, executionId, trigger, retryCount), ex)
        },
        timeoutRetryHandler = { _, _, _, _, _ -> RetrySignal.Retry(100.milliseconds) },
        onCompletion = { id, _, _, _, _, result -> recorder.completions += id to result },
    )

/** Starts executors before the scheduler and stops them after it — the documented lifecycle order. */
suspend fun <R> running(
    scheduler: Scheduler,
    vararg executors: EventReactionExecutor<*, *>,
    block: suspend () -> R,
): R {
    executors.forEach { it.start() }
    scheduler.start()
    try {
        return block()
    } finally {
        scheduler.stop()
        executors.forEach { it.stop() }
    }
}

suspend fun eventually(
    timeout: Duration = 10.seconds,
    condition: () -> Boolean,
) {
    withTimeout(timeout) {
        while (!condition()) delay(50)
    }
}

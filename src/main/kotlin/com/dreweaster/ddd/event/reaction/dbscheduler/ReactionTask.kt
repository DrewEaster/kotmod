package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionExecutionId
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.randomId
import com.github.kagkarlsson.scheduler.task.CompletionHandler
import com.github.kagkarlsson.scheduler.task.helper.CustomTask
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

private val log = LoggerFactory.getLogger("com.dreweaster.ddd.event.reaction.dbscheduler.ReactionTask")

/** Builds the db-scheduler task that decodes each stored reaction and runs it through the subscribed executor. */
internal fun <T : EventReactionTrigger> reactionTask(
    taskName: String,
    triggerSerializer: EventReactionTriggerSerializer<T>,
    source: DbSchedulerTriggerSource<T>,
    unsubscribedRetryDelay: Duration,
): CustomTask<String> =
    Tasks
        .custom(taskName, String::class.java)
        .onFailure(CappedExponentialBackoffFailureHandler(initialDelay = 10.seconds, maximumDelay = 1.hours))
        .execute { instance, _ ->
            val handler = source.handler
            val outcome =
                if (handler == null) {
                    log.warn(
                        "No event reaction executor subscribed to task {}; rescheduling {} in {}. " +
                            "Start executors before the db-scheduler Scheduler and stop them after it.",
                        taskName,
                        instance.id,
                        unsubscribedRetryDelay,
                    )
                    outcomeWhenUnsubscribed(instance.data, Instant.now(), unsubscribedRetryDelay)
                } else {
                    // Decoding and deserialization failures throw, handing the row to the failure handler.
                    val data = ReactionTaskData.decode(instance.data)
                    val executionId = EventReactionExecutionId(randomId())
                    log.debug(
                        "Executing event reaction {} [task={}, executionId={}, retryCount={}]",
                        instance.id,
                        taskName,
                        executionId.value,
                        data.retryCount,
                    )
                    val result =
                        runBlocking {
                            handler(
                                EventReactionId(instance.id),
                                executionId,
                                triggerSerializer.deserialize(data.trigger),
                                data.retryCount,
                            )
                        }
                    outcomeAfterExecution(result, data, Instant.now())
                }
            outcome.toCompletionHandler()
        }

private fun ReactionOutcome.toCompletionHandler(): CompletionHandler<String> =
    CompletionHandler { executionComplete, executionOperations ->
        when (this) {
            ReactionOutcome.Remove -> executionOperations.remove()
            is ReactionOutcome.Reschedule -> executionOperations.reschedule(executionComplete, at, taskData)
        }
    }

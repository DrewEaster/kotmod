package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.EventReactionExecutionId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.randomId
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.CompletionHandler
import com.github.kagkarlsson.scheduler.task.TaskInstanceId
import com.github.kagkarlsson.scheduler.task.helper.CustomTask
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

private val log = LoggerFactory.getLogger("io.kotmod.event.reaction.dbscheduler.ReactionTask")

/**
 * Builds the db-scheduler task that decodes each stored reaction and runs it through the subscribed executor.
 *
 * An ordered reaction runs only once no earlier reaction of its aggregate is pending for this task; until then it
 * is rechecked after [orderedRecheckDelay], doubling per wait up to a minute, without using up a retry. A blocked reaction stays parked.
 */
internal fun <T : EventReactionTrigger> reactionTask(
    taskName: String,
    triggerSerializer: EventReactionTriggerSerializer<T>,
    source: DbSchedulerTriggerSource<T>,
    unsubscribedRetryDelay: Duration,
    orderedQueries: OrderedQueries? = null,
    orderedRecheckDelay: Duration = 2.seconds,
): CustomTask<String> =
    Tasks
        .custom(taskName, String::class.java)
        .onFailure(CappedExponentialBackoffFailureHandler(initialDelay = 10.seconds, maximumDelay = 1.hours))
        .execute { instance, context ->
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
                    val ordering = data.ordering
                    if (data.blocked) {
                        outcomeWhenParked(instance.data, Instant.now(), PARKED_DELAY)
                    } else if (ordering != null &&
                        checkNotNull(orderedQueries) {
                            "Task $taskName has an ordered reaction but no JdbcContext to order it with"
                        }.earlierPending(taskName, ordering.key, instance.id)
                    ) {
                        log.debug("Event reaction {} waits for an earlier reaction of {} [task={}]", instance.id, ordering.key, taskName)
                        outcomeWhenWaitingForEarlier(data, Instant.now(), orderedRecheckDelay)
                    } else {
                        // The handler sees the original reaction id, not the sortable instance id.
                        val reactionId = EventReactionId(ordering?.reactionId ?: instance.id)
                        val executionId = EventReactionExecutionId(randomId())
                        log.debug(
                            "Executing event reaction {} [task={}, executionId={}, retryCount={}]",
                            reactionId.value,
                            taskName,
                            executionId.value,
                            data.retryCount,
                        )
                        val result =
                            runBlocking {
                                handler(
                                    reactionId,
                                    executionId,
                                    triggerSerializer.deserialize(data.trigger),
                                    data.retryCount,
                                    data.notBefore?.let { kotlin.time.Instant.parse(it) },
                                )
                            }
                        outcomeAfterExecution(result, data, Instant.now())
                    }
                }
            outcome.toCompletionHandler(context.schedulerClient, taskName, orderedQueries)
        }

private fun TaskRowOutcome.toCompletionHandler(
    client: SchedulerClient,
    taskName: String,
    orderedQueries: OrderedQueries?,
): CompletionHandler<String> =
    CompletionHandler { executionComplete, executionOperations ->
        when (this) {
            is TaskRowOutcome.Remove -> {
                executionOperations.remove()
                if (nudgeKey != null && orderedQueries != null) nudgeNext(client, taskName, orderedQueries, nudgeKey)
            }
            is TaskRowOutcome.Reschedule -> executionOperations.reschedule(executionComplete, at, taskData)
        }
    }

/**
 * Asks db-scheduler to run the next pending, not-parked reaction of [key] now, so it need not wait for its recheck.
 * Best effort: a reaction that is missed (e.g. picked concurrently) still runs at its next recheck.
 */
internal fun nudgeNext(
    client: SchedulerClient,
    taskName: String,
    orderedQueries: OrderedQueries,
    key: String,
) {
    runCatching {
        orderedQueries.nextPending(taskName, key)?.let { next ->
            client.reschedule(TaskInstanceId.of(taskName, next), Instant.now())
        }
    }.onFailure { log.warn("Could not nudge the next event reaction of {} [task={}]", key, taskName, it) }
}

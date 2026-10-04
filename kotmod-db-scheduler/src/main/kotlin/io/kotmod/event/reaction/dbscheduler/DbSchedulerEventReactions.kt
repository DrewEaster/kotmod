package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.jdbc.JdbcContext
import com.github.kagkarlsson.scheduler.ScheduledExecution
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Durable event reactions stored and scheduled by db-scheduler. Create one per
 * [io.kotmod.event.reaction.EventReactionExecutor]: it becomes one db-scheduler task named
 * [taskName], and each reaction is an instance of that task identified by its
 * [io.kotmod.event.reaction.EventReactionId].
 *
 * The app owns the db-scheduler `Scheduler` and its `scheduled_tasks` table:
 * ```
 * val reactions = DbSchedulerEventReactions("billing-reactions", BillingTriggerSerializer)
 * val scheduler = Scheduler.create(dataSource, reactions.task).threads(10).build()
 * val executor = EventReactionExecutor(sink = reactions.sink(scheduler), source = reactions.source, ...)
 *
 * executor.start()   // executors start before the scheduler...
 * scheduler.start()
 * ...
 * scheduler.stop()
 * executor.stop()    // ...and stop after it
 * ```
 * Retries requested by the executor reschedule the reaction with an incremented retry count. If the
 * scheduler runs a reaction while no executor is subscribed, it is rescheduled
 * [unsubscribedRetryDelay] later without using up a retry. If a reaction's stored data cannot be read,
 * db-scheduler retries it with backoff from 10 seconds up to 1 hour. Interrupted executions are retried
 * later with the same retry count.
 *
 * Delivery is at-least-once: reaction ids must be deterministic per (event, reaction kind) so duplicate
 * dispatches are ignored while pending, and the executor's handlers must be idempotent.
 *
 * Ordered reactions ([io.kotmod.event.reaction.ReactionOrdering.PerAggregate]) need [jdbc], used to query db-scheduler's
 * [tableName] table. Each is stored under an instance id that sorts by aggregate, sequence and ordinal; when picked it
 * runs only if no earlier reaction of its aggregate is pending for this task, and otherwise is rechecked after
 * [orderedRecheckDelay], doubling per consecutive wait up to a minute, without using up a retry. Finishing one nudges
 * the aggregate's next reaction to run now, which is the usual way a waiting reaction gets its turn. One
 * that gives up with [OnGiveUp.BlockAggregate] is parked, holding back its aggregate, until [retryBlocked] or
 * [skipBlocked] is called (see [blockedReactions]).
 */
class DbSchedulerEventReactions<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    unsubscribedRetryDelay: Duration = 5.seconds,
    jdbc: JdbcContext? = null,
    orderedRecheckDelay: Duration = 2.seconds,
    tableName: String = "scheduled_tasks",
) {
    private val orderedQueries = jdbc?.let { OrderedQueries(it, tableName) }
    private val triggerSource = DbSchedulerTriggerSource<T>()

    /** Whether this can run ordered reactions (requires a [JdbcContext]). */
    val supportsOrdering: Boolean get() = orderedQueries != null

    /** The db-scheduler task to register when building the app's `Scheduler`. */
    val task: Task<String> =
        reactionTask(taskName, triggerSerializer, triggerSource, unsubscribedRetryDelay, orderedQueries, orderedRecheckDelay)

    /** The tasks to register with the app's `Scheduler`. */
    val tasks: List<Task<*>> get() = listOf(task)

    /** The source to pass to this reaction type's executor. */
    val source: EventReactionTriggerSource<T> get() = triggerSource

    /** Returns the sink to pass to this reaction type's executor, scheduling through [client] (usually the app's `Scheduler`). */
    fun sink(client: SchedulerClient): EventReactionTriggerSink<T> =
        DbSchedulerTriggerSink(taskName, triggerSerializer, client, supportsOrdering = supportsOrdering)

    /** Ordered reactions that gave up with [OnGiveUp.BlockAggregate] and are holding back their aggregate. */
    fun blockedReactions(client: SchedulerClient): List<BlockedReaction> =
        client
            .getScheduledExecutionsForTask(taskName, String::class.java)
            .mapNotNull { execution ->
                val data = ReactionTaskData.decode(execution.data)
                val ordering = data.ordering
                if (data.blocked && ordering != null) {
                    BlockedReaction(ordering.key, EventReactionId(ordering.reactionId), ordering.sequence)
                } else {
                    null
                }
            }

    /** Runs a blocked reaction again now, with its retry count reset. */
    fun retryBlocked(
        client: SchedulerClient,
        id: EventReactionId,
    ) {
        val execution = blockedExecution(client, id)
        val data = ReactionTaskData.decode(execution.data)
        client.reschedule(execution.taskInstance, Instant.now(), data.copy(blocked = false, retryCount = 0).encode())
    }

    /** Drops a blocked reaction without running it again, so its aggregate's next reaction can run. */
    fun skipBlocked(
        client: SchedulerClient,
        id: EventReactionId,
    ) {
        val execution = blockedExecution(client, id)
        val key = checkNotNull(ReactionTaskData.decode(execution.data).ordering).key
        client.cancel(execution.taskInstance)
        orderedQueries?.let { nudgeNext(client, taskName, it, key) }
    }

    private fun blockedExecution(
        client: SchedulerClient,
        id: EventReactionId,
    ): ScheduledExecution<String> =
        client.getScheduledExecutionsForTask(taskName, String::class.java).singleOrNull { execution ->
            val data = ReactionTaskData.decode(execution.data)
            data.blocked && data.ordering?.reactionId == id.value
        } ?: throw IllegalArgumentException("No blocked event reaction ${id.value} for task $taskName")
}

/** An ordered reaction holding back its aggregate (identified by [key]) after giving up. */
data class BlockedReaction(
    val key: String,
    val reactionId: EventReactionId,
    val sequence: Long,
)

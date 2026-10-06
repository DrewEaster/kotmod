package io.kotmod.event.reaction.dbscheduler

import io.kotmod.EventId
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
 * One db-scheduler task, as one of [DbSchedulerQueues]' queues: each reaction is an instance of task [taskName],
 * identified by its reaction id.
 *
 * Retries requested by the runtime subscribed to the queue reschedule the reaction with an incremented retry count.
 * If the scheduler runs a reaction while no runtime is subscribed, it is rescheduled [unsubscribedRetryDelay] later
 * without using up a retry. If a reaction's stored data cannot be read, db-scheduler retries it with backoff from
 * 10 seconds up to 1 hour. Interrupted executions are retried later with the same retry count.
 *
 * Delivery is at-least-once: reaction ids must be deterministic per (event, reaction kind) so duplicate
 * dispatches are ignored while pending, and the subscribed runtime's handling must be idempotent.
 *
 * Ordered reactions ([io.kotmod.event.reaction.ReactionOrdering.PerAggregate]) need [jdbc], used to query db-scheduler's
 * [tableName] table. Each is stored under an instance id that sorts by aggregate, sequence and ordinal; when picked it
 * runs only if no earlier reaction of its aggregate is pending for this task, and otherwise is rechecked after
 * [orderedRecheckDelay], doubling per consecutive wait up to a minute, without using up a retry. Finishing one nudges
 * the aggregate's next reaction to run now, which is the usual way a waiting reaction gets its turn. One
 * that gives up with [OnGiveUp.BlockAggregate] is parked, holding back its aggregate, until [retryBlocked] or
 * [skipBlocked] is called (see [blockedReactions]).
 */
internal class DbSchedulerEventReactions<T : EventReactionTrigger>(
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

    /** The source the runtime subscribes to. */
    val source: EventReactionTriggerSource<T> get() = triggerSource

    /** Returns the sink the runtime publishes to, scheduling through [client] (usually the app's `Scheduler`). */
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

    /** The parked mappings of event policy [policy] (this task): reactions with id `<policy>/<eventId>/mapping`. */
    fun parkedMappings(
        client: SchedulerClient,
        policy: String,
    ): List<ParkedMapping> =
        client
            .getScheduledExecutionsForTask(taskName, String::class.java)
            .mapNotNull { execution ->
                val data = ReactionTaskData.decode(execution.data)
                val reactionId = data.ordering?.reactionId ?: execution.taskInstance.id
                parkedEventId(policy, reactionId)?.let { ParkedMapping(EventId(it), EventReactionId(reactionId), data.retryCount) }
            }

    /** Drops [policy]'s parked mapping of [eventId] without retrying it, so its aggregate's next reaction can run. */
    fun skipParked(
        client: SchedulerClient,
        policy: String,
        eventId: EventId,
    ) {
        val reactionId = parkedMappingId(policy, eventId)
        val execution =
            client.getScheduledExecutionsForTask(taskName, String::class.java).singleOrNull { execution ->
                val data = ReactionTaskData.decode(execution.data)
                (data.ordering?.reactionId ?: execution.taskInstance.id) == reactionId
            } ?: throw IllegalArgumentException("No parked mapping $reactionId for task $taskName")
        val key = ReactionTaskData.decode(execution.data).ordering?.key
        client.cancel(execution.taskInstance)
        if (key != null) orderedQueries?.let { nudgeNext(client, taskName, it, key) }
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

// An event policy parks an event it couldn't map as reaction `<policy>/<eventId>/mapping` (see io.kotmod.reaction.PolicyRuntime).
private fun parkedMappingId(
    policy: String,
    eventId: EventId,
) = "$policy/${eventId.value}/mapping"

private fun parkedEventId(
    policy: String,
    reactionId: String,
): String? {
    val prefix = "$policy/"
    val suffix = "/mapping"
    if (!reactionId.startsWith(prefix) || !reactionId.endsWith(suffix) || reactionId.length <= prefix.length + suffix.length) return null
    return reactionId.substring(prefix.length, reactionId.length - suffix.length)
}

package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSink
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSource
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Durable event reactions stored and scheduled by db-scheduler. Create one per
 * [com.dreweaster.ddd.event.reaction.EventReactionExecutor]: it becomes one db-scheduler task named
 * [taskName], and each reaction is an instance of that task identified by its
 * [com.dreweaster.ddd.event.reaction.EventReactionId].
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
 */
class DbSchedulerEventReactions<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    unsubscribedRetryDelay: Duration = 5.seconds,
) {
    private val triggerSource = DbSchedulerTriggerSource<T>()

    /** The db-scheduler task to register when building the app's `Scheduler`. */
    val task: Task<String> = reactionTask(taskName, triggerSerializer, triggerSource, unsubscribedRetryDelay)

    /** The source to pass to this reaction type's executor. */
    val source: EventReactionTriggerSource<T> get() = triggerSource

    /** Returns the sink to pass to this reaction type's executor, scheduling through [client] (usually the app's `Scheduler`). */
    fun sink(client: SchedulerClient): EventReactionTriggerSink<T> = DbSchedulerTriggerSink(taskName, triggerSerializer, client)
}

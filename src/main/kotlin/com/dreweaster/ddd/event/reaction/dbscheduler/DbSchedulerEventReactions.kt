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
 * Durable event reactions backed by db-scheduler. Create one per
 * [com.dreweaster.ddd.event.reaction.EventReactionExecutor]; each instance is one db-scheduler
 * task named [taskName], and each reaction is a task instance whose id is its
 * [com.dreweaster.ddd.event.reaction.EventReactionId].
 *
 * The app owns the db-scheduler `Scheduler`:
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
 * If the scheduler runs a reaction while no executor is subscribed, the reaction is rescheduled
 * [unsubscribedRetryDelay] later (without consuming a retry) and a warning is logged.
 *
 * Delivery is at-least-once: reaction ids must be deterministic per (event, reaction kind) so that
 * duplicate dispatches are absorbed while pending, and handlers must be idempotent.
 *
 * If `scheduler.stop()` interrupts a reaction that is still running, the executor's retry and
 * completion handlers are not called; db-scheduler's failure handling retries the reaction later
 * with the same retry count.
 * The app must create db-scheduler's `scheduled_tasks` table itself.
 */
class DbSchedulerEventReactions<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    unsubscribedRetryDelay: Duration = 5.seconds,
) {
    private val triggerSource = DbSchedulerTriggerSource<T>()

    /** Register this with the app's db-scheduler `Scheduler`. */
    val task: Task<String> = reactionTask(taskName, triggerSerializer, triggerSource, unsubscribedRetryDelay)

    val source: EventReactionTriggerSource<T> get() = triggerSource

    fun sink(client: SchedulerClient): EventReactionTriggerSink<T> = DbSchedulerTriggerSink(taskName, triggerSerializer, client)
}

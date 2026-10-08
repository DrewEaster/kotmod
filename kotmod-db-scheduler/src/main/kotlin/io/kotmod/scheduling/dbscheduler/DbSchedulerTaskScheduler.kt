package io.kotmod.scheduling.dbscheduler

import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.CompletionHandler
import com.github.kagkarlsson.scheduler.task.Task
import com.github.kagkarlsson.scheduler.task.TaskInstance
import com.github.kagkarlsson.scheduler.task.helper.CustomTask
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import io.kotmod.scheduling.Cancellable
import io.kotmod.scheduling.TaskOutcome
import io.kotmod.scheduling.TaskQueue
import io.kotmod.scheduling.TaskScheduler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toJavaInstant

private val log = LoggerFactory.getLogger(DbSchedulerTaskScheduler::class.java)

/**
 * [TaskScheduler] on db-scheduler: each queue is a db-scheduler task named after it, and each task an instance of it in
 * your `scheduled_tasks` table. Ask for every queue (register your event policies and build your process managers)
 * before reading [tasks]; register [tasks] when building the `Scheduler`; call [bind] before starting the reactor (or,
 * in a script using `ReactionOperations`, bind a `SchedulerClient` on the same database before using it). A
 * handler exception is retried with capped exponential backoff (10 seconds doubling to an hour). A task delivered while
 * nothing is subscribed to its queue (e.g. the reactor isn't started yet) runs again after [unsubscribedRetryDelay].
 */
class DbSchedulerTaskScheduler(
    private val unsubscribedRetryDelay: Duration = 5.seconds,
) : TaskScheduler {
    private val queues = linkedMapOf<String, Queue>()

    @Volatile
    private var client: SchedulerClient? = null

    /** One db-scheduler task per queue asked for so far. */
    val tasks: List<Task<String>> get() = synchronized(queues) { queues.values.map { it.task } }

    /** Gives the queues the client to schedule with (your `Scheduler`). */
    fun bind(client: SchedulerClient) {
        this.client = client
    }

    override fun queue(name: String): TaskQueue = synchronized(queues) { queues.getOrPut(name) { Queue(name) } }

    private inner class Queue(
        private val taskName: String,
    ) : TaskQueue {
        @Volatile
        private var handler: (suspend (String, String) -> TaskOutcome)? = null

        val task: CustomTask<String> =
            Tasks
                .custom(taskName, String::class.java)
                .onFailure(CappedExponentialBackoffFailureHandler(initialDelay = 10.seconds, maximumDelay = 1.hours))
                .execute { instance, _ ->
                    val current = handler
                    val outcome =
                        if (current == null) {
                            log.warn(
                                "Nothing is subscribed to task {}; running {} again in {}. " +
                                    "Start the reactor and process managers before the db-scheduler Scheduler and stop them after it.",
                                taskName,
                                instance.id,
                                unsubscribedRetryDelay,
                            )
                            TaskOutcome.RunAgain(Clock.System.now() + unsubscribedRetryDelay, instance.data)
                        } else {
                            // A handler exception propagates, so db-scheduler hands the execution to the failure handler.
                            runBlocking { current(instance.id, instance.data) }
                        }
                    when (outcome) {
                        TaskOutcome.Done -> CompletionHandler.OnCompleteRemove()
                        is TaskOutcome.RunAgain ->
                            CompletionHandler { complete, operations ->
                                operations.reschedule(complete, outcome.at.toJavaInstant(), outcome.payload)
                            }
                    }
                }

        override suspend fun schedule(
            name: String,
            payload: String,
            at: Instant,
        ) {
            val bound =
                checkNotNull(client) {
                    "DbSchedulerTaskScheduler isn't bound: call bind with your Scheduler before starting the reactor, " +
                        "or, in a script using ReactionOperations, with a SchedulerClient on the same database"
                }
            withContext(Dispatchers.IO) { bound.scheduleIfNotExists(TaskInstance(taskName, name, payload), at.toJavaInstant()) }
        }

        override fun subscribe(handler: suspend (name: String, payload: String) -> TaskOutcome): Cancellable {
            this.handler = handler
            return Cancellable { if (this.handler === handler) this.handler = null }
        }
    }
}

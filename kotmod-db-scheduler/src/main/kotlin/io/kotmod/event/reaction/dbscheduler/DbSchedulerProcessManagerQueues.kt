package io.kotmod.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.jdbc.JdbcContext
import io.kotmod.process.ProcessChannel
import io.kotmod.process.ProcessManagerQueues
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/**
 * Runs a [io.kotmod.process.ProcessManager]'s channels on db-scheduler: one task per channel, named `<name>-<channel>`.
 *
 * ```
 * val queues = DbSchedulerProcessManagerQueues("refund-window", jdbc)
 * val refundWindow = ProcessManager(…, queues = queues)        // and any subscribeTo(…) calls
 * val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
 * queues.bind(scheduler)
 * refundWindow.start()
 * scheduler.start()
 * ```
 *
 * [jdbc] is needed only when the process manager's inputs are ordered.
 */
class DbSchedulerProcessManagerQueues(
    private val name: String,
    private val jdbc: JdbcContext? = null,
    private val unsubscribedRetryDelay: Duration = 5.seconds,
) : ProcessManagerQueues {
    private val reactions = mutableListOf<DbSchedulerEventReactions<*>>()

    @Volatile
    private var client: SchedulerClient? = null

    /** The tasks to register with the app's `Scheduler`, once the process manager (and its subscriptions) are built. */
    val tasks: List<Task<*>> get() = reactions.flatMap { it.tasks }

    /** Publishes through [client] (usually the app's `Scheduler`). Call it before starting the process manager. */
    fun bind(client: SchedulerClient) {
        this.client = client
    }

    override fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ProcessChannel<T> {
        require(!ordered || jdbc != null) { "Ordered process inputs need DbSchedulerProcessManagerQueues to be created with a JdbcContext" }
        val channelReactions =
            DbSchedulerEventReactions("${this.name}-$name", triggerSerializer, unsubscribedRetryDelay, jdbc = if (ordered) jdbc else null)
        reactions += channelReactions
        val sink =
            object : EventReactionTriggerSink<T> {
                override val supportsOrdering: Boolean = channelReactions.supportsOrdering

                override suspend fun publish(
                    id: EventReactionId,
                    trigger: T,
                    ordering: DispatchOrdering?,
                    notBefore: Instant?,
                ) {
                    val bound = checkNotNull(client) { "Call bind(scheduler) on DbSchedulerProcessManagerQueues before starting" }
                    channelReactions.sink(bound).publish(id, trigger, ordering, notBefore)
                }
            }
        return ProcessChannel(sink, channelReactions.source)
    }
}

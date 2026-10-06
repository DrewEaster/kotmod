package io.kotmod.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import io.kotmod.EventId
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionChannel
import io.kotmod.event.reaction.ReactionQueues
import io.kotmod.jdbc.JdbcContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/**
 * Runs a context's use cases and process managers on db-scheduler: each queue — one per use case, named after it, and
 * one per process manager channel, named `<process type>-<channel>` — is one db-scheduler task of the same name, and
 * each reaction an instance of it. The app owns the `Scheduler` and its `scheduled_tasks` table:
 *
 * ```
 * val queues = DbSchedulerQueues(jdbc)
 * val reactor = EventReactor(jdbc, queues, isLeader = { election.isLeader() })
 * reactor.register(FraudChecks(fraud))                       // every use case
 * // build every process manager here too, passing it queues, before reading tasks
 * val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
 * queues.bind(scheduler)
 * reactor.start()
 * scheduler.start()
 * ```
 *
 * Use one instance per context. Ordered reactions are checked against [tableName] through [jdbc]: an ordered reaction
 * runs only once no earlier reaction of its aggregate is pending in its queue, rechecking after [orderedRecheckDelay]
 * (doubling up to a minute). Every queue can run ordered reactions, so ones left queued by a use case that was ordered
 * when they were queued still run in order after it becomes unordered. A reaction delivered while nothing is
 * subscribed is pushed back [unsubscribedRetryDelay] without counting a retry. An ordered reaction that gives up with
 * [OnGiveUp.BlockAggregate] holds back its aggregate until [retryBlocked] or [skipBlocked] (see [blockedReactions]). A
 * use case's mapping that keeps failing stays parked until it succeeds or [skipParked] drops it (see [parkedMappings]).
 *
 * The operator helpers take a `SchedulerClient` (usually the app's `Scheduler`) and a queue name: a use case's name,
 * or a process manager channel's `<process type>-<channel>`.
 */
class DbSchedulerQueues(
    private val jdbc: JdbcContext,
    private val unsubscribedRetryDelay: Duration = 5.seconds,
    private val orderedRecheckDelay: Duration = 2.seconds,
    private val tableName: String = "scheduled_tasks",
) : ReactionQueues {
    private val queues = linkedMapOf<String, DbSchedulerEventReactions<*>>()
    private var tasksRead = false

    @Volatile
    private var client: SchedulerClient? = null

    /**
     * The tasks to register with the app's `Scheduler`: read it after registering every use case and building every
     * process manager (and their `subscribeTo` calls). No queue can be added afterwards.
     */
    val tasks: List<Task<*>>
        get() {
            tasksRead = true
            return queues.values.flatMap { it.tasks }
        }

    /** Publishes through [client] (usually the app's `Scheduler`). Call it before starting the reactor or process managers. */
    fun bind(client: SchedulerClient) {
        this.client = client
    }

    override fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ReactionChannel<T> {
        check(!tasksRead) {
            "Queue $name was asked for after DbSchedulerQueues.tasks was read, so its task would never be registered: read tasks " +
                "after registering every use case and building every process manager"
        }
        require(name !in queues) {
            "DbSchedulerQueues already has a queue named $name: use-case names and process manager channels must be unique"
        }
        // Every queue gets jdbc, so every queue supports ordering: the reactor never refuses an ordered use case after
        // its queue (and so its task) was created here, and ordered rows left by a use case that has since become
        // unordered still run (in order) instead of failing for want of a JdbcContext.
        val reactions =
            DbSchedulerEventReactions(
                name,
                triggerSerializer,
                unsubscribedRetryDelay,
                jdbc = jdbc,
                orderedRecheckDelay = orderedRecheckDelay,
                tableName = tableName,
            )
        queues[name] = reactions
        val sink =
            object : EventReactionTriggerSink<T> {
                override val supportsOrdering: Boolean = reactions.supportsOrdering

                override suspend fun publish(
                    id: EventReactionId,
                    trigger: T,
                    ordering: DispatchOrdering?,
                    notBefore: Instant?,
                ) {
                    val bound = checkNotNull(client) { "Call bind(scheduler) on DbSchedulerQueues before starting" }
                    reactions.sink(bound).publish(id, trigger, ordering, notBefore)
                }
            }
        return ReactionChannel(sink, reactions.source)
    }

    /**
     * The ordered reactions of queue [useCase] (a use case's name, or a process manager channel's) that gave up with
     * [OnGiveUp.BlockAggregate] and hold back their aggregate.
     */
    fun blockedReactions(
        client: SchedulerClient,
        useCase: String,
    ): List<BlockedReaction> = queue(useCase).blockedReactions(client)

    /** Runs blocked reaction [id] of queue [useCase] (a use case's name, or a process manager channel's) again now, with its retry count reset. */
    fun retryBlocked(
        client: SchedulerClient,
        useCase: String,
        id: EventReactionId,
    ) = queue(useCase).retryBlocked(client, id)

    /**
     * Drops blocked reaction [id] of queue [useCase] (a use case's name, or a process manager channel's) without
     * running it again, so its aggregate's next reaction can run.
     */
    fun skipBlocked(
        client: SchedulerClient,
        useCase: String,
        id: EventReactionId,
    ) = queue(useCase).skipBlocked(client, id)

    /**
     * The events [useCase] couldn't map to triggers, parked in its queue as `<useCase>/<eventId>/mapping` and retried
     * until the mapping succeeds.
     */
    fun parkedMappings(
        client: SchedulerClient,
        useCase: String,
    ): List<ParkedMapping> = queue(useCase).parkedMappings(client, useCase)

    /**
     * Drops [useCase]'s parked mapping of event [eventId] without retrying it, for an event that will never map: the
     * event's work in this use case never runs, and with ordering its aggregate's next work can run. Fails if there
     * is no such parked mapping, or if it is running at that moment (call it again).
     */
    fun skipParked(
        client: SchedulerClient,
        useCase: String,
        eventId: EventId,
    ) = queue(useCase).skipParked(client, useCase, eventId)

    private fun queue(name: String): DbSchedulerEventReactions<*> = requireNotNull(queues[name]) { "DbSchedulerQueues has no queue named $name" }
}

/** An ordered reaction holding back its aggregate (identified by [key]) after giving up. */
data class BlockedReaction(
    val key: String,
    val reactionId: EventReactionId,
    val sequence: Long,
)

/**
 * A use case's mapping of event [eventId] that failed and is parked in its queue as [reactionId], after [retryCount]
 * retries.
 */
data class ParkedMapping(
    val eventId: EventId,
    val reactionId: EventReactionId,
    val retryCount: Int,
)

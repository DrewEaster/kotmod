package io.kotmod.reaction

import io.kotmod.EventId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionRow
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.event.reaction.RowKind
import io.kotmod.jdbc.JdbcContext
import io.kotmod.postgres.PostgresReactionRows
import io.kotmod.scheduling.TaskScheduler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.time.Clock
import kotlin.time.Instant

/** An ordered reaction holding back its aggregate's line ([key]) after giving up, with the attempts it took. */
data class BlockedReaction(
    val key: String,
    val reactionId: EventReactionId,
    val sequence: Long,
    val attempts: Int,
)

/** An event a policy couldn't map, parked as [reactionId], after [attempts] attempts. */
data class ParkedMapping(
    val eventId: EventId,
    val reactionId: EventReactionId,
    val attempts: Int,
)

/**
 * Operator tools for event policies' and process managers' work kept in `ddd_reaction_row`, the same on every
 * scheduler. [queue] is a policy name or a process manager channel name (`<type>-inputs`, …). Usable from an admin
 * endpoint or a script, without a running reactor: the queue's policy needn't be registered, or even still exist.
 *
 * Each change runs under the lock of the line it touches, so it is safe while the reactor and the scheduler run. A
 * change refused because the work isn't there (already finished, skipped or retried) throws [IllegalArgumentException].
 */
class ReactionOperations internal constructor(
    private val rows: ReactionRows,
    private val scheduler: TaskScheduler,
    private val clock: () -> Instant,
) {
    constructor(jdbc: JdbcContext, scheduler: TaskScheduler) : this(PostgresReactionRows(jdbc), scheduler, { Clock.System.now() })

    /** The reactions of [queue] that gave up and are holding back their aggregate's line, by line. */
    fun blockedReactions(queue: String): List<BlockedReaction> =
        rows.list(queue).filter { it.kind == RowKind.ORDERED && it.blocked }.map {
            BlockedReaction(checkNotNull(it.key), EventReactionId(it.reactionId), checkNotNull(it.sequence), it.attempts)
        }

    /** Unblocks reaction [id] of [queue] with its attempts reset to 0, and schedules it: it is its line's front. */
    suspend fun retryBlocked(
        queue: String,
        id: EventReactionId,
    ) {
        val key = blockedKey(queue, id)
        val front =
            io {
                rows.inLine(queue, key) {
                    val row = get(id.value)
                    require(row != null && row.blocked) { "Reaction ${id.value} of $queue is not blocked" }
                    update(row.copy(attempts = 0, blocked = false, leaseUntil = null))
                    front()?.takeUnless { it.blocked }
                }
            }
        scheduleFront(queue, front)
    }

    /** Deletes blocked reaction [id] of [queue], and schedules the next reaction of its line. */
    suspend fun skipBlocked(
        queue: String,
        id: EventReactionId,
    ) {
        val key = blockedKey(queue, id)
        val front =
            io {
                rows.inLine(queue, key) {
                    val row = get(id.value)
                    require(row != null && row.blocked) { "Reaction ${id.value} of $queue is not blocked" }
                    delete(id.value)
                    front()?.takeUnless { it.blocked }
                }
            }
        scheduleFront(queue, front)
    }

    /** The events event policy [policy] couldn't map, parked in its line or on their own. */
    fun parkedMappings(policy: String): List<ParkedMapping> =
        rows.list(policy).filter { it.reactionId.endsWith(MAPPING_SUFFIX) }.map {
            ParkedMapping(EventId(it.reactionId.removePrefix("$policy/").removeSuffix(MAPPING_SUFFIX)), EventReactionId(it.reactionId), it.attempts)
        }

    /**
     * Deletes [policy]'s parked mapping of event [eventId]: the event is never mapped. For an ordered policy, the next
     * reaction of its aggregate's line is scheduled.
     */
    suspend fun skipParked(
        policy: String,
        eventId: EventId,
    ) {
        val id = "$policy/${eventId.value}$MAPPING_SUFFIX"
        val row = io { rows.inQueue(policy) { get(id) } }
        requireNotNull(row) { "Event policy $policy has no parked mapping of event ${eventId.value}" }
        when (row.kind) {
            RowKind.KEPT ->
                io {
                    rows.inQueue(policy) {
                        require(get(id) != null) { "Event policy $policy has no parked mapping of event ${eventId.value}" }
                        delete(id)
                    }
                }
            RowKind.ORDERED -> {
                val front =
                    io {
                        rows.inLine(policy, checkNotNull(row.key)) {
                            require(get(id) != null) { "Event policy $policy has no parked mapping of event ${eventId.value}" }
                            delete(id)
                            front()?.takeUnless { it.blocked }
                        }
                    }
                scheduleFront(policy, front)
            }
        }
    }

    /** The line of ordered reaction [id], read without its lock; the caller checks the row again under the lock. */
    private suspend fun blockedKey(
        queue: String,
        id: EventReactionId,
    ): String {
        val row = io { rows.inQueue(queue) { get(id.value) } }
        require(row != null && row.kind == RowKind.ORDERED && row.blocked) { "Reaction ${id.value} of $queue is not blocked" }
        return checkNotNull(row.key)
    }

    private suspend fun scheduleFront(
        queue: String,
        front: ReactionRow?,
    ) {
        front?.let { ReactionTasks.scheduleFront(scheduler.queue(queue), checkNotNull(it.key), it.reactionId, clock()) }
    }

    private suspend fun <R> io(block: () -> R): R = withContext(Dispatchers.IO) { block() }

    private companion object {
        const val MAPPING_SUFFIX = "/mapping"
    }
}

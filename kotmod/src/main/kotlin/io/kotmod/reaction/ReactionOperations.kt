package io.kotmod.reaction

import io.kotmod.EventId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionRow
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.event.reaction.RowKind
import io.kotmod.event.reaction.RowTx
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
 *
 * A change is saved before its work is scheduled, so [scheduler] must be able to schedule (a `DbSchedulerTaskScheduler`
 * must be bound). If scheduling fails, the change stays and the reactor's repair sweep schedules the line later.
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

    /**
     * Unblocks reaction [id] of [queue] with its attempts reset to 0, and schedules its line's front (normally the
     * retried reaction).
     */
    suspend fun retryBlocked(
        queue: String,
        id: EventReactionId,
    ) {
        val front =
            io {
                rows.inLine(queue, lineOf(queue, id)) {
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
        val front =
            io {
                rows.inLine(queue, lineOf(queue, id)) {
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
     * reaction of its aggregate's line is scheduled. Refused with [IllegalStateException] while the mapping is being
     * retried (try again shortly).
     */
    suspend fun skipParked(
        policy: String,
        eventId: EventId,
    ) {
        val id = "$policy/${eventId.value}$MAPPING_SUFFIX"
        val missing = "Event policy $policy has no parked mapping of event ${eventId.value}"
        val key = io { rows.inQueue(policy) { requireNotNull(get(id)) { missing } } }.key
        val skip: RowTx.() -> Unit = {
            val row = requireNotNull(get(id)) { missing }
            val leaseUntil = row.leaseUntil
            check(leaseUntil == null || leaseUntil <= clock()) { "The parked mapping of event ${eventId.value} in $policy is running; try again" }
            delete(id)
        }
        if (key == null) {
            io { rows.inQueue(policy) { skip() } }
        } else {
            scheduleFront(policy, io { rows.inLine(policy, key) { skip(); front()?.takeUnless { it.blocked } } })
        }
    }

    /** The line of ordered reaction [id], read without its lock: every check on the row is made again under it. */
    private fun lineOf(
        queue: String,
        id: EventReactionId,
    ): String {
        val row = rows.inQueue(queue) { get(id.value) }
        return requireNotNull(row?.key) { "Reaction ${id.value} of $queue is not blocked" }
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

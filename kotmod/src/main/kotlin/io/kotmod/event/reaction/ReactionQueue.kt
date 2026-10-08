package io.kotmod.event.reaction

import io.kotmod.scheduling.Cancellable
import io.kotmod.scheduling.TaskOutcome
import io.kotmod.scheduling.TaskQueue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

private val log = LoggerFactory.getLogger(ReactionQueue::class.java)

/**
 * Added to an item's timeout to give its lease: how long a started ordered item is protected from a duplicate run. The
 * lease also covers the policy's `onFailure` and `onCompletion`, which run outside the timeout, so they must be quick.
 */
internal val LEASE_MARGIN: Duration = 30.seconds


/** One delivery of [item], recognised by [id]; [attempt] counts earlier attempts, from 0. */
internal data class Delivery<T>(
    val id: EventReactionId,
    val item: T,
    val attempt: Int,
)

/** Unordered work to queue: [item] as [id], not before [notBefore] if given. */
internal data class Produced<T>(
    val id: EventReactionId,
    val item: T,
    val notBefore: Instant? = null,
)

/** Ordered work: [item] as [id], at ([sequence], [ordinal]) in line [key]. */
internal data class OrderedItem<T>(
    val id: EventReactionId,
    val key: String,
    val sequence: Long,
    val ordinal: Int,
    val item: T,
)

/** What a delivery's handler decided. */
internal sealed interface ItemResult<out T> {
    /** Done (succeeded, or gave up and moves on). */
    data object Completed : ItemResult<Nothing>

    /** Run again after [delay]. */
    data class Retry(
        val delay: Duration,
    ) : ItemResult<Nothing>

    /** Gave up and holds back its line until an operator acts. Unordered or kept work has no line, so it just finishes. */
    data object Blocked : ItemResult<Nothing>

    /**
     * Replace this item with [items] (a recovered parked mapping): ordered, they take its place in line, in order;
     * kept, they are queued as unordered work.
     */
    data class Replaced<T>(
        val items: List<Produced<T>>,
    ) : ItemResult<T>
}

/** A task's payload: kotmod JSON, opaque to the scheduler. */
@Serializable
internal sealed interface TaskPayload {
    /** Unordered work, carried whole: its state lives in the task. */
    @Serializable
    @SerialName("unordered")
    data class Unordered(
        val reactionId: String,
        val item: String,
        val attempt: Int = 0,
        val notBefore: String? = null,
    ) : TaskPayload

    /** "Run line [key]'s front item, [reactionId]." */
    @Serializable
    @SerialName("front")
    data class Front(
        val key: String,
        val reactionId: String,
    ) : TaskPayload

    /** "Run kept row [reactionId]." */
    @Serializable
    @SerialName("kept")
    data class Kept(
        val reactionId: String,
    ) : TaskPayload
}

/** Task names and payloads, shared by [ReactionQueue] and the operator tools. */
internal object ReactionTasks {
    fun frontName(
        key: String,
        reactionId: String,
    ): String = "line/$key/$reactionId"

    fun encode(payload: TaskPayload): String = Json.encodeToString(TaskPayload.serializer(), payload)

    fun decode(payload: String): TaskPayload = Json.decodeFromString(TaskPayload.serializer(), payload)

    suspend fun scheduleFront(
        tasks: TaskQueue,
        key: String,
        reactionId: String,
        at: Instant,
    ) = tasks.schedule(frontName(key, reactionId), encode(TaskPayload.Front(key, reactionId)), at)

    suspend fun scheduleKept(
        tasks: TaskQueue,
        reactionId: String,
        at: Instant,
    ) = tasks.schedule(reactionId, encode(TaskPayload.Kept(reactionId)), at)
}

/**
 * Rethrows [error] if it is a [CancellationException] and this coroutine was cancelled. A CancellationException thrown
 * while the coroutine is still active came from app code: a failure like any other.
 */
internal suspend fun rethrowIfCancelled(error: Throwable) {
    if (error is CancellationException && !currentCoroutineContext().isActive) throw error
}

/**
 * Runs one queue's work on a [TaskQueue]:
 *
 * - **Unordered work** lives in its task: the payload carries the item, its attempt count and `notBefore`.
 * - **Ordered work** lives in [rows], one line per key. Only a line's front item is scheduled, as a task named after
 *   it. A delivery takes the line's lock, checks the item is still the front and not running, counts the attempt and
 *   leases it (its timeout plus [LEASE_MARGIN]), then runs it without the lock. Finishing it deletes or replaces the
 *   row and schedules the new front before returning, so a crash in between is repaired when the backend delivers the
 *   task again. Leases compare times from this node's clock; the margin absorbs normal clock differences.
 * - **Kept work** (unordered parked mappings) lives in [rows] without a line, with its own task.
 *
 * Every write to [rows] and every schedule is idempotent, so any step can be repeated after a crash.
 */
internal class ReactionQueue<T : Any>(
    val name: String,
    private val itemSerializer: KSerializer<T>,
    private val tasks: TaskQueue,
    private val rows: ReactionRows,
    private val lease: Duration,
    private val clock: () -> Instant = { Clock.System.now() },
) {
    @Volatile
    private var subscription: Cancellable? = null

    /** Queues unordered [work]. Publishing an id that is pending does nothing. */
    suspend fun publish(work: Produced<T>) {
        val payload = TaskPayload.Unordered(work.id.value, encode(work.item), 0, work.notBefore?.toString())
        tasks.schedule(work.id.value, ReactionTasks.encode(payload), work.notBefore ?: clock())
    }

    /** Adds [items] to their lines (ignoring ones already there), then schedules each affected line's front. */
    suspend fun publishOrdered(items: List<OrderedItem<T>>) {
        val fronts =
            items.groupBy { it.key }.mapNotNull { (key, line) ->
                io {
                    rows.inLine(name, key) {
                        line.forEach { insert(ReactionRow(name, it.id.value, RowKind.ORDERED, key, it.sequence, it.ordinal, encode(it.item))) }
                        front()?.takeUnless { it.blocked }
                    }
                }
            }
        fronts.forEach { ReactionTasks.scheduleFront(tasks, checkNotNull(it.key), it.reactionId, clock()) }
    }

    /** Keeps [item] as kept row [id] (an unordered parked mapping) and schedules it. */
    suspend fun keep(
        id: EventReactionId,
        item: T,
    ) {
        io { rows.inQueue(name) { insert(ReactionRow(name, id.value, RowKind.KEPT, null, null, null, encode(item))) } }
        ReactionTasks.scheduleKept(tasks, id.value, clock())
    }

    /** Starts delivering this queue's work to [handle]. Does nothing if already started. */
    fun start(handle: suspend (Delivery<T>) -> ItemResult<T>) {
        if (subscription != null) return
        subscription =
            tasks.subscribe { _, payload ->
                when (val task = ReactionTasks.decode(payload)) {
                    is TaskPayload.Unordered -> runUnordered(task, handle)
                    is TaskPayload.Front -> runFront(task, handle)
                    is TaskPayload.Kept -> runKept(task, handle)
                }
            }
    }

    /** Stops delivering. */
    fun stop() {
        subscription?.cancel()
        subscription = null
    }

    /** Schedules again every line front and kept row idle for [idleFor] (a task a backend lost). Idempotent. */
    suspend fun sweep(idleFor: Duration) {
        val now = clock()
        io { rows.stale(name, now, now - idleFor) }.forEach { row ->
            when (row.kind) {
                RowKind.ORDERED -> ReactionTasks.scheduleFront(tasks, checkNotNull(row.key), row.reactionId, now)
                RowKind.KEPT -> ReactionTasks.scheduleKept(tasks, row.reactionId, now)
            }
        }
    }

    private suspend fun runUnordered(
        task: TaskPayload.Unordered,
        handle: suspend (Delivery<T>) -> ItemResult<T>,
    ): TaskOutcome {
        val notBefore = task.notBefore?.let(Instant::parse)
        if (notBefore != null && clock() < notBefore) return TaskOutcome.RunAgain(notBefore, ReactionTasks.encode(task))
        return when (val result = handle(Delivery(EventReactionId(task.reactionId), decode(task.item), task.attempt))) {
            ItemResult.Completed -> TaskOutcome.Done
            is ItemResult.Retry -> TaskOutcome.RunAgain(clock() + result.delay, ReactionTasks.encode(task.copy(attempt = task.attempt + 1)))
            // A policy switched to ordered BlockAggregate can give up on work queued while it was unordered.
            ItemResult.Blocked -> {
                log.warn("Reaction {} in {} gave up and would block its line, but unordered work has no line to block; finishing it", task.reactionId, name)
                TaskOutcome.Done
            }
            is ItemResult.Replaced -> error("Unordered reaction ${task.reactionId} in $name can't be replaced")
        }
    }

    private sealed interface Start {
        data class Run(
            val row: ReactionRow,
        ) : Start

        data class Busy(
            val until: Instant,
        ) : Start

        data class Skip(
            val front: ReactionRow?,
        ) : Start
    }

    private suspend fun runFront(
        task: TaskPayload.Front,
        handle: suspend (Delivery<T>) -> ItemResult<T>,
    ): TaskOutcome {
        val now = clock()
        val start =
            io {
                rows.inLine(name, task.key) {
                    val front = front()
                    val leaseUntil = front?.leaseUntil
                    when {
                        front == null || front.blocked -> Start.Skip(null)
                        front.reactionId != task.reactionId -> Start.Skip(front)
                        leaseUntil != null && leaseUntil > now -> Start.Busy(leaseUntil)
                        else -> front.copy(attempts = front.attempts + 1, leaseUntil = now + lease).also { update(it) }.let { Start.Run(it) }
                    }
                }
            }
        return when (start) {
            is Start.Skip -> {
                start.front?.let { ReactionTasks.scheduleFront(tasks, task.key, it.reactionId, now) }
                TaskOutcome.Done
            }
            is Start.Busy -> TaskOutcome.RunAgain(start.until, ReactionTasks.encode(task))
            is Start.Run -> runStarted(task, start.row, handle)
        }
    }

    private suspend fun runStarted(
        task: TaskPayload.Front,
        row: ReactionRow,
        handle: suspend (Delivery<T>) -> ItemResult<T>,
    ): TaskOutcome {
        val result =
            try {
                handle(Delivery(EventReactionId(row.reactionId), decode(row.item), row.attempts - 1))
            } catch (e: CancellationException) {
                // A shutdown interrupted the attempt: give it back, so restarts don't use up attempts.
                if (!currentCoroutineContext().isActive) withContext(NonCancellable) { release(task.key, row.reactionId, giveBack = true) }
                throw e
            }
        return when (result) {
            ItemResult.Completed -> advance(task.key) { delete(row.reactionId) }
            is ItemResult.Replaced ->
                advance(task.key) {
                    delete(row.reactionId)
                    result.items.forEachIndexed { n, work ->
                        insert(ReactionRow(name, work.id.value, RowKind.ORDERED, task.key, row.sequence, n, encode(work.item)))
                    }
                }
            is ItemResult.Retry -> {
                release(task.key, row.reactionId, giveBack = false)
                TaskOutcome.RunAgain(clock() + result.delay, ReactionTasks.encode(task))
            }
            ItemResult.Blocked -> {
                io { rows.inLine(name, task.key) { get(row.reactionId)?.let { update(it.copy(blocked = true, leaseUntil = null)) } } }
                TaskOutcome.Done
            }
        }
    }

    private suspend fun release(
        key: String,
        reactionId: String,
        giveBack: Boolean,
    ) {
        io {
            rows.inLine(name, key) {
                get(reactionId)?.let { update(it.copy(attempts = if (giveBack) it.attempts - 1 else it.attempts, leaseUntil = null)) }
            }
        }
    }

    private suspend fun advance(
        key: String,
        change: LineTx.() -> Unit,
    ): TaskOutcome {
        val next =
            io {
                rows.inLine(name, key) {
                    change()
                    front()?.takeUnless { it.blocked }
                }
            }
        next?.let { ReactionTasks.scheduleFront(tasks, key, it.reactionId, clock()) }
        return TaskOutcome.Done
    }

    private suspend fun runKept(
        task: TaskPayload.Kept,
        handle: suspend (Delivery<T>) -> ItemResult<T>,
    ): TaskOutcome {
        val row = io { rows.inQueue(name) { get(task.reactionId) } } ?: return TaskOutcome.Done
        return when (val result = handle(Delivery(EventReactionId(row.reactionId), decode(row.item), row.attempts))) {
            ItemResult.Completed -> {
                io { rows.inQueue(name) { delete(row.reactionId) } }
                TaskOutcome.Done
            }
            is ItemResult.Replaced -> {
                result.items.forEach { publish(it) }
                io { rows.inQueue(name) { delete(row.reactionId) } }
                TaskOutcome.Done
            }
            is ItemResult.Retry -> {
                io { rows.inQueue(name) { get(row.reactionId)?.let { update(it.copy(attempts = it.attempts + 1)) } } }
                TaskOutcome.RunAgain(clock() + result.delay, ReactionTasks.encode(task))
            }
            ItemResult.Blocked -> {
                log.warn("Kept reaction {} in {} would block its line, but kept work has no line to block; finishing it", row.reactionId, name)
                io { rows.inQueue(name) { delete(row.reactionId) } }
                TaskOutcome.Done
            }
        }
    }

    private fun encode(item: T): String = Json.encodeToString(itemSerializer, item)

    private fun decode(item: String): T = Json.decodeFromString(itemSerializer, item)

    private suspend fun <R> io(block: () -> R): R = withContext(Dispatchers.IO) { block() }
}

package io.kotmod.scheduling

import kotlin.time.Instant

/**
 * Runs kotmod's work: one [TaskQueue] per event policy and per process manager channel, named after it. kotmod keeps
 * ordering, attempt counts and blocked work itself, so a scheduler only needs to run named tasks at a time.
 * `kotmod-db-scheduler` provides `DbSchedulerTaskScheduler`.
 */
interface TaskScheduler {
    /**
     * The queue named [name]. Calling it again with the same name returns a queue for the same tasks. kotmod asks for
     * each queue it runs before starting; a backend may refuse new names after it started, but must still let kotmod
     * schedule into a queue it already knows.
     */
    fun queue(name: String): TaskQueue
}

/** One queue of named tasks, each with an opaque payload. */
interface TaskQueue {
    /**
     * Runs [payload] as task [name] at [at] or later, unless a task named [name] is already pending in this queue (then
     * it does nothing).
     *
     * kotmod sometimes schedules a name again after its earlier task finished: an operator's
     * `ReactionOperations.retryBlocked` and the repair sweep schedule a line's front under its usual name. A backend
     * must accept that (db-scheduler does). A backend that refuses recently finished names (such as Cloud Tasks) must
     * still make such a schedule succeed, for example by deriving a unique name.
     */
    suspend fun schedule(
        name: String,
        payload: String,
        at: Instant,
    )

    /**
     * Delivers due tasks to [handler], at least once each, until the returned handle is cancelled. [handler] returns
     * [TaskOutcome.Done] when the task is finished, or [TaskOutcome.RunAgain] to run it again later with a new
     * payload. If [handler] throws, the task is delivered again later, after the backend's own backoff.
     */
    fun subscribe(handler: suspend (name: String, payload: String) -> TaskOutcome): Cancellable
}

/** What happens to a delivered task. */
sealed interface TaskOutcome {
    /** The task is finished; remove it. */
    data object Done : TaskOutcome

    /** Run the task again at [at] or later, with [payload], under the same name. */
    data class RunAgain(
        val at: Instant,
        val payload: String,
    ) : TaskOutcome
}

/** A handle for undoing a subscription. */
fun interface Cancellable {
    /** Undoes the subscription. Calling it more than once has no further effect. */
    fun cancel()
}

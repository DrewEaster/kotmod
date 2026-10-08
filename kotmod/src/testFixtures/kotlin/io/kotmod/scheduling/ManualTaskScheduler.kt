package io.kotmod.scheduling

import kotlin.time.Instant

/**
 * A [TaskScheduler] for tests that delivers nothing by itself. A test delivers tasks explicitly, in schedule order
 * (strictly first in, first out, ignoring due times), and can make scheduling fail, deliver a task twice at once, or
 * lose a task. A task stays pending while it is being delivered, as in a real backend.
 */
class ManualTaskScheduler : TaskScheduler {
    /** A pending task. */
    data class Task(
        val name: String,
        val payload: String,
        val at: Instant,
    )

    private val queues = mutableMapOf<String, Queue>()

    override fun queue(name: String): Queue = synchronized(queues) { queues.getOrPut(name) { Queue(name) } }

    /** One manual queue. */
    class Queue internal constructor(
        val name: String,
    ) : TaskQueue {
        private class Entry(
            var task: Task,
            var running: Boolean = false,
        )

        private val entries = mutableListOf<Entry>()

        @Volatile
        private var handler: (suspend (String, String) -> TaskOutcome)? = null

        /** Makes the next this-many calls to [schedule] throw, without recording anything. */
        @Volatile
        var failNextSchedules: Int = 0

        /** Runs at the start of every [schedule] call; a test can suspend in it to control interleavings. */
        @Volatile
        var beforeSchedule: (suspend (name: String) -> Unit)? = null

        /** The pending tasks, in delivery order. */
        val pending: List<Task> get() = synchronized(entries) { entries.map { it.task } }

        override suspend fun schedule(
            name: String,
            payload: String,
            at: Instant,
        ) {
            beforeSchedule?.invoke(name)
            synchronized(entries) {
                if (failNextSchedules > 0) {
                    failNextSchedules--
                    throw IllegalStateException("Scheduling $name failed (simulated)")
                }
                if (entries.none { it.task.name == name }) entries += Entry(Task(name, payload, at))
            }
        }

        override fun subscribe(handler: suspend (name: String, payload: String) -> TaskOutcome): Cancellable {
            this.handler = handler
            return Cancellable { this.handler = null }
        }

        /** Delivers the first pending task that isn't being delivered; `null` if there is none. */
        suspend fun deliverNext(): TaskOutcome? {
            val (entry, current) =
                synchronized(entries) {
                    val candidate = entries.firstOrNull { !it.running } ?: return null
                    // Check for a handler before marking the entry running, so the task stays deliverable.
                    val current = checkNotNull(handler) { "Nothing subscribed to ${this.name}" }
                    candidate.running = true
                    candidate to current
                }
            return run(entry, current)
        }

        /** Delivers the pending task named [name]. */
        suspend fun deliver(name: String): TaskOutcome {
            val (entry, current) =
                synchronized(entries) {
                    val candidate = checkNotNull(entries.firstOrNull { it.task.name == name && !it.running }) { "No pending task $name in ${this.name}" }
                    val current = checkNotNull(handler) { "Nothing subscribed to ${this.name}" }
                    candidate.running = true
                    candidate to current
                }
            return run(entry, current)
        }

        /** Runs the pending task named [name] without taking it or marking it, as a duplicate delivery would. */
        suspend fun deliverDuplicate(name: String): TaskOutcome {
            val task = synchronized(entries) { checkNotNull(entries.firstOrNull { it.task.name == name }) { "No task $name" }.task }
            return checkNotNull(handler) { "Nothing subscribed to ${this.name}" }(task.name, task.payload)
        }

        /** Delivers tasks until none is pending (at most [max] deliveries). */
        suspend fun deliverAll(max: Int = 1000): List<TaskOutcome> {
            val outcomes = mutableListOf<TaskOutcome>()
            repeat(max) { outcomes += deliverNext() ?: return outcomes }
            error("Still delivering after $max deliveries in ${this.name}")
        }

        /** Drops the pending task named [name], as a backend that lost it would. */
        fun lose(name: String) {
            synchronized(entries) { entries.removeAll { it.task.name == name } }
        }

        private suspend fun run(
            entry: Entry,
            current: suspend (String, String) -> TaskOutcome,
        ): TaskOutcome {
            val outcome =
                try {
                    current(entry.task.name, entry.task.payload)
                } catch (e: Throwable) {
                    synchronized(entries) { moveToBack(entry) }
                    throw e
                }
            synchronized(entries) {
                when (outcome) {
                    TaskOutcome.Done -> entries.remove(entry)
                    is TaskOutcome.RunAgain -> {
                        entry.task = entry.task.copy(payload = outcome.payload, at = outcome.at)
                        moveToBack(entry)
                    }
                }
            }
            return outcome
        }

        private fun moveToBack(entry: Entry) {
            entry.running = false
            entries.remove(entry)
            entries += entry
        }
    }
}

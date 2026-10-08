# kotmod-owned ordering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move per-aggregate ordering into a kotmod-owned Postgres table and run all reaction work on a minimal `TaskScheduler` interface, with db-scheduler as its first adapter.

**Architecture:** A new public `io.kotmod.scheduling` package (`TaskScheduler`, `TaskQueue`, `TaskOutcome`, `Cancellable`) replaces the old queue interfaces. An internal engine, `ReactionQueue<T>` (package `io.kotmod.event.reaction`), runs three kinds of work on one `TaskQueue`: unordered items (state in the task payload), ordered items (rows in `ddd_reaction_row`, one line per aggregate, only the front scheduled), and kept items (unordered parked mappings, rows without a line). Event policies (`PolicyRuntime`) and process managers build on `ReactionQueue`; `ReactionOperations` gives operators SQL-backed tools; `kotmod-db-scheduler` shrinks to `DbSchedulerTaskScheduler`.

**Tech Stack:** Kotlin 2.x, kotlinx.coroutines, kotlinx.serialization, JDBC on Postgres 17 (Testcontainers), db-scheduler, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-10-08-kotmod-owned-ordering-design.md`

## Global Constraints

- Release 0.4.0. No backwards compatibility: no deprecated aliases, no migration, old APIs are deleted outright.
- Every `@Test` function returns `Unit` (use `runBlocking<Unit> { … }`); the guard test fails any non-void `@Test`.
- No new compiler warnings.
- README compiled snippets stay byte-identical to the example files in `examples/src/integrationTest/kotlin/io/kotmod/readme/`.
- Commit messages are imperative and end with exactly `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Table and column names: table `ddd_reaction_row`; the spec's `policy` column is named `queue_name` (it holds policy names and process manager channel names).
- Ordered lines are keyed `"<aggregateType>/<aggregateId>"` (as today's ordering key).
- Reaction ids are unchanged: `<policy>/<eventId>/<n>` and `<policy>/<eventId>/mapping`; process manager ids `in-…`, `sched-…`, `cmd-…`, `rejected-…`.
- Lease = item timeout + `LEASE_MARGIN` (30 seconds). Repair sweep: every 10 minutes, for rows idle 30 minutes.
- Locked steps (`ReactionRows.inLine`) never call app code (`handle`, `onFailure`, `onCompletion`, `on(...)` blocks).
- Tasks 6–8 change the core API; `kotmod-db-scheduler` and `examples` don't compile again until Tasks 9–10. Until then, verify with `./gradlew :kotmod:test :kotmod:integrationTest`.

## Review Focus

- **An event policy switched from ordered to unordered while ordered rows exist.** Its existing `front` tasks must still run (the engine runs any payload kind it receives), so nothing is stranded. Test in Task 6.
- **Very long aggregate ids and policy names.** All string columns are `TEXT`; task names may be long. Test a 300-character aggregate id in Task 2.
- **An event policy removed from the app while it has rows.** Nothing runs them and the sweep skips unregistered queues; document it in Known limitations (Task 10) and make `ReactionOperations` able to list and skip them (Task 8 test).
- **Clock differences between nodes.** Leases compare `lease_until` with the delivering node's clock; the 30-second margin absorbs normal skew. Document in the `ReactionQueue` KDoc (Task 4).
- **A process manager's ordered inputs and a contract subscription for the same aggregate.** They use different queues (`<type>-inputs`, `<type>-contract-<name>`) and so different lines; ordering holds within each. Test in Task 7.

---

### Task 1: The scheduler interface and the manual test scheduler

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/scheduling/TaskScheduler.kt`
- Create: `kotmod/src/testFixtures/kotlin/io/kotmod/scheduling/ManualTaskScheduler.kt`
- Create: `kotmod/src/testFixtures/kotlin/io/kotmod/scheduling/TaskSchedulerContract.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/scheduling/ManualTaskSchedulerTest.kt`

**Interfaces:**
- Produces: `TaskScheduler`, `TaskQueue`, `TaskOutcome` (`Done`, `RunAgain(at, payload)`), `fun interface Cancellable` in `io.kotmod.scheduling`; `ManualTaskScheduler` (test fixture) with `queue(name): ManualTaskScheduler.Queue`, `Queue.pending`, `deliverNext()`, `deliver(name)`, `deliverDuplicate(name)`, `deliverAll(max)`, `lose(name)`, `failNextSchedules`, `beforeSchedule`; abstract `TaskSchedulerContract` with hooks `scheduler()`, `start()`, `stop()`.

- [ ] **Step 1: Write the interface**

```kotlin
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
     * it does nothing). A backend may also refuse a name that finished recently; kotmod never reuses a finished name.
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
```

- [ ] **Step 2: Write `ManualTaskSchedulerTest` (failing: the class doesn't exist yet)**

```kotlin
package io.kotmod.scheduling

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Instant

class ManualTaskSchedulerTest {
    private val t0 = Instant.parse("2026-10-08T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val queue = scheduler.queue("q")

    @Test
    fun `schedules once per pending name and delivers in schedule order`() =
        runBlocking<Unit> {
            val seen = mutableListOf<String>()
            queue.subscribe { name, payload ->
                seen += "$name:$payload"
                TaskOutcome.Done
            }
            queue.schedule("b", "1", t0)
            queue.schedule("a", "2", t0)
            queue.schedule("b", "3", t0)

            queue.deliverAll()

            assertEquals(listOf("b:1", "a:2"), seen)
            assertTrue(queue.pending.isEmpty())
        }

    @Test
    fun `RunAgain moves the task to the back with its new payload and time`() =
        runBlocking<Unit> {
            var first = true
            queue.subscribe { _, payload ->
                if (first) {
                    first = false
                    TaskOutcome.RunAgain(t0, "again:$payload")
                } else {
                    TaskOutcome.Done
                }
            }
            queue.schedule("a", "1", t0)
            queue.schedule("b", "2", t0)

            queue.deliverNext()

            assertEquals(listOf("b", "a"), queue.pending.map { it.name })
            assertEquals("again:1", queue.pending.last().payload)
        }

    @Test
    fun `a task being delivered is still pending, so scheduling its name again is ignored`() =
        runBlocking<Unit> {
            queue.subscribe { name, _ ->
                queue.schedule(name, "duplicate", t0)
                TaskOutcome.Done
            }
            queue.schedule("a", "1", t0)

            queue.deliverNext()

            assertTrue(queue.pending.isEmpty())
        }

    @Test
    fun `a throwing handler leaves the task pending and rethrows`() =
        runBlocking<Unit> {
            queue.subscribe { _, _ -> error("boom") }
            queue.schedule("a", "1", t0)

            assertFailsWith<IllegalStateException> { queue.deliverNext() }
            assertEquals(listOf("a"), queue.pending.map { it.name })
        }

    @Test
    fun `failNextSchedules makes schedule throw without recording the task`() =
        runBlocking<Unit> {
            queue.failNextSchedules = 1
            assertFailsWith<IllegalStateException> { queue.schedule("a", "1", t0) }
            queue.schedule("b", "1", t0)
            assertEquals(listOf("b"), queue.pending.map { it.name })
        }

    @Test
    fun `deliverDuplicate runs a pending task without taking it, and lose drops one`() =
        runBlocking<Unit> {
            val release = CompletableDeferred<Unit>()
            var calls = 0
            queue.subscribe { _, _ ->
                calls++
                release.await()
                TaskOutcome.Done
            }
            queue.schedule("a", "1", t0)
            val firstDelivery = async { queue.deliverNext() }
            val duplicate = async { queue.deliverDuplicate("a") }
            release.complete(Unit)
            firstDelivery.await()
            duplicate.await()
            assertEquals(2, calls)

            queue.schedule("b", "1", t0)
            queue.lose("b")
            assertNull(queue.deliverNext())
        }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.scheduling.ManualTaskSchedulerTest'`
Expected: compilation FAILS (`ManualTaskScheduler` unresolved).

- [ ] **Step 4: Write `ManualTaskScheduler`**

```kotlin
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
            val entry = synchronized(entries) { entries.firstOrNull { !it.running }?.also { it.running = true } } ?: return null
            return run(entry)
        }

        /** Delivers the pending task named [name]. */
        suspend fun deliver(name: String): TaskOutcome {
            val entry =
                synchronized(entries) {
                    checkNotNull(entries.firstOrNull { it.task.name == name && !it.running }) { "No pending task $name in $this.name" }
                        .also { it.running = true }
                }
            return run(entry)
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

        private suspend fun run(entry: Entry): TaskOutcome {
            val current = checkNotNull(handler) { "Nothing subscribed to ${this.name}" }
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.scheduling.ManualTaskSchedulerTest'`
Expected: PASS (6 tests).

- [ ] **Step 6: Write the contract suite (test fixture; run by real backends from Task 10 on)**

```kotlin
package io.kotmod.scheduling

import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds

/**
 * What every [TaskScheduler] backend must do. A backend's test extends this: [scheduler] returns the backend under
 * test (the same instance for the whole test); queues named [QUEUE_A] and [QUEUE_B] are created with it before
 * [start], which starts delivery; [stop] stops it.
 */
abstract class TaskSchedulerContract {
    protected abstract fun scheduler(): TaskScheduler

    protected abstract fun start()

    protected abstract fun stop()

    @AfterTest
    fun stopScheduler() = stop()

    private fun recording(queue: TaskQueue): MutableList<Pair<String, String>> {
        val seen = CopyOnWriteArrayList<Pair<String, String>>()
        queue.subscribe { name, payload ->
            seen += name to payload
            TaskOutcome.Done
        }
        return seen
    }

    @Test
    fun `a scheduled task is delivered once with its payload`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = recording(queue)
            queue.schedule("t-1", "payload-1", Clock.System.now())
            start()
            eventually { seen.isNotEmpty() }
            delay(1.seconds)
            assertEquals(listOf("t-1" to "payload-1"), seen)
        }

    @Test
    fun `scheduling a pending name again does nothing`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = recording(queue)
            val at = Clock.System.now() + 2.seconds
            queue.schedule("t-1", "first", at)
            queue.schedule("t-1", "second", at)
            start()
            eventually { seen.isNotEmpty() }
            delay(1.seconds)
            assertEquals(listOf("t-1" to "first"), seen)
        }

    @Test
    fun `a task is not delivered before its time`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = recording(queue)
            val at = Clock.System.now() + 3.seconds
            queue.schedule("t-1", "later", at)
            start()
            delay(1.seconds)
            assertTrue(seen.isEmpty())
            eventually { seen.isNotEmpty() }
            assertTrue(Clock.System.now() >= at)
        }

    @Test
    fun `RunAgain delivers the task again later with the new payload`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = CopyOnWriteArrayList<String>()
            queue.subscribe { _, payload ->
                seen += payload
                if (payload == "first") TaskOutcome.RunAgain(Clock.System.now() + 1.seconds, "second") else TaskOutcome.Done
            }
            queue.schedule("t-1", "first", Clock.System.now())
            start()
            eventually { seen.size == 2 }
            assertEquals(listOf("first", "second"), seen)
        }

    @Test
    fun `a task whose handler throws is delivered again`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val attempts = CopyOnWriteArrayList<String>()
            queue.subscribe { name, _ ->
                attempts += name
                if (attempts.size == 1) error("transient") else TaskOutcome.Done
            }
            queue.schedule("t-1", "p", Clock.System.now())
            start()
            eventually(timeout = 60.seconds) { attempts.size == 2 }
        }

    @Test
    fun `queues are separate, and asking for a queue again reaches the same tasks`() =
        runBlocking<Unit> {
            val a = scheduler().queue(QUEUE_A)
            val b = scheduler().queue(QUEUE_B)
            val seenA = recording(a)
            val seenB = recording(b)
            scheduler().queue(QUEUE_A).schedule("t-1", "for-a", Clock.System.now())
            start()
            eventually { seenA.isNotEmpty() }
            delay(1.seconds)
            assertEquals(listOf("t-1" to "for-a"), seenA)
            assertTrue(seenB.isEmpty())
        }

    @Test
    fun `nothing is delivered to a cancelled subscription`() =
        runBlocking<Unit> {
            val queue = scheduler().queue(QUEUE_A)
            val seen = CopyOnWriteArrayList<String>()
            val subscription =
                queue.subscribe { name, _ ->
                    seen += name
                    TaskOutcome.Done
                }
            subscription.cancel()
            queue.schedule("t-1", "p", Clock.System.now())
            start()
            delay(2.seconds)
            assertTrue(seen.isEmpty())
        }

    companion object {
        const val QUEUE_A = "contract-a"
        const val QUEUE_B = "contract-b"
    }
}
```

The last contract test only requires that a cancelled subscription receives nothing; a backend may keep the task for a later subscriber.

- [ ] **Step 7: Compile test fixtures and run the unit tests**

Run: `./gradlew :kotmod:compileTestFixturesKotlin :kotmod:test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: Commit**

```bash
git add kotmod/src/main/kotlin/io/kotmod/scheduling kotmod/src/testFixtures/kotlin/io/kotmod/scheduling kotmod/src/test/kotlin/io/kotmod/scheduling
git commit -m "Add the TaskScheduler interface, a manual test scheduler and the scheduler contract suite"
```

---

### Task 2: The `ddd_reaction_row` table and its Postgres access

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/event/reaction/ReactionRows.kt`
- Create: `kotmod/src/main/kotlin/io/kotmod/postgres/PostgresReactionRows.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/postgres/DddSchema.kt` (append the table)
- Modify: `kotmod/src/testFixtures/kotlin/io/kotmod/postgres/support/IntegrationTest.kt` (truncate `ddd_reaction_row`)
- Create: `kotmod/src/test/kotlin/io/kotmod/event/reaction/InMemoryReactionRows.kt`
- Test: `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PostgresReactionRowsIntegrationTest.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/event/reaction/InMemoryReactionRowsTest.kt`

**Interfaces:**
- Produces (all `internal`): `RowKind { ORDERED, KEPT }`, `ReactionRow`, `RowTx` (`get`, `insert`, `delete`, `update`), `LineTx : RowTx` (`front`), `ReactionRows` (`inLine`, `inQueue`, `list`, `stale`), `PostgresReactionRows(jdbc, clock)`, test-only `InMemoryReactionRows`.

- [ ] **Step 1: Write the row types and interface**

```kotlin
package io.kotmod.event.reaction

import kotlin.time.Instant

/** An ordered item in its aggregate's line, or a kept item (an unordered parked mapping) outside any line. */
internal enum class RowKind { ORDERED, KEPT }

/**
 * One row of `ddd_reaction_row`. Ordered rows have a [key] (the line: `<aggregateType>/<aggregateId>`) and a place in
 * it ([sequence], [ordinal]); kept rows have none. [item] is the queue's own JSON. [attempts] counts attempts started
 * (ordered) or failed (kept).
 */
internal data class ReactionRow(
    val queue: String,
    val reactionId: String,
    val kind: RowKind,
    val key: String?,
    val sequence: Long?,
    val ordinal: Int?,
    val item: String,
    val attempts: Int = 0,
    val blocked: Boolean = false,
    val leaseUntil: Instant? = null,
)

/** Reads and writes one queue's rows inside a transaction. */
internal interface RowTx {
    fun get(reactionId: String): ReactionRow?

    /** Inserts [row] unless its reaction id, or (ordered) its place in line, is taken; returns whether it did. */
    fun insert(row: ReactionRow): Boolean

    fun delete(reactionId: String)

    /** Saves [row]'s attempts, blocked flag and lease, and touches its update time. */
    fun update(row: ReactionRow)
}

/** A transaction holding one line's lock. */
internal interface LineTx : RowTx {
    /** The line's first row by (sequence, ordinal), blocked or not; `null` if the line is empty. */
    fun front(): ReactionRow?
}

/** Where kotmod keeps ordered work and kept items. Calls block the thread (JDBC). */
internal interface ReactionRows {
    /** Runs [block] in one transaction that holds the lock on line ([queue], [key]). */
    fun <R> inLine(
        queue: String,
        key: String,
        block: LineTx.() -> R,
    ): R

    /** Runs [block] in one transaction, without a line lock (for kept rows). */
    fun <R> inQueue(
        queue: String,
        block: RowTx.() -> R,
    ): R

    /** Every row of [queue], ordered by line and place in line (kept rows last, by reaction id). */
    fun list(queue: String): List<ReactionRow>

    /**
     * The rows of [queue] that should be running but may have lost their task: unblocked line fronts and kept rows,
     * not leased at [now], last changed before [before].
     */
    fun stale(
        queue: String,
        now: Instant,
        before: Instant,
    ): List<ReactionRow>
}
```

- [ ] **Step 2: Add the DDL to `DddSchema.ddl`** (after `ddd_consumer_offset`, inside the same script string)

```sql
CREATE TABLE ddd_reaction_row (
    queue_name    TEXT        NOT NULL,
    reaction_id   TEXT        NOT NULL,
    kind          TEXT        NOT NULL,
    line_key      TEXT,
    line_sequence BIGINT,
    line_ordinal  INT,
    item          TEXT        NOT NULL,
    attempts      INT         NOT NULL DEFAULT 0,
    blocked       BOOLEAN     NOT NULL DEFAULT FALSE,
    lease_until   TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_name, reaction_id)
);
CREATE UNIQUE INDEX ddd_reaction_row_line ON ddd_reaction_row (queue_name, line_key, line_sequence, line_ordinal);
```

Add `ddd_reaction_row` to the `TRUNCATE` list in `IntegrationTest.truncateDddTables`.

- [ ] **Step 3: Write the Postgres integration test (failing: no implementation yet)**

`PostgresReactionRowsIntegrationTest extends IntegrationTest`, with `private val now = Instant.parse("2026-10-08T10:00:00Z")` and `private val rows = PostgresReactionRows(jdbc) { now }`, and a helper `fun ordered(id: String, key: String = "Order/o-1", seq: Long, ord: Int = 0) = ReactionRow("q", id, RowKind.ORDERED, key, seq, ord, """{"n":"$id"}""")`. Tests (each `runBlocking<Unit>` not needed — these are blocking calls; plain `fun … { }` returning Unit):

1. `front is the lowest sequence then ordinal, whatever the insert order` — insert seq 6 ord 0, seq 5 ord 1, seq 5 ord 0 → `inLine("q","Order/o-1") { front() }?.reactionId` is the seq 5 ord 0 row.
2. `insert ignores a taken reaction id or a taken place in line` — insert `a`(5,0) → true; insert `a`(9,0) → false; insert `b`(5,0) → false; `list("q").map { it.reactionId } == listOf("a")`.
3. `kept rows have no line and never conflict on place` — two KEPT rows (null key/sequence/ordinal) both insert → true.
4. `update saves attempts, blocked and lease; delete removes` — round-trip through `get`.
5. `lines and queues are separate` — same key in queue `q2` and another key in `q` don't affect `front()`.
6. `stale returns unblocked, unleased fronts and kept rows changed before the cutoff` — rows: front of line A (idle), second of line A (not a front), front of line B blocked, front of line C leased until `now + 1.minutes`, a KEPT row; with `before = now + 1.seconds` → exactly line A's front and the KEPT row. With `before = now - 1.seconds` → empty.
7. `the line lock serialises two transactions on the same line` — thread 1 enters `inLine("q","k")`, signals a latch, sleeps 500 ms, inserts `x`; thread 2 waits for the latch then calls `inLine("q","k") { front() }` → sees `x` and took ≥ 400 ms. A third call on line `"k2"` during the sleep returns in < 300 ms.
8. `a 300-character aggregate id works as a line key` — insert and `front()` with `key = "Order/" + "x".repeat(300)`.

Run: `./gradlew :kotmod:integrationTest --tests '*PostgresReactionRowsIntegrationTest*'`
Expected: FAIL to compile (`PostgresReactionRows` unresolved).

- [ ] **Step 4: Write `PostgresReactionRows`**

```kotlin
package io.kotmod.postgres

import io.kotmod.event.reaction.LineTx
import io.kotmod.event.reaction.ReactionRow
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.RowKind
import io.kotmod.event.reaction.RowTx
import io.kotmod.jdbc.JdbcContext
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Timestamp
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import kotlin.time.toKotlinInstant

/** [ReactionRows] on `ddd_reaction_row`; a line's lock is a transaction-scoped advisory lock on its queue and key. */
internal class PostgresReactionRows(
    private val jdbc: JdbcContext,
    private val clock: () -> Instant = { Clock.System.now() },
) : ReactionRows {
    override fun <R> inLine(
        queue: String,
        key: String,
        block: LineTx.() -> R,
    ): R =
        jdbc.inTransaction {
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))").use { ps ->
                    ps.setString(1, "ddd_reaction_row\u0000$queue\u0000$key")
                    ps.executeQuery().close()
                }
                Line(conn, queue, key).block()
            }
        }

    override fun <R> inQueue(
        queue: String,
        block: RowTx.() -> R,
    ): R = jdbc.inTransaction { jdbc.withConnection { conn -> Rows(conn, queue).block() } }

    override fun list(queue: String): List<ReactionRow> =
        jdbc.withConnection { conn ->
            conn.query(
                "SELECT * FROM ddd_reaction_row WHERE queue_name = ? " +
                    "ORDER BY line_key NULLS LAST, line_sequence, line_ordinal, reaction_id",
                queue,
            )
        }

    override fun stale(
        queue: String,
        now: Instant,
        before: Instant,
    ): List<ReactionRow> =
        jdbc.withConnection { conn ->
            conn.query(
                """
                SELECT * FROM ddd_reaction_row r
                WHERE r.queue_name = ? AND NOT r.blocked
                  AND (r.lease_until IS NULL OR r.lease_until <= ?)
                  AND r.updated_at < ?
                  AND (r.kind = 'KEPT' OR NOT EXISTS (
                      SELECT 1 FROM ddd_reaction_row e
                      WHERE e.queue_name = r.queue_name AND e.line_key = r.line_key
                        AND (e.line_sequence, e.line_ordinal) < (r.line_sequence, r.line_ordinal)))
                ORDER BY r.line_key NULLS LAST, r.reaction_id
                """.trimIndent(),
                queue,
                Timestamp.from(now.toJavaInstant()),
                Timestamp.from(before.toJavaInstant()),
            )
        }

    private open inner class Rows(
        protected val conn: Connection,
        protected val queue: String,
    ) : RowTx {
        override fun get(reactionId: String): ReactionRow? =
            conn.query("SELECT * FROM ddd_reaction_row WHERE queue_name = ? AND reaction_id = ?", queue, reactionId).firstOrNull()

        override fun insert(row: ReactionRow): Boolean =
            conn
                .prepareStatement(
                    "INSERT INTO ddd_reaction_row (queue_name, reaction_id, kind, line_key, line_sequence, line_ordinal, " +
                        "item, attempts, blocked, lease_until, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT DO NOTHING",
                ).use { ps ->
                    ps.setString(1, queue)
                    ps.setString(2, row.reactionId)
                    ps.setString(3, row.kind.name)
                    ps.setString(4, row.key)
                    ps.setObject(5, row.sequence)
                    ps.setObject(6, row.ordinal)
                    ps.setString(7, row.item)
                    ps.setInt(8, row.attempts)
                    ps.setBoolean(9, row.blocked)
                    ps.setTimestamp(10, row.leaseUntil?.let { Timestamp.from(it.toJavaInstant()) })
                    ps.setTimestamp(11, Timestamp.from(clock().toJavaInstant()))
                    ps.executeUpdate() == 1
                }

        override fun delete(reactionId: String) {
            conn.prepareStatement("DELETE FROM ddd_reaction_row WHERE queue_name = ? AND reaction_id = ?").use { ps ->
                ps.setString(1, queue)
                ps.setString(2, reactionId)
                ps.executeUpdate()
            }
        }

        override fun update(row: ReactionRow) {
            conn
                .prepareStatement(
                    "UPDATE ddd_reaction_row SET attempts = ?, blocked = ?, lease_until = ?, updated_at = ? " +
                        "WHERE queue_name = ? AND reaction_id = ?",
                ).use { ps ->
                    ps.setInt(1, row.attempts)
                    ps.setBoolean(2, row.blocked)
                    ps.setTimestamp(3, row.leaseUntil?.let { Timestamp.from(it.toJavaInstant()) })
                    ps.setTimestamp(4, Timestamp.from(clock().toJavaInstant()))
                    ps.setString(5, queue)
                    ps.setString(6, row.reactionId)
                    ps.executeUpdate()
                }
        }
    }

    private inner class Line(
        conn: Connection,
        queue: String,
        private val key: String,
    ) : Rows(conn, queue),
        LineTx {
        override fun front(): ReactionRow? =
            conn
                .query(
                    "SELECT * FROM ddd_reaction_row WHERE queue_name = ? AND line_key = ? " +
                        "ORDER BY line_sequence, line_ordinal LIMIT 1",
                    queue,
                    key,
                ).firstOrNull()
    }
}

private fun Connection.query(
    sql: String,
    vararg params: Any,
): List<ReactionRow> =
    prepareStatement(sql).use { ps ->
        params.forEachIndexed { i, p -> ps.setObject(i + 1, p) }
        ps.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.toRow()) } }
    }

private fun ResultSet.toRow(): ReactionRow =
    ReactionRow(
        queue = getString("queue_name"),
        reactionId = getString("reaction_id"),
        kind = RowKind.valueOf(getString("kind")),
        key = getString("line_key"),
        sequence = getObject("line_sequence") as Long?,
        ordinal = getObject("line_ordinal") as Int?,
        item = getString("item"),
        attempts = getInt("attempts"),
        blocked = getBoolean("blocked"),
        leaseUntil = getTimestamp("lease_until")?.toInstant()?.toKotlinInstant(),
    )
```

- [ ] **Step 5: Run the integration test**

Run: `./gradlew :kotmod:integrationTest --tests '*PostgresReactionRowsIntegrationTest*'`
Expected: PASS (8 tests).

- [ ] **Step 6: Write `InMemoryReactionRows` (unit-test double) and its test**

`InMemoryReactionRows` implements `ReactionRows` with one `ReentrantLock` around every call (stronger than per-line locks; fine for unit tests) and a mutable list of rows. Each `inLine`/`inQueue` works on a copy of the list and replaces the list only if `block` returns normally (so a throwing block rolls back). `front()` = the queue+key rows sorted by `(sequence, ordinal)`, first. `insert` returns false if a row with the same `(queue, reactionId)` exists, or (ORDERED) the same `(queue, key, sequence, ordinal)`. `stale` applies the same rules as the SQL, using an `updatedAt` it records per row from an injected `clock: () -> Instant`. Expose `rows(queue): List<ReactionRow>` for assertions.

`InMemoryReactionRowsTest` repeats Postgres tests 1–6 against it, plus `a throwing block changes nothing`.

Run: `./gradlew :kotmod:test --tests '*InMemoryReactionRowsTest*'`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A kotmod/src
git commit -m "Add ddd_reaction_row with Postgres and in-memory access"
```

---

### Task 3: `ReactionQueue` — unordered work

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/event/reaction/ReactionQueue.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/event/reaction/ReactionQueueUnorderedTest.kt`

**Interfaces:**
- Consumes: Task 1's `TaskQueue`/`TaskOutcome`; Task 2's `ReactionRows`.
- Produces (internal): `LEASE_MARGIN`, `Delivery<T>(id, item, attempt)`, `Produced<T>(id, item, notBefore)`, `OrderedItem<T>(id, key, sequence, ordinal, item)`, `ItemResult<T>` (`Completed`, `Retry(delay)`, `Blocked`, `Replaced(items: List<Produced<T>>)`), `TaskPayload` (`Unordered`, `Front`, `Kept`), `ReactionTasks` (`frontName`, `encode`, `decode`, `scheduleFront`, `scheduleKept`), `ReactionQueue<T>(name, itemSerializer, tasks, rows, lease, clock)` with `publish(Produced<T>)`, `publishOrdered(List<OrderedItem<T>>)`, `keep(id, item)`, `start(handle)`, `stop()`, `sweep(idleFor)`; `suspend fun rethrowIfCancelled(error: Throwable)` (moved here from `PolicyRuntime`, made `internal`).

- [ ] **Step 1: Write the failing tests**

Test class setup: `var now = Instant.parse("2026-10-08T10:00:00Z")`, `val scheduler = ManualTaskScheduler()`, `val rows = InMemoryReactionRows { now }`, `fun queue() = ReactionQueue("q", String.serializer(), scheduler.queue("q"), rows, 90.seconds) { now }`, and a recording handler. Tests:

1. `published work is delivered with attempt 0 and finishes` — `publish(Produced(EventReactionId("r-1"), "hello"))`; handler records `Delivery` and returns `Completed`; `deliverAll()` → recorded `[Delivery(r-1, "hello", 0)]`, outcomes `[Done]`, nothing pending, `rows.rows("q")` empty (unordered work never touches the table).
2. `Retry runs it again after the delay with the attempt counted` — handler returns `Retry(5.seconds)` on attempt 0, `Completed` on 1; after one `deliverNext()` the pending task's `at == now + 5.seconds`; `deliverAll()` → attempts seen `[0, 1]`.
3. `work delivered before notBefore waits without running and without counting` — publish with `notBefore = now + 1.hours`; `deliverNext()` → `RunAgain(at = now + 1.hours)`, handler not called; set `now += 2.hours`; `deliverNext()` → handler sees attempt 0.
4. `publishing the same id while pending queues it once` — publish `r-1` twice → one pending task.
5. `a throwing handler leaves the task for the backend to deliver again` — handler throws `IllegalStateException`; `deliverNext()` rethrows; task pending; attempt still 0 on the next delivery.
6. `a stopped queue receives nothing` — `start`, `stop`, then `deliverNext()` fails with "Nothing subscribed".

Run: `./gradlew :kotmod:test --tests '*ReactionQueueUnorderedTest*'`
Expected: compilation FAILS.

- [ ] **Step 2: Write `ReactionQueue.kt`** (the ordered and kept paths are filled in by Tasks 4–5; write them now as shown, they are exercised by those tasks' tests)

```kotlin
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
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/** Added to an item's timeout to give its lease: how long a started ordered item is protected from a duplicate run. */
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

    /** Gave up and holds back its line until an operator acts (ordered only). */
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
            ItemResult.Blocked, is ItemResult.Replaced -> error("Unordered reaction ${task.reactionId} in $name can't block or be replaced")
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
            ItemResult.Blocked -> error("Kept reaction ${row.reactionId} in $name can't block an aggregate")
        }
    }

    private fun encode(item: T): String = Json.encodeToString(itemSerializer, item)

    private fun decode(item: String): T = Json.decodeFromString(itemSerializer, item)

    private suspend fun <R> io(block: () -> R): R = withContext(Dispatchers.IO) { block() }
}
```

- [ ] **Step 3: Run the unordered tests**

Run: `./gradlew :kotmod:test --tests '*ReactionQueueUnorderedTest*'`
Expected: PASS (6 tests).

- [ ] **Step 4: Commit**

```bash
git add -A kotmod/src
git commit -m "Add ReactionQueue, running unordered work on a TaskQueue"
```

---

### Task 4: `ReactionQueue` — ordered lines

**Files:**
- Test: `kotmod/src/test/kotlin/io/kotmod/event/reaction/ReactionQueueOrderedTest.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/event/reaction/ReactionQueue.kt` only if a test exposes a defect.

**Interfaces:**
- Consumes: Task 3's `ReactionQueue`, `ReactionTasks.frontName`.

Setup as in Task 3, plus `fun item(id: String, seq: Long, ord: Int = 0, key: String = "Order/o-1") = OrderedItem(EventReactionId(id), key, seq, ord, id)` and a handler whose behaviour per item id is set by each test (a `MutableMap<String, ArrayDeque<ItemResult<String>>>`, default `Completed`), recording `Delivery`s in order.

- [ ] **Step 1: Write the tests** (each `runBlocking<Unit>`):

1. `a line runs one item at a time in sequence and ordinal order, scheduling only its front` — `publishOrdered([e6/0, e5/1, e5/0])` → exactly one pending task, named `frontName("Order/o-1", "e5/0")`; `deliverAll()` → handled `[e5/0, e5/1, e6/0]`; rows empty.
2. `work added later with a lower place runs first` — publish `e6`; publish `e5`; `deliverAll()` → `[e5, e6]` (the stale `e6` front task is skipped and reschedules the real front).
3. `a replaced item's replacements take its place, ahead of later work, on a first-in-first-out scheduler` — publish `m5` (seq 5) and `e6` (seq 6); handler: `m5 → Replaced([Produced(t5a,"t5a"), Produced(t5b,"t5b")])`; `deliverAll()` → `[m5, t5a, t5b, e6]`. **This is the parked-mapping gap test.**
4. `lines don't wait for each other` — `a1` on line A returns `Retry(1.minutes)` forever (cap with `deliverNext` calls); `b1` on line B completes on its first delivery.
5. `Retry runs the same item again later, counting the attempt at the start` — `e5` returns Retry, Retry, Completed → attempts seen `[0, 1, 2]`; after each Retry the pending task has `at == now + delay` and the row's lease is cleared.
6. `Blocked holds back the line and schedules nothing` — `e5` returns `Blocked`; `e6` pending in rows; `deliverAll()` → `[e5]`; nothing pending; row `e5.blocked`.
7. `a delivered task for an item that isn't the front schedules the real front without running anything` — publish `e5`, `e6`; `scheduler.queue("q").schedule(frontName(key,"e6"), encode(Front(key,"e6")), now)`; `deliver(frontName(key,"e6"))` → `Done`, handler not called, `e5`'s task still pending.
8. `a duplicate delivery while the item runs waits for the lease` — handler for `e5` suspends on a `CompletableDeferred`; start `deliverNext()` in `async`; when the handler has started, `deliverDuplicate(name)` → `RunAgain(at = now + 90.seconds)` and the handler ran once; release; the first delivery completes.
9. `a crash after finishing an item but before scheduling the next is repaired by the redelivery` — publish `e5`, `e6`; `queue.failNextSchedules = 1` after the initial publish; `deliverNext()` throws (scheduling `e6` failed after `e5`'s row was deleted); `e5`'s task is still pending; `deliverAll()` → `e5`'s redelivery finds its row gone, schedules `e6`; handled `[e5, e6]`.
10. `a crash mid-run counts the attempt; the item runs again once its lease ends` — `e5`'s handler throws `IllegalStateException` (an infrastructure failure, not an `ItemResult`); `deliverNext()` throws; row has `attempts = 1`, leased; `deliverNext()` → `RunAgain(lease end)`, handler not called; `now += 2.minutes`; `deliverNext()` → handler sees attempt 1.
11. `a shutdown mid-run gives the attempt back` — handler `awaitCancellation()`; run `deliverNext()` in a job, cancel it, `join`; row `attempts = 0`, lease null; next delivery sees attempt 0.
12. `the reactor adding work while the front finishes never strands the line (front finishes first)` — publish `e5`; set `queue.beforeSchedule` to suspend on a gate when scheduling `e5`'s next front; interleave: deliver `e5` (Completed: deletes `e5`, finds no front, so schedules nothing), then `publishOrdered([e6])` → `e6` scheduled; `deliverAll()` → `[e5, e6]`.
13. `the reactor adding work while the front finishes never strands the line (reactor first)` — publish `e5`; `publishOrdered([e6])` runs to the point after its locked step but is paused in `beforeSchedule` (gate) before scheduling the front it saw (`e5`); meanwhile `deliverNext()` completes `e5` and schedules `e6`; release the gate (scheduling `e5`'s name again: its task is gone, so a stale `e5` task is added and later skipped); `deliverAll()` → `[e5, e6]`, nothing pending, rows empty.
14. `a line's front task still runs after its policy stops being ordered` — rows hold `e5` with a pending front task; a new `ReactionQueue` on the same queue (as after a redeploy) runs it: handled `[e5]`. (Review focus.)

Run: `./gradlew :kotmod:test --tests '*ReactionQueueOrderedTest*'`
Expected: PASS (14 tests). If any fails, fix `ReactionQueue` — don't weaken the test.

- [ ] **Step 2: Mutation check** — temporarily make `advance` schedule the new front *before* the locked step (move `front()` out of the transaction): test 13 or 9 must fail. Restore.

- [ ] **Step 3: Commit**

```bash
git add -A kotmod/src
git commit -m "Test ReactionQueue's ordered lines: order, replacement, leases, crashes and races"
```

---

### Task 5: `ReactionQueue` — kept items and the repair sweep

**Files:**
- Test: `kotmod/src/test/kotlin/io/kotmod/event/reaction/ReactionQueueKeptAndSweepTest.kt`

- [ ] **Step 1: Write the tests** (setup as Task 4):

1. `a kept item is stored and scheduled, and finishing it deletes it` — `keep(k1, "x")`; one row (KEPT), one task named `k1`; deliver → Completed → rows empty, nothing pending.
2. `a kept item's failures are counted in its row` — Retry, Retry, Completed → attempts seen `[0, 1, 2]`.
3. `a replaced kept item queues its replacements as unordered work, then disappears` — `Replaced([t1, t2 with notBefore = now + 1.hours])` → row gone; tasks `t1`, `t2` pending as unordered payloads; `t2.at == now + 1.hours`.
4. `a kept task whose row is gone finishes without running` — keep `k1`; delete the row directly; deliver → Done, handler not called.
5. `the sweep reschedules a lost front and a lost kept task` — publish `e5`, keep `k1`; `lose` both tasks; `now += 31.minutes`; `sweep(30.minutes)` → both tasks pending again; `deliverAll()` runs both.
6. `the sweep leaves blocked, leased, recent and non-front rows alone` — rows: blocked front of line A; leased front of line B (`leaseUntil = now + 10.minutes`); front of line C changed 1 minute ago; second item of line D (its front `d1` changed long ago) → after `sweep(30.minutes)` only `d1`'s front task is scheduled.

Run: `./gradlew :kotmod:test --tests '*ReactionQueueKeptAndSweepTest*'`
Expected: PASS (6 tests).

- [ ] **Step 2: Commit**

```bash
git add -A kotmod/src
git commit -m "Test ReactionQueue's kept items and repair sweep"
```

---

### Task 6: Event policies on `ReactionQueue`

**Files:**
- Modify: `kotmod/src/main/kotlin/io/kotmod/reaction/PolicyRuntime.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/reaction/EventReactor.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/reaction/ReactionSource.kt` (no behaviour change; imports)
- Modify: `kotmod/src/main/kotlin/io/kotmod/reaction/EventPolicy.kt` (KDoc mentions of queues; delete the deprecated `Reactions`/`ReactionsDsl` aliases)
- Modify: `kotmod/src/main/kotlin/io/kotmod/outbox/DomainEventPoller.kt` (an `afterTick` hook)
- Modify tests: `kotmod/src/test/kotlin/io/kotmod/reaction/{PolicyRuntimeTest,PolicyMappingTest,EventReactorTest,ContractSourceTest,PolicyFixtures,EventPolicyTest}.kt`; delete `DeprecatedReactionsAliasTest.kt`; keep `PolicyItemSerializationTest.kt` (rename `ParkedMapping` → `ParkedItem` inside it).

**Interfaces:**
- Consumes: `ReactionQueue`, `Produced`, `OrderedItem`, `ItemResult`, `Delivery`, `LEASE_MARGIN`, `rethrowIfCancelled`, `ReactionRows`, `PostgresReactionRows`, `TaskScheduler`.
- Produces: `PolicyRuntime(policy, scheduler, rows, readEvent, clock)`; `EventReactor(jdbc, scheduler: TaskScheduler, isLeader, name, pollInterval, batchSize)` public constructor; internal constructor gains `rows: ReactionRows` and `sweepEvery: Duration = 10.minutes`, `sweepIdle: Duration = 30.minutes`; `DomainEventPoller(..., afterTick: suspend () -> Unit = {})`; internal class `ParkedMapping` (the queue item) renamed `ParkedItem` (keep `@SerialName("parked")`), freeing `ParkedMapping` for Task 8's public type; `internal fun lineKey(metadata: EventMetadata): String = "${metadata.aggregateType.value}/${metadata.aggregateId.value}"` in `io.kotmod.event.reaction` (replaces `stampFor`).

- [ ] **Step 1: Port the tests first.** Replace `ManualQueues` with `ManualTaskScheduler` + `InMemoryReactionRows` throughout `PolicyRuntimeTest`, `PolicyMappingTest`, `EventReactorTest`, `ContractSourceTest`, `PolicyFixtures`:
  - `runtime(policy)` becomes `PolicyRuntime(policy, scheduler, rows, readEvent = { … }) { now }.also { it.start() }`.
  - `queues.deliver("confirmations")` becomes `scheduler.queue("confirmations").deliverAll()` (or `deliverNext()` where a test counts single deliveries); outcomes are now `TaskOutcome`s: `ReactionOutcome.Retry(d)` → `TaskOutcome.RunAgain` with `at == now + d`; `Finished` → `Done`; ordered `Wait` behaviour no longer exists (assertions about waiting become assertions that only the front is pending).
  - Tests that called `runtime.publish(id, trigger, ordering, notBefore)` directly publish through `mapAndPublish` with a test source, or through `routeLocal(event)`.
  - Add: `an ordered policy's BlockAggregate give-up blocks its line` (row `blocked`, later work never delivered); `an ordered policy's parked mapping, once fixed, runs its triggers before the aggregate's later work` (strict FIFO; the gap); `an unordered policy's parked mapping is kept in the table and queues its triggers as unordered work once fixed`; `a policy switched from ordered to unordered still runs its existing line` (review focus: rows from an ordered runtime, then an unordered runtime with the same name delivers them).
  - Keep every existing scenario's intent: timeouts, `onFailure` throwing, `onCompletion` throwing, the app's own `CancellationException` (in `handle`, `onFailure`, `onCompletion`, `on(...)`) as a failure, real shutdown not counted, undecodable stored trigger thrown back to the backend, dropping a parked mapping whose source was removed.

Run: `./gradlew :kotmod:test --tests 'io.kotmod.reaction.*'`
Expected: compilation FAILS (constructors and types don't exist yet).

- [ ] **Step 2: Rewrite `PolicyRuntime`**

```kotlin
internal class PolicyRuntime<T : Any>(
    val policy: EventPolicy<T>,
    scheduler: TaskScheduler,
    rows: ReactionRows,
    private val readEvent: (EventId) -> PersistedEvent?,
    private val clock: () -> Instant = { Clock.System.now() },
) {
    private val ordered = policy.ordering is ReactionOrdering.PerAggregate
    private val onGiveUp = (policy.ordering as? ReactionOrdering.PerAggregate)?.onGiveUp
    private val queue = ReactionQueue(policy.name, PolicyItem.serializer(), scheduler.queue(policy.name), rows, policy.timeout + LEASE_MARGIN, clock)
    private val backoff = BackoffStrategy()

    fun start() = queue.start(::deliver)

    fun stop() = queue.stop()

    suspend fun sweep(idleFor: Duration) = queue.sweep(idleFor)

    suspend fun routeLocal(event: PersistedEvent) { /* unchanged */ }

    suspend fun mapAndPublish(metadata: EventMetadata, source: ReactionSource<T>, map: () -> List<ProducedTrigger<T>>) {
        val produced =
            try {
                produce(metadata, map())
            } catch (e: Throwable) {
                rethrowIfCancelled(e)
                park(metadata, source, e)
                return
            }
        queueAll(metadata, produced)
    }

    /** Numbers [triggers] `<policy>/<eventId>/<n>`; refuses delayed triggers for an ordered policy (unchanged message). */
    private fun produce(metadata: EventMetadata, triggers: List<ProducedTrigger<T>>): List<Produced<PolicyItem>>

    private suspend fun queueAll(metadata: EventMetadata, produced: List<Produced<PolicyItem>>) {
        if (ordered) {
            queue.publishOrdered(produced.mapIndexed { n, p -> OrderedItem(p.id, lineKey(metadata), metadata.sequence, n, p.item) })
        } else {
            produced.forEach { queue.publish(it) }
        }
    }

    private suspend fun park(metadata: EventMetadata, source: ReactionSource<T>, error: Throwable) {
        // log as today
        val id = EventReactionId("${policy.name}/${metadata.eventId.value}/mapping")
        val item = ParkedItem(metadata.eventId.value, metadata.aggregateType.value, metadata.aggregateId.value, source.description)
        if (ordered) queue.publishOrdered(listOf(OrderedItem(id, lineKey(metadata), metadata.sequence, 0, item))) else queue.keep(id, item)
    }

    private suspend fun deliver(delivery: Delivery<PolicyItem>): ItemResult<PolicyItem> =
        when (val item = delivery.item) {
            // A trigger that can't be decoded throws here, back to the backend, which delivers it again until a fix is deployed.
            is TriggerItem -> runTrigger(delivery.id, Json.decodeFromString(policy.triggers, item.trigger), delivery.attempt)
            is ParkedItem -> runParked(delivery.id, item, delivery.attempt)
        }
}
```

`runTrigger` keeps today's timeout/`onFailure`/`onCompletion` logic and logging, mapping outcomes:

| Today | Now |
|---|---|
| `finish(Completed)` → `Finished(false)` | `onCompletion(Completed)` then `ItemResult.Completed` |
| `Retry(d)` | `ItemResult.Retry(d)` |
| `GiveUp` → `finish(GaveUp)` | `onCompletion(GaveUp(error))`, then `ItemResult.Blocked` if `onGiveUp == BlockAggregate`, else `ItemResult.Completed` |
| `onFailure`/`onCompletion` threw | `ItemResult.Retry(backoff.calculateBackoff(attempt))` |

`runParked` re-reads the event (`readEvent`), drops it with today's warning (`ItemResult.Completed`) if the policy no longer listens to its type, otherwise returns `ItemResult.Replaced(produce(event.metadata, source.map(event)))`; any failure (rethrowing real cancellation) logs as today and returns `ItemResult.Retry(backoff.calculateBackoff(attempt))`.

Delete `publish(id, trigger, ordering, notBefore)`, `stampFor` uses, the `channel`/`supportsOrdering` check (every scheduler supports ordering now).

- [ ] **Step 3: Update `EventReactor`**

Public constructor: `EventReactor(jdbc: JdbcContext, scheduler: TaskScheduler, isLeader: () -> Boolean, name: String = "reactor", pollInterval: Duration = 500.milliseconds, batchSize: Int = 100)`, passing `rows = PostgresReactionRows(jdbc)`. `register` builds `PolicyRuntime(policy, scheduler, rows, readEvent, clock)`. Add to `DomainEventPoller` a constructor parameter `afterTick: suspend () -> Unit = {}` called at the end of `tick()` (only reached on the leader). The reactor passes `afterTick = ::sweepIfDue`:

```kotlin
private var lastSweep: Instant? = null

private suspend fun sweepIfDue() {
    val now = clock()
    val last = lastSweep
    if (last != null && now - last < sweepEvery) return
    lastSweep = now
    runtimes.forEach { runtime ->
        try {
            runtime.sweep(sweepIdle)
        } catch (e: Exception) {
            log.warn("Repair sweep of event policy {} failed; trying again next time", runtime.policy.name, e)
        }
    }
}
```

The first tick sweeps (recovering anything lost while no node was leader). Update the class KDoc: "queues each policy's triggers on that policy's queue from [scheduler]"; ordered policies keep their lines in `ddd_reaction_row`; the repair sweep.

Add `EventReactorTest` cases: `the first tick sweeps, then at most every sweepEvery`; `a sweep failure doesn't stop reading`.

- [ ] **Step 4: Run**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.reaction.*'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A kotmod/src
git commit -m "Run event policies on ReactionQueue and a TaskScheduler"
```

---

### Task 7: Contracts and process managers on `ReactionQueue`

Contracts lose their executor-based `subscribe` (process managers were its only user) in the same task, because neither change compiles without the other.

**Files:**
- Modify: `kotmod/src/main/kotlin/io/kotmod/contract/PublicEventContract.kt` (delete `subscribe(executor, ordering, block)`, `Subscription`, `fanOut` and the shared ordinal counter; keep `listen` and `ensureCanListen`; `handleEvent` calls each listener in registration order; KDoc: event policies listen with `on(contract)`, process managers with `subscribeTo`, both through `listen`)
- Modify tests: `kotmod/src/test/kotlin/io/kotmod/contract/PublicEventContractTest.kt`, `kotmod/src/integrationTest/kotlin/io/kotmod/contract/PublicEventContractIntegrationTest.kt` (every test that subscribed an executor via `RecordingExecutor` registers a recording `listen { … }` instead; keep envelope order, skipped internal/other-type/private events, a listener failure re-reading the whole event, `start` refusing later listeners; drop tests that only covered ordering stamps and executor claims, now covered by Task 4)
- Delete: `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/support/RecordingExecutor.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/process/ProcessManager.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/process/ProcessRuntime.kt`
- Modify tests: `kotmod/src/test/kotlin/io/kotmod/process/{ProcessManagerTest,ProcessRuntimeTest}.kt`; delete `ManualQueues.kt`, `ManualQueuesTest.kt`; update `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestProcess.kt` if it references queues.

**Interfaces:**
- Produces: `ProcessManager(…, scheduler: TaskScheduler, inputOrdering, …)` replacing `queues: ReactionQueues` in both constructors (internal constructor also takes `rows: ReactionRows`; public one builds `PostgresReactionRows(jdbc)`); `InputTrigger`/`CommandTrigger` no longer implement `EventReactionTrigger` and lose `timeout`; `JsonTriggerSerializer` and `processExecutor` deleted; `internal fun <T : Any> processQueue(name, serializer, scheduler, rows, clock): ReactionQueue<T>` and `internal fun <T : Any> ReactionQueue<T>.startProcess(run: suspend (T) -> Unit)` in `ProcessRuntime.kt`; `ProcessManager.sweep(idleFor)` called from its poller's `afterTick` like the reactor (every 10 minutes, idle 30 minutes).

- [ ] **Step 1: Port the tests** from `ManualQueues` to `ManualTaskScheduler` + `InMemoryReactionRows` (same patterns as Task 6). Add: `ordered inputs run in their source aggregate's order on a first-in-first-out scheduler`; `ordered inputs from the process manager's own reader and from a contract subscription use separate lines` (review focus); `a failing input is retried forever with capped backoff, attempts counted` (attempt passed in `Delivery` grows; the delay is `BackoffStrategy().calculateBackoff(attempt)`); `an input that runs past 60 seconds is retried`.

- [ ] **Step 2: Rewrite the channels.**

```kotlin
/** How long one process manager input or command may run. */
internal val PROCESS_TIMEOUT: Duration = 60.seconds

internal fun <T : Any> processQueue(
    name: String,
    serializer: KSerializer<T>,
    scheduler: TaskScheduler,
    rows: ReactionRows,
    clock: () -> Instant,
): ReactionQueue<T> = ReactionQueue(name, serializer, scheduler.queue(name), rows, PROCESS_TIMEOUT + LEASE_MARGIN, clock)

/** Runs [run] for each item; any failure or timeout is retried with capped backoff and never given up. */
internal fun <T : Any> ReactionQueue<T>.startProcess(run: suspend (T) -> Unit) {
    val backoff = BackoffStrategy()
    start { delivery ->
        try {
            withTimeout(PROCESS_TIMEOUT) { run(delivery.item) }
            ItemResult.Completed
        } catch (e: TimeoutCancellationException) {
            log.error("Process manager reaction {} timed out and will be retried [attempt={}]", delivery.id.value, delivery.attempt)
            ItemResult.Retry(backoff.calculateBackoff(delivery.attempt))
        } catch (e: Throwable) {
            rethrowIfCancelled(e)
            log.error("Process manager reaction {} failed and will be retried [attempt={}]", delivery.id.value, delivery.attempt, e)
            ItemResult.Retry(backoff.calculateBackoff(delivery.attempt))
        }
    }
}
```

In `ProcessManager`: `inputs = processQueue("${type.value}-inputs", InputTrigger.serializer(), …)`, `internal = processQueue("${type.value}-internal", …)`, `commands = processQueue("${type.value}-commands", CommandTrigger.serializer(), …)`, and per `subscribeTo` a `processQueue("${type.value}-contract-$name", …)` fed by `contract.listen { envelope -> … }` which publishes `in-<eventId>` ordered (`OrderedItem(id, lineKey(envelope.metadata), envelope.metadata.sequence, 0, trigger)`) when `inputOrdering` is `PerAggregate`, else `publish(Produced(id, trigger))`. `route` publishes: commands `Produced(cmd-…)`, scheduled inputs `Produced(sched-…, notBefore = at)`, translated inputs ordered or unordered as above; `runCommand` publishes rejection feedback as `Produced(rejected-…)` on `internal`. Delete `claimOrderedSource` and the `supportsOrdering` requirement. `start()` calls `startProcess` on every queue, then the poller; `stop()` stops the poller, then the queues.

- [ ] **Step 3: Run**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.*' --tests 'io.kotmod.contract.*' && ./gradlew :kotmod:integrationTest --tests 'io.kotmod.contract.*'`
Expected: PASS. (Tests of the old executor still compile against the old interface until Task 8 deletes them.)

- [ ] **Step 4: Commit**

```bash
git add -A kotmod/src
git commit -m "Run process managers on ReactionQueue and feed them from contracts through listen"
```

---

### Task 8: Delete the old queue interface, add `ReactionOperations`, and test on Postgres

**Files:**
- Delete: `kotmod/src/main/kotlin/io/kotmod/event/reaction/ReactionQueues.kt`; from `EventReaction.kt` delete everything except `EventReactionId` and `BackoffStrategy` (the executor, `EventReaction`, sink/source/serializer, `EventReactionTrigger`, `RetryCount`, `EventReactionExecutionId`, results, `RetrySignal`, the old `Cancellable`); from `Ordering.kt` delete `DispatchOrdering`, `ReactionOutcome`, `stampFor`.
- Delete tests: `kotmod/src/test/kotlin/io/kotmod/event/reaction/{EventReactionExecutorTest,DelayedReactionsTest,OrderedSourceGuardTest}.kt`.
- Create: `kotmod/src/main/kotlin/io/kotmod/reaction/ReactionOperations.kt`
- Test: `kotmod/src/integrationTest/kotlin/io/kotmod/reaction/ReactionOperationsIntegrationTest.kt`
- Test: `kotmod/src/integrationTest/kotlin/io/kotmod/reaction/OrderedLinesIntegrationTest.kt`
- Modify: `kotmod/src/integrationTest/kotlin/io/kotmod/reaction/EventReactorIntegrationTest.kt` (new constructor)

**Interfaces:**
- Produces (public, `io.kotmod.reaction`):

```kotlin
/** An ordered reaction holding back its aggregate's line ([key]) after giving up, with the attempts it took. */
data class BlockedReaction(val key: String, val reactionId: EventReactionId, val sequence: Long, val attempts: Int)

/** An event a policy couldn't map, parked as [reactionId], after [attempts] attempts. */
data class ParkedMapping(val eventId: EventId, val reactionId: EventReactionId, val attempts: Int)

/**
 * Operator tools for event policies' and process managers' work kept in `ddd_reaction_row`, the same on every
 * scheduler. [queue] is a policy name or a process manager channel name (`<type>-inputs`, …). Usable from an admin
 * endpoint or a script, without a running reactor.
 */
class ReactionOperations internal constructor(private val rows: ReactionRows, private val scheduler: TaskScheduler, private val clock: () -> Instant) {
    constructor(jdbc: JdbcContext, scheduler: TaskScheduler) : this(PostgresReactionRows(jdbc), scheduler, { Clock.System.now() })

    fun blockedReactions(queue: String): List<BlockedReaction>
    suspend fun retryBlocked(queue: String, id: EventReactionId)   // unblock, attempts = 0, schedule it (it is its line's front)
    suspend fun skipBlocked(queue: String, id: EventReactionId)    // delete it, schedule the line's new front
    fun parkedMappings(policy: String): List<ParkedMapping>        // rows whose reaction id ends "/mapping"
    suspend fun skipParked(policy: String, eventId: EventId)       // delete `<policy>/<eventId>/mapping`; if ordered, schedule the line's new front
}
```

Each mutating call fails with `IllegalArgumentException` if the row doesn't exist or isn't blocked/parked; ordered rows are changed under the line lock (`rows.inLine`), kept rows under `inQueue`; scheduling uses `ReactionTasks.scheduleFront` on `scheduler.queue(queue)`.

- [ ] **Step 1: Delete the old interface and its tests**; fix the remaining compile errors in `kotmod` (imports of `Cancellable` move to `io.kotmod.scheduling.Cancellable`).

- [ ] **Step 2: Write `ReactionOperationsIntegrationTest`** (Postgres rows, `ManualTaskScheduler`): blocked list contents; `retryBlocked` unblocks, resets attempts, schedules the front, and the item runs; `skipBlocked` deletes and the line's next item runs; `parkedMappings` lists ordered and kept parked rows with attempts; `skipParked` on an ordered policy lets the line continue, on an unordered policy deletes the kept row (its pending task then finishes without running); each refuses an unknown id; a queue whose policy is no longer registered can still be listed and skipped (review focus).

- [ ] **Step 3: Implement `ReactionOperations`**; run the test to green.

- [ ] **Step 4: Write `OrderedLinesIntegrationTest`** (Postgres `PostgresReactionRows`, `ManualTaskScheduler`, real threads):
  1. `concurrent publishing and finishing never strands a line` — 200 iterations: line with `e(n)` pending; concurrently (two `Dispatchers.IO` coroutines) `publishOrdered([e(n+1)])` and deliver `e(n)`'s front; afterwards `deliverAll()` must process `e(n+1)`; at the end every row is gone and handled order is `e0…e200`.
  2. `two nodes delivering the same front at once run it once` — two `ReactionQueue`s on the same queue/rows; the handler of the first suspends; the second's delivery of the same task returns `RunAgain`.
  3. `the sweep recovers a line whose task was lost` — lose the front task; advance the clock 31 minutes; `sweep(30.minutes)`; it runs.
  4. `an event policy end to end on Postgres rows` — `EventReactor` (internal constructor with `ManualTaskScheduler` and Postgres) with an ordered policy: write events with `AggregateManager`, `tickForTest()`, deliver all; handled in sequence order; a mapping that fails once is parked, then (after fixing) its triggers run before the aggregate's later work.

- [ ] **Step 5: Run the whole core module**

Run: `./gradlew :kotmod:test :kotmod:integrationTest --rerun-tasks`
Expected: PASS. (`kotmod-db-scheduler` and `examples` still don't compile.)

- [ ] **Step 6: Commit**

```bash
git add -A kotmod/src
git commit -m "Remove the old queue interface and add ReactionOperations"
```

---

### Task 9: `DbSchedulerTaskScheduler`

**Files:**
- Create: `kotmod-db-scheduler/src/main/kotlin/io/kotmod/scheduling/dbscheduler/DbSchedulerTaskScheduler.kt`
- Keep (moved to the new package): `CappedExponentialBackoffFailureHandler.kt`
- Delete: everything else in `kotmod-db-scheduler/src/main/kotlin/io/kotmod/event/reaction/dbscheduler/` and its unit tests (`DbSchedulerQueuesTest`, `DbSchedulerTriggerSinkTest`, `DbSchedulerTriggerSourceTest`, `TaskRowOutcomeTest`).
- Test: `kotmod-db-scheduler/src/integrationTest/kotlin/io/kotmod/scheduling/dbscheduler/DbSchedulerTaskSchedulerContractTest.kt`
- Port: `kotmod-db-scheduler/src/integrationTest/kotlin/io/kotmod/event/reaction/dbscheduler/{PolicyTestSupport,EventPolicyReactionsIntegrationTest,EventPolicyFailuresIntegrationTest,OrderedReactionsIntegrationTest,ProcessManagerIntegrationTest,DbSchedulerTestSupport}.kt` → package `io.kotmod.scheduling.dbscheduler`; delete `DbSchedulerQueuesIntegrationTest.kt` (replaced by the contract test and `ReactionOperations` tests).

**Interfaces:**
- Produces: `class DbSchedulerTaskScheduler(unsubscribedRetryDelay: Duration = 5.seconds) : TaskScheduler` with `val tasks: List<Task<String>>` (one per queue asked for so far), `fun bind(client: SchedulerClient)`, `override fun queue(name: String): TaskQueue`.

- [ ] **Step 1: Write the contract test** (fails: no class yet)

```kotlin
class DbSchedulerTaskSchedulerContractTest : TaskSchedulerContract() {
    private val shared = object : IntegrationTest() {}
    private val backend = DbSchedulerTaskScheduler().also { it.queue(QUEUE_A); it.queue(QUEUE_B) }
    private var scheduler: Scheduler? = null

    override fun scheduler(): TaskScheduler = backend

    override fun start() {
        val built = Scheduler.create(shared.dataSourceForTests(), *backend.tasks.toTypedArray())
            .pollingInterval(Duration.ofMillis(200)).enableImmediateExecution().threads(2).build()
        backend.bind(built)
        built.start()
        scheduler = built
    }

    override fun stop() { scheduler?.stop(); scheduler = null }
}
```

`IntegrationTest` exposes `dataSource` as `protected`; add a small public accessor in the db-scheduler module's test support (as the existing `DbSchedulerTestSupport` does) rather than changing the fixture's visibility. Truncation runs because `IntegrationTest`'s `@BeforeEach` isn't inherited here: call `shared.truncateDddTables()` in an `@BeforeTest`. Note `bind` must happen before `schedule` in real use; in the contract tests `schedule` is called before `start()`, so `bind` the client in the constructor path instead: build an unstarted `Scheduler` lazily in `scheduler()` the first time and `bind` it, then `start()` starts it.

Run: `./gradlew :kotmod-db-scheduler:integrationTest --tests '*ContractTest*'`
Expected: compilation FAILS.

- [ ] **Step 2: Write the adapter**

```kotlin
package io.kotmod.scheduling.dbscheduler

/**
 * [TaskScheduler] on db-scheduler: each queue is a db-scheduler task named after it, and each task an instance of
 * it in your `scheduled_tasks` table. Ask for every queue (register your event policies and build your process
 * managers) before reading [tasks]; register [tasks] when building the `Scheduler`; call [bind] before starting the
 * reactor. A handler exception is retried with capped exponential backoff (10 seconds doubling to an hour).
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
    fun bind(client: SchedulerClient) { this.client = client }

    override fun queue(name: String): TaskQueue = synchronized(queues) { queues.getOrPut(name) { Queue(name) } }

    private inner class Queue(private val taskName: String) : TaskQueue {
        @Volatile
        var handler: (suspend (String, String) -> TaskOutcome)? = null

        val task: CustomTask<String> =
            Tasks.custom(taskName, String::class.java)
                .onFailure(CappedExponentialBackoffFailureHandler(initialDelay = 10.seconds, maximumDelay = 1.hours))
                .execute { instance, _ ->
                    val current = handler
                    val outcome =
                        if (current == null) {
                            log.warn("Nothing is subscribed to task {}; running {} again in {}", taskName, instance.id, unsubscribedRetryDelay)
                            TaskOutcome.RunAgain(Clock.System.now() + unsubscribedRetryDelay, instance.data)
                        } else {
                            runBlocking { current(instance.id, instance.data) }
                        }
                    when (outcome) {
                        TaskOutcome.Done -> CompletionHandler.OnCompleteRemove()
                        is TaskOutcome.RunAgain ->
                            CompletionHandler { complete, ops -> ops.reschedule(complete, outcome.at.toJavaInstant(), outcome.payload) }
                    }
                }

        override suspend fun schedule(name: String, payload: String, at: Instant) {
            val bound = checkNotNull(client) { "Call bind(scheduler) on DbSchedulerTaskScheduler before starting" }
            withContext(Dispatchers.IO) { bound.scheduleIfNotExists(TaskInstance(taskName, name, payload), at.toJavaInstant()) }
        }

        override fun subscribe(handler: suspend (String, String) -> TaskOutcome): Cancellable {
            this.handler = handler
            return Cancellable { if (this.handler === handler) this.handler = null }
        }
    }
}
```

Check the exact db-scheduler API used today in the deleted `ReactionTask.kt`/`DbSchedulerTriggerSink.kt` (`CompletionHandler`, `reschedule` with new data, `scheduleIfNotExists`) and match it; the logic above is the requirement. A cancelled subscription leaves the task for a later subscriber (it runs again after `unsubscribedRetryDelay`).

- [ ] **Step 3: Run the contract test**

Run: `./gradlew :kotmod-db-scheduler:integrationTest --tests '*ContractTest*'`
Expected: PASS (7 tests).

- [ ] **Step 4: Port the module's integration tests** to `DbSchedulerTaskScheduler` + `EventReactor(jdbc, scheduler, …)` + `ReactionOperations`. Keep every scenario of `EventPolicyReactionsIntegrationTest`, `EventPolicyFailuresIntegrationTest`, `OrderedReactionsIntegrationTest` and `ProcessManagerIntegrationTest`; replace waits-for-earlier assertions with "only the front instance exists in `scheduled_tasks`"; replace `DbSchedulerQueues.blockedReactions/retryBlocked/skipBlocked/parkedMappings/skipParked` with `ReactionOperations`. Add `ordered work leaves at most one scheduled_tasks row per line` and `a parked mapping, once fixed, runs before the aggregate's later work`.

- [ ] **Step 5: Run the module**

Run: `./gradlew :kotmod-db-scheduler:test :kotmod-db-scheduler:integrationTest --rerun-tasks`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A kotmod-db-scheduler
git commit -m "Replace DbSchedulerQueues with DbSchedulerTaskScheduler"
```

---

### Task 10: Examples, README and 0.4.0

**Files:**
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/{Quickstart,ReadmeExamples,QuickstartTest}.kt`
- Modify: `README.md`
- Modify: `gradle.properties` (`version=0.4.0`)

- [ ] **Step 1: Update the examples** to `DbSchedulerTaskScheduler`, `EventReactor(jdbc, scheduler, …)`, `ReactionOperations`, `ProcessManager(…, scheduler = …)`; remove the deprecated alias test usage if any. Run `./gradlew :examples:integrationTest --rerun-tasks` → PASS.

- [ ] **Step 2: Update the README** (every compiled block byte-identical to the examples; verify with a script that each top-level ```kotlin block previously found in the examples is still found):
  - Quickstart step 4 and "Running event policies on db-scheduler": `DbSchedulerTaskScheduler`, `queues.tasks` → `scheduler.tasks`, `bind`.
  - Postgres setup: the `ddd_reaction_row` table in the DDL listing.
  - "Ordered event policies": each aggregate's line in `ddd_reaction_row`, only the front scheduled, attempts counted at start (a crash counts), leases, `BlockAggregate`, operator tools via `ReactionOperations`, the repair sweep.
  - "When a mapping fails": parked mappings for ordered policies sit in their line; for unordered policies they are kept in `ddd_reaction_row`; `ReactionOperations.parkedMappings`/`skipParked` for both. Remove the Pub/Sub ordering caveat.
  - Replace "Using another queue (e.g. Google Pub/Sub)" with "Using another scheduler": the `TaskScheduler` interface and contract (idempotent `schedule` per pending name, at least once, not before `at`, `RunAgain` with new payload, exception → redeliver), the `TaskSchedulerContract` test suite from `kotmod`'s test fixtures, why Pub/Sub doesn't qualify (no delayed delivery or run-again-at), Cloud Tasks planned. Remove the Pub/Sub sketch.
  - Running in production / shutdown order: unchanged except names.
  - Known limitations: remove the parked-mapping ordering gap; add "a crash during unordered work doesn't count as an attempt (ordered work counts attempts at the start)"; add "rows of an event policy removed from the app stay in `ddd_reaction_row` until skipped with `ReactionOperations`".
  - Contents: rename the "Using another queue" entry; add "Upgrading from 0.3" (short: `DbSchedulerQueues` → `DbSchedulerTaskScheduler`; `EventReactor`/`ProcessManager` take the scheduler; operator tools moved to `ReactionOperations`; custom queues implement `TaskScheduler`; create `ddd_reaction_row`; `Reactions` alias removed); install snippets to 0.4.0.

- [ ] **Step 3: Full build**

Run: `./gradlew test integrationTest --rerun-tasks`
Expected: BUILD SUCCESSFUL, no new warnings.

- [ ] **Step 4: Commit**

```bash
git add -A README.md examples gradle.properties
git commit -m "Document kotmod-owned ordering and TaskScheduler, and set version 0.4.0"
```

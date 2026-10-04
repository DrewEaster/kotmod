# Postgres Leader Election Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `PostgresLeaderElection`, a session advisory-lock leader election that plugs into `isLeader` of
`AggregateEventOutbox` and `PublicEventContract`.

**Architecture:**
- One class in the core module (`io.kotmod.postgres`) holds a dedicated JDBC connection opened through a
  user-supplied factory.
- A background coroutine loop takes the lock (`pg_try_advisory_lock`) or confirms it is still held (`SELECT 1`)
  every `checkInterval`.
- `isLeader()` reads volatile state and applies a `2 × checkInterval` lease on a monotonic clock.

**Tech Stack:** Kotlin 2.4.20 on JVM 25; kotlinx-coroutines 1.11.0; SLF4J; JDBC (pgjdbc 42.7.13 in tests);
JUnit 5 with kotlin.test; MockK (unit tests); Testcontainers Postgres 17 (integration tests).

**Spec:** `docs/superpowers/specs/2026-10-04-postgres-leader-election-design.md`

## Global Constraints

- Public API exactly: `class PostgresLeaderElection(connect: () -> Connection, val name: String, checkInterval: Duration = 5.seconds) : AutoCloseable` with `fun isLeader(): Boolean`, `fun start()`, `suspend fun stop()`, `override fun close()`.
- Lock key: `hashtextextended(?, 0)` bound to `"kotmod-leader:$name"`.
- Lease: `isLeader()` true only while leading and `nanoTime() - lastConfirmedAt <= 2 × checkInterval`.
- Query timeout per statement: `checkInterval` rounded up to whole seconds, at least 1.
- Drop connections with `Connection.abort(executor)`, never `close()` (except the clean close in `stop()`).
- No exception escapes the loop; logging levels as in the spec (INFO become/step down/recovered, WARN lost/first failure, DEBUG repeats).
- `start()` twice → `IllegalStateException`.
- No new dependencies in `kotmod/build.gradle.kts`.
- README snippets must appear verbatim (indentation-insensitive) in `examples/src/integrationTest/kotlin/io/kotmod/readme/*.kt`; check with `python3 /private/tmp/claude-501/-Users-dreweaster-projects-kotmod/c8057b84-7d0b-478a-b9c3-f240806f99f5/scratchpad/readme_check2.py .`
- Gradle runs that include integration tests need Docker (run outside the sandbox).
- Commit messages: imperative, plain English, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A check that blocks (stalled network) instead of failing — expect `isLeader()` to turn false after `2 × checkInterval` regardless (unit test `lease expires when checks stop succeeding` in Task 1).
2. `stop()` racing a tick that is just acquiring the lock — expect `isLeader()` false after `stop()` returns and no advisory lock left behind (integration test `stop releases the lock` in Task 1, plus `leading` cleared after `cancelAndJoin`).
3. A connection factory that throws (database down at boot) — expect no exception from `start()`, follower until the database is reachable (integration test `connect failures keep it a follower until a connection succeeds` in Task 1).
4. A connection handed out with autocommit off — expect the lock still to be taken and held (the class forces `autoCommit = true`; unit test `forces autocommit on the lock connection` in Task 1).
5. Restart after stop on the same instance — expect `start()` to work again after `stop()` (unit test `can start again after stop` in Task 1).

---

## File Structure

- Create `kotmod/src/main/kotlin/io/kotmod/postgres/PostgresLeaderElection.kt`: the class.
- Create `kotmod/src/test/kotlin/io/kotmod/postgres/PostgresLeaderElectionTest.kt`: unit tests with mocked JDBC
  (lease, lifecycle, autocommit). The `test` source set can see `internal` members; `integrationTest` cannot,
  so anything needing the internal clock lives here.
- Create `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PostgresLeaderElectionIntegrationTest.kt`:
  real Postgres behaviour.
- Modify `README.md` ("Running in production", quickstart pointer, TOC unaffected because the section is a
  bold paragraph-level subsection inside "Running in production").
- Modify `examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt`: compiled snippet.

---

### Task 1: PostgresLeaderElection

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/postgres/PostgresLeaderElection.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/postgres/PostgresLeaderElectionTest.kt`
- Test: `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PostgresLeaderElectionIntegrationTest.kt`

**Interfaces:**
- Consumes: `io.kotmod.postgres.support.IntegrationTest` (gives `dataSource: DataSource`, a direct
  `PGSimpleDataSource`), `io.kotmod.postgres.support.eventually(timeout: Duration = 10.seconds, condition: () -> Boolean)` (suspend).
- Produces: `io.kotmod.postgres.PostgresLeaderElection` with the public API in Global Constraints, plus
  `internal constructor(connect: () -> Connection, name: String, checkInterval: Duration, nanoTime: () -> Long)`.

- [ ] **Step 1: Write the failing integration tests**

Create `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PostgresLeaderElectionIntegrationTest.kt`:

```kotlin
package io.kotmod.postgres

import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class PostgresLeaderElectionIntegrationTest : IntegrationTest() {
    private val elections = mutableListOf<PostgresLeaderElection>()

    private fun election(
        name: String = "orders",
        connect: () -> Connection = dataSource::getConnection,
    ): PostgresLeaderElection =
        PostgresLeaderElection(connect, name, checkInterval = 200.milliseconds).also { elections += it }

    @AfterEach
    fun stopAll() =
        runBlocking {
            elections.forEach { it.stop() }
        }

    private fun advisoryLocksHeld(): Int =
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted").use { rs ->
                    rs.next()
                    rs.getInt(1)
                }
            }
        }

    @Test
    fun `is not leader before start and becomes leader after start`() =
        runBlocking {
            val a = election()
            assertFalse(a.isLeader())

            a.start()

            eventually(2.seconds) { a.isLeader() }
        }

    @Test
    fun `only one of two elections with the same name leads`() =
        runBlocking {
            val a = election().also { it.start() }
            val b = election().also { it.start() }

            eventually(2.seconds) { a.isLeader() || b.isLeader() }
            repeat(20) {
                assertEquals(1, listOf(a, b).count { it.isLeader() })
                delay(50)
            }
        }

    @Test
    fun `elections with different names lead independently`() =
        runBlocking {
            val orders = election("orders").also { it.start() }
            val invoices = election("invoices").also { it.start() }

            eventually(2.seconds) { orders.isLeader() && invoices.isLeader() }
        }

    @Test
    fun `stop releases the lock`() =
        runBlocking {
            val a = election().also { it.start() }
            eventually(2.seconds) { a.isLeader() }
            assertEquals(1, advisoryLocksHeld())

            a.stop()

            assertFalse(a.isLeader())
            assertEquals(0, advisoryLocksHeld())
        }

    @Test
    fun `stopping the leader hands over to the other election`() =
        runBlocking {
            val a = election().also { it.start() }
            eventually(2.seconds) { a.isLeader() }
            val b = election().also { it.start() }
            delay(500)
            assertFalse(b.isLeader())

            a.stop()

            eventually(1.seconds) { b.isLeader() }
        }

    @Test
    fun `a leader whose session the server ends steps down and the other takes over`() =
        runBlocking {
            val a = election().also { it.start() }
            eventually(2.seconds) { a.isLeader() }
            val b = election().also { it.start() }

            dataSource.connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(
                        "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted",
                    )
                }
            }

            eventually(2.seconds) { b.isLeader() && !a.isLeader() }

            // a has rejoined as a follower: it takes over once b stops.
            b.stop()
            eventually(2.seconds) { a.isLeader() }
        }

    @Test
    fun `connect failures keep it a follower until a connection succeeds`() =
        runBlocking {
            val attempts = AtomicInteger()
            val a =
                election(connect = {
                    if (attempts.incrementAndGet() <= 3) error("database unavailable")
                    dataSource.connection
                })

            a.start()

            eventually(3.seconds) { a.isLeader() }
            assertTrue(attempts.get() >= 4)
        }
}
```

- [ ] **Step 2: Write the failing unit tests**

Create `kotmod/src/test/kotlin/io/kotmod/postgres/PostgresLeaderElectionTest.kt`:

```kotlin
package io.kotmod.postgres

import io.kotmod.postgres.support.eventually
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

class PostgresLeaderElectionTest {
    // A connection on which every query returns a single `true` row: the lock is always granted.
    private fun grantingConnection(): Connection {
        val rs = mockk<ResultSet>(relaxed = true)
        every { rs.next() } returns true
        every { rs.getBoolean(1) } returns true
        val stmt = mockk<PreparedStatement>(relaxed = true)
        every { stmt.executeQuery() } returns rs
        val conn = mockk<Connection>(relaxed = true)
        every { conn.prepareStatement(any()) } returns stmt
        return conn
    }

    @Test
    fun `lease expires when checks stop succeeding`() =
        runBlocking {
            var now = 0L
            val election = PostgresLeaderElection(::grantingConnection, "orders", 1.hours) { now }
            election.start()
            eventually(2.seconds) { election.isLeader() }

            // With a 1-hour interval no further check runs; time alone ends the lease.
            now += 2.hours.inWholeNanoseconds
            assertTrue(election.isLeader())
            now += 1
            assertFalse(election.isLeader())

            election.stop()
        }

    @Test
    fun `start twice throws`() =
        runBlocking {
            val election = PostgresLeaderElection(::grantingConnection, "orders", 1.hours) { 0L }
            election.start()

            assertFailsWith<IllegalStateException> { election.start() }

            election.stop()
        }

    @Test
    fun `can start again after stop`() =
        runBlocking {
            val election = PostgresLeaderElection(::grantingConnection, "orders", 1.hours) { 0L }
            election.start()
            eventually(2.seconds) { election.isLeader() }
            election.stop()
            assertFalse(election.isLeader())

            election.start()

            eventually(2.seconds) { election.isLeader() }
            election.stop()
        }

    @Test
    fun `forces autocommit on the lock connection`() =
        runBlocking {
            val conn = grantingConnection()
            val election = PostgresLeaderElection({ conn }, "orders", 1.hours) { 0L }
            election.start()
            eventually(2.seconds) { election.isLeader() }

            verify { conn.autoCommit = true }
            election.stop()
        }

    @Test
    fun `stop unlocks and closes the connection`() =
        runBlocking {
            val conn = grantingConnection()
            val election = PostgresLeaderElection({ conn }, "orders", 1.hours) { 0L }
            election.start()
            eventually(2.seconds) { election.isLeader() }

            election.stop()

            verify { conn.prepareStatement("SELECT pg_advisory_unlock(hashtextextended(?, 0))") }
            verify { conn.close() }
            assertFalse(election.isLeader())
        }
}
```

Note: `eventually` lives in the `testFixtures` source set (`io.kotmod.postgres.support`); the `java-test-fixtures`
plugin applied in `kotmod/build.gradle.kts` makes it visible to `test` with no build change.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.postgres.PostgresLeaderElectionTest'`
Expected: compilation FAILS with `Unresolved reference 'PostgresLeaderElection'`.

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.postgres.PostgresLeaderElectionIntegrationTest'`
Expected: compilation FAILS with `Unresolved reference 'PostgresLeaderElection'`.

- [ ] **Step 4: Implement PostgresLeaderElection**

Create `kotmod/src/main/kotlin/io/kotmod/postgres/PostgresLeaderElection.kt`:

```kotlin
package io.kotmod.postgres

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.Executor
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Leader election on a Postgres session advisory lock, for the `isLeader` parameter of
 * [io.kotmod.outbox.AggregateEventOutbox] and [io.kotmod.contract.PublicEventContract].
 *
 * Elections with the same [name] compete for one lock; the winner is leader for as long as its connection lives.
 * Every `checkInterval` a follower tries to take the lock and the leader confirms its connection still answers.
 * [isLeader] also turns false when the last successful check is older than twice `checkInterval`, so a leader
 * whose checks hang stops claiming leadership. When Postgres ends the leader's session before the leader notices,
 * two nodes may both see themselves as leader for up to about one `checkInterval`.
 *
 * [connect] should open a direct connection outside any pool (one is held for the election's lifetime, and a new
 * one is opened after a failure); it doesn't work through PgBouncer in transaction mode.
 */
class PostgresLeaderElection internal constructor(
    private val connect: () -> Connection,
    val name: String,
    private val checkInterval: Duration,
    private val nanoTime: () -> Long,
) : AutoCloseable {
    constructor(
        connect: () -> Connection,
        name: String,
        checkInterval: Duration = 5.seconds,
    ) : this(connect, name, checkInterval, System::nanoTime)

    private val log = LoggerFactory.getLogger(PostgresLeaderElection::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val lockName = "kotmod-leader:$name"
    private val leaseNanos = (checkInterval * 2).inWholeNanoseconds
    private val queryTimeoutSeconds = maxOf(1, ((checkInterval.inWholeMilliseconds + 999) / 1000).toInt())
    private val abortExecutor = Executor { it.run() }

    @Volatile private var leading = false

    @Volatile private var lastConfirmedAt = 0L

    // Touched only by the loop, and by stop() after the loop has finished.
    private var connection: Connection? = null
    private var failing = false
    private var job: Job? = null

    /** True while this election holds the lock and its last successful check is recent. Never queries. */
    fun isLeader(): Boolean = leading && nanoTime() - lastConfirmedAt <= leaseNanos

    /** Starts competing for leadership in the background; the first attempt runs immediately. */
    @Synchronized
    fun start() {
        check(job == null) { "Leader election '$name' is already started" }
        job =
            scope.launch {
                while (isActive) {
                    tick()
                    delay(checkInterval)
                }
            }
    }

    /** Stops competing, releases the lock if held, and closes the connection. */
    suspend fun stop() {
        val running = synchronized(this) { job.also { job = null } } ?: return
        val wasLeading = leading
        leading = false
        running.cancelAndJoin()
        leading = false
        val conn = connection ?: return
        connection = null
        try {
            conn.prepareStatement("SELECT pg_advisory_unlock(hashtextextended(?, 0))").use { stmt ->
                stmt.queryTimeout = queryTimeoutSeconds
                stmt.setString(1, lockName)
                stmt.executeQuery().close()
            }
            conn.close()
            if (wasLeading) log.info("Stepped down as leader for '{}'", name)
        } catch (ex: Exception) {
            log.warn("Could not release leadership of '{}' cleanly", name, ex)
            abort(conn)
        }
    }

    /** Blocking [stop], for shutdown hooks and `use {}`. */
    override fun close() = runBlocking { stop() }

    private fun tick() {
        val conn = connection ?: openConnection() ?: return
        try {
            if (leading) {
                conn.querySingle("SELECT 1")
                lastConfirmedAt = nanoTime()
            } else if (conn.querySingle("SELECT pg_try_advisory_lock(hashtextextended(?, 0))", lockName)) {
                lastConfirmedAt = nanoTime()
                leading = true
                log.info("Became leader for '{}'", name)
            }
            recovered()
        } catch (ex: Exception) {
            if (leading) {
                leading = false
                log.warn("Lost leadership for '{}'", name, ex)
            } else {
                failed("Leader election check for '{}' failed", ex)
            }
            abort(conn)
            connection = null
        }
    }

    private fun openConnection(): Connection? =
        try {
            connect().also {
                it.autoCommit = true
                connection = it
                recovered()
            }
        } catch (ex: Exception) {
            failed("Could not connect for leader election '{}'", ex)
            null
        }

    // Returns the first column of the single row as a boolean (`SELECT 1` reads as true).
    private fun Connection.querySingle(
        sql: String,
        vararg params: String,
    ): Boolean =
        prepareStatement(sql).use { stmt ->
            stmt.queryTimeout = queryTimeoutSeconds
            params.forEachIndexed { i, p -> stmt.setString(i + 1, p) }
            stmt.executeQuery().use { rs ->
                rs.next()
                rs.getBoolean(1)
            }
        }

    private fun failed(
        message: String,
        ex: Exception,
    ) {
        if (failing) {
            log.debug(message, name, ex)
        } else {
            failing = true
            log.warn(message, name, ex)
        }
    }

    private fun recovered() {
        if (failing) {
            failing = false
            log.info("Leader election for '{}' is connected again", name)
        }
    }

    private fun abort(conn: Connection) {
        try {
            conn.abort(abortExecutor)
        } catch (ex: Exception) {
            log.debug("Aborting the leader election connection for '{}' failed", name, ex)
        }
    }
}
```

Notes for the implementer:
- `leading` is set to false both before and after `cancelAndJoin()`: before, so pollers stop at once; after, in
  case a tick that was already running acquired the lock in between.
- `stop()` waits for an in-flight check, which the query timeout bounds.
- `SELECT 1` read with `getBoolean(1)` returns true in pgjdbc (an integer 1 reads as true); the value isn't used
  for the check, only success matters.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.postgres.PostgresLeaderElectionTest'`
Expected: PASS (5 tests).

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.postgres.PostgresLeaderElectionIntegrationTest'`
Expected: PASS (7 tests).

- [ ] **Step 6: Run the whole build**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add kotmod/src/main/kotlin/io/kotmod/postgres/PostgresLeaderElection.kt \
  kotmod/src/test/kotlin/io/kotmod/postgres/PostgresLeaderElectionTest.kt \
  kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PostgresLeaderElectionIntegrationTest.kt
git commit -m "Add Postgres advisory-lock leader election

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Document leader election

**Files:**
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt`
- Modify: `README.md` (the "Run one active poller per consumer" paragraph in "Running in production", around
  line 909; the quickstart shutdown paragraph after the `outbox.start()` block, around line 374)

**Interfaces:**
- Consumes: `PostgresLeaderElection(connect: () -> Connection, name: String, checkInterval: Duration = 5.seconds)`,
  `isLeader()`, `start()`, `suspend stop()` from Task 1.
- Produces: nothing code-facing.

- [ ] **Step 1: Add the compiled example**

Append to `ReadmeExamples.kt` (add `import io.kotmod.postgres.PostgresLeaderElection` and
`import java.sql.DriverManager` to the imports, keeping them sorted):

```kotlin
fun leaderElection(
    url: String,
    user: String,
    password: String,
): PostgresLeaderElection {
    val election = PostgresLeaderElection({ DriverManager.getConnection(url, user, password) }, "order-service")
    election.start()
    return election
}

fun outboxWithLeaderElection(
    jdbc: JdbcContext,
    election: PostgresLeaderElection,
    offsets: PostgresOffsetManager,
    executor: EventReactionExecutor<OrderNotification, *>,
): AggregateEventOutbox<OrderNotification> =
    AggregateEventOutbox(
        backend = PostgresDomainPollingBackend(jdbc),
        executor = executor,
        eventToReactions = { emptyList() },
        getPosition = { offsets.getPosition("order-notifications") },
        savePosition = { offsets.savePosition("order-notifications", it) },
        isLeader = election::isLeader,
    )
```

If `AggregateEventOutbox`'s parameter list differs from this (check the existing `orderedOutbox` example in the
same file), match the existing example and keep `isLeader = election::isLeader`.

- [ ] **Step 2: Compile the examples**

Run: `./gradlew :examples:integrationTest`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Rewrite the README paragraph**

In `README.md`, replace the paragraph starting `**Run one active poller per consumer.**` (ending
`each reaction runs on one node at a time.`) with:

````markdown
**Run one active poller per consumer.** The outbox and public contracts only poll while `isLeader()`
returns `true`. Run your application on as many nodes as you like, but make sure only one of them polls
for each consumer. db-scheduler needs no such care: it is safe to run on every node, and each reaction
runs on one node at a time.

**Leader election.** `PostgresLeaderElection` picks the polling node with a Postgres advisory lock. Each
node creates one with the same name; whichever takes the lock leads until its connection ends, and another
node takes over within a few seconds:

```kotlin
fun leaderElection(
    url: String,
    user: String,
    password: String,
): PostgresLeaderElection {
    val election = PostgresLeaderElection({ DriverManager.getConnection(url, user, password) }, "order-service")
    election.start()
    return election
}
```

Pass `isLeader = election::isLeader` to each outbox and contract:

```kotlin
    AggregateEventOutbox(
        backend = PostgresDomainPollingBackend(jdbc),
        executor = executor,
        eventToReactions = { emptyList() },
        getPosition = { offsets.getPosition("order-notifications") },
        savePosition = { offsets.savePosition("order-notifications", it) },
        isLeader = election::isLeader,
    )
```

- **One election per application** is the simple default: one node polls for every consumer. To spread
  consumers across nodes, give each consumer its own election (its own name); each holds one connection.
- **Give it its own connection.** The election holds one connection for as long as it runs, so open it
  directly rather than from your pool (`dataSource::getConnection` also works). Set pgjdbc's `socketTimeout`
  on it so a stalled network fails a check instead of hanging. It does not work through PgBouncer in
  transaction mode.
- **How fast it reacts.** Every `checkInterval` (5 seconds by default) a follower tries to take the lock and
  the leader checks its connection. A leader whose checks fail or stall for two intervals stops polling.
- **Brief overlap.** If Postgres ends the leader's session first (a failover, `pg_terminate_backend`), another
  node can take over before the old leader notices, so two nodes may poll for up to about one
  `checkInterval`. That only causes duplicate dispatches, which deterministic reaction ids absorb.
- **Shutdown.** Stop outboxes and contracts first, then `election.stop()`, which releases the lock so another
  node takes over straight away.
````

Make sure the paragraph about **Start and stop in order** above it still reads correctly; if it lists the
start order, add the election: "start the leader election before the outbox and contracts, and stop it after
them."

- [ ] **Step 4: Point the quickstart at it**

In `README.md`, after the quickstart sentence ending ``then `scheduler.stop()`, then `executor.stop()`.``
add a sentence to the same paragraph:

```markdown
The quickstart passes `isLeader = { true }` because it runs on one node; see
[Running in production](#running-in-production) for leader election across several.
```

- [ ] **Step 5: Check the README**

Run: `python3 /private/tmp/claude-501/-Users-dreweaster-projects-kotmod/c8057b84-7d0b-478a-b9c3-f240806f99f5/scratchpad/readme_check2.py .`
Expected: `checked N kotlin blocks: OK` (N is two more than before).

- [ ] **Step 6: Run the whole build**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add README.md examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt
git commit -m "Document Postgres leader election

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

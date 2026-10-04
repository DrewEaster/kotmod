# Plain-JDBC Transactions, Modules and Outer Transactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace kotmod's SQLDelight dependency with a plain-JDBC `JdbcContext`, support several commands committing in one outer transaction, split the build into `kotmod`, `kotmod-sqldelight`, `kotmod-db-scheduler` and `examples` modules, and update the docs.

**Architecture:** `JdbcContext` (`withConnection`, `inTransaction`, `isInTransaction`) is implemented by `DataSourceJdbcContext` (core) and `SqlDelightJdbcContext` (adapter module). `DomainPersistenceBackend` gains `inTransaction`, so `AggregateManager`/`EventProducer` no longer take a `Transacter`. `JdbcContext.transaction { }` opens (or joins) a thread-bound transaction and marks the coroutine context with a `KotmodTransaction`; commands detect it and run their database work on the owning thread instead of switching to `Dispatchers.IO`. One abstract `JdbcContextContract` integration suite (published as a core test fixture) is run against both implementations.

**Tech Stack:** Kotlin 2.4.20 (JVM toolchain 25), kotlinx-coroutines 1.11.0, kotlinx-serialization 1.11.0, Gradle 9.8 with an included `build-logic` build, SQLDelight 2.4.0 (adapter only), db-scheduler 16.12.0, Testcontainers 2.0.5 + Postgres 17, MockK 1.14.11.

**Spec:** `docs/superpowers/specs/2026-10-04-jdbc-context-modules-design.md`

## Global Constraints

- Core (`io.kotmod:kotmod`) has **no SQLDelight and no db-scheduler** on its classpath.
- `kotmod-sqldelight`: SQLDelight `runtime` and `jdbc-driver` 2.4.0 as `api`. `kotmod-db-scheduler`: db-scheduler 16.12.0 as `api`. db-scheduler classes keep package `io.kotmod.event.reaction.dbscheduler`.
- `Repository` stays `get(id)` / `save(id, state)`.
- `JdbcContext.inTransaction` joins a transaction already open on the current thread (no savepoints); otherwise commits on success and rolls back + rethrows on any `Throwable`. Its block is non-suspending.
- `withConnection` inside a transaction passes the transaction's connection (never closed/committed by callers); outside, a fresh auto-commit connection that is released afterwards.
- Outer transactions: `suspend fun <R> JdbcContext.transaction(block: suspend () -> R): R`; commands inside must run sequentially on the owning thread; a wrong thread or a different `JdbcContext` throws `IllegalStateException`.
- Constructors after: `AggregateManager(aggregateType, repository, backend)`, `EventProducer(aggregateType, backend)`, `PostgresDomainPersistenceBackend(jdbc, serialization)`, `PostgresDomainPollingBackend(jdbc)`, `PostgresOffsetManager(jdbc)`.
- The database schema does not change.
- Full suite stays green at the end of every task: `./gradlew test integrationTest` (needs Docker; run with the sandbox disabled in sandboxed shells).

**Plan-level refinements of the spec (both needed to satisfy it):**
1. `JdbcContext` gains `fun isInTransaction(): Boolean` (current thread). Without it, `transaction { }` called from inside the app's own SQLDelight transaction would switch threads and open a second transaction, breaking the spec's SQLDelight requirement.
2. Context compatibility uses `==`, and `SqlDelightJdbcContext` equality is by driver identity — two adapters over the same driver share SQLDelight's transaction, so they must count as the same context.

## Review Focus

1. Cancelling the coroutine that is running `jdbc.transaction { }` mid-block — expect a rollback with nothing committed (test in Task 3).
2. Connection leaks — expect every borrowed connection to be closed and its auto-commit restored on success, on failure, and when rollback itself fails (unit test in Task 1).
3. Two `SqlDelightJdbcContext` instances over the same driver used inside one transaction (e.g. repository and backend built separately) — expect them to share the transaction, not fail the wrong-context check (test in Task 5).
4. Calling a kotmod command inside the app's own SQLDelight transaction from blocking code — expect it to join that transaction when wrapped in `jdbc.transaction { }` (test in Task 5).
5. A command whose block throws inside an outer transaction after an earlier command succeeded — expect the earlier command's state, events and command record rolled back too (test in Task 3).

---

## File Structure

**Task 1–3 (still the single-module layout under `src/`):**
- Create `src/main/kotlin/io/kotmod/jdbc/JdbcContext.kt` — the interface.
- Create `src/main/kotlin/io/kotmod/jdbc/DataSourceJdbcContext.kt` — thread-bound `DataSource` implementation.
- Create `src/main/kotlin/io/kotmod/jdbc/Transactions.kt` — `KotmodTransaction`, `transaction { }`, internal `databaseWork`.
- Modify `DomainBackend.kt`, `AggregateManager.kt`, `EventProducer.kt`, `postgres/PostgresDomainBackend.kt`, `postgres/PostgresOffsetManager.kt`, `contract/PublicEventContract.kt`.
- Tests: `src/test/kotlin/io/kotmod/jdbc/DataSourceJdbcContextTest.kt`, `src/integrationTest/kotlin/io/kotmod/jdbc/JdbcContextContract.kt`, `src/integrationTest/kotlin/io/kotmod/jdbc/DataSourceJdbcContextIntegrationTest.kt`; modify stubs, unit tests, integration tests, README example sources.

**Task 4 (module split):** `settings.gradle.kts`, `build-logic/`, `kotmod/`, `kotmod-db-scheduler/`, `examples/`, root `build.gradle.kts` removed.

**Task 5:** `kotmod-sqldelight/` with `SqlDelightJdbcContext` and its tests.

**Task 6:** `README.md` and the snippet checker path.

---

### Task 1: `JdbcContext` and `DataSourceJdbcContext`

**Files:**
- Create: `src/main/kotlin/io/kotmod/jdbc/JdbcContext.kt`
- Create: `src/main/kotlin/io/kotmod/jdbc/DataSourceJdbcContext.kt`
- Create: `src/main/kotlin/io/kotmod/jdbc/Transactions.kt` (only `KotmodTransaction` with `requireCompatible` in this task)
- Test: `src/test/kotlin/io/kotmod/jdbc/DataSourceJdbcContextTest.kt`
- Test: `src/integrationTest/kotlin/io/kotmod/jdbc/JdbcContextContract.kt`
- Test: `src/integrationTest/kotlin/io/kotmod/jdbc/DataSourceJdbcContextIntegrationTest.kt`

**Interfaces:**
- Produces:
  ```kotlin
  interface JdbcContext {
      fun <R> withConnection(block: (Connection) -> R): R
      fun <R> inTransaction(block: () -> R): R
      fun isInTransaction(): Boolean
  }
  class DataSourceJdbcContext(dataSource: DataSource) : JdbcContext
  class KotmodTransaction internal constructor(val jdbc: JdbcContext, val thread: Thread) : AbstractCoroutineContextElement(KotmodTransaction) {
      companion object Key : CoroutineContext.Key<KotmodTransaction> {
          fun requireCompatible(jdbc: JdbcContext)   // for JdbcContext implementations
      }
      fun requireOwningThread()
  }
  abstract class JdbcContextContract : IntegrationTest() { protected abstract fun createJdbcContext(dataSource: DataSource): JdbcContext }
  ```

- [ ] **Step 1: Write the failing unit test**

`DataSourceJdbcContextTest.kt`:

```kotlin
package io.kotmod.jdbc

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class DataSourceJdbcContextTest {
    private val connection: Connection = mockk(relaxed = true)
    private val dataSource: DataSource = mockk { every { getConnection() } returns connection }
    private val jdbc = DataSourceJdbcContext(dataSource)

    @Test
    fun `successful transaction commits, restores auto-commit and closes the connection`() {
        every { connection.autoCommit } returns true

        assertEquals(42, jdbc.inTransaction { 42 })

        verifyOrder {
            connection.autoCommit = false
            connection.commit()
            connection.autoCommit = true
            connection.close()
        }
        verify(exactly = 0) { connection.rollback() }
    }

    @Test
    fun `failing transaction rolls back, rethrows, restores auto-commit and closes the connection`() {
        every { connection.autoCommit } returns true

        val failure = assertFailsWith<IllegalStateException> { jdbc.inTransaction { error("boom") } }

        assertEquals("boom", failure.message)
        verifyOrder {
            connection.rollback()
            connection.autoCommit = true
            connection.close()
        }
        verify(exactly = 0) { connection.commit() }
    }

    @Test
    fun `a rollback failure is attached to the original exception and the connection is still closed`() {
        every { connection.autoCommit } returns true
        every { connection.rollback() } throws SQLException("rollback failed")

        val failure = assertFailsWith<IllegalStateException> { jdbc.inTransaction { error("boom") } }

        assertEquals("rollback failed", failure.suppressed.single().message)
        verify { connection.close() }
    }

    @Test
    fun `nested inTransaction and withConnection reuse the transaction's connection`() {
        every { connection.autoCommit } returns true

        jdbc.inTransaction {
            jdbc.inTransaction { jdbc.withConnection { assertSame(connection, it) } }
        }

        verify(exactly = 1) { dataSource.connection }
        verify(exactly = 1) { connection.commit() }
    }

    @Test
    fun `withConnection outside a transaction borrows and closes a connection`() {
        jdbc.withConnection { assertSame(connection, it) }

        verify(exactly = 1) { connection.close() }
        verify(exactly = 0) { connection.commit() }
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew test --tests 'io.kotmod.jdbc.DataSourceJdbcContextTest'`
Expected: FAIL — compilation error, `Unresolved reference 'DataSourceJdbcContext'`.

- [ ] **Step 3: Implement**

`JdbcContext.kt`:

```kotlin
package io.kotmod.jdbc

import java.sql.Connection

/**
 * How kotmod reaches the database: borrowing a connection and running work in a transaction.
 *
 * Transactions are bound to the thread that opened them. Pass the same `JdbcContext` to every kotmod
 * class and to your own repositories so they all share one transaction. Implementations:
 * [DataSourceJdbcContext] for a plain `DataSource`, and `SqlDelightJdbcContext` (module
 * `kotmod-sqldelight`) for apps using SQLDelight.
 */
interface JdbcContext {
    /**
     * Runs [block] with a connection. Inside a transaction on this thread it is the transaction's
     * connection — don't close, commit or roll it back. Outside one it is a fresh auto-commit connection,
     * released when [block] returns.
     */
    fun <R> withConnection(block: (Connection) -> R): R

    /**
     * Runs [block] in a transaction. If one is already open on this thread, [block] joins it; otherwise a
     * new one is committed when [block] returns, or rolled back if it throws (the exception is rethrown).
     */
    fun <R> inTransaction(block: () -> R): R

    /** Returns whether a transaction is open on the current thread. */
    fun isInTransaction(): Boolean
}
```

`DataSourceJdbcContext.kt`:

```kotlin
package io.kotmod.jdbc

import java.sql.Connection
import javax.sql.DataSource

/**
 * A [JdbcContext] over a plain [DataSource]. The outermost [inTransaction] borrows a connection, turns
 * auto-commit off and binds the connection to the current thread until it commits or rolls back; nested
 * calls on that thread reuse it. Uses the database's default isolation level.
 */
class DataSourceJdbcContext(
    private val dataSource: DataSource,
) : JdbcContext {
    private val transactionConnection = ThreadLocal<Connection?>()

    override fun <R> withConnection(block: (Connection) -> R): R {
        KotmodTransaction.requireCompatible(this)
        val open = transactionConnection.get()
        return if (open != null) block(open) else dataSource.connection.use(block)
    }

    override fun <R> inTransaction(block: () -> R): R {
        KotmodTransaction.requireCompatible(this)
        if (transactionConnection.get() != null) return block()

        val connection = dataSource.connection
        val previousAutoCommit = connection.autoCommit
        try {
            connection.autoCommit = false
            transactionConnection.set(connection)
            val result =
                try {
                    block()
                } catch (failure: Throwable) {
                    try {
                        connection.rollback()
                    } catch (rollbackFailure: Throwable) {
                        failure.addSuppressed(rollbackFailure)
                    }
                    throw failure
                }
            connection.commit()
            return result
        } finally {
            transactionConnection.remove()
            try {
                connection.autoCommit = previousAutoCommit
            } finally {
                connection.close()
            }
        }
    }

    override fun isInTransaction(): Boolean = transactionConnection.get() != null
}
```

`Transactions.kt` (this task only needs `KotmodTransaction`; Task 3 adds the rest of the file):

```kotlin
package io.kotmod.jdbc

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * Marks a coroutine as running inside an outer kotmod transaction opened by [transaction], recording the
 * [JdbcContext] it belongs to and the [thread] that owns it.
 */
class KotmodTransaction internal constructor(
    val jdbc: JdbcContext,
    val thread: Thread,
) : AbstractCoroutineContextElement(KotmodTransaction) {
    companion object Key : CoroutineContext.Key<KotmodTransaction> {
        internal val current = ThreadLocal<KotmodTransaction?>()

        /**
         * Throws [IllegalStateException] if an outer transaction for a different [JdbcContext] is open on
         * this thread. Called by [JdbcContext] implementations before touching the database, so a mismatched
         * context fails loudly instead of silently opening a second transaction.
         */
        fun requireCompatible(jdbc: JdbcContext) {
            val open = current.get() ?: return
            check(open.jdbc == jdbc) {
                "A kotmod transaction is open on this thread for a different JdbcContext; " +
                    "use the same JdbcContext for everything inside jdbc.transaction { }"
            }
        }
    }

    /** Throws [IllegalStateException] unless called on the thread that owns this transaction. */
    fun requireOwningThread() {
        check(Thread.currentThread() === thread) {
            "kotmod transaction used from thread '${Thread.currentThread().name}' but it belongs to " +
                "'${thread.name}'; don't switch threads (e.g. withContext) inside jdbc.transaction { }"
        }
    }
}
```

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `./gradlew test --tests 'io.kotmod.jdbc.DataSourceJdbcContextTest'`
Expected: PASS (5 tests).

- [ ] **Step 5: Write the contract suite and its first implementation (integration)**

`src/integrationTest/kotlin/io/kotmod/jdbc/JdbcContextContract.kt`:

```kotlin
package io.kotmod.jdbc

import io.kotmod.postgres.support.IntegrationTest
import org.junit.jupiter.api.BeforeEach
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Behaviour every [JdbcContext] implementation must have. Subclass it and implement [createJdbcContext]. */
abstract class JdbcContextContract : IntegrationTest() {
    protected abstract fun createJdbcContext(dataSource: DataSource): JdbcContext

    protected lateinit var context: JdbcContext

    @BeforeEach
    fun setUpContract() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS jdbc_probe (id TEXT PRIMARY KEY)")
                stmt.execute("TRUNCATE jdbc_probe")
            }
        }
        context = createJdbcContext(dataSource)
    }

    protected fun insertProbe(id: String) {
        context.withConnection { conn ->
            conn.prepareStatement("INSERT INTO jdbc_probe (id) VALUES (?)").use { ps ->
                ps.setString(1, id)
                ps.executeUpdate()
            }
        }
    }

    protected fun committedProbes(): List<String> =
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id FROM jdbc_probe ORDER BY id").use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1)) }
                }
            }
        }

    @Test
    fun `inTransaction commits when the block completes`() {
        context.inTransaction { insertProbe("a") }
        assertEquals(listOf("a"), committedProbes())
    }

    @Test
    fun `inTransaction rolls back and rethrows when the block throws`() {
        val failure = assertFailsWith<IllegalStateException> { context.inTransaction { insertProbe("a"); error("boom") } }
        assertEquals("boom", failure.message)
        assertEquals(emptyList(), committedProbes())
    }

    @Test
    fun `nested inTransaction joins the outer transaction`() {
        assertFailsWith<IllegalStateException> {
            context.inTransaction {
                context.inTransaction { insertProbe("a") }
                error("outer fails")
            }
        }
        assertEquals(emptyList(), committedProbes())
    }

    @Test
    fun `withConnection inside a transaction uses the transaction's connection`() {
        context.inTransaction {
            val first = context.withConnection { it }
            context.withConnection { conn ->
                assertSame(first, conn)
                assertFalse(conn.autoCommit)
            }
        }
    }

    @Test
    fun `withConnection outside a transaction is auto-commit`() {
        context.withConnection { assertTrue(it.autoCommit) }
        insertProbe("a")
        assertEquals(listOf("a"), committedProbes())
    }

    @Test
    fun `isInTransaction reflects the current thread`() {
        assertFalse(context.isInTransaction())
        context.inTransaction { assertTrue(context.isInTransaction()) }
        assertFalse(context.isInTransaction())
    }
}
```

`src/integrationTest/kotlin/io/kotmod/jdbc/DataSourceJdbcContextIntegrationTest.kt`:

```kotlin
package io.kotmod.jdbc

import javax.sql.DataSource

class DataSourceJdbcContextIntegrationTest : JdbcContextContract() {
    override fun createJdbcContext(dataSource: DataSource): JdbcContext = DataSourceJdbcContext(dataSource)
}
```

- [ ] **Step 6: Run the full suites**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL; all tests pass, including the 5 unit and 6 contract tests.

- [ ] **Step 7: Commit**

```bash
git add src/main/kotlin/io/kotmod/jdbc src/test/kotlin/io/kotmod/jdbc src/integrationTest/kotlin/io/kotmod/jdbc
git commit -m "Add JdbcContext with a DataSource implementation"
```

---

### Task 2: Move the library onto `JdbcContext` and drop SQLDelight

**Files:**
- Modify: `src/main/kotlin/io/kotmod/DomainBackend.kt`, `AggregateManager.kt`, `EventProducer.kt`
- Modify: `src/main/kotlin/io/kotmod/postgres/PostgresDomainBackend.kt`, `PostgresOffsetManager.kt`
- Modify: `src/main/kotlin/io/kotmod/contract/PublicEventContract.kt` (remove the unused `JdbcDriver` import)
- Create: `src/main/kotlin/io/kotmod/jdbc/Transactions.kt` additions — internal `databaseWork` (no outer-transaction behaviour yet)
- Modify: `src/test/kotlin/io/kotmod/support/StubPersistenceBackend.kt`; delete `src/test/kotlin/io/kotmod/support/StubTransacter.kt`
- Modify: `src/test/kotlin/io/kotmod/AggregateManagerCreateTest.kt`, `AggregateManagerExecuteTest.kt`, `AggregateManagerDedupTest.kt`, `EventProducerTest.kt`
- Modify: every integration test and README example source that uses `driver`/`TransacterImpl` (listed in Step 6)
- Modify: `build.gradle.kts` (remove SQLDelight plugin, `sqldelight { }` block and SQLDelight dependencies)

**Interfaces:**
- Consumes: Task 1's `JdbcContext`, `DataSourceJdbcContext`.
- Produces:
  - `DomainPersistenceBackend.inTransaction(block: () -> R): R`
  - `AggregateManager(aggregateType, repository, backend)`, `EventProducer(aggregateType, backend)`
  - `PostgresDomainPersistenceBackend(jdbc: JdbcContext, serialization)`, `PostgresDomainPollingBackend(jdbc: JdbcContext)`, `PostgresOffsetManager(jdbc: JdbcContext)`
  - `internal suspend fun <R> databaseWork(block: () -> R): R` in `io.kotmod.jdbc` (Task 3 extends it)
  - `IntegrationTest.jdbc: JdbcContext` (replaces `driver`)

- [ ] **Step 1: Write the failing unit test**

Add to `AggregateManagerCreateTest` (and adjust the construction as shown):

```kotlin
    @Test
    fun `create writes meta, events and the command record inside the backend's transaction`() =
        runTest {
            manager.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }

            assertEquals(emptyList(), backend.writesOutsideTransaction)
            assertEquals(1, backend.transactionsCommitted)
        }
```

In the test class, change the `AggregateManager(...)` construction to drop `transacter = StubTransacter(),` and the `StubTransacter` import. Do the same in `AggregateManagerExecuteTest`, `AggregateManagerDedupTest` and `EventProducerTest` (drop `transacter = StubTransacter(),` and its import). Use whatever the test class already names its backend/manager fields; if the create test's stub backend field is not named `backend`, use its existing name.

In `StubPersistenceBackend`, add:

```kotlin
    private var transactionDepth = 0
    var transactionsCommitted = 0
        private set
    val writesOutsideTransaction = mutableListOf<String>()

    override fun <R> inTransaction(block: () -> R): R {
        transactionDepth++
        try {
            val result = block()
            if (transactionDepth == 1) transactionsCommitted++
            return result
        } finally {
            transactionDepth--
        }
    }

    private fun recordWrite(name: String) {
        if (transactionDepth == 0) writesOutsideTransaction += name
    }
```

and call `recordWrite("saveMeta")`, `recordWrite("appendEvents")`, `recordWrite("recordCommandHandled")` at the start of `saveMeta`, `appendEvents` and `recordCommandHandled`. Delete `StubTransacter.kt`.

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew test --tests 'io.kotmod.AggregateManagerCreateTest'`
Expected: FAIL — compilation errors: `'inTransaction' overrides nothing` in `StubPersistenceBackend` and `No value passed for parameter 'transacter'`.

- [ ] **Step 3: Implement the library changes**

`DomainBackend.kt` — add to `DomainPersistenceBackend` (first member):

```kotlin
    /**
     * Runs [block] in a transaction, joining one already open on this thread. [AggregateManager] and
     * [EventProducer] make all of a command's writes inside it.
     */
    fun <R> inTransaction(block: () -> R): R
```

`Transactions.kt` — append:

```kotlin
/**
 * Runs kotmod's blocking database work for a command. Outside an outer transaction it switches to
 * [kotlinx.coroutines.Dispatchers.IO].
 */
internal suspend fun <R> databaseWork(block: () -> R): R = withContext(Dispatchers.IO) { block() }
```

with imports `kotlinx.coroutines.Dispatchers` and `kotlinx.coroutines.withContext`.

`AggregateManager.kt`:
- Remove the constructor parameter `private val transacter: Transacter,` and the imports `app.cash.sqldelight.Transacter`, `kotlinx.coroutines.Dispatchers`, `kotlinx.coroutines.withContext`; add `import io.kotmod.jdbc.databaseWork`.
- Replace every `withContext(Dispatchers.IO) {` with `databaseWork {`.
- Replace every `transacter.transactionWithResult {` with `backend.inTransaction {`.
- Replace `return@withContext ReadResult.Dedup(current)` with `return@databaseWork ReadResult.Dedup(current)`.

`EventProducer.kt`: the same four kinds of change (constructor parameter `transacter` removed; `withContext(Dispatchers.IO) {` → `databaseWork {`; `transacter.transactionWithResult {` → `backend.inTransaction {`; imports adjusted).

`postgres/PostgresDomainBackend.kt`:
- In both classes replace `private val driver: JdbcDriver,` with `private val jdbc: JdbcContext,`; import `io.kotmod.jdbc.JdbcContext`; remove `import app.cash.sqldelight.driver.jdbc.JdbcDriver` and `import java.sql.Connection` if unused.
- In `PostgresDomainPersistenceBackend`: replace each `useConnection { conn ->` with `jdbc.withConnection { conn ->`, `return@useConnection null` with `return@withConnection null`, delete the private `useConnection` function, and add:

```kotlin
    override fun <R> inTransaction(block: () -> R): R = jdbc.inTransaction(block)
```

- In `PostgresDomainPollingBackend.readEventsAfter`: replace the `val (conn, close) = driver.connectionAndClose()` / `try { return conn … } finally { close() }` structure with `return jdbc.withConnection { conn -> conn.prepareStatement(…).use { … } }` keeping the statement and result-set code unchanged.

`postgres/PostgresOffsetManager.kt`: replace `private val driver: JdbcDriver` with `private val jdbc: JdbcContext`, every `useConnection { conn ->` with `jdbc.withConnection { conn ->`, delete the private `useConnection`, fix imports.

`contract/PublicEventContract.kt`: delete `import app.cash.sqldelight.driver.jdbc.JdbcDriver`.

Update KDoc that mentions drivers or transacters: `PostgresDomainPersistenceBackend` ("Every call borrows a connection from [jdbc], so calls inside a transaction share it."), `AggregateManager` (phase 3 runs in `backend.inTransaction`), `Repository` ("must use the same [io.kotmod.jdbc.JdbcContext] (or SQLDelight driver) as the backend").

- [ ] **Step 4: Run the unit tests**

Run: `./gradlew test`
Expected: PASS (all unit tests, including the new one).

- [ ] **Step 5: Remove SQLDelight from the build**

In `build.gradle.kts`: delete `id("app.cash.sqldelight") version "2.4.0"`, the whole `sqldelight { … }` block, and both `app.cash.sqldelight` dependency lines.

- [ ] **Step 6: Update integration tests and README examples**

`src/integrationTest/kotlin/io/kotmod/postgres/support/IntegrationTest.kt`: replace `protected val driver: JdbcDriver get() = SharedPostgres.driver` with `protected val jdbc: JdbcContext get() = SharedPostgres.jdbc`, and in `SharedPostgres` replace `val driver: JdbcDriver = dataSource.asJdbcDriver()` with `val jdbc: JdbcContext = DataSourceJdbcContext(dataSource)`. Fix imports (`io.kotmod.jdbc.JdbcContext`, `io.kotmod.jdbc.DataSourceJdbcContext`; remove SQLDelight imports).

In every integration test, replace the `driver` argument with `jdbc` in calls to `PostgresDomainPersistenceBackend(`, `PostgresDomainPollingBackend(`, `PostgresOffsetManager(` (files: `postgres/PostgresDomainBackendIntegrationTest.kt`, `postgres/PostgresOffsetManagerIntegrationTest.kt`, `outbox/AggregateEventOutboxIntegrationTest.kt`, `contract/PublicEventContractIntegrationTest.kt`, `event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt`).

In `PostgresDomainBackendIntegrationTest`, replace the rollback test's transacter block

```kotlin
            // Same shape as a SQLDelight-generated Database: a Transacter over the backend's driver.
            val transacter = object : TransacterImpl(driver) {}

            try {
                transacter.transaction {
```

with

```kotlin
            try {
                jdbc.inTransaction {
```

rename the test to `` `backend operations inside a JdbcContext transaction roll back together` ``, and remove the `TransacterImpl` import.

README examples (`src/integrationTest/kotlin/io/kotmod/readme/`):
- `Quickstart.kt`: `OrderRepository(private val jdbc: JdbcContext)`; replace both `driver.withConnection { conn ->` with `jdbc.withConnection { conn ->`; delete the `// Borrows the driver's connection…` comment and the `JdbcDriver.withConnection` extension function; imports: add `io.kotmod.jdbc.JdbcContext`, remove `app.cash.sqldelight.driver.jdbc.JdbcDriver` and `java.sql.Connection`.
- `QuickstartTest.kt`: replace `val driver = dataSource.asJdbcDriver()` with `val jdbc = DataSourceJdbcContext(dataSource)`; replace `driver` with `jdbc` in `OrderRepository(`, `PostgresDomainPersistenceBackend(`, `PostgresDomainPollingBackend(`, `PostgresOffsetManager(`, and `auditLog(`; delete the line `transacter = object : TransacterImpl(driver) {},`; fix imports.
- `ReadmeExamples.kt`: `fun auditLog(jdbc: JdbcContext)`, passing `jdbc` to `PostgresDomainPersistenceBackend(` and deleting `transacter = object : TransacterImpl(driver) {},`; `orderContract(jdbc: JdbcContext, …)` passing `jdbc` to `PostgresDomainPollingBackend(`; fix imports.

- [ ] **Step 7: Run the full suites and confirm SQLDelight is gone**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL, all tests pass.
Run: `grep -rn "app.cash.sqldelight" src build.gradle.kts`
Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add -A src build.gradle.kts
git commit -m "Move kotmod onto JdbcContext and drop SQLDelight"
```

---

### Task 3: Outer transactions across commands

**Files:**
- Modify: `src/main/kotlin/io/kotmod/jdbc/Transactions.kt` (add `transaction`, extend `databaseWork`)
- Test: `src/integrationTest/kotlin/io/kotmod/jdbc/JdbcContextContract.kt` (add outer-transaction tests)

**Interfaces:**
- Consumes: Task 1 `KotmodTransaction`; Task 2 `databaseWork`, new constructors.
- Produces: `suspend fun <R> JdbcContext.transaction(block: suspend () -> R): R`.

- [ ] **Step 1: Write the failing tests**

Add to `JdbcContextContract` (imports: `io.kotmod.AggregateAlreadyExistsException`, `io.kotmod.AggregateId`, `io.kotmod.AggregateManager`, `io.kotmod.AggregateType`, `io.kotmod.OptimisticConcurrencyException`, `io.kotmod.Repository`, `io.kotmod.postgres.PostgresDomainPersistenceBackend`, `io.kotmod.postgres.support.orderEventSerialization`, `io.kotmod.support.Order`, `io.kotmod.support.OrderEvent`, `io.kotmod.support.OrderPlaced`, `io.kotmod.support.OrderShipped`, `io.kotmod.support.PendingOrder`, `io.kotmod.support.ShippedOrder`, `kotlinx.coroutines.CompletableDeferred`, `kotlinx.coroutines.Dispatchers`, `kotlinx.coroutines.async`, `kotlinx.coroutines.cancelAndJoin`, `kotlinx.coroutines.runBlocking`, `kotlinx.coroutines.withContext`, `kotlinx.coroutines.awaitCancellation`):

```kotlin
    private class ProbeOrderRepository(
        private val jdbc: JdbcContext,
    ) : Repository<Order> {
        override fun get(id: AggregateId): Order? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, name FROM outer_tx_state WHERE id = ?").use { ps ->
                    ps.setString(1, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) null else if (rs.getString(1) == "PENDING") PendingOrder(rs.getString(2)) else ShippedOrder(rs.getString(2))
                    }
                }
            }

        override fun save(
            id: AggregateId,
            state: Order,
        ) {
            val (status, name) =
                when (state) {
                    is PendingOrder -> "PENDING" to state.name
                    is ShippedOrder -> "SHIPPED" to state.name
                    else -> error("unsupported state $state")
                }
            jdbc.withConnection { conn ->
                conn
                    .prepareStatement(
                        "INSERT INTO outer_tx_state (id, status, name) VALUES (?, ?, ?) " +
                            "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, name = EXCLUDED.name",
                    ).use { ps ->
                        ps.setString(1, id.value)
                        ps.setString(2, status)
                        ps.setString(3, name)
                        ps.executeUpdate()
                    }
            }
        }
    }

    private fun manager(
        type: String,
        jdbc: JdbcContext = context,
    ) = AggregateManager(
        aggregateType = AggregateType(type),
        repository = ProbeOrderRepository(jdbc),
        backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
    )

    private fun count(table: String): Int =
        dataSource.connection.use { conn ->
            conn.createStatement().use { it.executeQuery("SELECT COUNT(*) FROM $table").use { rs -> rs.next(); rs.getInt(1) } }
        }

    @BeforeEach
    fun setUpOuterTransactionTable() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS outer_tx_state (id TEXT PRIMARY KEY, status TEXT NOT NULL, name TEXT NOT NULL)")
                stmt.execute("TRUNCATE outer_tx_state")
            }
        }
    }

    @Test
    fun `transaction commits commands on two aggregates together`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")

            context.transaction {
                orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                invoices.create(AggregateId("i-1")) { PendingOrder("invoice") to listOf(OrderPlaced("invoice")) }
            }

            assertEquals(2, count("ddd_aggregate_root"))
            assertEquals(2, count("ddd_domain_event"))
            assertEquals(2, count("ddd_command_history"))
            assertEquals(2, count("outer_tx_state"))
        }

    @Test
    fun `a failing second command rolls back the first command's state, events and command record`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            invoices.create(AggregateId("i-1")) { PendingOrder("invoice") to listOf(OrderPlaced("invoice")) }

            assertFailsWith<AggregateAlreadyExistsException> {
                context.transaction {
                    orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    invoices.create(AggregateId("i-1")) { PendingOrder("again") to listOf(OrderPlaced("again")) }
                }
            }

            assertEquals(1, count("ddd_aggregate_root"))
            assertEquals(1, count("ddd_domain_event"))
            assertEquals(1, count("ddd_command_history"))
            assertEquals(1, count("outer_tx_state"))
        }

    @Test
    fun `an optimistic concurrency conflict rolls back the whole transaction`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
            invoices.create(AggregateId("i-1")) { PendingOrder("invoice") to listOf(OrderPlaced("invoice")) }

            assertFailsWith<OptimisticConcurrencyException> {
                context.transaction {
                    orders.execute<PendingOrder>(AggregateId("o-1")) { ShippedOrder(it.name) to listOf(OrderShipped(it.name)) }
                    invoices.execute<PendingOrder>(AggregateId("i-1")) { order ->
                        // Someone else changes the invoice between our read and our write.
                        dataSource.connection.use { conn ->
                            conn.createStatement().use {
                                it.execute("UPDATE ddd_aggregate_root SET aggregate_version = aggregate_version + 1 WHERE aggregate_id = 'i-1'")
                            }
                        }
                        ShippedOrder(order.name) to listOf(OrderShipped(order.name))
                    }
                }
            }

            assertEquals(2, count("ddd_domain_event"))
            assertEquals(PendingOrder("book"), ProbeOrderRepository(context).get(AggregateId("o-1")))
        }

    @Test
    fun `commands inside a transaction see earlier uncommitted writes`() =
        runBlocking {
            val orders = manager("Order")

            val shipped =
                context.transaction {
                    orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    orders.execute<PendingOrder>(AggregateId("o-1")) { ShippedOrder(it.name) to listOf(OrderShipped(it.name)) }
                }

            assertEquals(ShippedOrder("book"), shipped)
            assertEquals(2, count("ddd_domain_event"))
        }

    @Test
    fun `nested transaction joins the outer one`() =
        runBlocking {
            val orders = manager("Order")

            assertFailsWith<IllegalStateException> {
                context.transaction {
                    context.transaction {
                        orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    }
                    error("outer fails")
                }
            }

            assertEquals(0, count("ddd_domain_event"))
        }

    @Test
    fun `switching threads inside a transaction fails loudly and commits nothing`() =
        runBlocking {
            val orders = manager("Order")

            val failure =
                assertFailsWith<IllegalStateException> {
                    context.transaction {
                        withContext(Dispatchers.Default) {
                            orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                        }
                    }
                }

            assertTrue(failure.message!!.contains("don't switch threads"))
            assertEquals(0, count("ddd_domain_event"))
        }

    @Test
    fun `using a different JdbcContext inside a transaction fails loudly`() =
        runBlocking {
            val other = manager("Order", jdbc = createJdbcContext(dataSource))

            val failure =
                assertFailsWith<IllegalStateException> {
                    context.transaction {
                        other.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    }
                }

            assertTrue(failure.message!!.contains("different JdbcContext"))
            assertEquals(0, count("ddd_domain_event"))
        }

    @Test
    fun `cancelling the caller rolls back the transaction`() =
        runBlocking {
            val orders = manager("Order")
            val written = CompletableDeferred<Unit>()

            val running =
                async(Dispatchers.Default) {
                    context.transaction {
                        orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                        written.complete(Unit)
                        awaitCancellation()
                    }
                }
            written.await()
            running.cancelAndJoin()

            assertEquals(0, count("ddd_domain_event"))
            assertEquals(0, count("outer_tx_state"))
        }
```

Add `import io.kotmod.execute`-style imports only if the narrowed `execute<T>` requires it (it is a member of `AggregateManager`, so no import is needed).

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew integrationTest --tests 'io.kotmod.jdbc.DataSourceJdbcContextIntegrationTest'`
Expected: FAIL — compilation error, `Unresolved reference 'transaction'`.

- [ ] **Step 3: Implement**

In `Transactions.kt`, replace `databaseWork` and add `transaction` (imports: `kotlinx.coroutines.Dispatchers`, `kotlinx.coroutines.Job`, `kotlinx.coroutines.asContextElement`, `kotlinx.coroutines.currentCoroutineContext`, `kotlinx.coroutines.runBlocking`, `kotlinx.coroutines.withContext`):

```kotlin
/**
 * Runs [block] so that every kotmod command inside it — on any number of aggregates — commits or rolls
 * back together:
 * ```
 * jdbc.transaction {
 *     orders.execute<PendingOrder>(orderId) { … }
 *     invoices.create(invoiceId) { … }
 * }
 * ```
 * Outside a transaction, it switches to an IO thread, opens a transaction there and runs [block] confined
 * to that thread. If a transaction is already open on the current thread (for example the app's own
 * SQLDelight transaction, or an enclosing `transaction { }`), [block] joins it. The transaction commits when
 * [block] returns and rolls back if it throws or is cancelled.
 *
 * Run commands one after another inside [block], never in parallel, and don't switch threads (e.g. with
 * `withContext`) — doing so throws [IllegalStateException]. Keep [block] short: it holds a database
 * transaction open.
 */
suspend fun <R> JdbcContext.transaction(block: suspend () -> R): R {
    val open = currentCoroutineContext()[KotmodTransaction]
    if (open != null) {
        check(open.jdbc == this) {
            "A kotmod transaction is open for a different JdbcContext; use the same JdbcContext for everything inside jdbc.transaction { }"
        }
        open.requireOwningThread()
        return block()
    }

    if (isInTransaction()) {
        val joined = KotmodTransaction(this, Thread.currentThread())
        return withContext(joined + KotmodTransaction.current.asContextElement(joined)) { block() }
    }

    return withContext(Dispatchers.IO) {
        val parentJob = currentCoroutineContext()[Job]
        inTransaction {
            val opened = KotmodTransaction(this@transaction, Thread.currentThread())
            val context = opened + KotmodTransaction.current.asContextElement(opened)
            runBlocking(if (parentJob != null) context + parentJob else context) { block() }
        }
    }
}

/**
 * Runs kotmod's blocking database work for a command: on the owning thread inside an outer
 * [transaction], otherwise on [Dispatchers.IO].
 */
internal suspend fun <R> databaseWork(block: () -> R): R {
    val open = currentCoroutineContext()[KotmodTransaction]
    return if (open == null) {
        withContext(Dispatchers.IO) { block() }
    } else {
        open.requireOwningThread()
        block()
    }
}
```

- [ ] **Step 4: Run them to verify they pass**

Run: `./gradlew integrationTest --tests 'io.kotmod.jdbc.DataSourceJdbcContextIntegrationTest'`
Expected: PASS (6 contract + 8 outer-transaction tests). If the cancellation test fails because `runBlocking` does not observe the parent job, record a ruling and pass the parent job as described in the failure, keeping the test.

- [ ] **Step 5: Run the full suites**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/io/kotmod/jdbc/Transactions.kt src/integrationTest/kotlin/io/kotmod/jdbc/JdbcContextContract.kt
git commit -m "Add outer transactions spanning several commands"
```

---

### Task 4: Split into modules

**Files:**
- Create: `build-logic/settings.gradle.kts`, `build-logic/build.gradle.kts`, `build-logic/src/main/kotlin/kotmod.library.gradle.kts`
- Create: `kotmod/build.gradle.kts`, `kotmod-db-scheduler/build.gradle.kts`, `examples/build.gradle.kts`
- Modify: `settings.gradle.kts`, `gradle.properties`
- Delete: root `build.gradle.kts`
- Move: sources as listed in Step 2.

**Interfaces:**
- Produces: Gradle projects `:kotmod`, `:kotmod-db-scheduler`, `:examples`; convention plugin id `kotmod.library`; core test fixtures containing `IntegrationTest`, `orderEventSerialization`, `eventually`, `io.kotmod.support` test aggregate types and `JdbcContextContract`.

- [ ] **Step 1: Create the build logic**

`build-logic/settings.gradle.kts`:

```kotlin
dependencyResolutionManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "build-logic"
```

`build-logic/build.gradle.kts`:

```kotlin
plugins {
    `kotlin-dsl`
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.4.20")
    implementation("org.jetbrains.kotlin:kotlin-serialization:2.4.20")
}
```

`build-logic/src/main/kotlin/kotmod.library.gradle.kts`:

```kotlin
plugins {
    `java-library`
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.plugin.serialization")
}

group = "io.kotmod"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(25)
}

val integrationTest: SourceSet =
    sourceSets.create("integrationTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    }

configurations[integrationTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[integrationTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    "testImplementation"(kotlin("test"))
    "testImplementation"("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    "testImplementation"("io.mockk:mockk:1.14.11")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against a Postgres Testcontainer (requires Docker)."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.named("test"))
}
```

`settings.gradle.kts` (replace entirely):

```kotlin
pluginManagement {
    includeBuild("build-logic")
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "kotmod-root"

include("kotmod", "kotmod-db-scheduler", "examples")
```

`gradle.properties`: delete the `kotlinVersion=2.4.20` line (keep `kotlin.code.style=official`). Delete root `build.gradle.kts`.

- [ ] **Step 2: Move the sources**

```bash
mkdir -p kotmod kotmod-db-scheduler/src/{main,test,integrationTest}/kotlin/io/kotmod/event/reaction examples/src/integrationTest/kotlin/io/kotmod
git mv src kotmod/src
# db-scheduler integration
git mv kotmod/src/main/kotlin/io/kotmod/event/reaction/dbscheduler kotmod-db-scheduler/src/main/kotlin/io/kotmod/event/reaction/dbscheduler
git mv kotmod/src/test/kotlin/io/kotmod/event/reaction/dbscheduler kotmod-db-scheduler/src/test/kotlin/io/kotmod/event/reaction/dbscheduler
git mv kotmod/src/integrationTest/kotlin/io/kotmod/event/reaction/dbscheduler kotmod-db-scheduler/src/integrationTest/kotlin/io/kotmod/event/reaction/dbscheduler
# README examples
git mv kotmod/src/integrationTest/kotlin/io/kotmod/readme examples/src/integrationTest/kotlin/io/kotmod/readme
# core test fixtures
mkdir -p kotmod/src/testFixtures/kotlin/io/kotmod/postgres/support kotmod/src/testFixtures/kotlin/io/kotmod/support kotmod/src/testFixtures/kotlin/io/kotmod/jdbc kotmod/src/testFixtures/resources
git mv kotmod/src/integrationTest/kotlin/io/kotmod/postgres/support/IntegrationTest.kt kotmod/src/testFixtures/kotlin/io/kotmod/postgres/support/
git mv kotmod/src/integrationTest/kotlin/io/kotmod/postgres/support/OrderEventSerialization.kt kotmod/src/testFixtures/kotlin/io/kotmod/postgres/support/
git mv kotmod/src/integrationTest/kotlin/io/kotmod/jdbc/JdbcContextContract.kt kotmod/src/testFixtures/kotlin/io/kotmod/jdbc/
git mv kotmod/src/test/kotlin/io/kotmod/support/TestAggregate.kt kotmod/src/testFixtures/kotlin/io/kotmod/support/
git mv kotmod/src/integrationTest/resources/db-scheduler kotmod/src/testFixtures/resources/db-scheduler
```

Move `eventually` into the fixtures: create `kotmod/src/testFixtures/kotlin/io/kotmod/postgres/support/Eventually.kt`:

```kotlin
package io.kotmod.postgres.support

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Polls [condition] every 50ms until it is true, failing after [timeout]. */
suspend fun eventually(
    timeout: Duration = 10.seconds,
    condition: () -> Boolean,
) {
    withTimeout(timeout) {
        while (!condition()) delay(50)
    }
}
```

delete the `eventually` function (and its now-unused imports) from `kotmod-db-scheduler/src/integrationTest/kotlin/io/kotmod/event/reaction/dbscheduler/DbSchedulerTestSupport.kt`, and add `import io.kotmod.postgres.support.eventually` to `DbSchedulerEventReactionsIntegrationTest.kt` and to `examples/src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt` (replacing `import io.kotmod.event.reaction.dbscheduler.eventually`).

- [ ] **Step 3: Create the module build files**

`kotmod/build.gradle.kts`:

```kotlin
plugins {
    id("kotmod.library")
    `java-test-fixtures`
}

dependencies {
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.slf4j:slf4j-api:2.0.20")
    implementation("com.aventrix.jnanoid:jnanoid:2.0.0")

    testFixturesApi(kotlin("test-junit5"))
    testFixturesApi("org.testcontainers:testcontainers-postgresql:2.0.5")
    testFixturesApi("org.postgresql:postgresql:42.7.13")
    testFixturesApi("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
}
```

(`kotlinx-serialization-json` is `api` because `toEventSerializer` and `jsonDataSerializationContext` expose `KSerializer` and `Json`.)

`kotmod-db-scheduler/build.gradle.kts`:

```kotlin
plugins {
    id("kotmod.library")
}

dependencies {
    api(project(":kotmod"))
    api("com.github.kagkarlsson:db-scheduler:16.12.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.slf4j:slf4j-api:2.0.20")

    "integrationTestImplementation"(testFixtures(project(":kotmod")))
}
```

`examples/build.gradle.kts`:

```kotlin
plugins {
    id("kotmod.library")
}

dependencies {
    implementation(project(":kotmod"))
    implementation(project(":kotmod-db-scheduler"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    "integrationTestImplementation"(testFixtures(project(":kotmod")))
}
```

- [ ] **Step 4: Build and run everything**

Run: `./gradlew clean test integrationTest`
Expected: BUILD SUCCESSFUL. Test counts across modules equal the totals at the end of Task 3 (moved, not lost). Compare with: `find . -path '*/build/test-results/*' -name 'TEST-*.xml' | xargs grep -ho 'tests="[0-9]*"' | awk -F'"' '{s+=$2} END {print s}'`.
Run: `./gradlew :kotmod:dependencies --configuration runtimeClasspath | grep -iE "sqldelight|db-scheduler"`
Expected: no output (core has neither).

If a moved file fails to compile because it used an `internal` declaration from another module, make the smallest visibility change in the test (not in main code) and record a ruling.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Split the build into kotmod, kotmod-db-scheduler and examples modules"
```

---

### Task 5: `kotmod-sqldelight`

**Files:**
- Create: `kotmod-sqldelight/build.gradle.kts`
- Create: `kotmod-sqldelight/src/main/kotlin/io/kotmod/sqldelight/SqlDelightJdbcContext.kt`
- Test: `kotmod-sqldelight/src/integrationTest/kotlin/io/kotmod/sqldelight/SqlDelightJdbcContextIntegrationTest.kt`
- Modify: `settings.gradle.kts`

**Interfaces:**
- Consumes: `JdbcContext`, `KotmodTransaction.requireCompatible`, `transaction`, `JdbcContextContract` (core test fixtures).
- Produces: `class SqlDelightJdbcContext(driver: JdbcDriver) : JdbcContext` (equality by driver identity).

- [ ] **Step 1: Add the module and write the failing tests**

`settings.gradle.kts`: change the include line to `include("kotmod", "kotmod-sqldelight", "kotmod-db-scheduler", "examples")`.

`kotmod-sqldelight/build.gradle.kts`:

```kotlin
plugins {
    id("kotmod.library")
}

dependencies {
    api(project(":kotmod"))
    api("app.cash.sqldelight:runtime:2.4.0")
    api("app.cash.sqldelight:jdbc-driver:2.4.0")

    "integrationTestImplementation"(testFixtures(project(":kotmod")))
}
```

`SqlDelightJdbcContextIntegrationTest.kt`:

```kotlin
package io.kotmod.sqldelight

import app.cash.sqldelight.TransacterImpl
import app.cash.sqldelight.driver.jdbc.JdbcDriver
import app.cash.sqldelight.driver.jdbc.asJdbcDriver
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.JdbcContextContract
import io.kotmod.jdbc.transaction
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.OrderPlaced
import io.kotmod.support.PendingOrder
import kotlinx.coroutines.runBlocking
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SqlDelightJdbcContextIntegrationTest : JdbcContextContract() {
    private lateinit var driver: JdbcDriver

    override fun createJdbcContext(dataSource: DataSource): JdbcContext {
        driver = dataSource.asJdbcDriver()
        return SqlDelightJdbcContext(driver)
    }

    private fun eventCount(): Int =
        dataSource.connection.use { conn ->
            conn.createStatement().use { it.executeQuery("SELECT COUNT(*) FROM ddd_domain_event").use { rs -> rs.next(); rs.getInt(1) } }
        }

    private fun statelessOrders(jdbc: JdbcContext) =
        AggregateManager(
            aggregateType = AggregateType("Order"),
            repository =
                object : io.kotmod.Repository<io.kotmod.support.Order> {
                    override fun get(id: AggregateId) = null

                    override fun save(
                        id: AggregateId,
                        state: io.kotmod.support.Order,
                    ) = Unit
                },
            backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
        )

    @Test
    fun `kotmod commands join the app's own SQLDelight transaction`() {
        val database = object : TransacterImpl(driver) {}
        val orders = statelessOrders(context)

        assertFailsWith<IllegalStateException> {
            database.transaction {
                driver.execute(null, "INSERT INTO jdbc_probe (id) VALUES ('app')", 0)
                runBlocking {
                    context.transaction {
                        orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    }
                }
                error("app fails after kotmod ran")
            }
        }

        assertEquals(emptyList(), committedProbes())
        assertEquals(0, eventCount())
    }

    @Test
    fun `SQLDelight work inside a kotmod transaction joins it`() =
        runBlocking {
            val orders = statelessOrders(context)

            context.transaction {
                driver.execute(null, "INSERT INTO jdbc_probe (id) VALUES ('app')", 0)
                orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
            }

            assertEquals(listOf("app"), committedProbes())
            assertEquals(1, eventCount())
        }

    @Test
    fun `two adapters over the same driver share one transaction`() =
        runBlocking {
            val sameDriverContext = SqlDelightJdbcContext(driver)
            val orders = statelessOrders(sameDriverContext)

            assertFailsWith<IllegalStateException> {
                context.transaction {
                    orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }
                    error("roll back both")
                }
            }

            assertEquals(0, eventCount())
        }
}
```

Note: this class also inherits and runs every `JdbcContextContract` test (basic contract + outer transactions) against `SqlDelightJdbcContext`. The contract's "different JdbcContext" test calls `createJdbcContext` again, which creates a **new driver**, so it remains a genuinely different context.

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod-sqldelight:integrationTest`
Expected: FAIL — compilation error, `Unresolved reference 'SqlDelightJdbcContext'`.

- [ ] **Step 3: Implement**

`SqlDelightJdbcContext.kt`:

```kotlin
package io.kotmod.sqldelight

import app.cash.sqldelight.TransacterImpl
import app.cash.sqldelight.driver.jdbc.JdbcDriver
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.KotmodTransaction
import java.sql.Connection

/**
 * A [JdbcContext] for apps that use SQLDelight. kotmod borrows connections from [driver] and runs its
 * transactions through SQLDelight's, so kotmod's writes and your SQLDelight queries share one transaction
 * whenever they run on the same thread — whether you open it with SQLDelight (`database.transaction { }`)
 * or with kotmod (`jdbc.transaction { }`).
 *
 * Two instances over the same [driver] are equal: they share the driver's transactions.
 */
class SqlDelightJdbcContext(
    private val driver: JdbcDriver,
) : JdbcContext {
    private val transacter = object : TransacterImpl(driver) {}

    override fun <R> withConnection(block: (Connection) -> R): R {
        KotmodTransaction.requireCompatible(this)
        val (connection, close) = driver.connectionAndClose()
        try {
            return block(connection)
        } finally {
            close()
        }
    }

    override fun <R> inTransaction(block: () -> R): R {
        KotmodTransaction.requireCompatible(this)
        return transacter.transactionWithResult { block() }
    }

    override fun isInTransaction(): Boolean = driver.currentTransaction() != null

    override fun equals(other: Any?): Boolean = other is SqlDelightJdbcContext && other.driver === driver

    override fun hashCode(): Int = System.identityHashCode(driver)
}
```

- [ ] **Step 4: Run them to verify they pass**

Run: `./gradlew :kotmod-sqldelight:integrationTest`
Expected: PASS (inherited contract tests + 3 SQLDelight tests).

- [ ] **Step 5: Run everything**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add settings.gradle.kts kotmod-sqldelight
git commit -m "Add kotmod-sqldelight with SqlDelightJdbcContext"
```

---

### Task 6: Documentation

**Files:**
- Modify: `README.md`
- Verify: `examples/src/integrationTest/kotlin/io/kotmod/readme/*.kt` (already updated in Task 2)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Update the snippet checker and confirm it now fails**

Recreate the checker from the previous README plan in the session scratchpad as `readme_check.py`, changing only the sources path to `examples/src/integrationTest/kotlin/io/kotmod/readme`. Run `python3 <scratchpad>/readme_check.py .`.
Expected: FAIL — README blocks still show `driver`, `TransacterImpl` and the old `withConnection` helper.

- [ ] **Step 2: Update the README**

- **Installation:** dependencies become `implementation("io.kotmod:kotmod:<version>")` and `implementation("io.kotmod:kotmod-db-scheduler:<version>")`, with an optional `implementation("io.kotmod:kotmod-sqldelight:<version>")` for SQLDelight users; remove the `app.cash.sqldelight:jdbc-driver` line; keep serialization plugin, coroutines and Postgres driver lines. Remove the sentence saying db-scheduler comes with kotmod; say it comes with `kotmod-db-scheduler`.
- **Quickstart step 3:** copy the new `jdbc`, `serialization`, `orderType`, `orders` lines from `QuickstartTest.kt` and the new `OrderRepository` from `Quickstart.kt`; replace the sentence about the `withConnection` helper with one saying the repository borrows the connection through `jdbc.withConnection`, which joins kotmod's transaction. Remove the `withConnection` extension snippet.
- **Quickstart step 5:** re-copy the `offsets` and `outbox` blocks (they now take `jdbc`).
- **Aggregates and commands guide:** replace the "Your repository joins the transaction" paragraph to reference `JdbcContext`; add a subsection `#### Several aggregates in one transaction` with a ```` ```kotlin ```` example copied from a new function `shipAndInvoice` added to `ReadmeExamples.kt`:

```kotlin
suspend fun shipAndInvoice(
    jdbc: JdbcContext,
    orders: AggregateManager<Order, OrderEvent>,
    invoices: AggregateManager<Order, OrderEvent>,
    orderId: AggregateId,
) {
    jdbc.transaction {
        orders.execute<PendingOrder>(orderId) { order ->
            ShippedOrder(order.item) to listOf(OrderShipped(order.item))
        }
        invoices.create(AggregateId("invoice-${orderId.value}")) {
            PendingOrder("invoice") to listOf(OrderPlaced("invoice"))
        }
    }
}
```

  and the rules: commands commit or roll back together; run them sequentially; don't switch threads; keep it short; a failure (including a concurrency conflict) rolls everything back.
- **Event-only aggregates guide:** re-copy `auditLog` (no `transacter`).
- **Postgres setup guide:** replace the "Transactions" paragraph: kotmod talks to the database through a `JdbcContext` — `DataSourceJdbcContext(dataSource)` for plain JDBC; pass the same instance to every kotmod class and to your repositories. Add `#### Using SQLDelight`: add `kotmod-sqldelight`, build `SqlDelightJdbcContext(driver)` from the same driver as your generated database; SQLDelight queries and kotmod share transactions either way round; calling kotmod inside your own `database.transaction { }` requires wrapping the call in `runBlocking { jdbc.transaction { … } }` (SQLDelight's block is not suspending). This subsection's code is prose-only (no new ```` ```kotlin ```` block), or uses a `<!-- not-compiled -->` marker.
- **Publishing events guide:** re-copy `orderContract` (takes `jdbc`).
- **Status and contributing:** mention the modules.

After adding `shipAndInvoice` to `ReadmeExamples.kt` (imports `io.kotmod.jdbc.JdbcContext`, `io.kotmod.jdbc.transaction`), run `./gradlew :examples:integrationTest` — Expected: PASS.

- [ ] **Step 3: Run the checks**

Run: `python3 <scratchpad>/readme_check.py .` — Expected: `OK`.
Run: `grep -nE "TransacterImpl|asJdbcDriver|app.cash.sqldelight:jdbc-driver" README.md` — Expected: matches only inside the "Using SQLDelight" subsection.
Run: `./gradlew test integrationTest` — Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add README.md examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt
git commit -m "Document JdbcContext, modules, outer transactions and SQLDelight"
```

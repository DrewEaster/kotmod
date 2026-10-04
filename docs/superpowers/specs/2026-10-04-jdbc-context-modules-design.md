# Plain-JDBC transactions, modules and multi-aggregate transactions

**Date:** 2026-10-04
**Status:** Approved design, pending implementation plan

## Goal

Remove kotmod's dependency on SQLDelight by talking to the database through a small plain-JDBC
abstraction, `JdbcContext`, while letting apps that use SQLDelight share transactions with kotmod through
an adapter. At the same time, split the build into modules, and make **several aggregate commands commit
in one outer transaction** — a deliberate design goal of kotmod (pragmatism over purism) that does not
work today.

## Current state (motivation)

- kotmod's SQL is already plain JDBC. SQLDelight is used only for `JdbcDriver.connectionAndClose()`
  (borrowing the current transaction's connection) and `Transacter` (opening transactions in
  `AggregateManager` / `EventProducer`). The SQLDelight Gradle plugin and its `InfrastructureDatabase`
  config are unused leftovers; `PublicEventContract.kt` has an unused `JdbcDriver` import.
- The `Transacter` and the backend's driver are passed separately; passing mismatched ones silently
  commits events outside the state's transaction.
- `AggregateManager` and `EventProducer` switch to `Dispatchers.IO` before opening their transaction.
  Transactions are thread-bound (SQLDelight's `JdbcDriver` keeps its open transaction in a private
  `ThreadLocal`), so commands wrapped in an outer transaction each commit separately.

## Decisions

- **Modules:** separate Gradle modules — core without SQLDelight or db-scheduler; `kotmod-sqldelight`;
  `kotmod-db-scheduler`. A Spring adapter is out of scope (same pattern later).
- **Repository:** the `Repository` interface is unchanged (`get(id)`, `save(id, state)`). Repositories join
  the transaction through the app's stack: SQLDelight repositories run their generated queries;
  plain-JDBC repositories call `jdbc.withConnection { … }`.
- **Abstraction:** one interface with two operations (approach A), so the connection source and the
  transaction runner cannot be mismatched.
- **The backend owns the transaction:** `DomainPersistenceBackend` gains `inTransaction`;
  `AggregateManager` and `EventProducer` lose their `Transacter` parameter.
- **Outer transactions are in scope:** `JdbcContext.transaction { … }` lets several commands (on any
  aggregates) commit or roll back together.

## Module layout

| Module (directory) | Artifact | Contents | Dependencies |
|---|---|---|---|
| `kotmod/` | `io.kotmod:kotmod` | All of `io.kotmod.*` except db-scheduler: aggregates, serialization, Postgres backends, outbox, contracts, reaction API, `JdbcContext`, `DataSourceJdbcContext`, `transaction { }` | coroutines, kotlinx-serialization-json, slf4j-api, jnanoid. No SQLDelight, no db-scheduler. |
| `kotmod-sqldelight/` | `io.kotmod:kotmod-sqldelight` | `io.kotmod.sqldelight.SqlDelightJdbcContext` | `kotmod`; SQLDelight `runtime` and `jdbc-driver` 2.4.0 as `api` |
| `kotmod-db-scheduler/` | `io.kotmod:kotmod-db-scheduler` | `io.kotmod.event.reaction.dbscheduler.*`, moved unchanged (same package) | `kotmod`; db-scheduler 16.12.0 as `api` |
| `examples/` (unpublished) | — | README examples (`Quickstart.kt`, `ReadmeExamples.kt`) and their tests | all three |

- A convention plugin in `build-logic/` applies the shared setup to library modules: Kotlin JVM
  toolchain 25, kotlinx-serialization plugin, JUnit Platform, and the `integrationTest` source set/task.
- Unit tests move with their code; core keeps today's `src/test` support classes.
- The Testcontainers `IntegrationTest` base (shared Postgres, schema setup, truncation) becomes a core
  **test fixture** (`java-test-fixtures`), reused by the other modules' integration tests.
- `./gradlew test integrationTest` at the root runs everything.
- Removed: SQLDelight Gradle plugin and `InfrastructureDatabase` config; unused import in
  `PublicEventContract.kt`.

## `JdbcContext`

```kotlin
interface JdbcContext {
    fun <R> withConnection(block: (Connection) -> R): R
    fun <R> inTransaction(block: () -> R): R
}
```

Contract for every implementation:

- `inTransaction`: if a transaction is already open **on the current thread** for this context, `block`
  joins it (no savepoints). Otherwise a transaction is started, `block` runs, and it commits; if `block`
  throws anything, it rolls back and rethrows.
- `withConnection`: inside a transaction, passes the transaction's connection (callers must not close or
  commit it); outside one, lends a fresh auto-commit connection and releases it afterwards.
- `block` is non-suspending, so code cannot suspend and resume on another thread mid-transaction.

### `DataSourceJdbcContext(dataSource: DataSource)` (core)

- The outermost `inTransaction` borrows a connection, sets auto-commit off, binds it to the current thread
  (a `ThreadLocal` owned by this instance), runs the block, commits or rolls back, restores auto-commit,
  and closes (returns) the connection.
- Nested `inTransaction` and `withConnection` calls on that thread reuse the bound connection.
- Default isolation level of the database; nothing configurable.

### `SqlDelightJdbcContext(driver: JdbcDriver)` (`kotmod-sqldelight`)

- `withConnection` uses `driver.connectionAndClose()`: the open SQLDelight transaction's connection, or a
  fresh connection released afterwards.
- `inTransaction` runs through SQLDelight's own transactions on the same driver
  (`object : TransacterImpl(driver) {}`), so it joins any SQLDelight transaction already open on the
  thread — e.g. the app's `database.transaction { … }`. Rollback follows SQLDelight's semantics.
- Only the driver is needed; SQLDelight tracks the open transaction on the driver.

## Outer transactions across commands

```kotlin
suspend fun <R> JdbcContext.transaction(block: suspend () -> R): R
```

```kotlin
jdbc.transaction {
    orders.execute<PendingOrder>(orderId) { order -> ShippedOrder(order.item) to listOf(OrderShipped(order.item)) }
    invoices.create(invoiceId) { Invoice(orderId) to listOf(InvoiceRaised(orderId)) }
}   // both aggregates' state, events and command records commit together — or neither does
```

Mechanism:

1. Outside any transaction, `transaction { }` switches to an IO thread, opens a transaction there with
   `inTransaction`, and runs `block` **confined to that thread** for the transaction's duration.
2. The transaction is recorded in the coroutine context and in a kotmod-owned thread-local marker, with
   the owning thread and the `JdbcContext` instance.
3. `AggregateManager` and `EventProducer` check the coroutine context before switching to
   `Dispatchers.IO`. If a transaction is present, they run their read and write phases on the current
   thread (inside the transaction) instead of switching.
4. A `transaction { }` inside another joins it.
5. The block finishing normally commits; any exception rolls back everything.

Consequences:

- Commands read inside the transaction, so a later command sees earlier commands' uncommitted writes
  (including on the same aggregate).
- Optimistic concurrency is still checked per aggregate; a conflict throws and rolls back the whole
  transaction, including other aggregates' events and command records.

Safety checks (fail loudly rather than silently splitting the transaction):

- **Wrong thread:** a command run inside `transaction { }` but on a thread other than the owning one (e.g.
  after `withContext(Dispatchers.Default)`) throws `IllegalStateException`.
- **Wrong context:** a Postgres backend (or offset manager) whose `JdbcContext` differs from the open
  transaction's throws `IllegalStateException` instead of opening its own transaction.

Rules for users (documented): run commands sequentially inside the block — never in parallel (a JDBC
connection is not safe for concurrent use); keep the block short (it holds a database transaction).

SQLDelight users can either start a kotmod `transaction { }` and run SQLDelight queries inside it, or call
kotmod commands inside their own `database.transaction { }` — the latter only from blocking code (e.g.
`runBlocking`), since SQLDelight's block is not suspending.

## API changes

`DomainPersistenceBackend` gains:

```kotlin
fun <R> inTransaction(block: () -> R): R
```

| Before | After |
|---|---|
| `AggregateManager(aggregateType, repository, backend, transacter)` | `AggregateManager(aggregateType, repository, backend)` |
| `EventProducer(aggregateType, backend, transacter)` | `EventProducer(aggregateType, backend)` |
| `PostgresDomainPersistenceBackend(driver: JdbcDriver, serialization)` | `PostgresDomainPersistenceBackend(jdbc: JdbcContext, serialization)` |
| `PostgresDomainPollingBackend(driver: JdbcDriver)` | `PostgresDomainPollingBackend(jdbc: JdbcContext)` |
| `PostgresOffsetManager(driver: JdbcDriver)` | `PostgresOffsetManager(jdbc: JdbcContext)` |

- `PostgresDomainPersistenceBackend.inTransaction` delegates to its `JdbcContext`.
- The app's repository must use the same `JdbcContext` (or SQLDelight driver) as the backend; documented.
- Breaking changes are acceptable pre-1.0. The database schema does not change.

## Testing

- **Contract suite:** an abstract `JdbcContextContract` in core's test fixtures, run against
  `DataSourceJdbcContext` (core) and `SqlDelightJdbcContext` (`kotmod-sqldelight`) on the Testcontainer
  Postgres:
  - commit visible after `inTransaction`; exception rolls back and is rethrown;
  - nested `inTransaction` joins (an exception inside rolls back everything);
  - `withConnection` inside a transaction gets the transaction's connection; outside, an auto-commit one;
  - auto-commit is restored after a transaction.
- **Outer transactions** (both implementations): two aggregates commit together; a failing second command
  (including an optimistic-concurrency conflict) rolls back the first aggregate's state, events and command
  record; nested `transaction { }` joins; the wrong-thread check throws; the wrong-context check throws.
- **SQLDelight specifics:** a kotmod command inside the app's own SQLDelight transaction commits/rolls back
  together with an app write made via `driver.execute(...)`; SQLDelight work inside a kotmod
  `transaction { }` joins it.
- **Existing tests:** unit tests replace `StubTransacter` with the stub backend's `inTransaction`;
  integration tests take a `JdbcContext`; db-scheduler tests move to `kotmod-db-scheduler`; README example
  tests move to `examples/` and the snippet checker reads from there. Full suite green throughout.

## Documentation

- README installation lists `kotmod` + `kotmod-db-scheduler` (+ optional `kotmod-sqldelight`); SQLDelight
  is no longer a required dependency.
- Quickstart uses `DataSourceJdbcContext`; the hand-written `withConnection` helper is removed in favour of
  `jdbc.withConnection`.
- Aggregates guide gains "Several aggregates in one transaction" (usage and rules).
- Postgres setup guide gains "Using SQLDelight".
- KDoc for `JdbcContext`, `DataSourceJdbcContext`, `transaction`, `SqlDelightJdbcContext` and the changed
  constructors.

## Out of scope

- A Spring (or jOOQ/Exposed) adapter.
- Configurable isolation levels or savepoints.
- Publishing setup.
- The skipped-events bug and per-aggregate ordering (separate, already-agreed pieces of work).

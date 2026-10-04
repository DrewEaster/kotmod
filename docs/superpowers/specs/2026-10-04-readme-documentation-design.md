# kotmod README documentation

**Date:** 2026-10-04
**Status:** Approved design, pending implementation plan

## Goal

Replace the two-line `README.md` with documentation that takes a developer who has never seen kotmod
from adding the dependency to a working aggregate whose events drive durable reactions, and then
explains each part of the library in depth — using only the README.

## Audience and positioning

- **Primary reader:** public open-source users who find kotmod on GitHub. Familiarity with DDD and the
  outbox pattern varies, so concepts are explained briefly before they are used.
- **Pitch leads with two points:**
  1. **Domain events without event sourcing** — aggregates keep plain state in the app's own tables;
     events are recorded alongside it in the same transaction.
  2. **Transactional outbox built in** — events are written atomically with state, then reliably turned
     into reactions (no dual-write problem; at-least-once delivery).
- Package names in all examples use `io.kotmod`.

## Format

A single `README.md` (approach A): pitch, installation and quickstart first, then one guide per
concept, then production guidance. Expected length 500–800 lines, with a table of contents. Guides are
written as self-contained sections so they can later be split into `docs/` pages without rewriting.

## Outline

1. **Title and one-paragraph pitch** — what kotmod is (a Kotlin library for building DDD aggregates on
   Postgres) and the two lead points.
2. **Table of contents.**
3. **Why kotmod** — 6–8 lines: state-stored aggregates with events recorded in the same transaction;
   the dual-write problem the outbox solves; what kotmod is not (not event sourcing, not a message
   broker, not a framework).
4. **Installation** — placeholder Maven Central coordinates in Gradle Kotlin DSL
   (`implementation("io.kotmod:kotmod:<version>")`) with a note that publishing is coming; requirements
   (JVM 25 toolchain as built, Kotlin, Postgres).
5. **Quickstart** — one runnable path through the orders example (see below), 80–120 lines of code.
6. **Core concepts** — a small diagram of the flow: command → aggregate → state + events (one
   transaction) → outbox → event reactions / public events.
7. **Guides** (each continues the orders example):
   1. Aggregates and commands
   2. Event-only aggregates (`EventProducer`)
   3. Event serialization and schema migrations
   4. Postgres setup
   5. The outbox and event reactions
   6. Durable reactions with db-scheduler
   7. Publishing events to other contexts
8. **Running in production.**
9. **Status and contributing** — pre-1.0, APIs may change; how to run the tests (`./gradlew test`,
   `./gradlew integrationTest` which needs Docker); licence (Apache 2.0, per `LICENSE`).

## Running example

One small orders domain, introduced in the quickstart and continued in every guide.

- **State:** `sealed interface Order` — `PendingOrder(item)`, `ShippedOrder(item)`,
  `CancelledOrder(item, reason)`.
- **Events:** `@Serializable sealed interface OrderEvent : DomainEvent` — `OrderPlaced(item)`,
  `OrderShipped(item)`, `OrderCancelled(item, reason)`.
- **Commands:** `place` via `AggregateManager.create`; `ship` and `cancel` via the narrowed
  `execute<PendingOrder>`, showing the state-machine guard and `UnexpectedAggregateStateException`.
- **State storage:** a small JDBC `Repository<Order>` over an app-owned `orders` table — making clear
  that the app owns state and kotmod owns events and bookkeeping.
- **Reaction:** `@Serializable sealed interface OrderNotification : EventReactionTrigger` with
  `SendOrderConfirmation(orderId)`, serialized with kotlinx JSON. `OrderPlaced` maps to a
  `SendOrderConfirmation` reaction with id `"confirmation-${eventId}"`; the handler `println`s
  "sending confirmation".
- **Extensions used by later guides:** `OrderCancelled` gains a field (shown with `migrateFormat`); an
  `OrderAuditLog` side example for `EventProducer`; `OrderPlaced` published as `OrderPlacedV1` through a
  `PublicEventContract`; a second executor (billing) to show multiple executors on one scheduler.

### Quickstart steps

1. **Create tables:** `DddSchema.ddl`, db-scheduler's `scheduled_tasks` DDL, and the app's `orders` table.
2. **Define state and events.**
3. **Wire persistence:** `DataSource` → `JdbcDriver` (`asJdbcDriver()`) → `jsonDataSerializationContext`
   → `PostgresDomainPersistenceBackend` → repository → `AggregateManager` (with `TransacterImpl(driver)`
   as the `Transacter`).
4. **Run commands:** place an order, then ship it.
5. **React to events:** trigger and serializer → `DbSchedulerEventReactions` → `Scheduler` →
   `EventReactionExecutor` → `AggregateEventOutbox` with `PostgresOffsetManager` → start in the correct
   order → the confirmation is printed.

## Guide contents

Each guide opens with one sentence on when to use the feature, shows code from the orders example,
lists the behaviours that matter, and names the relevant classes (whose KDoc holds the detail).

1. **Aggregates and commands** — the read / command / write phases (no database access while the
   command block runs); `create` vs `execute` vs narrowed `execute<T>`; `CommandId` for idempotent
   retries (no id = random id = not idempotent); `CorrelationId`; optimistic concurrency and
   `OptimisticConcurrencyException` (retry the command); `AggregateNotFoundException`,
   `AggregateAlreadyExistsException`, `UnexpectedAggregateStateException`; the `Repository` must use the
   driver's connection so it joins the same transaction.
2. **Event-only aggregates** — when to use `EventProducer` (an event log and idempotent commands
   without stored state); `emit` creates the aggregate's bookkeeping on first use.
3. **Event serialization and schema migrations** — registering events with
   `jsonDataSerializationContext { +X.serializer().toEventSerializer() }`; stored type and version;
   `migrateFormat` for JSON changes and `migrateClassName` for renames or moves, including the
   `initialClassName` caveat; old events migrate on read, new events are written at the latest version;
   bringing your own `DataSerializationContext`.
4. **Postgres setup** — what `DddSchema.ddl` creates and copying it into Flyway/Liquibase migrations;
   `scheduled_tasks` belongs to the app (db-scheduler's DDL); `JdbcDriver` from a `DataSource`;
   `TransacterImpl(driver)` or the app's SQLDelight database as the `Transacter`;
   `PostgresDomainPollingBackend`; `PostgresOffsetManager` with one consumer name per poller.
5. **The outbox and event reactions** — how `AggregateEventOutbox` polls, maps, dispatches and saves its
   offset; `EventReactionExecutor` parameters (`execute`, `failureRetryHandler`, `timeoutRetryHandler`,
   `onCompletion`, `createExecutionContext`, `defaultTimeout`, `BackoffStrategy`); a small table of the
   `EventReactionExecutionResult` → `RetrySignal` → `EventReactionCompletionResult` lifecycle; a brief
   note on implementing a custom sink/source.
6. **Durable reactions with db-scheduler** — `DbSchedulerEventReactions` wiring; the app owns the
   `Scheduler`; one task name per executor; multiple executors on one scheduler (billing +
   notifications); how retries, retry counts and execution ids map onto db-scheduler; the misordered-
   startup safety net; unreadable data retried with backoff; removing a reaction for good with
   `SchedulerClient.cancel`.
7. **Publishing events to other contexts** — why public events are separate from internal ones;
   `PublicEventContract` with `internalToPublic` (returning `null` keeps an event private); fan-out to
   several subscribers/executors; `subscribe` before `start`.

## Running in production

- **At-least-once delivery** — events are never lost, but a reaction can run more than once (crash
  between dispatch and offset save; shutdown interrupt), so `execute` and `onCompletion` must be
  idempotent.
- **Deterministic reaction ids** — built from `eventId` plus a label; this is what de-duplicates a
  re-dispatch while the reaction is pending.
- **Lifecycle order** — startup: executors → `scheduler.start()` → outbox/contracts; shutdown in
  reverse; what happens if misordered (safety net and its warning).
- **Leader election** — outbox and contracts poll only while `isLeader()` is true; run one active
  poller per consumer name (e.g. a Postgres advisory lock or the platform's leader election);
  db-scheduler is cluster-safe on every node.
- **Failure behaviour table** — handler failure → app's retry handler; unreadable stored data →
  backoff 10s–1h; node crash → execution revived; database down during dispatch → batch halts and is
  retried.
- **Tuning** — `pollInterval`, `batchSize`, scheduler threads (which bound reaction concurrency).

## Keeping the code correct

- The quickstart exists as a real integration test,
  `src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt`, run against the Testcontainer
  Postgres, asserting that the confirmation reaction executes.
- Guide snippets outside the quickstart live in
  `src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt`, which must compile (serialization
  migrations, narrowed `execute`, `EventProducer`, `PublicEventContract`, multi-executor wiring).
- README code blocks are copied from those files; each file carries a comment saying to keep it in sync
  with `README.md`.
- No snippet-extraction tooling (e.g. kotlinx-knit) for now.

## Out of scope

- Publishing setup (group/version, Maven Central) — the README uses placeholder coordinates.
- Splitting guides into `docs/` pages, or generated API docs (Dokka).
- Changes to library code, other than fixes found while writing examples (each such fix is called out
  separately).

# First-class commands with typed rejections

**Date:** 2026-10-05
**Status:** Approved design, pending implementation plan
**Prerequisite for:** process managers (brainstormed but not yet specified; resumes after this ships)

## Goal

Make commands first-class, serializable data that an aggregate either **accepts** (new state and events) or
**rejects** with a **typed, domain-defined rejection**. Each aggregate gets one routing point that maps
every command to a pure function on a specific state type. `AggregateManager.handle(id, command)` becomes
the only way to change an aggregate, so HTTP handlers, event reactions and, later, process managers all
run a command the same way.

## Why

Today a command is a lambda passed to `execute`/`create`. The pure decision (`fun Held.release()`) is
defined once, but the *invocation* is not: the state precondition (`execute<Held>`), the command id and the
translation of `UnexpectedAggregateStateException` into "rejected" are rewritten by every caller. A process
manager needs to request a command *as data* (serialized now, run later), and a lambda cannot be data, so
this logic needs a single home first. Rejections are part of the domain language, so they are typed, not
strings.

## Decisions

- **Routing: an exhaustive `when` over the command type** (one line per command, naming the required state
  and the pure function). IDE click-through navigability is explicitly *not* a design criterion; typed domain
  language that AI coding tools can follow is.
- **Pure functions stay on specific state types** and return an `Outcome` (`accept(...)` or `reject(...)`)
  instead of a `Pair`.
- **Every rejection is a domain type** (`R`), including "wrong state", "doesn't exist" and "already exists";
  kotmod defines no rejection cases of its own.
- **`handle` replaces `create` and `execute`** (no deprecation cycle; released as 0.2.0).
- **Rejections are recorded** against the command id, so every command id has one permanent answer.
- **`handle` retries optimistic-concurrency conflicts itself** (re-read, re-decide), except inside an outer
  transaction.

## 1. API

```kotlin
@Serializable sealed interface PayoutCommand
@Serializable data class Hold(val amount: Long) : PayoutCommand
@Serializable data class Release(val reference: String) : PayoutCommand

@Serializable sealed interface PayoutRejection
@Serializable data class AmountOverLimit(val limit: Long) : PayoutRejection
@Serializable data object PayoutNotHeld : PayoutRejection
@Serializable data object PayoutAlreadyExists : PayoutRejection

typealias PayoutOutcome = Outcome<Payout, PayoutEvent, PayoutRejection>

fun hold(amount: Long, limit: Long): PayoutOutcome =
    if (amount > limit) reject(AmountOverLimit(limit)) else accept(Held(amount), HoldPlaced(amount))

fun Held.release(reference: String): PayoutOutcome =
    accept(Released(reference), FundsReleased(reference))

object PayoutCommands : CommandHandlers<Payout, PayoutCommand, PayoutEvent, PayoutRejection>(
    rejectionSerializer = PayoutRejection.serializer(),
) {
    override fun PayoutCommand.handler() = when (this) {
        is Hold    -> creates(otherwise = { PayoutAlreadyExists }) { hold(amount, limit = 10_000) }
        is Release -> on<Held>(otherwise = { PayoutNotHeld }) { it.release(reference) }
    }
}

val payouts = AggregateManager(AggregateType("Payout"), payoutRepository, backend, PayoutCommands)

when (val result = payouts.handle(id, Release(ref), commandId = CommandId(requestId))) {
    is CommandResult.Accepted -> result.state
    is CommandResult.Rejected -> result.rejection   // PayoutRejection
}
```

### Types

- **`Outcome<out S, out E : DomainEvent, out R>`**: what a pure command function returns. It is either
  `Outcome.Accept(state: S, events: List<E>)` or `Outcome.Reject(rejection: R)`.
  - Top-level builders `accept(state, vararg events)` (returning `Outcome<S, E, Nothing>`) and
    `reject(rejection)` (returning `Outcome<Nothing, Nothing, R>`). Covariance makes both assignable to an
    aggregate's outcome type without type arguments.
  - Accepting with no events is allowed.
- **`CommandHandler<S, E, R>`**: what a branch of the routing `when` evaluates to; an opaque description of
  "given the current `S?`, produce an `Outcome`". Built only through the `CommandHandlers` members.
- **`CommandHandlers<S : Any, C : Any, E : DomainEvent, R : Any>(rejectionSerializer: KSerializer<R>)`**:
  an abstract base, normally extended by an `object`, with:
  - `abstract fun C.handler(): CommandHandler<S, E, R>`: the routing point. Being abstract, it must be
    written; being an exhaustive `when` over a sealed command type, every command must be routed.
  - `inline fun <reified T : S> on(noinline otherwise: (S?) -> R, noinline block: (T) -> Outcome<S, E, R>)`:
    runs `block` when the current state is a `T`; otherwise rejects with `otherwise(state)` (`null` means the
    aggregate does not exist).
  - `fun creates(otherwise: (S) -> R, block: () -> Outcome<S, E, R>)`: runs `block` only when the aggregate
    does not exist; otherwise rejects with `otherwise(existingState)`.
  - `fun any(block: (S?) -> Outcome<S, E, R>)`: the escape hatch for a command valid in several states.
  - These are members (not top-level functions) because Kotlin cannot infer `S`, `E` and `R` when a caller
    supplies only `T` in `on<Held>`; binding them on the class makes `on<Held>` compile with one type
    argument. The class is also the natural home for the rejection serializer.
- **`CommandResult<out S, out R>`**: what `handle` returns: `CommandResult.Accepted(state: S)` or
  `CommandResult.Rejected(rejection: R)`.
- **`AggregateManager<S : Any, E : DomainEvent, C : Any, R : Any>(aggregateType, repository, backend,
  commands: CommandHandlers<S, C, E, R>, maxConflictRetries: Int = 5)`** with a single command method:

  ```kotlin
  suspend fun handle(
      id: AggregateId,
      command: C,
      commandId: CommandId? = null,
      correlationId: CorrelationId? = null,
  ): CommandResult<S, R>
  ```

  `create`, `execute` and the narrowed `execute<T>` are removed. As today, a missing `commandId` is
  replaced by a random one, which makes the call non-idempotent.
- Commands are `@Serializable` by convention only; `AggregateManager` needs no command serializer.
- **`EventProducer` is unchanged** in API: it records events without deciding anything.

## 2. Runtime behaviour of `handle`

1. **Read.** Look up the command id with `findHandledCommand`:
   - **Accepted earlier:** load the state and return `Accepted(current state)` (the current state, not the
     state at the time, as `execute` does today).
   - **Rejected earlier:** deserialize the stored rejection with the rejection serializer and return
     `Rejected(r)` without deciding again.
   - **Not recorded:** load the aggregate's meta and state; both are absent (`null` state) if the aggregate
     does not exist.
2. **Decide.** Call `command.handler()` and run the resulting `CommandHandler` against the state. No
   database work happens here. If user code throws, the exception propagates and nothing is written.
3. **Write**, in one transaction (`backend.inTransaction`):
   - **Accept:** `saveMeta` with `expectedVersion = null` when the state was `null` (create) or with the
     loaded version (advance); `repository.save`; `appendEvents` (sequence numbers as today);
     `recordCommandHandled`.
   - **Reject:** `recordCommandRejected` with the rejection's JVM class name (informational, for operators
     and error messages) and its JSON payload (encoded with the rejection serializer, which is all that is
     needed to read it back). No version change, no events, no state write.

**Conflict retries.** If the write loses a race — `OptimisticConcurrencyException`,
`AggregateAlreadyExistsException` from a concurrent create, or a new `CommandAlreadyRecordedException`
(which backends throw when the same command id is recorded twice) — `handle` starts again from step 1, which
re-reads (and so returns the recorded answer if the other writer used the same command id) and re-decides.
After `maxConflictRetries` retries it rethrows the last conflict exception (normally
`OptimisticConcurrencyException`). Re-running is safe because deciding is pure.

**Inside an outer `jdbc.transaction { }`** (detected through `backend.isInTransaction()`), `handle` never
retries: a conflict propagates and the outer transaction rolls back, as documented today. A rejection is a
value: the caller decides whether to continue or throw, and the rejection record commits or rolls back with
the rest of the transaction.

**Unreadable recorded rejection.** If a duplicate command id finds a stored rejection that can no longer be
deserialized (the rejection class was renamed or reshaped), `handle` throws a new
`RejectionDeserializationException` naming the aggregate, command id and stored type. Documented, with the
`@SerialName` workaround; rejections get no versioned migration support (unlike events).

## 3. Persistence changes

**`DomainPersistenceBackend`** (breaking for custom implementations):

- `wasCommandHandled(type, id, commandId): Boolean` is replaced by
  `findHandledCommand(type, id, commandId): HandledCommand?`, where
  `sealed interface HandledCommand { data object Accepted; data class Rejected(val type: String, val payload: String) }`.
- New `recordCommandRejected(type, id, commandId, rejectionType: String, payload: String)`.
- `recordCommandHandled` and `recordCommandRejected` throw `CommandAlreadyRecordedException` when the command
  id is already recorded for the aggregate (Postgres: unique violation on the primary key).
- `EventProducer` switches to `findHandledCommand` (treating any recorded answer as "already handled");
  its behaviour does not change.

**Schema** (`DddSchema.ddl` for new installations):

```sql
CREATE TABLE ddd_command_history (
    aggregate_type    VARCHAR(72)  NOT NULL,
    aggregate_id      VARCHAR(72)  NOT NULL,
    command_id        VARCHAR(72)  NOT NULL,
    rejection_type    VARCHAR(255),
    rejection_payload TEXT,
    PRIMARY KEY (aggregate_type, aggregate_id, command_id)
);
```

A null `rejection_type` means accepted, so existing rows need no backfill. A rejection for a command
against a non-existent aggregate is recorded under that aggregate id even though no `ddd_aggregate_root`
row exists (there is no foreign key).

**Exceptions:** `UnexpectedAggregateStateException` is deleted (nothing throws it any more).
`AggregateAlreadyExistsException` is kept: `saveMeta` throws it on a concurrent create, and `handle` treats
it as a conflict. `AggregateNotFoundException` is kept for one inconsistency: the aggregate's bookkeeping
exists (or its command was accepted) but the repository has no state for it. `DddException` gains an
optional `cause`.

## Testing

- **Unit tests** (`StubPersistenceBackend`, rewriting `AggregateManagerCreateTest`, `...ExecuteTest`,
  `...DedupTest`):
  - routing through `on`, `creates` and `any`, including each `otherwise` path: `null` state, wrong state,
    existing state;
  - accept (state saved, events appended with sequences, command recorded) and reject (rejection recorded,
    nothing else written);
  - duplicates return the recorded answer: `Accepted(current state)` and the same `Rejected(r)`;
  - conflict retry: succeeds after a conflict; throws `OptimisticConcurrencyException` after
    `maxConflictRetries`;
  - no retry inside an outer transaction;
  - an exception from user code writes nothing;
  - an unreadable recorded rejection throws `RejectionDeserializationException`.
- **Postgres integration tests:** rejection persisted and read back; concurrent `handle` calls with the same
  command id get one answer; a pre-existing row with null rejection columns reads as accepted; concurrent
  creates of one aggregate (one accepted, the other rejected via `creates`' `otherwise`); `EventProducer`
  unchanged; `handle` inside `jdbc.transaction { }` with another aggregate.
- **Examples module:** the README code compiles and its tests pass against the new API.

## Documentation

- **Quickstart:** step 2 defines commands, rejections, pure functions returning outcomes and an
  `OrderCommands` object; step 4 uses `orders.handle(...)` with a `when` on the result.
- **"Aggregates and commands"** rewritten around `handle`, `on`/`creates`/`any`, typed rejections, recorded
  rejections and idempotency, and automatic conflict retries. The `retryOnConflict` helper and the
  `UnexpectedAggregateStateException` try/catch are removed.
- **"Several aggregates in one transaction"** uses `handle`, states that there are no automatic retries
  inside an outer transaction, and explains what a rejection means there.
- A short **"Why commands are data"** paragraph: one routing point, so every caller (including future
  process managers) behaves identically.
- **Known limitations:** reading back recorded rejections after renaming a rejection class.
- **Core concepts:** add **Rejection**; update **Command**.
- **"Upgrading from 0.1.0"** section with the two `ALTER TABLE` statements and the list of breaking changes.

## Migration

Released as **0.2.0** (`gradle.properties`). Breaking changes: `create`/`execute` removed; `AggregateManager`
gains the `C` and `R` type parameters and a `commands` constructor parameter; `DomainPersistenceBackend`
changes as above; `UnexpectedAggregateStateException` removed. Existing databases run:

```sql
ALTER TABLE ddd_command_history ADD COLUMN rejection_type    VARCHAR(255);
ALTER TABLE ddd_command_history ADD COLUMN rejection_payload TEXT;
```

## Out of scope

- Process managers, including routing requested commands to registered aggregates (a command registry).
- Versioned schema migration for rejections.
- Changes to `EventProducer`'s API.

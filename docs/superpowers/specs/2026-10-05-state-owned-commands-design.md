# State-owned commands

**Date:** 2026-10-05
**Status:** Approved design, pending implementation plan
**Revises:** `2026-10-05-first-class-commands-design.md` (merged to `main` as 180a12f; 0.2.0 not yet published)

## Goal

Make each state of an aggregate own its command handling. A state is a behavioural position of the aggregate,
so the state itself says what it does with every command: accept it (new state and events) or reject it with a
typed domain rejection. A separate initial-state object owns the commands that apply before the aggregate
exists. This replaces the central routing object (`CommandHandlers` with `on`/`creates`/`any`) introduced by the
first-class-commands work, before 0.2.0 is published.

## Why

In the first-class-commands design, behaviour lives in one `when` per aggregate that routes commands to
functions, with `otherwise` lambdas deciding rejections for "wrong state". Reading it in the documentation, the
behaviour of a state is scattered: what `ShippedOrder` does with `CancelOrder` is decided by a router's
`otherwise`, not by `ShippedOrder`. The State pattern puts each decision, including each rejection, in the
state where its reason lives.

## Decisions

- **States implement a kotmod interface** `AggregateState` with `suspend fun handle(command: C): Outcome<S, E, R>`.
  This is the one way to write commands; there is no function-based alternative.
- **A separate `InitialState` object** (outside the sealed state hierarchy) handles commands when no aggregate is
  stored. Repositories never store or load it; accepting from it always creates the aggregate.
- **Everything else from first-class commands stays:** commands as data, `Outcome`/`accept`/`reject`,
  `CommandResult`, `handle(id, command, commandId?, correlationId?)`, recorded typed rejections, automatic
  conflict retries (not inside outer transactions), suspendable decisions.
- **Removed:** `CommandHandlers`, `CommandHandler`, `on`, `creates`, `any`, and the public
  `CommandHandlers.decide` (states are tested by calling `handle` directly).
- **Version stays 0.2.0**: nothing was published with `CommandHandlers`, so there is nothing to deprecate.

## 1. API

```kotlin
// kotmod (Commands.kt)
interface AggregateState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> {
    /** Decides [command] in this state: accept it with a new state and events, or reject it. */
    suspend fun handle(command: C): Outcome<S, E, R>
}

interface InitialState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> {
    /** Decides [command] for an aggregate that does not exist yet; accepting creates it. */
    suspend fun handle(command: C): Outcome<S, E, R>
}

class AggregateManager<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any>(
    aggregateType: AggregateType,
    repository: Repository<S>,
    backend: DomainPersistenceBackend<E>,
    initial: InitialState<S, C, E, R>,
    rejectionSerializer: KSerializer<R>,
    maxConflictRetries: Int = 5,
)
```

The type parameters of `AggregateManager` are reordered to `S, C, E, R`, matching the interfaces (they were
`S, E, C, R`; unpublished, so this is free).

```kotlin
// the app's domain
typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

sealed interface Order : AggregateState<Order, OrderCommand, OrderEvent, OrderRejection>

object NoOrder : InitialState<Order, OrderCommand, OrderEvent, OrderRejection> {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> accept(PendingOrder(command.item), OrderPlaced(command.item))
            ShipOrder, is CancelOrder -> reject(OrderNotFound)
        }
}

data class PendingOrder(val item: String) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder -> accept(ShippedOrder(item), OrderShipped(item))
            is CancelOrder -> cancel(command.reason)
        }

    private fun cancel(reason: String): OrderOutcome =
        if (reason.isBlank()) reject(CancellationReasonMissing)
        else accept(CancelledOrder(item, reason), OrderCancelled(item, reason))
}

data class ShippedOrder(val item: String) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyShipped)
        }
}

data class CancelledOrder(val item: String, val reason: String) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyCancelled)
        }
}

val orders =
    AggregateManager(
        aggregateType = orderType,
        repository = OrderRepository(jdbc),
        backend = PostgresDomainPersistenceBackend(jdbc, serialization),
        initial = NoOrder,
        rejectionSerializer = OrderRejection.serializer(),
    )
```

**Guidance (documented, not enforced):** write each state's `when` over the sealed command type without an
`else`, so adding a command fails to compile until every state, and the initial state, has decided what to do
with it. Larger decisions can move into private functions of the state (as `cancel` above); shared helpers are
fine, but each accept/reject decision stays in the state.

## 2. Runtime

`AggregateManager.handle` is unchanged except for the decide phase, which becomes
`(state ?: initial).handle(command)`. Everything else stays as merged: recorded answers for repeated command ids
(`Accepted(current state)` / the same `Rejected(r)`); create when no aggregate meta exists, otherwise advance the
version; rejection recording (JVM class name truncated to 255 characters, JSON payload from
`rejectionSerializer`); conflict retries only around kotmod's own database phases, up to `maxConflictRetries`,
never inside an outer transaction; exceptions thrown from a decision propagate unchanged.

## 3. Code changes

- **`Commands.kt`:** keeps `Outcome`, `accept`, `reject`, `CommandResult`; adds `AggregateState`, `InitialState`;
  removes `CommandHandler`, `CommandHandlers` (and with them `on`, `creates`, `any`, `decide`).
- **`AggregateManager.kt`:** new type parameters and constructor (`initial`, `rejectionSerializer`), decide phase as
  above; KDoc updated.
- **Test fixtures (`TestAggregate.kt`):** `Order` implements `AggregateState`; each state implements `handle`; new
  `NoOrder` initial state. The test-only `DecideWith(block: suspend (Order?) -> Outcome)` command is handled by
  every state as `is DecideWith -> command.block(this)` and by `NoOrder` as `command.block(null)`. Rejections stay
  `OrderAlreadyExists`, `OrderNotFound`, `OrderNotPending(actual)`.
- **Tests:** `CommandHandlersTest` is replaced by `AggregateStateTest` (states and the initial state decide through
  plain `handle` calls; a suspending decision works). `AggregateManagerHandleTest`, `AggregateManagerIntegrationTest`,
  `JdbcContextContract` and the SQLDelight tests change only how the manager is constructed (and any use of
  `OrderCommands`).
- **Examples (`Quickstart.kt`, `QuickstartTest.kt`, `ReadmeExamples.kt`):** the order domain above replaces
  `OrderCommands` and `notPending`; constructions pass `initial = NoOrder` and the rejection serializer.

## Documentation

- **Quickstart step 2:** commands and rejections as before; states own their commands; `NoOrder` owns creation;
  remove the `OrderCommands` paragraph and code.
- **Quickstart step 3:** the `AggregateManager(...)` snippet passes `initial` and `rejectionSerializer`.
- **"Aggregates and commands":** states own behaviour; the initial state; no `else` in `when`; testing by calling
  `handle` on a state (replaces the `OrderCommands.decide` sentence); "Why commands are data" stays.
- **"Upgrading from 0.1.0":** step 2 describes implementing `AggregateState` on the state types and an
  `InitialState` object, instead of a `CommandHandlers` object; step 3 passes `initial` and `rejectionSerializer`.
- **Core concepts:** the Command bullet mentions that each state decides the commands it receives.
- Any other README mention of `CommandHandlers`, `on<`, `creates`, `any {` or `decide(` is removed or rewritten.

## Testing

- `AggregateStateTest`: a state accepts (new state and events), rejects, and the initial state accepts creation and
  rejects other commands; a suspending `handle` works.
- `AggregateManagerHandleTest`: all existing behaviours pass with the new construction, plus: a command on a missing
  aggregate is decided by the initial state; accepting from the initial state creates the aggregate (meta at
  version 1).
- Postgres integration, JDBC contract and SQLDelight suites pass unchanged in behaviour.
- `examples` compile and pass; README snippets are byte-identical to the example files.

## Out of scope

- Process managers (paused; their requested commands will be run through `handle` on the target aggregate).
- Any change to rejection storage, retries or `EventProducer`.

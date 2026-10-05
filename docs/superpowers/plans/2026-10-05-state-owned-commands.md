# State-Owned Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the central `CommandHandlers` router with states that own their commands: each state implements `AggregateState.handle(command)`, and a separate `InitialState` object decides commands for an aggregate that doesn't exist yet.

**Architecture:** `Commands.kt` drops `CommandHandler`/`CommandHandlers` and gains two interfaces, `AggregateState` and `InitialState`, each with `suspend fun handle(command: C): Outcome<S, E, R>`. `AggregateManager` takes `initial` and `rejectionSerializer` instead of `commands`, and its decide phase calls `state.handle(command)`, or `initial.handle(command)` when nothing is stored. The rest of `handle` (recorded answers, retries, rejection storage) is unchanged. Test fixtures, tests, examples and the README move to the new shape.

**Tech Stack:** Kotlin (JVM toolchain 25), kotlinx.serialization 1.11.0, kotlinx.coroutines 1.11.0, Postgres 17 via Testcontainers, JUnit 5 / kotlin.test, Gradle.

**Spec:** `docs/superpowers/specs/2026-10-05-state-owned-commands-design.md`

## Global Constraints

- `interface AggregateState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> { suspend fun handle(command: C): Outcome<S, E, R> }`
- `interface InitialState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> { suspend fun handle(command: C): Outcome<S, E, R> }`
- `AggregateManager<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any>(aggregateType, repository, backend, initial: InitialState<S, C, E, R>, rejectionSerializer: KSerializer<R>, maxConflictRetries: Int = 5)`. The type parameters are reordered to `S, C, E, R`.
- `CommandHandlers`, `CommandHandler`, `on`, `creates`, `any` and `decide` are removed; nothing else in the public API changes.
- The `handle` runtime is unchanged except the decide phase: recorded answers, create-vs-advance from meta, rejection recording (class name truncated to 255), conflict retries only around kotmod's own database phases, no retries inside an outer transaction, and decision exceptions propagating unchanged.
- Documentation guidance: write each state's `when` over the sealed command type without an `else`.
- Version stays `0.2.0`.
- README Kotlin snippets must be byte-identical to `examples/src/integrationTest/kotlin/io/kotmod/readme/{Quickstart.kt,QuickstartTest.kt,ReadmeExamples.kt}`, apart from indentation inside test functions.
- Commit messages follow the repo style (imperative sentence, no prefix) and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Between Task 1 and Task 2 the `examples` module does not compile (it still uses `OrderCommands`). Task 2 restores it. Every other module is green after Task 1.

## Review Focus

1. **A command that arrives for a missing aggregate.** Expected: the initial state decides it; accepting creates the aggregate at version 1, and rejecting records the rejection without creating anything. Pinned in Task 1 (`AggregateManagerHandleTest`).
2. **Bookkeeping exists but the repository returns no state.** Expected: `AggregateNotFoundException`, as now, never a silent fallback to the initial state (that would re-create a live aggregate). Pinned in Task 1 (an existing test, kept).
3. **A state whose `handle` throws.** Expected: the exception propagates, nothing is written, and it is not retried. Pinned in Task 1 (existing tests, kept with the new fixtures).
4. **Generic inference at user call sites** (the `S : AggregateState<S, …>` self-bound). Expected: `sealed interface Order : AggregateState<Order, …>` compiles, and `accept(PendingOrder(...), OrderPlaced(...))` and `reject(OrderNotFound)` infer against `OrderOutcome` with no explicit type arguments. Pinned in Task 2 (the examples module compiles).
5. **README drift.** Expected: no remaining mention of `CommandHandlers`, `OrderCommands`, `on<T>`, `creates(`, `any { }`, `otherwise` (as an API name) or `.decide(`. Pinned in Task 2 (grep step).

---

## File Structure

- **Modify** `kotmod/src/main/kotlin/io/kotmod/Commands.kt`: remove the router, add the two interfaces.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`: new signature and decide phase.
- **Modify** `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`: states own commands, add `NoOrder`.
- **Delete** `kotmod/src/test/kotlin/io/kotmod/CommandHandlersTest.kt`; **create** `kotmod/src/test/kotlin/io/kotmod/AggregateStateTest.kt`.
- **Modify** `kotmod/src/test/kotlin/io/kotmod/AggregateManagerHandleTest.kt`, `kotmod/src/testFixtures/kotlin/io/kotmod/jdbc/JdbcContextContract.kt`, `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/AggregateManagerIntegrationTest.kt`, `kotmod-sqldelight/src/integrationTest/kotlin/io/kotmod/sqldelight/SqlDelightJdbcContextIntegrationTest.kt`: construction only.
- **Modify** `examples/src/integrationTest/kotlin/io/kotmod/readme/{Quickstart.kt,QuickstartTest.kt,ReadmeExamples.kt}` and `README.md`.

---

### Task 1: States own their commands (library, fixtures and tests)

**Files:**
- Modify: `kotmod/src/main/kotlin/io/kotmod/Commands.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`
- Modify: `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`
- Delete: `kotmod/src/test/kotlin/io/kotmod/CommandHandlersTest.kt`
- Create: `kotmod/src/test/kotlin/io/kotmod/AggregateStateTest.kt`
- Modify: `kotmod/src/test/kotlin/io/kotmod/AggregateManagerHandleTest.kt` (imports, `orders` type, `manager()`, two test names)
- Modify: `kotmod/src/testFixtures/kotlin/io/kotmod/jdbc/JdbcContextContract.kt` (`manager()` around line 160, import)
- Modify: `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/AggregateManagerIntegrationTest.kt` (`orders` type at line 74, construction at line 84, import)
- Modify: `kotmod-sqldelight/src/integrationTest/kotlin/io/kotmod/sqldelight/SqlDelightJdbcContextIntegrationTest.kt` (`statelessOrders`, around line 34)

**Interfaces:**
- Consumes: the existing `Outcome`, `accept`, `reject` and `CommandResult` in `Commands.kt`; the existing `AggregateManager.handle` implementation.
- Produces:
  - `io.kotmod.AggregateState<S, C, E, R>` and `io.kotmod.InitialState<S, C, E, R>`, as in Global Constraints
  - `AggregateManager(aggregateType, repository, backend, initial, rejectionSerializer, maxConflictRetries = 5)`
  - Fixtures in `io.kotmod.support`: `Order : AggregateState<Order, OrderCommand, OrderEvent, OrderRejection>`, `PendingOrder`, `ShippedOrder`, `CancelledOrder`, `object NoOrder : InitialState<…>`. Unchanged: `OrderCommand` (`PlaceOrder`, `ShipOrder`, `CancelOrder`, `DecideWith`), `OrderRejection` (`OrderAlreadyExists`, `OrderNotFound`, `OrderNotPending(actual)`), `OrderOutcome`, `placeOrder`, `ship`, `cancel`, and the events.

- [ ] **Step 1: Write the failing state tests**

Delete `kotmod/src/test/kotlin/io/kotmod/CommandHandlersTest.kt`. Create `kotmod/src/test/kotlin/io/kotmod/AggregateStateTest.kt`:

```kotlin
package io.kotmod

import io.kotmod.support.CancelOrder
import io.kotmod.support.DecideWith
import io.kotmod.support.NoOrder
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.OrderNotPending
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ShipOrder
import io.kotmod.support.ShippedOrder
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class AggregateStateTest {
    @Test
    fun `a state accepts a command it allows with a new state and events`() =
        runTest {
            assertEquals(Outcome.Accept(ShippedOrder("book"), listOf(OrderShipped("book"))), PendingOrder("book").handle(ShipOrder))
        }

    @Test
    fun `a state rejects a command it does not allow`() =
        runTest {
            assertEquals(Outcome.Reject(OrderNotPending("ShippedOrder")), ShippedOrder("book").handle(CancelOrder("too late")))
        }

    @Test
    fun `the initial state accepts creating the aggregate`() =
        runTest {
            assertEquals(Outcome.Accept(PendingOrder("book"), listOf(OrderPlaced("book"))), NoOrder.handle(PlaceOrder("book")))
        }

    @Test
    fun `the initial state rejects commands for an aggregate that does not exist`() =
        runTest {
            assertEquals(Outcome.Reject(OrderNotFound), NoOrder.handle(ShipOrder))
        }

    @Test
    fun `a state may suspend while deciding`() =
        runTest {
            val outcome =
                PendingOrder("book").handle(
                    DecideWith { state ->
                        delay(1)
                        accept(state!!)
                    },
                )

            assertEquals(Outcome.Accept(PendingOrder("book"), emptyList<OrderEvent>()), outcome)
        }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.AggregateStateTest'`
Expected: compilation FAILS: unresolved `NoOrder`, and `handle` is not a member of `PendingOrder`.

- [ ] **Step 3: Replace the router with the two interfaces**

In `kotmod/src/main/kotlin/io/kotmod/Commands.kt`:
- Delete the `CommandHandler` class and the `CommandHandlers` class, with their KDoc.
- Delete the now-unused `import kotlinx.serialization.KSerializer`.
- In the KDoc of `Outcome`, change "What a command function decides" to "What a state decides about a command".
- Add these after the `reject` function:

```kotlin
/**
 * A behavioural position of an aggregate that decides the commands it receives. Implement it on your sealed state
 * type, and decide every command in [handle]: accept it with [accept], or reject it with [reject].
 *
 * Write the `when` over your sealed command type without an `else`, so adding a command doesn't compile until every
 * state has decided what to do with it. Decisions should be pure; they may suspend, but [AggregateManager.handle]
 * runs a decision again on each conflict retry, so anything it calls out to may be called more than once.
 *
 * @param S the aggregate's state type (your sealed state type itself).
 * @param C the aggregate's command type.
 * @param E the aggregate's domain event type.
 * @param R the aggregate's rejection type.
 */
interface AggregateState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> {
    /** Decides [command] in this state: accept it with a new state and events, or reject it. */
    suspend fun handle(command: C): Outcome<S, E, R>
}

/**
 * Decides commands for an aggregate that doesn't exist yet. Accepting a command here creates the aggregate.
 * It is not one of the aggregate's stored states, so your [Repository] never saves or loads it.
 */
interface InitialState<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any> {
    /** Decides [command] for an aggregate that doesn't exist yet: accepting it creates the aggregate. */
    suspend fun handle(command: C): Outcome<S, E, R>
}
```

- [ ] **Step 4: Make AggregateManager use the states**

In `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`:

Replace the class header and constructor with:

```kotlin
class AggregateManager<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any>(
    private val aggregateType: AggregateType,
    private val repository: Repository<S>,
    private val backend: DomainPersistenceBackend<E>,
    private val initial: InitialState<S, C, E, R>,
    private val rejectionSerializer: KSerializer<R>,
    private val maxConflictRetries: Int = 5,
) {
```

and add `import kotlinx.serialization.KSerializer`.

In the class KDoc, replace phase 2 with:

```
 * 2. **Decide:** the aggregate's current state decides the command with [AggregateState.handle], or [initial] does
 *    when the aggregate doesn't exist yet. It accepts the command (new state and events) or rejects it with one of
 *    the aggregate's rejection types. No database work happens in this phase.
```

and reorder the `@param` lines to `S`, `C`, `E`, `R`. Add:

```
 * @param initial decides commands for an aggregate that doesn't exist yet.
 * @param rejectionSerializer serializes rejections, which are recorded so a repeated command id gets the same answer.
```

In `handleOnce`, replace the decide phase's `commands.decide(command, undecided.state)` with the following, keeping the surrounding `try`/`catch (e: DddException) { throw DecisionFailed(e) }` exactly as it is:

```kotlin
                val state = undecided.state
                if (state != null) state.handle(command) else initial.handle(command)
```

Replace both uses of `commands.rejectionSerializer` (encode in the write phase, decode in `decodeRejection`) with `rejectionSerializer`.

- [ ] **Step 5: Make the fixture states own their commands**

In `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`:
- Replace the imports with:
  ```kotlin
  import io.kotmod.AggregateState
  import io.kotmod.DomainEvent
  import io.kotmod.InitialState
  import io.kotmod.Outcome
  import io.kotmod.accept
  import io.kotmod.reject
  import kotlinx.serialization.Serializable
  ```
- Delete the `object OrderCommands` and its `notPending` function.
- Replace the four state declarations at the top of the file (`sealed interface Order`, `PendingOrder`, `ShippedOrder`, `CancelledOrder`) with the block below.
- Keep `OrderCommand`, `DecideWith`, the rejections, `OrderOutcome`, `placeOrder`, `ship`, `cancel` and the events unchanged.

```kotlin
sealed interface Order : AggregateState<Order, OrderCommand, OrderEvent, OrderRejection>

data class PendingOrder(
    val name: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyExists)
            ShipOrder -> ship()
            is CancelOrder -> cancel(command.reason)
            is DecideWith -> command.block(this)
        }
}

data class ShippedOrder(
    val name: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyExists)
            ShipOrder, is CancelOrder -> reject(OrderNotPending("ShippedOrder"))
            is DecideWith -> command.block(this)
        }
}

data class CancelledOrder(
    val name: String,
    val reason: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyExists)
            ShipOrder, is CancelOrder -> reject(OrderNotPending("CancelledOrder"))
            is DecideWith -> command.block(this)
        }
}

/** The order before it exists: only placing it is accepted. */
object NoOrder : InitialState<Order, OrderCommand, OrderEvent, OrderRejection> {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> placeOrder(command.name)
            ShipOrder, is CancelOrder -> reject(OrderNotFound)
            is DecideWith -> command.block(null)
        }
}
```

These answers match what the old `OrderCommands` routing produced:
- `PlaceOrder` on an existing order: `OrderAlreadyExists`
- `ShipOrder` or `CancelOrder` on a missing order: `OrderNotFound`
- on a non-pending order: `OrderNotPending(<simple class name>)`
- `DecideWith`: the current state, or `null`

So existing test expectations hold.

- [ ] **Step 6: Update the manager constructions**

`AggregateManagerHandleTest.kt`:
- Replace the import `io.kotmod.support.OrderCommands` with `io.kotmod.support.NoOrder`.
- Change the `orders` property type to `AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>`.
- Replace `manager()` with:

```kotlin
    private fun manager(maxConflictRetries: Int = 5) =
        AggregateManager(type, repo, backend, NoOrder, OrderRejection.serializer(), maxConflictRetries = maxConflictRetries)
```

- Rename `a command on a missing aggregate is rejected through otherwise, and recorded` to `a command on a missing aggregate is rejected by the initial state, and recorded`.
- Rename `creating an existing aggregate is rejected through otherwise` to `creating an existing aggregate is rejected by its current state`.
- Rename `an accepted create saves state, meta at version 1, events and the command` to `accepting from the initial state creates the aggregate: state, meta at version 1, events and the command`.

`JdbcContextContract.kt`:
- Replace the import `io.kotmod.support.OrderCommands` with `io.kotmod.support.NoOrder` and `io.kotmod.support.OrderRejection`.
- In `manager(...)`, replace `commands = OrderCommands,` with:

```kotlin
        initial = NoOrder,
        rejectionSerializer = OrderRejection.serializer(),
```

`AggregateManagerIntegrationTest.kt`:
- Replace the import `io.kotmod.support.OrderCommands` with `io.kotmod.support.NoOrder`.
- Change line 74's type to `AggregateManager<Order, io.kotmod.support.OrderCommand, io.kotmod.support.OrderEvent, io.kotmod.support.OrderRejection>`.
- Change the construction to:

```kotlin
        orders =
            AggregateManager(
                AggregateType("Order"),
                OrderTable(jdbc),
                PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
                NoOrder,
                io.kotmod.support.OrderRejection.serializer(),
            )
```

`SqlDelightJdbcContextIntegrationTest.kt`: in `statelessOrders`, replace `commands = io.kotmod.support.OrderCommands,` with:

```kotlin
            initial = io.kotmod.support.NoOrder,
            rejectionSerializer = io.kotmod.support.OrderRejection.serializer(),
```

- [ ] **Step 7: Run the tests**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.AggregateStateTest'`
Expected: PASS (5 tests).

Run: `./gradlew :kotmod:test :kotmod:integrationTest :kotmod-sqldelight:integrationTest :kotmod-db-scheduler:test :kotmod-db-scheduler:integrationTest`
Expected: PASS. The `kotmod` unit tests include `AggregateManagerHandleTest` (all existing tests, renamed as above) and `AggregateStateTest`. Compiling the touched files must produce no `w:` lines.

Run: `grep -rn -e 'CommandHandlers' -e 'OrderCommands' -e 'handlerFor' --include='*.kt' kotmod kotmod-sqldelight kotmod-db-scheduler`
Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add -A kotmod kotmod-sqldelight
git commit -m "Let each aggregate state decide its own commands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Examples and README

**Files:**
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt`
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt` (the `AggregateManager(...)` call around line 67)
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt` (the `AggregateManager<...>` parameter types at lines 48, 65 and 66)
- Modify: `README.md` (Quickstart steps 2 and 3, Core concepts, "Aggregates and commands", "Upgrading from 0.1.0")

**Interfaces:**
- Consumes: `AggregateState`, `InitialState`, and the `AggregateManager(aggregateType, repository, backend, initial, rejectionSerializer)` constructor from Task 1.
- Produces: nothing used later.

- [ ] **Step 1: Rewrite the example order domain**

In `Quickstart.kt`:
- Replace the imports `io.kotmod.CommandHandlers` with `io.kotmod.AggregateState` and `io.kotmod.InitialState`, keeping the file's import order style.
- Delete the plain state declarations at the top: `sealed interface Order`, `PendingOrder`, `ShippedOrder`, `CancelledOrder`.
- Delete the functions `placeOrder`, `PendingOrder.ship`, `PendingOrder.cancel`, and the `object OrderCommands`.
- Keep the events, commands and rejections where they are.
- Put the block below immediately after the last rejection, `CancellationReasonMissing`, replacing the old `typealias OrderOutcome` line:

```kotlin
typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

sealed interface Order : AggregateState<Order, OrderCommand, OrderEvent, OrderRejection>

object NoOrder : InitialState<Order, OrderCommand, OrderEvent, OrderRejection> {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> accept(PendingOrder(command.item), OrderPlaced(command.item))
            ShipOrder, is CancelOrder -> reject(OrderNotFound)
        }
}

data class PendingOrder(
    val item: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder -> accept(ShippedOrder(item), OrderShipped(item))
            is CancelOrder -> cancel(command.reason)
        }

    private fun cancel(reason: String): OrderOutcome =
        if (reason.isBlank()) {
            reject(CancellationReasonMissing)
        } else {
            accept(CancelledOrder(item, reason), OrderCancelled(item, reason))
        }
}

data class ShippedOrder(
    val item: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyShipped)
        }
}

data class CancelledOrder(
    val item: String,
    val reason: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyCancelled)
        }
}
```

`OrderRepository` in the same file is unchanged: it builds `PendingOrder(item)`, `ShippedOrder(item)` and `CancelledOrder(item, reason)`, which still exist.

In `QuickstartTest.kt`, replace `commands = OrderCommands,` in the `AggregateManager(...)` call with:

```kotlin
                    initial = NoOrder,
                    rejectionSerializer = OrderRejection.serializer(),
```

In `ReadmeExamples.kt`, change each `AggregateManager<Order, OrderEvent, OrderCommand, OrderRejection>` to `AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>`.

Run: `./gradlew :examples:integrationTest --rerun-tasks`
Expected: PASS (4 tests). This confirms that the examples compile and that user-side type inference works (Review Focus 4).

- [ ] **Step 2: Rewrite Quickstart step 2 in the README**

Keep the heading `### 2. Define state, events, commands and rejections`. Replace everything from the heading down to (not including) `### 3. Wire up persistence` with, in this order:

1. The paragraph:
   > An aggregate's **state** is whatever your application needs to make decisions. Here, an order is pending,
   > shipped or cancelled. Its **events** record what happened. They are stored as JSON, so they are
   > `@Serializable`:
2. The events code block, unchanged from the current README.
3. The paragraph that starts "A **command** asks the order to change.", unchanged. Then the code block with the commands and rejections, copied from `Quickstart.kt` (`OrderCommand` through `CancellationReasonMissing`).
4. The paragraph:
   > Each state owns its behaviour. It implements `handle` and decides every command it might receive:
   > `accept(newState, events…)` or `reject(rejection)`. `NoOrder` is the order before it exists: placing it is
   > accepted there, and everything else is rejected. Each `when` lists every command and has no `else`, so adding
   > a command doesn't compile until every state has decided what to do with it. Decisions are plain code that
   > you can unit-test without a database.
5. A code block with the block from Step 1 (from `typealias OrderOutcome` through `CancelledOrder`), copied verbatim from `Quickstart.kt`.

- [ ] **Step 3: Update Quickstart step 3, Core concepts, the guide and the upgrade section**

**Step 3 snippet:** replace `commands = OrderCommands,` with the two lines from `QuickstartTest.kt`:

```kotlin
        initial = NoOrder,
        rejectionSerializer = OrderRejection.serializer(),
```

**Core concepts:** replace the **Command** bullet with:

> - **Command** — a request to change an aggregate, as serializable data. The aggregate's current state
>   decides it: accept it (new state and events) or reject it. It is idempotent when given a `CommandId`.

**"Aggregates and commands":**
- In phase 2 of the numbered list, change "your pure function runs" to "the aggregate's current state (or the initial state, if the aggregate doesn't exist yet) decides the command".
- Replace the **Routing.** paragraph and the following "Because decisions are pure…" paragraph with:

> **States own their commands.** Each state implements `AggregateState` and decides every command in
> `handle`, returning `accept(newState, events…)` or `reject(rejection)`. An `InitialState` object (`NoOrder`)
> decides commands for an aggregate that doesn't exist yet, and accepting there creates it. Write each `when`
> without an `else`: then adding a command doesn't compile until every state, and the initial state, has
> decided what to do with it.
>
> Decisions are plain code, so you can unit-test them without a database:
> `PendingOrder("book").handle(ShipOrder)` and `NoOrder.handle(PlaceOrder("book"))` return the `Outcome`.

**"Upgrading from 0.1.0":** replace steps 2, 3 and 4 with:

> 2. For each aggregate, define a sealed command type and a sealed rejection type. Make your state type
>    implement `AggregateState` and decide each command in `handle`, returning `accept(...)` or `reject(...)`.
>    Then add an `InitialState` object for commands on an aggregate that doesn't exist yet, as in
>    [the quickstart](#2-define-state-events-commands-and-rejections).
> 3. Pass that object as `initial`, and your rejection type's serializer as `rejectionSerializer`, to
>    `AggregateManager`, and replace `create { }` and `execute<T> { }` calls with `handle(id, command)`.
> 4. Replace `catch (e: UnexpectedAggregateStateException)` with a rejection decided by the state, and drop any
>    retry loop around `OptimisticConcurrencyException`: `handle` retries itself.

Keep steps 1 and 5 as they are.

- [ ] **Step 4: Check for leftovers and drift**

Run: `grep -n -e 'CommandHandlers' -e 'OrderCommands' -e '`on<' -e 'on<PendingOrder>' -e 'creates(' -e '`any' -e '\.decide(' -e '`otherwise' -e 'otherwise =' README.md`
Expected: no output. If something matches, rewrite it in the new terms.

Compare every README Kotlin snippet touched in Steps 2-3 with its source in `Quickstart.kt`, `QuickstartTest.kt` and `ReadmeExamples.kt`. They must be identical, apart from indentation inside the test function.

Run: `./gradlew :examples:integrationTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add README.md examples
git commit -m "Document states that own their commands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

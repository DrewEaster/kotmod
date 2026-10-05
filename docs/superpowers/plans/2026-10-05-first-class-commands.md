# First-Class Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `AggregateManager.create`/`execute` (commands as lambdas) with `handle(id, command)`, where commands are data routed by an exhaustive `when` to pure functions that accept or reject with typed, recorded domain rejections.

**Architecture:** A new `Commands.kt` in the core module holds the command model: `Outcome`, `accept`/`reject`, `CommandHandler`, the abstract `CommandHandlers` routing base, and `CommandResult`. The command-history storage learns to record rejections: `DomainPersistenceBackend.findHandledCommand`/`recordCommandRejected` and two new nullable columns. `AggregateManager` is rewritten around `handle`, with read → decide → write phases and automatic conflict retries outside outer transactions. Every caller (tests, SQLDelight module tests, examples, README) moves to the new API, released as 0.2.0.

**Tech Stack:** Kotlin (JVM toolchain 25), kotlinx.serialization 1.11.0, kotlinx.coroutines 1.11.0, Postgres 17 via Testcontainers, JUnit 5 / kotlin.test, Gradle.

**Spec:** `docs/superpowers/specs/2026-10-05-first-class-commands-design.md`

## Global Constraints

- Every rejection is a domain type `R`; kotmod defines no rejection cases of its own.
- `handle` is the only method that changes an aggregate; `create`, `execute` and the narrowed `execute<T>` are removed (no deprecation cycle).
- `CommandHandlers` members: `on<T>(otherwise: (S?) -> R, block: (T) -> Outcome<S, E, R>)`, `creates(otherwise: (S) -> R, block: () -> Outcome<S, E, R>)`, `any(block: (S?) -> Outcome<S, E, R>)`; abstract `fun C.handler(): CommandHandler<S, E, R>`; constructor parameter `rejectionSerializer: KSerializer<R>`.
- `AggregateManager<S : Any, E : DomainEvent, C : Any, R : Any>(aggregateType, repository, backend, commands, maxConflictRetries: Int = 5)`.
- Command functions are pure and **not** `suspend` (a change from `create`/`execute`, whose blocks were `suspend`).
- Rejections are recorded in `ddd_command_history.rejection_type VARCHAR(255)` (JVM class name, informational) and `rejection_payload TEXT` (JSON from the rejection serializer); a null `rejection_type` means accepted.
- Conflicts (`OptimisticConcurrencyException`, `AggregateAlreadyExistsException`, `CommandAlreadyRecordedException`) are retried up to `maxConflictRetries` times, never inside an outer transaction; when retries run out, the last conflict exception is rethrown.
- A duplicate command id returns the recorded answer: `Accepted(current state)` or the same `Rejected(r)`.
- `EventProducer`'s public API does not change.
- Version becomes `0.2.0` in `gradle.properties`.
- Commit messages follow the repo style (imperative sentence, no prefix) and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Integration tests need Docker (Testcontainers). Commands: `./gradlew :kotmod:test`, `./gradlew :kotmod:integrationTest`, `./gradlew :kotmod-sqldelight:integrationTest`, `./gradlew :examples:integrationTest`.

## Review Focus

1. **Two callers send the same command id at the same moment.** Expected: one decision is recorded and both get the same answer (accepted or rejected), with no duplicate events. Pinned in Task 4 (accept race and reject race).
2. **A rejection inside an outer `jdbc.transaction { }`.** Expected: the record commits with the transaction if the caller carries on, and disappears if the caller throws. Pinned in Task 3 (contract test, rollback) and Task 4 (commit).
3. **`maxConflictRetries = 0` or an exhausted retry budget.** Expected: no retry at all with 0, and the last conflict exception rethrown when the budget runs out (never swallowed or turned into a rejection). Pinned in Task 3.
4. **Accepting with no events.** Expected: version bumped, state saved, command recorded, nothing appended. Pinned in Task 3.
5. **A stored rejection that no longer deserializes** (class renamed). Expected: `RejectionDeserializationException` naming the aggregate, command id and stored type, not a raw `SerializationException`. Pinned in Task 3.

---

## File Structure

- **Create** `kotmod/src/main/kotlin/io/kotmod/Commands.kt`: the command model (`Outcome`, `accept`, `reject`, `CommandHandler`, `CommandHandlers`, `CommandResult`).
- **Create** `kotmod/src/test/kotlin/io/kotmod/CommandHandlersTest.kt`: pure routing tests.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/DomainBackend.kt`: `HandledCommand`, `findHandledCommand`, `recordCommandRejected`.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/Exceptions.kt`: `cause` on `DddException`, `CommandAlreadyRecordedException`, `RejectionDeserializationException`; delete `UnexpectedAggregateStateException`.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/postgres/PostgresDomainBackend.kt` and `DddSchema.kt`: rejection columns.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/EventProducer.kt`: use `findHandledCommand`.
- **Rewrite** `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt` around `handle`.
- **Modify** `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`: test commands, rejections and `OrderCommands`.
- **Modify** `kotmod/src/test/kotlin/io/kotmod/support/StubPersistenceBackend.kt`: map-based command history, `isInTransaction`, conflict injection.
- **Delete** `AggregateManagerCreateTest.kt`, `AggregateManagerExecuteTest.kt`, `AggregateManagerDedupTest.kt`; **create** `AggregateManagerHandleTest.kt`.
- **Modify** `kotmod/src/testFixtures/kotlin/io/kotmod/jdbc/JdbcContextContract.kt`, `kotmod-sqldelight/.../SqlDelightJdbcContextIntegrationTest.kt`, `kotmod/src/test/kotlin/io/kotmod/EventProducerTest.kt`, `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PostgresDomainBackendIntegrationTest.kt`.
- **Create** `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/AggregateManagerIntegrationTest.kt`.
- **Modify** `examples/src/integrationTest/kotlin/io/kotmod/readme/{Quickstart.kt,QuickstartTest.kt,ReadmeExamples.kt}`, `README.md`, `gradle.properties`.

---

### Task 1: The command model

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/Commands.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/CommandHandlersTest.kt`

**Interfaces:**
- Consumes: `io.kotmod.DomainEvent` (existing marker interface).
- Produces (all in package `io.kotmod`):
  - `sealed interface Outcome<out S, out E : DomainEvent, out R>` with `data class Accept<out S, out E : DomainEvent>(val state: S, val events: List<E>) : Outcome<S, E, Nothing>` and `data class Reject<out R>(val rejection: R) : Outcome<Nothing, Nothing, R>`
  - `fun <S, E : DomainEvent> accept(state: S, vararg events: E): Outcome<S, E, Nothing>`
  - `fun <R> reject(rejection: R): Outcome<Nothing, Nothing, R>`
  - `class CommandHandler<S : Any, out E : DomainEvent, out R : Any>` with `internal val decide: (S?) -> Outcome<S, E, R>`
  - `abstract class CommandHandlers<S : Any, C : Any, E : DomainEvent, R : Any>(val rejectionSerializer: KSerializer<R>)` with `abstract fun C.handler()`, `on`, `creates`, `any`, and `internal fun handlerFor(command: C): CommandHandler<S, E, R>`
  - `sealed interface CommandResult<out S, out R>` with `data class Accepted<out S>(val state: S) : CommandResult<S, Nothing>` and `data class Rejected<out R>(val rejection: R) : CommandResult<Nothing, R>`

- [ ] **Step 1: Write the failing test**

Create `kotmod/src/test/kotlin/io/kotmod/CommandHandlersTest.kt`. It defines its own tiny payout aggregate, so it does not depend on the fixtures that Task 3 changes:

```kotlin
package io.kotmod

import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals

class CommandHandlersTest {
    private sealed interface Payout

    private data class Held(val amount: Long) : Payout

    private data class Released(val reference: String) : Payout

    private sealed interface PayoutEvent : DomainEvent

    private data class HoldPlaced(val amount: Long) : PayoutEvent

    private data class FundsReleased(val reference: String) : PayoutEvent

    private sealed interface PayoutCommand

    private data class Hold(val amount: Long) : PayoutCommand

    private data class Release(val reference: String) : PayoutCommand

    private data object Inspect : PayoutCommand

    @Serializable
    sealed interface PayoutRejection

    @Serializable
    data class AmountOverLimit(val limit: Long) : PayoutRejection

    @Serializable
    data object PayoutNotFound : PayoutRejection

    @Serializable
    data class PayoutNotHeld(val actual: String) : PayoutRejection

    @Serializable
    data object PayoutAlreadyExists : PayoutRejection

    private fun hold(amount: Long): Outcome<Payout, PayoutEvent, PayoutRejection> =
        if (amount > 100) reject(AmountOverLimit(100)) else accept(Held(amount), HoldPlaced(amount))

    private fun Held.release(reference: String): Outcome<Payout, PayoutEvent, PayoutRejection> =
        accept(Released(reference), FundsReleased(reference))

    private object PayoutCommands : CommandHandlers<Payout, PayoutCommand, PayoutEvent, PayoutRejection>(
        rejectionSerializer = PayoutRejection.serializer(),
    ) {
        override fun PayoutCommand.handler() =
            when (this) {
                is Hold -> creates(otherwise = { PayoutAlreadyExists }) { hold(amount) }
                is Release ->
                    on<Held>(otherwise = { state -> if (state == null) PayoutNotFound else PayoutNotHeld(state::class.simpleName!!) }) {
                        it.release(reference)
                    }
                Inspect -> any { state -> accept(state ?: Held(0)) }
            }
    }

    private fun decide(
        command: PayoutCommand,
        state: Payout?,
    ) = PayoutCommands.handlerFor(command).decide(state)

    @Test
    fun `on runs the block when the state has the required type`() {
        assertEquals(Outcome.Accept(Released("r-1"), listOf(FundsReleased("r-1"))), decide(Release("r-1"), Held(5)))
    }

    @Test
    fun `on rejects with otherwise when the state has another type`() {
        assertEquals(Outcome.Reject(PayoutNotHeld("Released")), decide(Release("r-2"), Released("r-1")))
    }

    @Test
    fun `on passes null to otherwise when the aggregate does not exist`() {
        assertEquals(Outcome.Reject(PayoutNotFound), decide(Release("r-1"), null))
    }

    @Test
    fun `creates runs the block only when the aggregate does not exist`() {
        assertEquals(Outcome.Accept(Held(5), listOf(HoldPlaced(5))), decide(Hold(5), null))
        assertEquals(Outcome.Reject(PayoutAlreadyExists), decide(Hold(5), Held(1)))
    }

    @Test
    fun `a pure function can reject for a business reason`() {
        assertEquals(Outcome.Reject(AmountOverLimit(100)), decide(Hold(500), null))
    }

    @Test
    fun `any sees the full state, including null`() {
        assertEquals(Outcome.Accept(Held(0), emptyList()), decide(Inspect, null))
        assertEquals(Outcome.Accept(Released("r"), emptyList()), decide(Inspect, Released("r")))
    }

    @Test
    fun `accept keeps events in order`() {
        val outcome: Outcome<Payout, PayoutEvent, PayoutRejection> = accept(Held(1), HoldPlaced(1), HoldPlaced(2))
        assertEquals(listOf(HoldPlaced(1), HoldPlaced(2)), (outcome as Outcome.Accept).events)
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.CommandHandlersTest'`
Expected: compilation FAILS with unresolved references `Outcome`, `CommandHandlers`, `accept`, `reject`.

- [ ] **Step 3: Write the implementation**

Create `kotmod/src/main/kotlin/io/kotmod/Commands.kt`:

```kotlin
package io.kotmod

import kotlinx.serialization.KSerializer

/**
 * What a command function decides: accept the command with a new state and events, or reject it with one of
 * the aggregate's own rejection types. Build one with [accept] or [reject].
 *
 * @param S the aggregate's state type.
 * @param E the aggregate's domain event type.
 * @param R the aggregate's rejection type.
 */
sealed interface Outcome<out S, out E : DomainEvent, out R> {
    /** The command is accepted: the aggregate becomes [state] and [events] are recorded, in order. */
    data class Accept<out S, out E : DomainEvent>(
        val state: S,
        val events: List<E>,
    ) : Outcome<S, E, Nothing>

    /** The command is rejected with [rejection]. Nothing changes, except that the rejection is recorded. */
    data class Reject<out R>(
        val rejection: R,
    ) : Outcome<Nothing, Nothing, R>
}

/** Accepts a command: the aggregate becomes [state] and [events] are recorded, in order. */
fun <S, E : DomainEvent> accept(
    state: S,
    vararg events: E,
): Outcome<S, E, Nothing> = Outcome.Accept(state, events.toList())

/** Rejects a command with [rejection], one of the aggregate's own rejection types. */
fun <R> reject(rejection: R): Outcome<Nothing, Nothing, R> = Outcome.Reject(rejection)

/**
 * What one branch of [CommandHandlers.handler] does with the aggregate's current state (`null` if the aggregate
 * does not exist). Built with [CommandHandlers.on], [CommandHandlers.creates] or [CommandHandlers.any].
 */
class CommandHandler<S : Any, out E : DomainEvent, out R : Any>
    @PublishedApi
    internal constructor(
        internal val decide: (S?) -> Outcome<S, E, R>,
    )

/**
 * The commands of one aggregate type, and the single place that routes each command to a pure function.
 *
 * Extend it with an `object` and implement [handler] as an exhaustive `when` over your sealed command type, one
 * line per command:
 *
 * ```
 * object PayoutCommands : CommandHandlers<Payout, PayoutCommand, PayoutEvent, PayoutRejection>(
 *     rejectionSerializer = PayoutRejection.serializer(),
 * ) {
 *     override fun PayoutCommand.handler() = when (this) {
 *         is Hold    -> creates(otherwise = { PayoutAlreadyExists }) { hold(amount) }
 *         is Release -> on<Held>(otherwise = { PayoutNotHeld }) { it.release(reference) }
 *     }
 * }
 * ```
 *
 * Every rejection is one of your own types, so callers can match on them exhaustively.
 *
 * @param rejectionSerializer serializes rejections, which are recorded so a repeated command id gets the same
 *   answer.
 */
abstract class CommandHandlers<S : Any, C : Any, E : DomainEvent, R : Any>(
    val rejectionSerializer: KSerializer<R>,
) {
    /** Routes this command to the function that decides it. */
    abstract fun C.handler(): CommandHandler<S, E, R>

    /**
     * Runs [block] when the current state is a [T]. Otherwise rejects with [otherwise], which receives the actual
     * state (`null` when the aggregate does not exist).
     */
    inline fun <reified T : S> on(
        noinline otherwise: (S?) -> R,
        noinline block: (T) -> Outcome<S, E, R>,
    ): CommandHandler<S, E, R> =
        CommandHandler { state -> if (state is T) block(state) else Outcome.Reject(otherwise(state)) }

    /** Runs [block] only when the aggregate does not exist yet. Otherwise rejects with [otherwise]. */
    fun creates(
        otherwise: (S) -> R,
        block: () -> Outcome<S, E, R>,
    ): CommandHandler<S, E, R> = CommandHandler { state -> if (state == null) block() else Outcome.Reject(otherwise(state)) }

    /** Runs [block] with the full current state (`null` when the aggregate does not exist), for commands valid in several states. */
    fun any(block: (S?) -> Outcome<S, E, R>): CommandHandler<S, E, R> = CommandHandler(block)

    internal fun handlerFor(command: C): CommandHandler<S, E, R> = command.handler()
}

/** What [AggregateManager.handle] returns. */
sealed interface CommandResult<out S, out R> {
    /** The command was accepted; [state] is the aggregate's state (its current state, for a repeated command id). */
    data class Accepted<out S>(
        val state: S,
    ) : CommandResult<S, Nothing>

    /** The command was rejected with [rejection]. */
    data class Rejected<out R>(
        val rejection: R,
    ) : CommandResult<Nothing, R>
}
```

If the compiler rejects smart-casting `state` to `T` inside the `on` lambda, replace `block(state)` with `block(state as T)`. Keep the `is T` check.

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.CommandHandlersTest'`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add kotmod/src/main/kotlin/io/kotmod/Commands.kt kotmod/src/test/kotlin/io/kotmod/CommandHandlersTest.kt
git commit -m "Add the command model: outcomes, typed rejections and command routing

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Record rejections in the command history

**Files:**
- Modify: `kotmod/src/main/kotlin/io/kotmod/DomainBackend.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/Exceptions.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/postgres/PostgresDomainBackend.kt` (methods `wasCommandHandled`, `recordCommandHandled`, around lines 175-210)
- Modify: `kotmod/src/main/kotlin/io/kotmod/postgres/DddSchema.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/EventProducer.kt` (read phase)
- Modify: `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt` (the two `wasCommandHandled` calls only; it is rewritten in Task 3)
- Modify: `kotmod/src/test/kotlin/io/kotmod/support/StubPersistenceBackend.kt`
- Modify: `kotmod/src/test/kotlin/io/kotmod/EventProducerTest.kt:68`
- Test: `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PostgresDomainBackendIntegrationTest.kt` (replace the three `wasCommandHandled` tests, around lines 221-249)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces:
  - `sealed interface HandledCommand { data object Accepted : HandledCommand; data class Rejected(val type: String, val payload: String) : HandledCommand }` in `io.kotmod`
  - `DomainPersistenceBackend.findHandledCommand(type: AggregateType, id: AggregateId, commandId: CommandId): HandledCommand?`
  - `DomainPersistenceBackend.recordCommandHandled(type, id, commandId)` (unchanged signature; now throws `CommandAlreadyRecordedException` on a duplicate)
  - `DomainPersistenceBackend.recordCommandRejected(type: AggregateType, id: AggregateId, commandId: CommandId, rejectionType: String, payload: String)`
  - `class CommandAlreadyRecordedException(val aggregateType: AggregateType, val aggregateId: AggregateId, val commandId: CommandId) : DddException`
  - `class RejectionDeserializationException(val aggregateType: AggregateType, val aggregateId: AggregateId, val commandId: CommandId, val rejectionType: String, cause: Throwable) : DddException`
  - `StubPersistenceBackend.commands: MutableMap<CommandKey, HandledCommand>` and `override fun isInTransaction() = transactionDepth > 0`

- [ ] **Step 1: Write the failing integration tests**

In `PostgresDomainBackendIntegrationTest.kt`, delete the tests `wasCommandHandled returns false when unseen`, `recordCommandHandled then wasCommandHandled returns true` and `wasCommandHandled is scoped per type, id, and commandId`. Add these in their place (add the imports `io.kotmod.CommandAlreadyRecordedException` and `io.kotmod.HandledCommand`):

```kotlin
    @Test
    fun `findHandledCommand returns null when unseen`() =
        runTest {
            assertNull(backend.findHandledCommand(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-1")))
        }

    @Test
    fun `an accepted command reads back as accepted`() =
        runTest {
            backend.recordCommandHandled(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-1"))

            assertEquals(HandledCommand.Accepted, backend.findHandledCommand(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-1")))
        }

    @Test
    fun `a rejected command reads back with its type and payload`() =
        runTest {
            backend.recordCommandRejected(
                AggregateType("Order"),
                AggregateId("o-1"),
                CommandId("cmd-1"),
                rejectionType = "com.example.OrderNotPending",
                payload = """{"type":"OrderNotPending"}""",
            )

            assertEquals(
                HandledCommand.Rejected("com.example.OrderNotPending", """{"type":"OrderNotPending"}"""),
                backend.findHandledCommand(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-1")),
            )
        }

    @Test
    fun `a row written before rejections were recorded reads as accepted`() =
        runTest {
            dataSource.connection.use { conn ->
                conn.createStatement().use {
                    it.execute("INSERT INTO ddd_command_history (aggregate_type, aggregate_id, command_id) VALUES ('Order', 'o-1', 'old')")
                }
            }

            assertEquals(HandledCommand.Accepted, backend.findHandledCommand(AggregateType("Order"), AggregateId("o-1"), CommandId("old")))
        }

    @Test
    fun `recording the same command id twice throws CommandAlreadyRecordedException`() =
        runTest {
            val type = AggregateType("Order")
            val id = AggregateId("o-1")
            backend.recordCommandHandled(type, id, CommandId("cmd-1"))

            assertFailsWith<CommandAlreadyRecordedException> { backend.recordCommandHandled(type, id, CommandId("cmd-1")) }
            assertFailsWith<CommandAlreadyRecordedException> {
                backend.recordCommandRejected(type, id, CommandId("cmd-1"), "T", "{}")
            }
        }

    @Test
    fun `findHandledCommand is scoped per type, id, and commandId`() =
        runTest {
            backend.recordCommandHandled(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-1"))

            assertNull(backend.findHandledCommand(AggregateType("Order"), AggregateId("o-2"), CommandId("cmd-1")))
            assertNull(backend.findHandledCommand(AggregateType("Order"), AggregateId("o-1"), CommandId("cmd-2")))
            assertNull(backend.findHandledCommand(AggregateType("Widget"), AggregateId("o-1"), CommandId("cmd-1")))
        }

    @Test
    fun `the 0_1_0 upgrade statements add the rejection columns`() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("DROP SCHEMA IF EXISTS upgrade_check CASCADE")
                stmt.execute("CREATE SCHEMA upgrade_check")
                stmt.execute(
                    "CREATE TABLE upgrade_check.ddd_command_history (aggregate_type VARCHAR(72) NOT NULL, " +
                        "aggregate_id VARCHAR(72) NOT NULL, command_id VARCHAR(72) NOT NULL, " +
                        "PRIMARY KEY (aggregate_type, aggregate_id, command_id))",
                )
                // Must match the "Upgrading from 0.1.0" section of README.md.
                stmt.execute("ALTER TABLE upgrade_check.ddd_command_history ADD COLUMN rejection_type    VARCHAR(255)")
                stmt.execute("ALTER TABLE upgrade_check.ddd_command_history ADD COLUMN rejection_payload TEXT")
                val columns =
                    stmt
                        .executeQuery(
                            "SELECT column_name FROM information_schema.columns " +
                                "WHERE table_schema = 'upgrade_check' AND table_name = 'ddd_command_history' ORDER BY ordinal_position",
                        ).use { rs -> generateSequence { if (rs.next()) rs.getString(1) else null }.toList() }
                assertEquals(listOf("aggregate_type", "aggregate_id", "command_id", "rejection_type", "rejection_payload"), columns)
                stmt.execute("DROP SCHEMA upgrade_check CASCADE")
            }
        }
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.postgres.PostgresDomainBackendIntegrationTest'`
Expected: compilation FAILS with unresolved references `findHandledCommand`, `recordCommandRejected`, `HandledCommand`, `CommandAlreadyRecordedException`.

- [ ] **Step 3: Add the exceptions**

In `Exceptions.kt`, give `DddException` an optional cause and add two exceptions after `OptimisticConcurrencyException`:

```kotlin
/** Base class for all exceptions thrown by the library. */
sealed class DddException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
```

```kotlin
/**
 * Thrown by a [DomainPersistenceBackend] when [commandId] is already recorded for an aggregate, which happens when
 * two writers handle the same command id at the same time. [AggregateManager.handle] retries, and the retry
 * returns the recorded answer.
 */
class CommandAlreadyRecordedException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val commandId: CommandId,
) : DddException("Command ${commandId.value} is already recorded for ${aggregateType.value}/${aggregateId.value}")

/**
 * Thrown by [AggregateManager.handle] when a command id was rejected earlier but the recorded rejection can no
 * longer be read, usually because its class was renamed or reshaped. Keep old names with `@SerialName`.
 */
class RejectionDeserializationException(
    val aggregateType: AggregateType,
    val aggregateId: AggregateId,
    val commandId: CommandId,
    val rejectionType: String,
    cause: Throwable,
) : DddException(
        "Recorded rejection $rejectionType for command ${commandId.value} on ${aggregateType.value}/${aggregateId.value} " +
            "can't be read; keep renamed rejection classes readable with @SerialName",
        cause,
    )
```

- [ ] **Step 4: Change the backend interface**

In `DomainBackend.kt`, replace `wasCommandHandled` and the KDoc of `recordCommandHandled` with:

```kotlin
    /** Returns what was recorded for [commandId] on aggregate [type]/[id], or `null` if nothing was. */
    fun findHandledCommand(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): HandledCommand?

    /**
     * Records that [commandId] was accepted by aggregate [type]/[id]. Throws [CommandAlreadyRecordedException] if
     * the command id is already recorded for that aggregate.
     */
    fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    )

    /**
     * Records that [commandId] was rejected by aggregate [type]/[id]: [rejectionType] names the rejection's class and
     * [payload] is its JSON. Throws [CommandAlreadyRecordedException] if the command id is already recorded for that
     * aggregate.
     */
    fun recordCommandRejected(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
        rejectionType: String,
        payload: String,
    )
```

and add, after the interface:

```kotlin
/** What [DomainPersistenceBackend.findHandledCommand] knows about a command id. */
sealed interface HandledCommand {
    /** The command was accepted. */
    data object Accepted : HandledCommand

    /** The command was rejected; [type] names the rejection's class and [payload] is its JSON. */
    data class Rejected(
        val type: String,
        val payload: String,
    ) : HandledCommand
}
```

- [ ] **Step 5: Implement it in Postgres**

In `DddSchema.kt`, replace the `ddd_command_history` statement with:

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

In `PostgresDomainBackend.kt`, replace `wasCommandHandled` and `recordCommandHandled` with the following (add the imports `io.kotmod.CommandAlreadyRecordedException` and `io.kotmod.HandledCommand`):

```kotlin
    override fun findHandledCommand(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): HandledCommand? =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT rejection_type, rejection_payload FROM ddd_command_history " +
                        "WHERE aggregate_type = ? AND aggregate_id = ? AND command_id = ?",
                ).use { ps ->
                    ps.setString(1, type.value)
                    ps.setString(2, id.value)
                    ps.setString(3, commandId.value)
                    ps.executeQuery().use { rs ->
                        when {
                            !rs.next() -> null
                            rs.getString(1) == null -> HandledCommand.Accepted
                            else -> HandledCommand.Rejected(rs.getString(1), rs.getString(2))
                        }
                    }
                }
        }

    override fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ) = insertCommand(type, id, commandId, rejectionType = null, payload = null)

    override fun recordCommandRejected(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
        rejectionType: String,
        payload: String,
    ) = insertCommand(type, id, commandId, rejectionType, payload)

    private fun insertCommand(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
        rejectionType: String?,
        payload: String?,
    ) {
        jdbc.withConnection { conn ->
            try {
                conn
                    .prepareStatement(
                        "INSERT INTO ddd_command_history " +
                            "(aggregate_type, aggregate_id, command_id, rejection_type, rejection_payload) VALUES (?, ?, ?, ?, ?)",
                    ).use { ps ->
                        ps.setString(1, type.value)
                        ps.setString(2, id.value)
                        ps.setString(3, commandId.value)
                        ps.setString(4, rejectionType)
                        ps.setString(5, payload)
                        ps.executeUpdate()
                    }
            } catch (e: SQLException) {
                if (e.sqlState == UNIQUE_VIOLATION) throw CommandAlreadyRecordedException(type, id, commandId)
                throw e
            }
        }
    }
```

- [ ] **Step 6: Move the callers and the stub to the new interface**

In `EventProducer.kt`, change the read phase's check to:

```kotlin
                if (backend.findHandledCommand(aggregateType, id, resolvedCommandId) != null) {
```

In `AggregateManager.kt`, change both `backend.wasCommandHandled(aggregateType, id, resolvedCommandId)` calls to `backend.findHandledCommand(aggregateType, id, resolvedCommandId) != null`. That file is replaced in Task 3; this keeps it compiling.

In `StubPersistenceBackend.kt`, add the imports `io.kotmod.CommandAlreadyRecordedException` and `io.kotmod.HandledCommand`, replace `val commands = mutableSetOf<CommandKey>()` with `val commands = mutableMapOf<CommandKey, HandledCommand>()`, add `override fun isInTransaction(): Boolean = transactionDepth > 0` after `inTransaction`, and replace `wasCommandHandled`/`recordCommandHandled` with:

```kotlin
    override fun findHandledCommand(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): HandledCommand? = commands[CommandKey(type, id, commandId)]

    override fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ) = record(type, id, commandId, HandledCommand.Accepted, "recordCommandHandled")

    override fun recordCommandRejected(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
        rejectionType: String,
        payload: String,
    ) = record(type, id, commandId, HandledCommand.Rejected(rejectionType, payload), "recordCommandRejected")

    private fun record(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
        handled: HandledCommand,
        name: String,
    ) {
        recordWrite(name)
        val key = CommandKey(type, id, commandId)
        if (key in commands) throw CommandAlreadyRecordedException(type, id, commandId)
        commands[key] = handled
    }
```

In `EventProducerTest.kt` line 68, change `backend.commands.any { it.commandId == CommandId("c-1") }` to `backend.commands.keys.any { it.commandId == CommandId("c-1") }`.

- [ ] **Step 7: Run all core tests**

Run: `./gradlew :kotmod:test :kotmod:integrationTest`
Expected: PASS, including the 7 new `PostgresDomainBackendIntegrationTest` tests. (The old `AggregateManager*Test` files still compile and pass: `backend.commands.size` works on a map.)

- [ ] **Step 8: Commit**

```bash
git add kotmod/src
git commit -m "Record rejected commands in the command history

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Replace create and execute with handle

**Files:**
- Rewrite: `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/Exceptions.kt` (delete `UnexpectedAggregateStateException`; update KDoc of `AggregateNotFoundException` and `OptimisticConcurrencyException`)
- Modify: `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`
- Modify: `kotmod/src/test/kotlin/io/kotmod/support/StubPersistenceBackend.kt` (conflict injection)
- Delete: `kotmod/src/test/kotlin/io/kotmod/AggregateManagerCreateTest.kt`, `AggregateManagerExecuteTest.kt`, `AggregateManagerDedupTest.kt`
- Create: `kotmod/src/test/kotlin/io/kotmod/AggregateManagerHandleTest.kt`
- Modify: `kotmod/src/testFixtures/kotlin/io/kotmod/jdbc/JdbcContextContract.kt`
- Modify: `kotmod-sqldelight/src/integrationTest/kotlin/io/kotmod/sqldelight/SqlDelightJdbcContextIntegrationTest.kt`
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt`, `QuickstartTest.kt`, `ReadmeExamples.kt`

**Interfaces:**
- Consumes: everything Task 1 produces; `HandledCommand`, `findHandledCommand`, `recordCommandRejected`, `CommandAlreadyRecordedException` and `RejectionDeserializationException` from Task 2.
- Produces:
  - `AggregateManager<S : Any, E : DomainEvent, C : Any, R : Any>(aggregateType: AggregateType, repository: Repository<S>, backend: DomainPersistenceBackend<E>, commands: CommandHandlers<S, C, E, R>, maxConflictRetries: Int = 5)`
  - `suspend fun handle(id: AggregateId, command: C, commandId: CommandId? = null, correlationId: CorrelationId? = null): CommandResult<S, R>`
  - Test fixtures in `io.kotmod.support`: `OrderCommand` (`PlaceOrder(name)`, `ShipOrder`, `CancelOrder(reason)`, `DecideWith(block)`), `OrderRejection` (`OrderAlreadyExists`, `OrderNotFound`, `OrderNotPending(actual)`), `OrderOutcome`, `placeOrder(name)`, `object OrderCommands`
  - Examples in `io.kotmod.readme`: `OrderCommand` (`PlaceOrder(item)`, `ShipOrder`, `CancelOrder(reason)`), `OrderRejection` (`OrderAlreadyPlaced`, `OrderNotFound`, `OrderAlreadyShipped`, `OrderAlreadyCancelled`, `CancellationReasonMissing`), `OrderOutcome`, `object OrderCommands` (Task 5 copies these into the README)

- [ ] **Step 1: Change the test fixtures**

Replace the part of `TestAggregate.kt` above `sealed interface OrderEvent` (the state classes and the `ship`/`cancel` functions) with the following. Keep the event classes as they are:

```kotlin
package io.kotmod.support

import io.kotmod.CommandHandlers
import io.kotmod.DomainEvent
import io.kotmod.Outcome
import io.kotmod.accept
import kotlinx.serialization.Serializable

sealed interface Order

data class PendingOrder(
    val name: String,
) : Order

data class ShippedOrder(
    val name: String,
) : Order

data class CancelledOrder(
    val name: String,
    val reason: String,
) : Order

sealed interface OrderCommand

data class PlaceOrder(
    val name: String,
) : OrderCommand

data object ShipOrder : OrderCommand

data class CancelOrder(
    val reason: String,
) : OrderCommand

/** Test-only: decides with [block], for scenarios such as a side effect between read and write. */
class DecideWith(
    val block: (Order?) -> Outcome<Order, OrderEvent, OrderRejection>,
) : OrderCommand

@Serializable
sealed interface OrderRejection

@Serializable
data object OrderAlreadyExists : OrderRejection

@Serializable
data object OrderNotFound : OrderRejection

@Serializable
data class OrderNotPending(
    val actual: String,
) : OrderRejection

typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

fun placeOrder(name: String): OrderOutcome = accept(PendingOrder(name), OrderPlaced(name))

fun PendingOrder.ship(): OrderOutcome = accept(ShippedOrder(name), OrderShipped(name))

fun PendingOrder.cancel(reason: String): OrderOutcome = accept(CancelledOrder(name, reason), OrderCancelled(name, reason))

object OrderCommands : CommandHandlers<Order, OrderCommand, OrderEvent, OrderRejection>(
    rejectionSerializer = OrderRejection.serializer(),
) {
    override fun OrderCommand.handler() =
        when (this) {
            is PlaceOrder -> creates(otherwise = { OrderAlreadyExists }) { placeOrder(name) }
            ShipOrder -> on<PendingOrder>(otherwise = ::notPending) { it.ship() }
            is CancelOrder -> on<PendingOrder>(otherwise = ::notPending) { it.cancel(reason) }
            is DecideWith -> any(block)
        }

    private fun notPending(order: Order?): OrderRejection = if (order == null) OrderNotFound else OrderNotPending(order::class.simpleName!!)
}
```

- [ ] **Step 2: Add conflict injection to the stub backend**

In `StubPersistenceBackend.kt`, add a property after `writesOutsideTransaction`:

```kotlin
    /** The next this-many `saveMeta` calls throw [OptimisticConcurrencyException], to simulate a concurrent writer. */
    var conflictsToInject = 0
```

and make this the first line of `saveMeta`, after `recordWrite("saveMeta")`:

```kotlin
        if (conflictsToInject > 0) {
            conflictsToInject--
            throw OptimisticConcurrencyException(type, id, expectedVersion ?: 0)
        }
```

- [ ] **Step 3: Write the failing unit tests**

Delete `AggregateManagerCreateTest.kt`, `AggregateManagerExecuteTest.kt` and `AggregateManagerDedupTest.kt`. Create `kotmod/src/test/kotlin/io/kotmod/AggregateManagerHandleTest.kt`:

```kotlin
package io.kotmod

import io.kotmod.support.CancelOrder
import io.kotmod.support.CancelledOrder
import io.kotmod.support.DecideWith
import io.kotmod.support.Order
import io.kotmod.support.OrderAlreadyExists
import io.kotmod.support.OrderCommand
import io.kotmod.support.OrderCommands
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.OrderNotPending
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderRejection
import io.kotmod.support.OrderShipped
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ShipOrder
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class AggregateManagerHandleTest {
    private val type = AggregateType("Order")
    private val id = AggregateId("o-1")
    private lateinit var backend: StubPersistenceBackend<OrderEvent>
    private lateinit var repo: StubRepository<Order>
    private lateinit var orders: AggregateManager<Order, OrderEvent, OrderCommand, OrderRejection>

    @BeforeTest
    fun setUp() {
        backend = StubPersistenceBackend()
        repo = StubRepository()
        orders = manager()
    }

    private fun manager(maxConflictRetries: Int = 5) =
        AggregateManager(type, repo, backend, OrderCommands, maxConflictRetries = maxConflictRetries)

    private fun version() = backend.metas[StubPersistenceBackend.Key(type, id)]?.version

    @Test
    fun `an accepted create saves state, meta at version 1, events and the command`() =
        runTest {
            val result = orders.handle(id, PlaceOrder("book"), commandId = CommandId("c-1"))

            assertEquals(CommandResult.Accepted(PendingOrder("book")), result)
            assertEquals(PendingOrder("book"), repo.store[id])
            assertEquals(1L, version())
            assertEquals(listOf(OrderPlaced("book")), backend.events.map { it.event })
            assertEquals(HandledCommand.Accepted, backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-1"))])
        }

    @Test
    fun `an accepted command on an existing aggregate advances its version`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))

            val result = orders.handle(id, ShipOrder)

            assertEquals(CommandResult.Accepted(ShippedOrder("book")), result)
            assertEquals(2L, version())
        }

    @Test
    fun `events carry the command id as causation, the correlation id and per-aggregate sequences`() =
        runTest {
            orders.handle(id, DecideWith { accept(PendingOrder("book"), OrderPlaced("book"), OrderPlaced("pen")) }, commandId = CommandId("c-1"), correlationId = CorrelationId("flow-1"))
            orders.handle(id, ShipOrder)

            assertEquals(listOf(1L, 2L, 3L), backend.events.map { it.metadata.sequence })
            assertEquals(CommandId("c-1"), backend.events.first().metadata.causationId)
            assertEquals(CorrelationId("flow-1"), backend.events.first().metadata.correlationId)
        }

    @Test
    fun `accepting with no events still bumps the version, saves state and records the command`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))

            val result = orders.handle(id, DecideWith { state -> accept(state!!) }, commandId = CommandId("noop"))

            assertEquals(CommandResult.Accepted(PendingOrder("book")), result)
            assertEquals(2L, version())
            assertEquals(1, backend.events.size)
            assertEquals(HandledCommand.Accepted, backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("noop"))])
        }

    @Test
    fun `a rejection records the rejection and writes nothing else`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, ShipOrder)

            val result = orders.handle(id, CancelOrder("too late"), commandId = CommandId("c-cancel"))

            assertEquals(CommandResult.Rejected(OrderNotPending("ShippedOrder")), result)
            assertEquals(ShippedOrder("book"), repo.store[id])
            assertEquals(2L, version())
            assertEquals(2, backend.events.size)
            val recorded = backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-cancel"))]
            assertIs<HandledCommand.Rejected>(recorded)
            assertEquals(OrderNotPending::class.java.name, recorded.type)
        }

    @Test
    fun `a command on a missing aggregate is rejected through otherwise, and recorded`() =
        runTest {
            val result = orders.handle(id, ShipOrder, commandId = CommandId("c-1"))

            assertEquals(CommandResult.Rejected(OrderNotFound), result)
            assertNull(version())
            assertIs<HandledCommand.Rejected>(backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-1"))])
        }

    @Test
    fun `creating an existing aggregate is rejected through otherwise`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))

            assertEquals(CommandResult.Rejected(OrderAlreadyExists), orders.handle(id, PlaceOrder("again")))
        }

    @Test
    fun `a repeated accepted command id returns the current state without deciding again`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, CancelOrder("changed mind"), commandId = CommandId("c-cancel"))
            var decided = false

            val again = orders.handle(id, DecideWith { decided = true; error("must not decide") }, commandId = CommandId("c-cancel"))

            assertEquals(CommandResult.Accepted(CancelledOrder("book", "changed mind")), again)
            assertEquals(false, decided)
            assertEquals(2L, version())
        }

    @Test
    fun `a repeated rejected command id returns the same rejection even if the state now allows it`() =
        runTest {
            val first = orders.handle(id, ShipOrder, commandId = CommandId("c-ship"))
            orders.handle(id, PlaceOrder("book"))

            val again = orders.handle(id, ShipOrder, commandId = CommandId("c-ship"))

            assertEquals(CommandResult.Rejected(OrderNotFound), first)
            assertEquals(first, again)
            assertEquals(PendingOrder("book"), repo.store[id])
        }

    @Test
    fun `without a command id every call gets a fresh one and is recorded`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, ShipOrder)

            assertEquals(2, backend.commands.size)
        }

    @Test
    fun `all writes happen inside one backend transaction per command`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, CancelOrder("x"))
            orders.handle(id, ShipOrder)

            assertEquals(emptyList(), backend.writesOutsideTransaction)
            assertEquals(3, backend.transactionsCommitted)
        }

    @Test
    fun `an exception from the decision writes nothing and propagates`() =
        runTest {
            assertFailsWith<IllegalStateException> { orders.handle(id, DecideWith { error("bug") }) }

            assertNull(version())
            assertEquals(0, backend.commands.size)
        }

    @Test
    fun `a conflict is retried by re-reading and deciding again`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            backend.conflictsToInject = 2
            var decisions = 0

            val result = orders.handle(id, DecideWith { state -> decisions++; (state as PendingOrder).ship() })

            assertEquals(CommandResult.Accepted(ShippedOrder("book")), result)
            assertEquals(3, decisions)
        }

    @Test
    fun `when retries run out the last conflict exception is rethrown`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            backend.conflictsToInject = 6

            assertFailsWith<OptimisticConcurrencyException> { orders.handle(id, ShipOrder) }
            assertEquals(0, backend.conflictsToInject)
        }

    @Test
    fun `maxConflictRetries of 0 does not retry`() =
        runTest {
            orders = manager(maxConflictRetries = 0)
            orders.handle(id, PlaceOrder("book"))
            backend.conflictsToInject = 1

            assertFailsWith<OptimisticConcurrencyException> { orders.handle(id, ShipOrder) }
        }

    @Test
    fun `a negative maxConflictRetries is refused`() {
        assertFailsWith<IllegalArgumentException> { manager(maxConflictRetries = -1) }
    }

    @Test
    fun `inside an outer transaction a conflict is not retried`() {
        runBlocking { orders.handle(id, PlaceOrder("book")) }
        backend.conflictsToInject = 1

        assertFailsWith<OptimisticConcurrencyException> {
            backend.inTransaction { runBlocking { orders.handle(id, ShipOrder) } }
        }
    }

    @Test
    fun `an unreadable recorded rejection throws RejectionDeserializationException`() =
        runTest {
            backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("c-old"))] =
                HandledCommand.Rejected("com.example.RenamedRejection", """{"type":"com.example.RenamedRejection"}""")

            val failure = assertFailsWith<RejectionDeserializationException> { orders.handle(id, ShipOrder, commandId = CommandId("c-old")) }

            assertEquals("com.example.RenamedRejection", failure.rejectionType)
            assertEquals(CommandId("c-old"), failure.commandId)
        }

    @Test
    fun `bookkeeping without repository state throws AggregateNotFoundException`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            repo.store.clear()

            assertFailsWith<AggregateNotFoundException> { orders.handle(id, ShipOrder) }
        }

    @Test
    fun `accepted events are appended in command order`() =
        runTest {
            orders.handle(id, PlaceOrder("book"))
            orders.handle(id, ShipOrder)

            assertEquals(listOf(OrderPlaced("book"), OrderShipped("book")), backend.events.map { it.event })
        }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.AggregateManagerHandleTest'`
Expected: compilation FAILS: `AggregateManager` has no `commands` parameter and no `handle`.

- [ ] **Step 5: Rewrite AggregateManager**

Replace the whole of `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt` with:

```kotlin
package io.kotmod

import io.kotmod.jdbc.KotmodTransaction
import io.kotmod.jdbc.databaseWork
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.serialization.json.Json
import kotlin.time.toKotlinInstant

/**
 * Runs commands against aggregates of one [AggregateType] whose state is stored by a [Repository].
 *
 * [handle] is the only way to change an aggregate. Each command runs in three phases:
 * 1. **Read:** if the command id has already been handled, return the recorded answer (the current state if
 *    it was accepted, the same rejection if it was rejected). Otherwise load the aggregate's version and state.
 * 2. **Decide:** [commands] routes the command to a pure function, which accepts it (new state and events) or
 *    rejects it with one of the aggregate's rejection types. No database work happens in this phase.
 * 3. **Write:** in one transaction, either advance the aggregate's version (optimistic concurrency), save the
 *    new state, append the events and record the command as accepted; or record the rejection.
 *
 * If the write loses a race with another writer, [handle] starts again from the read, up to
 * [maxConflictRetries] times, except inside an outer transaction, where the conflict propagates.
 *
 * Events appended here are later picked up by [io.kotmod.outbox.AggregateEventOutbox] and
 * [io.kotmod.contract.PublicEventContract].
 *
 * @param S the aggregate's state type.
 * @param E the aggregate's domain event type.
 * @param C the aggregate's command type.
 * @param R the aggregate's rejection type.
 */
class AggregateManager<S : Any, E : DomainEvent, C : Any, R : Any>(
    private val aggregateType: AggregateType,
    private val repository: Repository<S>,
    private val backend: DomainPersistenceBackend<E>,
    private val commands: CommandHandlers<S, C, E, R>,
    private val maxConflictRetries: Int = 5,
) {
    init {
        require(maxConflictRetries >= 0) { "maxConflictRetries must not be negative" }
    }

    /**
     * Handles [command] for aggregate [id] and returns whether it was accepted (with the new state) or rejected
     * (with the rejection).
     *
     * If [commandId] has already been handled for [id], the recorded answer is returned without deciding again:
     * the current state if it was accepted, or the same rejection if it was rejected. A random command id is
     * used when none is given, which makes the call non-idempotent. [correlationId] is stored with every event.
     */
    suspend fun handle(
        id: AggregateId,
        command: C,
        commandId: CommandId? = null,
        correlationId: CorrelationId? = null,
    ): CommandResult<S, R> {
        val resolvedCommandId = commandId ?: CommandId(randomId())
        val retries = if (inOuterTransaction()) 0 else maxConflictRetries
        var attempt = 0
        while (true) {
            try {
                return handleOnce(id, command, resolvedCommandId, correlationId)
            } catch (e: DddException) {
                if (!e.isConflict() || attempt >= retries) throw e
                attempt++
            }
        }
    }

    private suspend fun inOuterTransaction(): Boolean =
        currentCoroutineContext()[KotmodTransaction] != null || backend.isInTransaction()

    private fun DddException.isConflict(): Boolean =
        this is OptimisticConcurrencyException ||
            this is AggregateAlreadyExistsException ||
            this is CommandAlreadyRecordedException

    private suspend fun handleOnce(
        id: AggregateId,
        command: C,
        commandId: CommandId,
        correlationId: CorrelationId?,
    ): CommandResult<S, R> {
        // Phase 1: Read — the recorded answer, or the current version and state
        val read =
            databaseWork(backend::isInTransaction) {
                when (val handled = backend.findHandledCommand(aggregateType, id, commandId)) {
                    HandledCommand.Accepted -> Read.Answered<S, R>(CommandResult.Accepted(requireState(id)))
                    is HandledCommand.Rejected -> Read.Answered<S, R>(CommandResult.Rejected(decodeRejection(id, commandId, handled)))
                    null -> {
                        val meta = backend.loadMeta(aggregateType, id)
                        Read.Undecided<S, R>(meta, if (meta == null) null else requireState(id))
                    }
                }
            }
        val undecided =
            when (read) {
                is Read.Answered -> return read.result
                is Read.Undecided -> read
            }

        // Phase 2: Decide — pure, no database work
        val outcome = commands.handlerFor(command).decide(undecided.state)

        // Phase 3: Write — tight transaction
        databaseWork(backend::isInTransaction) {
            backend.inTransaction {
                when (outcome) {
                    is Outcome.Accept -> {
                        val lastSequence =
                            backend.saveMeta(aggregateType, id, expectedVersion = undecided.meta?.version, eventCount = outcome.events.size)
                        repository.save(id, outcome.state)
                        if (outcome.events.isNotEmpty()) {
                            backend.appendEvents(
                                wrapPending(
                                    id = id,
                                    causationId = commandId,
                                    correlationId = correlationId,
                                    events = outcome.events,
                                    firstSequence = lastSequence - outcome.events.size + 1,
                                ),
                            )
                        }
                        backend.recordCommandHandled(aggregateType, id, commandId)
                    }
                    is Outcome.Reject ->
                        backend.recordCommandRejected(
                            aggregateType,
                            id,
                            commandId,
                            rejectionType = outcome.rejection::class.java.name,
                            payload = Json.encodeToString(commands.rejectionSerializer, outcome.rejection),
                        )
                }
            }
        }
        return when (outcome) {
            is Outcome.Accept -> CommandResult.Accepted(outcome.state)
            is Outcome.Reject -> CommandResult.Rejected(outcome.rejection)
        }
    }

    private fun requireState(id: AggregateId): S = repository.get(id) ?: throw AggregateNotFoundException(aggregateType, id)

    private fun decodeRejection(
        id: AggregateId,
        commandId: CommandId,
        handled: HandledCommand.Rejected,
    ): R =
        try {
            Json.decodeFromString(commands.rejectionSerializer, handled.payload)
        } catch (e: IllegalArgumentException) {
            // kotlinx.serialization's SerializationException is an IllegalArgumentException.
            throw RejectionDeserializationException(aggregateType, id, commandId, handled.type, e)
        }

    private sealed interface Read<S, R> {
        class Answered<S, R>(
            val result: CommandResult<S, R>,
        ) : Read<S, R>

        class Undecided<S, R>(
            val meta: AggregateMeta?,
            val state: S?,
        ) : Read<S, R>
    }

    private fun wrapPending(
        id: AggregateId,
        causationId: CommandId,
        correlationId: CorrelationId?,
        events: List<E>,
        firstSequence: Long,
    ): List<PendingEvent<E>> {
        val now =
            java.time.Instant
                .now()
                .toKotlinInstant()
        return events.mapIndexed { index, event ->
            PendingEvent(
                metadata =
                    EventMetadata(
                        eventId = EventId(randomId()),
                        aggregateType = aggregateType,
                        aggregateId = id,
                        causationId = causationId,
                        correlationId = correlationId,
                        timestamp = now,
                        sequence = firstSequence + index,
                    ),
                event = event,
            )
        }
    }
}
```

Notes for the implementer:
- `KotmodTransaction` is declared in `io.kotmod.jdbc.Transactions.kt` with an `internal` constructor. The class is public, so `currentCoroutineContext()[KotmodTransaction]` compiles from `io.kotmod`.
- If smart casts on `outcome` inside `when` do not give `outcome.state`/`outcome.events`/`outcome.rejection` their element types, cast explicitly (`outcome as Outcome.Accept<S, E>`). Do not change the behaviour.

In `Exceptions.kt`, delete `UnexpectedAggregateStateException`, and update these two KDocs:

```kotlin
/**
 * Thrown when an aggregate's bookkeeping exists, or one of its commands was accepted, but its [Repository] has no
 * state for it: the state was deleted or saved somewhere else.
 */
class AggregateNotFoundException(
```

```kotlin
/**
 * Thrown when an aggregate was changed by someone else between being read and being written: its stored version
 * no longer matches [expectedVersion]. [AggregateManager.handle] retries automatically; you only see this when its
 * retries run out, or inside an outer transaction.
 */
class OptimisticConcurrencyException(
```

- [ ] **Step 6: Run the unit tests to verify they pass**

Run: `./gradlew :kotmod:test`
Expected: PASS, including `AggregateManagerHandleTest` (19 tests) and `CommandHandlersTest`.

- [ ] **Step 7: Migrate the JDBC contract tests**

In `JdbcContextContract.kt`, add the imports `io.kotmod.CommandResult`, `io.kotmod.support.DecideWith`, `io.kotmod.support.OrderCommands`, `io.kotmod.support.PlaceOrder`, `io.kotmod.support.ShipOrder` and `io.kotmod.support.ship`; remove imports that become unused (`OrderShipped`, and `AggregateAlreadyExistsException` once no test uses it). Then make these changes:

`manager(...)` passes the commands:

```kotlin
    private fun manager(
        type: String,
        jdbc: JdbcContext = context,
    ) = AggregateManager(
        aggregateType = AggregateType(type),
        repository = ProbeOrderRepository(jdbc),
        backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
        commands = OrderCommands,
    )
```

Every `X.create(AggregateId("…")) { PendingOrder("n") to listOf(OrderPlaced("n")) }` becomes `X.handle(AggregateId("…"), PlaceOrder("n"))`, keeping the same receiver, id and name, apart from the three tests below.

Replace `a failing second command rolls back the first command's state, events and command record` with:

```kotlin
    @Test
    fun `throwing on a rejection rolls back the first command's state, events and command record, and the rejection`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            invoices.handle(AggregateId("i-1"), PlaceOrder("invoice"))

            assertFailsWith<IllegalStateException> {
                context.transaction {
                    orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                    val again = invoices.handle(AggregateId("i-1"), PlaceOrder("again"))
                    if (again is CommandResult.Rejected) error("rejected: ${again.rejection}")
                }
            }

            assertEquals(1, count("ddd_aggregate_root"))
            assertEquals(1, count("ddd_domain_event"))
            assertEquals(1, count("ddd_command_history"))
            assertEquals(1, count("outer_tx_state"))
        }
```

Replace the body of `an optimistic concurrency conflict rolls back the whole transaction` with the following (inside the outer transaction there is no retry, so the conflict propagates):

```kotlin
    @Test
    fun `an optimistic concurrency conflict rolls back the whole transaction`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
            invoices.handle(AggregateId("i-1"), PlaceOrder("invoice"))

            assertFailsWith<OptimisticConcurrencyException> {
                context.transaction {
                    orders.handle(AggregateId("o-1"), ShipOrder)
                    invoices.handle(
                        AggregateId("i-1"),
                        DecideWith { order ->
                            // Someone else changes the invoice between our read and our write.
                            bumpVersionElsewhere("i-1")
                            (order as PendingOrder).ship()
                        },
                    )
                }
            }

            assertEquals(2, count("ddd_domain_event"))
            assertEquals(PendingOrder("book"), ProbeOrderRepository(context).get(AggregateId("o-1")))
        }

    private fun bumpVersionElsewhere(aggregateId: String) {
        dataSource.connection.use { conn ->
            conn.createStatement().use {
                it.execute("UPDATE ddd_aggregate_root SET aggregate_version = aggregate_version + 1 WHERE aggregate_id = '$aggregateId'")
            }
        }
    }
```

`commands inside a transaction see earlier uncommitted writes` becomes:

```kotlin
            val shipped =
                context.transaction {
                    orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                    orders.handle(AggregateId("o-1"), ShipOrder)
                }

            assertEquals(CommandResult.Accepted(ShippedOrder("book")), shipped)
            assertEquals(2, count("ddd_domain_event"))
```

Replace `carrying on after a command failed inside transaction rolls back and fails loudly` with:

```kotlin
    @Test
    fun `carrying on after a command failed inside transaction rolls back and fails loudly`() =
        runBlocking {
            val orders = manager("Order")
            val invoices = manager("Invoice")
            invoices.handle(AggregateId("i-1"), PlaceOrder("invoice"))

            assertFailsWith<TransactionRolledBackException> {
                context.transaction {
                    orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                    try {
                        invoices.handle(
                            AggregateId("i-1"),
                            DecideWith { order ->
                                bumpVersionElsewhere("i-1")
                                (order as PendingOrder).ship()
                            },
                        )
                    } catch (_: OptimisticConcurrencyException) {
                        // carry on regardless
                    }
                }
            }

            assertEquals(1, count("ddd_domain_event"))
            assertEquals(1, count("outer_tx_state"))
        }
```

- [ ] **Step 8: Migrate the SQLDelight tests**

In `SqlDelightJdbcContextIntegrationTest.kt`, pass `commands = io.kotmod.support.OrderCommands` to the `AggregateManager` in `statelessOrders`, replace each `orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book")) }` with `orders.handle(AggregateId("o-1"), PlaceOrder("book"))`, and change the imports from `OrderPlaced`/`PendingOrder` to `io.kotmod.support.PlaceOrder`. The stateless repository's `get` returns `null`, so `PlaceOrder` (a `creates` branch) is accepted.

- [ ] **Step 9: Migrate the examples**

In `examples/src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt`, replace the three `fun placeOrder` / `fun PendingOrder.ship` / `fun PendingOrder.cancel` functions with the following, and add the imports `io.kotmod.CommandHandlers`, `io.kotmod.Outcome`, `io.kotmod.accept` and `io.kotmod.reject`. Update the header comment to `// Keep in sync with README.md (Quickstart, steps 2, 3 and 5).` (unchanged steps).

```kotlin
@Serializable
sealed interface OrderCommand

@Serializable
data class PlaceOrder(
    val item: String,
) : OrderCommand

@Serializable
data object ShipOrder : OrderCommand

@Serializable
data class CancelOrder(
    val reason: String,
) : OrderCommand

@Serializable
sealed interface OrderRejection

@Serializable
data object OrderAlreadyPlaced : OrderRejection

@Serializable
data object OrderNotFound : OrderRejection

@Serializable
data object OrderAlreadyShipped : OrderRejection

@Serializable
data object OrderAlreadyCancelled : OrderRejection

@Serializable
data object CancellationReasonMissing : OrderRejection

typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

fun placeOrder(item: String): OrderOutcome = accept(PendingOrder(item), OrderPlaced(item))

fun PendingOrder.ship(): OrderOutcome = accept(ShippedOrder(item), OrderShipped(item))

fun PendingOrder.cancel(reason: String): OrderOutcome =
    if (reason.isBlank()) {
        reject(CancellationReasonMissing)
    } else {
        accept(CancelledOrder(item, reason), OrderCancelled(item, reason))
    }

object OrderCommands : CommandHandlers<Order, OrderCommand, OrderEvent, OrderRejection>(
    rejectionSerializer = OrderRejection.serializer(),
) {
    override fun OrderCommand.handler() =
        when (this) {
            is PlaceOrder -> creates(otherwise = { OrderAlreadyPlaced }) { placeOrder(item) }
            ShipOrder -> on<PendingOrder>(otherwise = ::notPending) { it.ship() }
            is CancelOrder -> on<PendingOrder>(otherwise = ::notPending) { it.cancel(reason) }
        }

    private fun notPending(order: Order?): OrderRejection =
        when (order) {
            is ShippedOrder -> OrderAlreadyShipped
            is CancelledOrder -> OrderAlreadyCancelled
            else -> OrderNotFound
        }
}
```

In `QuickstartTest.kt`, add `commands = OrderCommands,` to the `AggregateManager(...)` call. Replace the two command lines with:

```kotlin
            orders.handle(orderId, PlaceOrder("book"))

            val shipped = orders.handle(orderId, ShipOrder)
```

and change the assertion at the end from `assertEquals(ShippedOrder("book"), shipped)` to `assertEquals(CommandResult.Accepted(ShippedOrder("book")), shipped)` (import `io.kotmod.CommandResult`).

In `ReadmeExamples.kt`, remove the imports of `OptimisticConcurrencyException` and `UnexpectedAggregateStateException`, add `io.kotmod.CommandResult`, and replace `cancelOrder`, `retryOnConflict` and `shipAndInvoice` with:

```kotlin
suspend fun cancelOrder(
    orders: AggregateManager<Order, OrderEvent, OrderCommand, OrderRejection>,
    orderId: AggregateId,
    reason: String,
    requestId: String,
): String =
    when (val result = orders.handle(orderId, CancelOrder(reason), commandId = CommandId(requestId))) {
        is CommandResult.Accepted -> "Cancelled"
        is CommandResult.Rejected ->
            when (result.rejection) {
                OrderAlreadyShipped -> "Too late: the order has shipped"
                CancellationReasonMissing -> "Please give a reason"
                else -> "Can't cancel: ${result.rejection}"
            }
    }

suspend fun shipAndInvoice(
    jdbc: JdbcContext,
    orders: AggregateManager<Order, OrderEvent, OrderCommand, OrderRejection>,
    invoices: AggregateManager<Order, OrderEvent, OrderCommand, OrderRejection>,
    orderId: AggregateId,
) {
    jdbc.transaction {
        // A rejection is a value: throw to roll the whole transaction back.
        val shipped = orders.handle(orderId, ShipOrder)
        if (shipped is CommandResult.Rejected) throw IllegalStateException("Can't ship: ${shipped.rejection}")
        val invoiced = invoices.handle(AggregateId("invoice-${orderId.value}"), PlaceOrder("invoice"))
        if (invoiced is CommandResult.Rejected) throw IllegalStateException("Can't invoice: ${invoiced.rejection}")
    }
}
```

If any other code in `ReadmeExamples.kt` constructs an `AggregateManager` or calls `create`/`execute`, migrate it the same way (`commands = OrderCommands`, `handle(...)`).

- [ ] **Step 10: Run every suite**

Run: `./gradlew :kotmod:test :kotmod:integrationTest :kotmod-sqldelight:integrationTest :kotmod-db-scheduler:test :kotmod-db-scheduler:integrationTest :examples:integrationTest`
Expected: PASS. Then confirm nothing still uses the old API:

Run: `grep -rn -e 'UnexpectedAggregateStateException' -e '\.create(AggregateId' -e 'execute<' --include='*.kt' kotmod kotmod-sqldelight kotmod-db-scheduler examples`
Expected: no output.

- [ ] **Step 11: Commit**

```bash
git add -A kotmod kotmod-sqldelight examples
git commit -m "Replace create and execute with handle, routing commands to pure functions

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Postgres behaviour of handle under concurrency

**Files:**
- Create: `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/AggregateManagerIntegrationTest.kt`

**Interfaces:**
- Consumes: `AggregateManager.handle`, `CommandResult` and the fixtures (`OrderCommands`, `PlaceOrder`, `ShipOrder`, `DecideWith`, `OrderAlreadyExists`, `placeOrder`) from Task 3; `IntegrationTest` and `orderEventSerialization()` (existing test fixtures).
- Produces: nothing used later.

- [ ] **Step 1: Write the tests**

These tests exercise code that already exists, so they are expected to pass. A failure here is a real bug in Task 3's implementation: fix `AggregateManager` or the Postgres backend, not the test.

```kotlin
package io.kotmod.postgres

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CommandResult
import io.kotmod.Repository
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.transaction
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.reject
import io.kotmod.support.DecideWith
import io.kotmod.support.Order
import io.kotmod.support.OrderAlreadyExists
import io.kotmod.support.OrderCommands
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ShipOrder
import io.kotmod.support.ShippedOrder
import io.kotmod.support.placeOrder
import io.kotmod.support.ship
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class AggregateManagerIntegrationTest : IntegrationTest() {
    private class OrderTable(
        private val jdbc: JdbcContext,
    ) : Repository<Order> {
        override fun get(id: AggregateId): Order? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, name FROM handle_test_order WHERE id = ?").use { ps ->
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
                        "INSERT INTO handle_test_order (id, status, name) VALUES (?, ?, ?) " +
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

    private lateinit var orders: AggregateManager<Order, io.kotmod.support.OrderEvent, io.kotmod.support.OrderCommand, io.kotmod.support.OrderRejection>

    @BeforeEach
    fun setUp() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS handle_test_order (id TEXT PRIMARY KEY, status TEXT NOT NULL, name TEXT NOT NULL)")
                stmt.execute("TRUNCATE handle_test_order")
            }
        }
        orders = AggregateManager(AggregateType("Order"), OrderTable(jdbc), PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()), OrderCommands)
    }

    private fun count(sql: String): Int =
        dataSource.connection.use { conn -> conn.createStatement().use { it.executeQuery(sql).use { rs -> rs.next(); rs.getInt(1) } } }

    /** A decision that waits until [parties] callers have read, so they all decide on the same snapshot. Only the first [parties] decisions wait. */
    private fun rendezvous(parties: Int): () -> Unit {
        val barrier = CyclicBarrier(parties)
        val calls = AtomicInteger()
        return { if (calls.incrementAndGet() <= parties) barrier.await(10, TimeUnit.SECONDS) }
    }

    @Test
    fun `two callers sending the same command id at once get one acceptance and one set of events`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
            val meet = rendezvous(2)
            val ship = DecideWith { state -> meet(); (state as PendingOrder).ship() }

            val results =
                (1..2).map { async(Dispatchers.IO) { orders.handle(AggregateId("o-1"), ship, commandId = CommandId("ship-1")) } }.awaitAll()

            assertEquals(List(2) { CommandResult.Accepted(ShippedOrder("book")) }, results)
            assertEquals(2, count("SELECT COUNT(*) FROM ddd_domain_event"))
            assertEquals(2, count("SELECT COUNT(*) FROM ddd_command_history"))
        }

    @Test
    fun `two callers rejecting the same command id at once get the same recorded rejection`() =
        runBlocking {
            val meet = rendezvous(2)
            val refuse = DecideWith { meet(); reject(OrderAlreadyExists) }

            val results =
                (1..2).map { async(Dispatchers.IO) { orders.handle(AggregateId("o-1"), refuse, commandId = CommandId("c-1")) } }.awaitAll()

            assertEquals(List(2) { CommandResult.Rejected(OrderAlreadyExists) }, results)
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_command_history WHERE rejection_type IS NOT NULL"))
        }

    @Test
    fun `two callers creating the same aggregate with different command ids get one acceptance and one rejection`() =
        runBlocking {
            val meet = rendezvous(2)
            val create = DecideWith { state -> meet(); if (state == null) placeOrder("book") else reject(OrderAlreadyExists) }

            val results =
                (1..2).map { n -> async(Dispatchers.IO) { orders.handle(AggregateId("o-1"), create, commandId = CommandId("create-$n")) } }.awaitAll()

            assertEquals(setOf(CommandResult.Accepted(PendingOrder("book")), CommandResult.Rejected(OrderAlreadyExists)), results.toSet())
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_aggregate_root"))
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_domain_event"))
        }

    @Test
    fun `a rejection inside an outer transaction commits with it when the caller carries on`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))

            val result = jdbc.transaction { orders.handle(AggregateId("o-1"), PlaceOrder("again"), commandId = CommandId("again")) }

            assertEquals(CommandResult.Rejected(OrderAlreadyExists), result)
            assertEquals(1, count("SELECT COUNT(*) FROM ddd_command_history WHERE command_id = 'again' AND rejection_type IS NOT NULL"))
        }

    @Test
    fun `a recorded rejection survives in Postgres and is returned for the same command id`() =
        runBlocking {
            val first = orders.handle(AggregateId("o-1"), ShipOrder, commandId = CommandId("ship-early"))
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))

            val again = orders.handle(AggregateId("o-1"), ShipOrder, commandId = CommandId("ship-early"))

            assertEquals(first, again)
            assertEquals(PendingOrder("book"), OrderTable(jdbc).get(AggregateId("o-1")))
        }
}
```

- [ ] **Step 2: Run the tests**

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.postgres.AggregateManagerIntegrationTest'`
Expected: PASS (5 tests). If the first test fails with a `CommandAlreadyRecordedException` or `OptimisticConcurrencyException` escaping, the retry classification in `AggregateManager.handle` is wrong. If the rejection race produces two rows, the Postgres primary key or the unique-violation mapping in `insertCommand` is wrong.

- [ ] **Step 3: Commit**

```bash
git add kotmod/src/integrationTest/kotlin/io/kotmod/postgres/AggregateManagerIntegrationTest.kt
git commit -m "Test handle against Postgres under concurrent commands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Documentation and the 0.2.0 version

**Files:**
- Modify: `README.md` (sections: Core concepts, Quickstart steps 2-4, Aggregates and commands, Several aggregates in one transaction, Known limitations; new "Upgrading from 0.1.0" before "Status and contributing"; Contents list)
- Modify: `gradle.properties`

**Interfaces:**
- Consumes: the example code written in Task 3, Step 9. The README must match `Quickstart.kt`, `QuickstartTest.kt` and `ReadmeExamples.kt` exactly.
- Produces: nothing used later.

- [ ] **Step 1: Bump the version**

In `gradle.properties`, change `version=0.1.0` to `version=0.2.0`. If the README's Installation section quotes `0.1.0` in dependency snippets, change those to `0.2.0` too.

- [ ] **Step 2: Rewrite Quickstart step 2**

Rename the heading `### 2. Define state, events and commands` to `### 2. Define state, events, commands and rejections`. Keep the state and event code. Replace the paragraph starting "Write each **command** as a plain function" and its code block with this text, followed by the command, rejection, outcome, function and `OrderCommands` code copied verbatim from `Quickstart.kt`:

> A **command** asks the order to change. Commands are data, so they are `@Serializable`, and so is the
> aggregate's **rejection** type: every way a command can be refused, in your domain's own words.
>
> Write the decisions as plain functions on specific states. Each returns an outcome, `accept(newState,
> events…)` or `reject(rejection)`, so your domain decisions stay pure code that you can unit-test without a
> database. `OrderCommands` is the one place that routes each command to its function: `creates` only runs
> for an order that doesn't exist yet, `on<PendingOrder>` only runs for a pending order, and `otherwise`
> names the rejection in every other case. The `when` is exhaustive, so adding a command without routing it
> doesn't compile.

- [ ] **Step 3: Update Quickstart steps 3 and 4**

In step 3's `AggregateManager(...)` snippet, add `commands = OrderCommands,` exactly as in `QuickstartTest.kt`. Replace step 4's text and code with:

> Send the commands from step 2 through the aggregate manager. `handle` is a `suspend` function, and it
> returns either `CommandResult.Accepted` with the new state or `CommandResult.Rejected` with one of your
> rejections:
>
> ```kotlin
> val orderId = AggregateId("order-1")
>
> orders.handle(orderId, PlaceOrder("book"))
>
> val shipped = orders.handle(orderId, ShipOrder)
> ```
>
> An accepted command saves the order's state, appends its events to the event log and records the command,
> all in one transaction. A rejected command changes nothing, but its rejection is recorded too.

- [ ] **Step 4: Rewrite "Aggregates and commands"**

Replace the guide section from `### Aggregates and commands` up to (not including) `#### Several aggregates in one transaction` with prose covering exactly these points, in this order, keeping the README's plain style (short sentences, bold lead-ins):

1. **Three phases**: read (if the command id was handled before, return the recorded answer), decide (your pure function runs; no database work, so keep side effects out and put them in event reactions), write (one transaction: version advance, state, events, command record; or the rejection record).
2. **Routing**: `on<T>(otherwise)`, `creates(otherwise)`, and `any { state -> … }` for a command valid in several states. `otherwise` receives the actual state, `null` when the order doesn't exist, so you can reject differently by state.
3. The `cancelOrder` example copied from `ReadmeExamples.kt`, introduced as "Callers match on the result; a rejection is a value, never an exception:".
4. **Why commands are data**: every caller (an HTTP handler, an event reaction, and later a process manager) runs a command the same way, through the one routing point, so the rules for which state a command needs and how it is refused live in one place.
5. **Event sequence numbers**: keep the existing paragraph unchanged.
6. **Idempotency**: pass a `CommandId` you control. A repeated id returns the recorded answer: an accepted command returns the aggregate's *current* state without running again, and a rejected one returns the same rejection, even if the state would now allow the command. Without a command id, kotmod generates a random one and the call is not idempotent. Keep the existing `CorrelationId` sentence.
7. **Concurrency**: each aggregate has a version. If someone else changes the aggregate between your read and your write, `handle` reads again and decides again, up to `maxConflictRetries` times (5 by default; deciding is pure, so that is safe), and then throws `OptimisticConcurrencyException`. Delete the `retryOnConflict` example.
8. **Your repository joins the transaction**: keep the existing paragraph unchanged.

- [ ] **Step 5: Update "Several aggregates in one transaction"**

Replace the `shipAndInvoice` snippet with the one from `ReadmeExamples.kt`. Change the bullets so that they:
- Keep the bullet about a failing command rolling everything back, but say "a conflict" instead of "an `OptimisticConcurrencyException` on one aggregate", and add: inside the block, `handle` does not retry conflicts; the exception propagates and the whole transaction rolls back.
- Add a bullet: a rejection is a value. If you carry on, its record commits with everything else; to undo the other commands, throw, as `shipAndInvoice` does.
- Keep the other bullets unchanged.

- [ ] **Step 6: Update Core concepts, Known limitations, Contents, and add the upgrade section**

In **Core concepts**, replace the **Command** bullet and add **Rejection** after it:

> - **Command** — a request to change an aggregate, as serializable data. The aggregate accepts it (new state
>   and events) or rejects it. It is idempotent when given a `CommandId`.
> - **Rejection** — why an aggregate refused a command, as one of your own types. Rejections are recorded,
>   so a repeated command id gets the same answer.

In **Known limitations**, add a group before **Leader election**:

> **Commands**
>
> - **Renaming a rejection class breaks reading back old rejections.** Recorded rejections are plain JSON,
>   without the versioned migrations events have. If a duplicate of a command rejected under the old name
>   arrives, `handle` throws `RejectionDeserializationException`. Keep old names readable with `@SerialName`.
>   Duplicates normally arrive within minutes of the original, so this rarely matters.

Add a section `## Upgrading from 0.1.0` before `## Status and contributing`, and add it to the Contents list:

> 0.2.0 replaces `create` and `execute` with `handle`, and commands become data. To upgrade:
>
> 1. Add the rejection columns to the command history:
>
>    ```sql
>    ALTER TABLE ddd_command_history ADD COLUMN rejection_type    VARCHAR(255);
>    ALTER TABLE ddd_command_history ADD COLUMN rejection_payload TEXT;
>    ```
>
>    Existing rows read as accepted commands.
> 2. For each aggregate, define a sealed command type and a sealed rejection type, change your command
>    functions to return `accept(...)` or `reject(...)` (they are no longer `suspend`), and route the commands in a
>    `CommandHandlers` object, as in [the quickstart](#2-define-state-events-commands-and-rejections).
> 3. Pass that object as `commands` to `AggregateManager`, and replace `create { }` and `execute<T> { }` calls
>    with `handle(id, command)`.
> 4. Replace `catch (e: UnexpectedAggregateStateException)` with a rejection from `otherwise`, and drop any
>    retry loop around `OptimisticConcurrencyException`: `handle` retries itself.
> 5. If you implemented `DomainPersistenceBackend` yourself, replace `wasCommandHandled` with
>    `findHandledCommand`, add `recordCommandRejected`, and throw `CommandAlreadyRecordedException` when a
>    command id is recorded twice.

Search the rest of the README for leftovers and fix each one:

Run: `grep -n -e 'execute<' -e '\.create(' -e 'execute {' -e 'UnexpectedAggregateStateException' -e 'retryOnConflict' -e 'Pair<' README.md`
Expected after fixing: matches only inside the "Upgrading from 0.1.0" section (`create { }`, `execute<T> { }`, `UnexpectedAggregateStateException`).

- [ ] **Step 7: Verify that the docs match the compiled examples**

Run: `./gradlew :examples:integrationTest`
Expected: PASS. Then compare every README Kotlin snippet touched in Steps 2-5 with its source in `Quickstart.kt`, `QuickstartTest.kt` and `ReadmeExamples.kt`. They must be identical, apart from the leading indentation inside the test function.

- [ ] **Step 8: Commit**

```bash
git add README.md gradle.properties
git commit -m "Document commands, typed rejections and upgrading to 0.2.0

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

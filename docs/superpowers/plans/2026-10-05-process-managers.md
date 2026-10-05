# Process Managers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add process managers: long-running workflows whose states own their inputs, translated from events. They record their own facts, request commands to aggregates of this context (with typed rejection feedback), and schedule inputs to themselves (timeouts), all asynchronously.

**Architecture:**
- **Persistence.** A process instance is persisted by an internal `AggregateManager` whose "commands" are the process's inputs. A wrapper state turns a `ProcessOutcome` into a core `Outcome`:
  - `transition(...)` becomes accept, with the app's events plus envelope events (`CommandRequested`, `InputScheduled`);
  - `ignore()` becomes a reject with an internal `Ignored` marker.
- **The facade.** `ProcessManager` owns three executors (`inputs`, `internal`, `commands`) on queues from a `ProcessManagerQueues` factory, plus one executor per named contract subscription.
- **Its poller.** A `DomainEventPoller` routes the process's own envelopes to the right channel and passes foreign events through `translate`.
- **Command execution.** Commands run through the target's `AggregateManager.handle`, and a typed rejection is mapped back to an input.
- **db-scheduler.** `kotmod-db-scheduler` provides `DbSchedulerProcessManagerQueues`.

**Tech Stack:** Kotlin 2.4.20 (JVM toolchain 25), kotlinx.serialization 1.11.0, kotlinx.coroutines 1.11.0, db-scheduler, Postgres 17 via Testcontainers, JUnit 5 / kotlin.test, Gradle.

**Spec:** `docs/superpowers/specs/2026-10-05-process-managers-design.md` (including its "Implementation notes" section).

## Global Constraints

- Package `io.kotmod.process` for the process manager types; `RequestedCommand` and `AggregateKind.command` in `io.kotmod`.
- `interface ProcessState<S : ProcessState<S, I, E>, I : Any, E : DomainEvent> { suspend fun handle(input: I): ProcessOutcome<S, E, I> }`, and `ProcessInitialState<S, I, E>` with the same `handle`.
- `ProcessOutcome` is built only by `transition(state, events = emptyList(), commands = emptyList(), schedule = emptyList())` and `ignore()`. Inputs can't be rejected.
- `schedule(input, at: kotlin.time.Instant)` builds a `ScheduledInput<I>`. `AggregateKind<C, R>.command(id, command)` builds a `RequestedCommand<C>`.
- `target(manager) { command: C, rejection: R -> input }` builds a `ProcessTarget<I>`. Targets are matched by `kind.type`, and the types must be distinct.
- **Channels:**
  - names `inputs` (ordered if `inputOrdering` is `PerAggregate`), `internal` (unordered), `commands` (unordered), `contract-<name>`;
  - queue task names `<processManagerName>-<channel>` in db-scheduler.
- **Ids:**
  - translated input `in-<eventId>`; scheduled input `sched-<eventId>` (with `notBefore`);
  - requested command reaction `cmd-<eventId>`, with command id `<pmType>-<eventId>` and correlation id `<pmType>/<processId>`;
  - feedback `rejected-<commandId>`.
- **Failures:**
  - Input delivery and command execution failures retry with capped backoff (`BackoffStrategy`) and never give up.
  - A requested command whose kind type is not among the targets throws at decision time, so nothing is recorded.
- The process manager's own domain events are never fed to `translate`.
- Version stays `0.2.0`.
- README Kotlin snippets must be byte-identical to the compiled example files, apart from indentation inside test functions.
- Commit messages follow the repo style (imperative sentence, no prefix) and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. No new compiler warnings.

## Review Focus

1. **A command redelivered after a crash.** Expected: the target records one answer; the feedback input is published once per id and applied once. Pinned in Task 4.
2. **A timeout delivered before it is due** (clock lag, queue early). Expected: it waits without running and runs once due; it is never lost. Pinned in Task 4 (unit) and Task 5 (end to end with a lagging clock).
3. **A command addressed to an unregistered aggregate type.** Expected: input delivery fails loudly and retries, with nothing recorded, not a silently dropped command. Pinned in Tasks 2 and 4.
4. **An input for a process that doesn't exist, which the initial state ignores.** Expected: no instance is created, and the input is recorded so a redelivery is a no-op. Pinned in Task 2.
5. **The process manager reading its own stream.** Expected: its own domain events are not translated (no feedback loop); its envelopes are routed exactly once. Pinned in Task 4.

---

## File Structure

- **Modify** `kotmod/src/main/kotlin/io/kotmod/AggregateKind.kt`: `command(...)`, `RequestedCommand`.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`: `kind` becomes `internal`.
- **Create** `kotmod/src/main/kotlin/io/kotmod/process/ProcessState.kt`: states, outcome, builders.
- **Create** `kotmod/src/main/kotlin/io/kotmod/process/ProcessInstances.kt`: envelopes, serialization, internal manager.
- **Create** `kotmod/src/main/kotlin/io/kotmod/process/ProcessRuntime.kt`: queues interface, triggers, targets, executor factory.
- **Create** `kotmod/src/main/kotlin/io/kotmod/process/ProcessManager.kt`: the facade and poller routing.
- **Create** `kotmod-db-scheduler/src/main/kotlin/io/kotmod/event/reaction/dbscheduler/DbSchedulerProcessManagerQueues.kt`.
- **Modify** `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`: serializable test commands, `testOrders`.
- **Create** `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestProcess.kt`: a test process ("window").
- **Create** tests in `kotmod/src/test/kotlin/io/kotmod/process/` and `kotmod-db-scheduler/src/integrationTest/kotlin/io/kotmod/event/reaction/dbscheduler/ProcessManagerIntegrationTest.kt`.
- **Modify** `examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt` and `README.md`.

---

### Task 1: Process types, requested commands and the test process

**Files:**
- Modify: `kotmod/src/main/kotlin/io/kotmod/AggregateKind.kt`, `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`
- Create: `kotmod/src/main/kotlin/io/kotmod/process/ProcessState.kt`
- Modify: `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt` (`TestOrderCommandSerializer`, `testOrders`)
- Create: `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestProcess.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/process/ProcessStateTest.kt`

**Interfaces:**
- Consumes: `AggregateKind`, `AggregateManager`, the test aggregate fixtures (`OrderCommand`, `PlaceOrder`, `ShipOrder`, `CancelOrder`, `DecideWith`, `OrderRejection`, `testOrderKind`).
- Produces:
  - `AggregateKind<C, R>.command(id: AggregateId, command: C): RequestedCommand<C>`;
  - `data class RequestedCommand<C : Any>(val kind: AggregateKind<C, *>, val targetId: AggregateId, val command: C)` with `internal fun encodeCommand(): String`;
  - `AggregateManager.kind` is `internal val`;
  - in `io.kotmod.process`: `ProcessState`, `ProcessInitialState`, `ProcessOutcome` (`Transition`, `Ignore`), `ScheduledInput`, `transition`, `ignore`, `schedule`;
  - fixtures in `io.kotmod.support`:
    - `val testOrders`;
    - the window process: `WindowInput` (`Opened(orderId, closeAtEpochSeconds)`, `Elapsed`, `Refunded`, `ReleaseBlocked(rejection)`), `WindowEvent` (`WindowClosed(orderId)`), `Window` (`OpenWindow(orderId)`, `ClosedWindow(orderId, blocked: OrderRejection? = null)`), `NoWindow`;
    - `fun windowEventSerialization(): DataSerializationContext<WindowEvent>`.

- [ ] **Step 1: Write the failing tests**

Create `kotmod/src/test/kotlin/io/kotmod/process/ProcessStateTest.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.RequestedCommand
import io.kotmod.support.CancelOrder
import io.kotmod.support.ClosedWindow
import io.kotmod.support.DecideWith
import io.kotmod.support.Elapsed
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.OrderCommand
import io.kotmod.support.OrderNotFound
import io.kotmod.support.PlaceOrder
import io.kotmod.support.Refunded
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShipOrder
import io.kotmod.support.TestOrderCommandSerializer
import io.kotmod.support.WindowClosed
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.time.Instant

class ProcessStateTest {
    @Test
    fun `the initial state starts the process and schedules its timeout`() =
        runBlocking {
            assertEquals(
                ProcessOutcome.Transition(OpenWindow("o-1"), emptyList(), emptyList(), listOf(ScheduledInput(Elapsed, Instant.fromEpochSeconds(100)))),
                NoWindow.handle(Opened("o-1", closeAtEpochSeconds = 100)),
            )
        }

    @Test
    fun `the initial state ignores inputs that don't start the process`() =
        runBlocking {
            assertEquals(ProcessOutcome.Ignore, NoWindow.handle(Elapsed))
        }

    @Test
    fun `a state records a fact and requests a command`() =
        runBlocking {
            assertEquals(
                ProcessOutcome.Transition(
                    ClosedWindow("o-1"),
                    listOf(WindowClosed("o-1")),
                    listOf(RequestedCommand(testOrders, AggregateId("o-1"), ShipOrder)),
                    emptyList(),
                ),
                OpenWindow("o-1").handle(Elapsed),
            )
        }

    @Test
    fun `a closed window records why a command was refused`() =
        runBlocking {
            assertEquals(
                ProcessOutcome.Transition(ClosedWindow("o-1", blocked = OrderNotFound), emptyList(), emptyList(), emptyList()),
                ClosedWindow("o-1").handle(ReleaseBlocked(OrderNotFound)),
            )
            assertEquals(ProcessOutcome.Ignore, ClosedWindow("o-1").handle(Refunded))
        }

    @Test
    fun `a kind builds a requested command for one of its aggregates`() {
        assertEquals(RequestedCommand(testOrders, AggregateId("o-1"), ShipOrder), testOrders.command(AggregateId("o-1"), ShipOrder))
        assertEquals("\"ship\"", testOrders.command(AggregateId("o-1"), ShipOrder).encodeCommand())
    }

    @Test
    fun `test commands round-trip, except the lambda-holding DecideWith`() {
        for (command in listOf<OrderCommand>(PlaceOrder("book"), ShipOrder, CancelOrder("changed mind"))) {
            assertEquals(command, Json.decodeFromString(TestOrderCommandSerializer, Json.encodeToString(TestOrderCommandSerializer, command)))
        }
        assertFails { Json.encodeToString(TestOrderCommandSerializer, DecideWith { error("never") }) }
    }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.ProcessStateTest'`
Expected: compilation FAILS (unresolved `io.kotmod.process`, `RequestedCommand`, window fixtures).

- [ ] **Step 3: Add requested commands and expose the kind**

In `AggregateKind.kt`, give the class a body, and add the data class after it (add the import `kotlinx.serialization.json.Json`):

```kotlin
open class AggregateKind<C : Any, R : Any>(
    val type: AggregateType,
    val commandSerializer: KSerializer<C>,
    val rejectionSerializer: KSerializer<R>,
) {
    /** Requests [command] for the aggregate [id] of this kind; a process manager runs it later, asynchronously. */
    fun command(
        id: AggregateId,
        command: C,
    ): RequestedCommand<C> = RequestedCommand(this, id, command)
}

/** A command a process manager asks to be run against aggregate [targetId] of [kind]. Build it with [AggregateKind.command]. */
data class RequestedCommand<C : Any>(
    val kind: AggregateKind<C, *>,
    val targetId: AggregateId,
    val command: C,
) {
    internal fun encodeCommand(): String = Json.encodeToString(kind.commandSerializer, command)
}
```

In `AggregateManager.kt`, change `private val kind: AggregateKind<C, R>,` to `internal val kind: AggregateKind<C, R>,`.

- [ ] **Step 4: Add the process types**

Create `kotmod/src/main/kotlin/io/kotmod/process/ProcessState.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.DomainEvent
import io.kotmod.RequestedCommand
import kotlin.time.Instant

/**
 * A behavioural position of a process manager, deciding the inputs it receives. Implement it on your sealed process
 * state type and decide every input in [handle]: [transition] to a new state (recording facts, requesting commands and
 * scheduling inputs), or [ignore] it. Inputs are facts, so they can't be rejected.
 *
 * Write the `when` over your sealed input type without an `else`, so adding an input doesn't compile until every state
 * has decided what to do with it. Decisions should be pure; they may suspend, but a decision runs again if its write
 * loses a race.
 *
 * @param S the process's state type (your sealed state type itself).
 * @param I the process's input type.
 * @param E the process's own domain event type.
 */
interface ProcessState<S : ProcessState<S, I, E>, I : Any, E : DomainEvent> {
    /** Decides [input] in this state. */
    suspend fun handle(input: I): ProcessOutcome<S, E, I>
}

/**
 * Decides inputs for a process that doesn't exist yet: [transition] starts it, [ignore] leaves it unstarted (the input
 * is still recorded, so a redelivery is recognised). Your repository never stores it.
 */
interface ProcessInitialState<S : ProcessState<S, I, E>, I : Any, E : DomainEvent> {
    /** Decides [input] for a process that doesn't exist yet. */
    suspend fun handle(input: I): ProcessOutcome<S, E, I>
}

/** What a process state decides about an input. Build it with [transition] or [ignore]. */
sealed interface ProcessOutcome<out S, out E : DomainEvent, out I> {
    /**
     * Move to [state], record [events] in the process's own stream, request [commands] and [schedule] inputs, all in
     * one transaction. The commands and scheduled inputs are carried out later, asynchronously.
     */
    data class Transition<out S, out E : DomainEvent, out I>(
        val state: S,
        val events: List<E>,
        val commands: List<RequestedCommand<*>>,
        val schedule: List<ScheduledInput<I>>,
    ) : ProcessOutcome<S, E, I>

    /** Nothing changes; the input is recorded so a redelivery is recognised. */
    data object Ignore : ProcessOutcome<Nothing, Nothing, Nothing>
}

/** An input the process sends to itself at [at], such as a timeout. */
data class ScheduledInput<out I>(
    val input: I,
    val at: Instant,
)

/** Moves the process to [state], recording [events], requesting [commands] and scheduling [schedule]. */
fun <S, E : DomainEvent, I> transition(
    state: S,
    events: List<E> = emptyList(),
    commands: List<RequestedCommand<*>> = emptyList(),
    schedule: List<ScheduledInput<I>> = emptyList(),
): ProcessOutcome<S, E, I> = ProcessOutcome.Transition(state, events, commands, schedule)

/** Ignores the input: nothing changes. */
fun ignore(): ProcessOutcome<Nothing, Nothing, Nothing> = ProcessOutcome.Ignore

/** Schedules [input] to be delivered to this process instance at [at]. */
fun <I> schedule(
    input: I,
    at: Instant,
): ScheduledInput<I> = ScheduledInput(input, at)
```

- [ ] **Step 5: Make the test commands serializable and add the test process**

In `TestAggregate.kt`, replace `TestOrderCommandSerializer` with the following, and add `val testOrders` after `testOrderKind`:

```kotlin
/** Serializes test commands as short strings, so the test aggregate can be a process manager target; DecideWith can't be. */
object TestOrderCommandSerializer : KSerializer<OrderCommand> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("io.kotmod.support.TestOrderCommand", PrimitiveKind.STRING)

    override fun serialize(
        encoder: Encoder,
        value: OrderCommand,
    ) = encoder.encodeString(
        when (value) {
            is PlaceOrder -> "place:${value.name}"
            ShipOrder -> "ship"
            is CancelOrder -> "cancel:${value.reason}"
            is DecideWith -> error("DecideWith holds a lambda and can't be serialized")
        },
    )

    override fun deserialize(decoder: Decoder): OrderCommand {
        val text = decoder.decodeString()
        return when {
            text == "ship" -> ShipOrder
            text.startsWith("place:") -> PlaceOrder(text.removePrefix("place:"))
            text.startsWith("cancel:") -> CancelOrder(text.removePrefix("cancel:"))
            else -> error("unknown test command $text")
        }
    }
}
```

```kotlin
/** The test order kind, shared so requested commands compare equal. */
val testOrders: AggregateKind<OrderCommand, OrderRejection> = testOrderKind()
```

Create `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestProcess.kt`:

```kotlin
package io.kotmod.support

import io.kotmod.AggregateId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.process.ProcessInitialState
import io.kotmod.process.ProcessOutcome
import io.kotmod.process.ProcessState
import io.kotmod.process.ignore
import io.kotmod.process.schedule
import io.kotmod.process.transition
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlin.time.Instant

// A test process: a window that opens for an order and, when it elapses, asks to ship the order.

@Serializable
sealed interface WindowInput

@Serializable
data class Opened(
    val orderId: String,
    val closeAtEpochSeconds: Long,
) : WindowInput

@Serializable
data object Elapsed : WindowInput

@Serializable
data object Refunded : WindowInput

@Serializable
data class ReleaseBlocked(
    val rejection: OrderRejection,
) : WindowInput

@Serializable
sealed interface WindowEvent : DomainEvent

@Serializable
data class WindowClosed(
    val orderId: String,
) : WindowEvent

typealias WindowOutcome = ProcessOutcome<Window, WindowEvent, WindowInput>

sealed interface Window : ProcessState<Window, WindowInput, WindowEvent>

object NoWindow : ProcessInitialState<Window, WindowInput, WindowEvent> {
    override suspend fun handle(input: WindowInput): WindowOutcome =
        when (input) {
            is Opened -> transition(OpenWindow(input.orderId), schedule = listOf(schedule(Elapsed, Instant.fromEpochSeconds(input.closeAtEpochSeconds))))
            Elapsed, Refunded, is ReleaseBlocked -> ignore()
        }
}

data class OpenWindow(
    val orderId: String,
) : Window {
    override suspend fun handle(input: WindowInput): WindowOutcome =
        when (input) {
            Elapsed ->
                transition(
                    ClosedWindow(orderId),
                    events = listOf(WindowClosed(orderId)),
                    commands = listOf(testOrders.command(AggregateId(orderId), ShipOrder)),
                )
            Refunded -> transition(ClosedWindow(orderId))
            is Opened, is ReleaseBlocked -> ignore()
        }
}

data class ClosedWindow(
    val orderId: String,
    val blocked: OrderRejection? = null,
) : Window {
    override suspend fun handle(input: WindowInput): WindowOutcome =
        when (input) {
            is ReleaseBlocked -> transition(ClosedWindow(orderId, input.rejection))
            is Opened, Elapsed, Refunded -> ignore()
        }
}

fun windowEventSerialization(): DataSerializationContext<WindowEvent> = jsonDataSerializationContext { +WindowClosed.serializer().toEventSerializer() }
```

If the compiler can't infer `transition`'s type arguments in the `NoWindow`/`OpenWindow` branches, give it explicit ones: `transition<Window, WindowEvent, WindowInput>(...)`. Record that in your report: it affects how the README example is written.

- [ ] **Step 6: Run the tests**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.ProcessStateTest'`
Expected: PASS (6 tests).

Run: `./gradlew :kotmod:test :kotmod:integrationTest :kotmod-sqldelight:integrationTest`
Expected: PASS, with no `w:` lines. The test kind now serializes, which changes nothing for existing tests.

- [ ] **Step 7: Commit**

```bash
git add -A kotmod
git commit -m "Add process manager states, outcomes and requested commands

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Persisting process instances

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/process/ProcessInstances.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/process/ProcessInstancesTest.kt`

**Interfaces:**
- Consumes: Task 1's types; `AggregateManager`, `AggregateKind`, `AggregateState`, `InitialState`, `Outcome`, `reject`, `Repository`, `DomainPersistenceBackend`, `DataSerializationContext`, `SerializedEvent`; test support `StubPersistenceBackend`, `StubRepository`.
- Produces (all `internal`, package `io.kotmod.process`):
  - `sealed interface ProcessEnvelope : DomainEvent`, with `data class CommandRequested(targetType: String, targetId: String, command: String)` and `data class InputScheduled(input: String, at: String)`;
  - `class ProcessEventSerialization<E : DomainEvent>(events: DataSerializationContext<E>) : DataSerializationContext<DomainEvent>` with constants `COMMAND_REQUESTED = "io.kotmod.process.CommandRequested"` and `INPUT_SCHEDULED = "io.kotmod.process.InputScheduled"` in its companion;
  - `class ProcessInstances<S, I, E>(type, repository: Repository<S>, backend: DomainPersistenceBackend<DomainEvent>, initial: ProcessInitialState<S, I, E>, inputSerializer: KSerializer<I>, targetTypes: Set<AggregateType>)` with `suspend fun deliver(processId: AggregateId, input: I, inputId: String)`.

- [ ] **Step 1: Write the failing tests**

Create `kotmod/src/test/kotlin/io/kotmod/process/ProcessInstancesTest.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DomainEvent
import io.kotmod.HandledCommand
import io.kotmod.SerializedEvent
import io.kotmod.support.ClosedWindow
import io.kotmod.support.Elapsed
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.Window
import io.kotmod.support.WindowClosed
import io.kotmod.support.WindowInput
import io.kotmod.support.windowEventSerialization
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class ProcessInstancesTest {
    private val type = AggregateType("Window")
    private val id = AggregateId("window-o-1")
    private val backend = StubPersistenceBackend<DomainEvent>()
    private val repository = StubRepository<Window>()

    private fun instances(targetTypes: Set<AggregateType> = setOf(AggregateType("Order"))) =
        ProcessInstances(type, repository, backend, NoWindow, WindowInput.serializer(), targetTypes)

    @Test
    fun `the first input starts the process and records its scheduled input`() =
        runBlocking {
            instances().deliver(id, Opened("o-1", closeAtEpochSeconds = 100), "in-e-1")

            assertEquals(OpenWindow("o-1"), repository.store[id])
            assertEquals(
                listOf(InputScheduled(input = """{"type":"io.kotmod.support.Elapsed"}""", at = "1970-01-01T00:01:40Z")),
                backend.events.map { it.event },
            )
        }

    @Test
    fun `an ignored input for a process that doesn't exist creates nothing but is recorded`() =
        runBlocking {
            instances().deliver(id, Elapsed, "in-e-1")

            assertNull(repository.store[id])
            assertNull(backend.metas[StubPersistenceBackend.Key(type, id)])
            assertIs<HandledCommand.Rejected>(backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("in-e-1"))])
        }

    @Test
    fun `a redelivered input is not applied twice`() =
        runBlocking {
            instances().deliver(id, Opened("o-1", 100), "in-e-1")
            instances().deliver(id, Opened("o-1", 100), "in-e-1")

            assertEquals(1, backend.events.size)
        }

    @Test
    fun `a transition records the app's events and the requested command`() =
        runBlocking {
            instances().deliver(id, Opened("o-1", 100), "in-e-1")
            instances().deliver(id, Elapsed, "sched-e-2")

            assertEquals(ClosedWindow("o-1"), repository.store[id])
            assertEquals(
                listOf(WindowClosed("o-1"), CommandRequested(targetType = "Order", targetId = "o-1", command = "\"ship\"")),
                backend.events.drop(1).map { it.event },
            )
        }

    @Test
    fun `a command for an unregistered aggregate type fails before anything is recorded`() =
        runBlocking {
            instances(targetTypes = emptySet()).deliver(id, Opened("o-1", 100), "in-e-1")

            assertFailsWith<IllegalArgumentException> { instances(targetTypes = emptySet()).deliver(id, Elapsed, "sched-e-2") }
            assertEquals(OpenWindow("o-1"), repository.store[id])
            assertNull(backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("sched-e-2"))])
        }

    @Test
    fun `envelopes round-trip and the app's events go through its own serialization`() {
        val serialization = ProcessEventSerialization(windowEventSerialization())
        val requested = CommandRequested("Order", "o-1", "\"ship\"")
        val scheduled = InputScheduled("{}", "2026-10-05T10:00:00Z")

        assertEquals(ProcessEventSerialization.COMMAND_REQUESTED, serialization.serialize(requested).type)
        assertEquals(requested, serialization.deserialize(serialization.serialize(requested)))
        assertEquals(scheduled, serialization.deserialize(serialization.serialize(scheduled)))
        assertEquals(WindowClosed("o-1"), serialization.deserialize(serialization.serialize(WindowClosed("o-1"))))
        assertEquals(windowEventSerialization().serialize(WindowClosed("o-1")), serialization.serialize(WindowClosed("o-1")))
    }
}
```

The exact JSON for a serialized `Elapsed` depends on kotlinx's polymorphic encoding of a sealed `data object`. If the first test's expected `input` string differs, assert instead that `Json.decodeFromString(WindowInput.serializer(), scheduled.input) == Elapsed` and `scheduled.at == "1970-01-01T00:01:40Z"`. Record what you did.

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.ProcessInstancesTest'`
Expected: compilation FAILS (unresolved `ProcessInstances`, `InputScheduled`, `CommandRequested`, `ProcessEventSerialization`).

- [ ] **Step 3: Implement it**

Create `kotmod/src/main/kotlin/io/kotmod/process/ProcessInstances.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateKind
import io.kotmod.AggregateManager
import io.kotmod.AggregateState
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainPersistenceBackend
import io.kotmod.InitialState
import io.kotmod.Outcome
import io.kotmod.Repository
import io.kotmod.SerializedEvent
import io.kotmod.reject
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/** kotmod's own events in a process manager's stream: its intents, carried out later by its channels. */
internal sealed interface ProcessEnvelope : DomainEvent

/** The process asked for [command] (JSON) to be run against aggregate [targetType]/[targetId]. */
@Serializable
internal data class CommandRequested(
    val targetType: String,
    val targetId: String,
    val command: String,
) : ProcessEnvelope

/** The process scheduled [input] (JSON) to be delivered to itself at [at] (ISO-8601). */
@Serializable
internal data class InputScheduled(
    val input: String,
    val at: String,
) : ProcessEnvelope

/** Stores a process manager's stream: kotmod's envelopes itself, everything else through the app's [events]. */
internal class ProcessEventSerialization<E : DomainEvent>(
    private val events: DataSerializationContext<E>,
) : DataSerializationContext<DomainEvent> {
    override fun serialize(event: DomainEvent): SerializedEvent =
        when (event) {
            is CommandRequested -> SerializedEvent(COMMAND_REQUESTED, 1, Json.encodeToString(CommandRequested.serializer(), event))
            is InputScheduled -> SerializedEvent(INPUT_SCHEDULED, 1, Json.encodeToString(InputScheduled.serializer(), event))
            else -> {
                @Suppress("UNCHECKED_CAST")
                events.serialize(event as E)
            }
        }

    override fun deserialize(serialized: SerializedEvent): DomainEvent =
        when (serialized.type) {
            COMMAND_REQUESTED -> Json.decodeFromString(CommandRequested.serializer(), serialized.payload)
            INPUT_SCHEDULED -> Json.decodeFromString(InputScheduled.serializer(), serialized.payload)
            else -> events.deserialize(serialized)
        }

    companion object {
        const val COMMAND_REQUESTED = "io.kotmod.process.CommandRequested"
        const val INPUT_SCHEDULED = "io.kotmod.process.InputScheduled"
    }
}

/** The core rejection that records an ignored input. */
@Serializable
internal data object Ignored

/** Turns a process decision into a core outcome: a transition is accepted with its events and envelopes; ignore is a rejection. */
internal class ProcessDecider<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    private val inputSerializer: KSerializer<I>,
    private val targetTypes: Set<AggregateType>,
) {
    fun toOutcome(outcome: ProcessOutcome<S, E, I>): Outcome<Held<S, I, E>, DomainEvent, Ignored> =
        when (outcome) {
            ProcessOutcome.Ignore -> reject(Ignored)
            is ProcessOutcome.Transition -> {
                outcome.commands.forEach { requested ->
                    require(requested.kind.type in targetTypes) {
                        "The process requested a command for aggregate type ${requested.kind.type.value}, which isn't one " +
                            "of its targets: register it with target(manager) { … }"
                    }
                }
                val envelopes =
                    outcome.commands.map { CommandRequested(it.kind.type.value, it.targetId.value, it.encodeCommand()) } +
                        outcome.schedule.map { InputScheduled(Json.encodeToString(inputSerializer, it.input), it.at.toString()) }
                Outcome.Accept(Held(outcome.state, this), outcome.events + envelopes)
            }
        }
}

/** A process state seen by the core as an aggregate state. */
internal class Held<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    val state: S,
    private val decider: ProcessDecider<S, I, E>,
) : AggregateState<Held<S, I, E>, I, DomainEvent, Ignored> {
    override suspend fun handle(command: I): Outcome<Held<S, I, E>, DomainEvent, Ignored> = decider.toOutcome(state.handle(command))
}

private class HeldInitial<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    private val initial: ProcessInitialState<S, I, E>,
    private val decider: ProcessDecider<S, I, E>,
) : InitialState<Held<S, I, E>, I, DomainEvent, Ignored> {
    override suspend fun handle(command: I): Outcome<Held<S, I, E>, DomainEvent, Ignored> = decider.toOutcome(initial.handle(command))
}

private class HeldRepository<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    private val repository: Repository<S>,
    private val decider: ProcessDecider<S, I, E>,
) : Repository<Held<S, I, E>> {
    override fun get(id: AggregateId): Held<S, I, E>? = repository.get(id)?.let { Held(it, decider) }

    override fun save(
        id: AggregateId,
        state: Held<S, I, E>,
    ) = repository.save(id, state.state)
}

/** Persists process instances of one [type]: each instance is an aggregate whose commands are its inputs. */
internal class ProcessInstances<S : ProcessState<S, I, E>, I : Any, E : DomainEvent>(
    type: AggregateType,
    repository: Repository<S>,
    backend: DomainPersistenceBackend<DomainEvent>,
    initial: ProcessInitialState<S, I, E>,
    inputSerializer: KSerializer<I>,
    targetTypes: Set<AggregateType>,
) {
    private val decider = ProcessDecider<S, I, E>(inputSerializer, targetTypes)
    private val manager =
        AggregateManager(
            AggregateKind(type, inputSerializer, Ignored.serializer()),
            HeldRepository(repository, decider),
            backend,
            HeldInitial(initial, decider),
        )

    /** Applies [input] to process [processId] once: a redelivery with the same [inputId] is recognised and skipped. */
    suspend fun deliver(
        processId: AggregateId,
        input: I,
        inputId: String,
    ) {
        manager.handle(processId, input, commandId = CommandId(inputId))
    }
}
```

If `when (outcome)` doesn't smart-cast `outcome.state`/`events`/`commands`/`schedule`, cast to `ProcessOutcome.Transition<S, E, I>`. The `@Suppress("UNCHECKED_CAST")` is needed: the composite context receives the app's events as `DomainEvent`.

- [ ] **Step 4: Run the tests**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.*'`
Expected: PASS. No `w:` lines.

- [ ] **Step 5: Commit**

```bash
git add -A kotmod
git commit -m "Persist process instances as aggregates of their inputs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Queues, triggers, targets and the executor factory

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/process/ProcessRuntime.kt`
- Create: `kotmod/src/test/kotlin/io/kotmod/process/ManualQueues.kt` (test helper)
- Test: `kotmod/src/test/kotlin/io/kotmod/process/ProcessRuntimeTest.kt`

**Interfaces:**
- Consumes: `EventReactionExecutor` (its constructor incl. `clock`), `EventReactionTrigger`, `EventReactionTriggerSerializer`, `EventReactionTriggerSink`, `EventReactionTriggerSource`, `ReactionOutcome`, `RetrySignal`, `BackoffStrategy`; `AggregateManager.handle` and `internal kind`; `CommandResult`.
- Produces:
  - Public:
    - `interface ProcessManagerQueues { fun <T : EventReactionTrigger> channel(name: String, triggerSerializer: EventReactionTriggerSerializer<T>, ordered: Boolean): ProcessChannel<T> }`;
    - `class ProcessChannel<T : EventReactionTrigger>(val sink: EventReactionTriggerSink<T>, val source: EventReactionTriggerSource<T>)`;
    - `class ProcessTarget<I : Any>` (built only by `target`);
    - `fun <C : Any, R : Any, I : Any> target(manager: AggregateManager<*, C, *, R>, onRejected: (command: C, rejection: R) -> I): ProcessTarget<I>`.
  - Internal:
    - `data class InputTrigger(processId, input, inputId)` and `data class CommandTrigger(processId, targetType, targetId, command, commandId)`, both `EventReactionTrigger`s with a `@Transient` `timeout = null`;
    - `class JsonTriggerSerializer<T>(serializer: KSerializer<T>)`;
    - `ProcessTarget.type: AggregateType`, `ProcessTarget.send(targetId, commandJson, commandId, correlationId): I?`;
    - `fun <T : EventReactionTrigger> processExecutor(channel: ProcessChannel<T>, clock: () -> Instant, run: suspend (T) -> Unit): EventReactionExecutor<T, Unit>`.
  - Test helper `ManualQueues` (described in Step 1).

- [ ] **Step 1: Write the test helper and the failing tests**

Create `kotmod/src/test/kotlin/io/kotmod/process/ManualQueues.kt`, an in-memory `ProcessManagerQueues` that tests drive by hand:

```kotlin
package io.kotmod.process

import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionExecutionId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.event.reaction.RetryCount
import kotlin.time.Instant

/**
 * In-memory queues for tests: a publish of an id that is still pending is ignored, as real queues do. [deliver] runs
 * every pending reaction of a channel once and removes the ones that finish.
 */
class ManualQueues(
    private val supportsOrdering: Boolean = true,
) : ProcessManagerQueues {
    data class Published(
        val channel: String,
        val id: EventReactionId,
        val trigger: EventReactionTrigger,
        val ordering: DispatchOrdering?,
        val notBefore: Instant?,
    )

    val published = mutableListOf<Published>()
    private val pending = linkedMapOf<Pair<String, EventReactionId>, Published>()
    private val handlers =
        mutableMapOf<String, suspend (EventReactionId, EventReactionExecutionId, EventReactionTrigger, RetryCount, Instant?) -> ReactionOutcome>()
    val channels = mutableListOf<Pair<String, Boolean>>()

    override fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ProcessChannel<T> {
        channels += name to ordered
        val sink =
            object : EventReactionTriggerSink<T> {
                override val supportsOrdering = this@ManualQueues.supportsOrdering

                override suspend fun publish(
                    id: EventReactionId,
                    trigger: T,
                    ordering: DispatchOrdering?,
                    notBefore: Instant?,
                ) {
                    val entry = Published(name, id, trigger, ordering, notBefore)
                    published += entry
                    pending.putIfAbsent(name to id, entry)
                }
            }
        val source =
            object : EventReactionTriggerSource<T> {
                override fun subscribe(
                    block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, Instant?) -> ReactionOutcome,
                ): Cancellable {
                    handlers[name] = { id, executionId, trigger, retryCount, notBefore ->
                        @Suppress("UNCHECKED_CAST")
                        block(id, executionId, trigger as T, retryCount, notBefore)
                    }
                    return object : Cancellable {
                        override fun cancel() {
                            handlers.remove(name)
                        }
                    }
                }
            }
        return ProcessChannel(sink, source)
    }

    fun pending(channel: String): List<Published> = pending.values.filter { it.channel == channel }

    /** Delivers every pending reaction of [channel] once, in publish order; finished ones are removed. */
    suspend fun deliver(channel: String): List<ReactionOutcome> =
        pending(channel).map { entry ->
            val handler = checkNotNull(handlers[channel]) { "no executor subscribed to $channel" }
            val outcome = handler(entry.id, EventReactionExecutionId("x-${entry.id.value}"), entry.trigger, 0, entry.notBefore)
            if (outcome is ReactionOutcome.Finished) pending.remove(channel to entry.id)
            outcome
        }

    /** Puts a finished reaction back, as a queue redelivering it after a crash would. */
    fun redeliver(entry: Published) {
        pending[entry.channel to entry.id] = entry
    }
}
```

Create `kotmod/src/test/kotlin/io/kotmod/process/ProcessRuntimeTest.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.support.NoOrder
import io.kotmod.support.Order
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.WindowInput
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Instant

class ProcessRuntimeTest {
    private val orderRepository = StubRepository<Order>()
    private val orders = AggregateManager(testOrders, orderRepository, StubPersistenceBackend<OrderEvent>(), NoOrder)
    private val payoutTarget: ProcessTarget<WindowInput> = target(orders) { _, rejection -> ReleaseBlocked(rejection) }

    @Test
    fun `a target runs an accepted command and returns no feedback`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("o-1"))

            val feedback = payoutTarget.send(AggregateId("o-1"), "\"ship\"", CommandId("Window-e-9"), CorrelationId("Window/window-o-1"))

            assertNull(feedback)
            assertEquals(ShippedOrder("o-1"), orderRepository.store[AggregateId("o-1")])
            assertEquals(testOrders.type, payoutTarget.type)
        }

    @Test
    fun `a target maps a typed rejection to an input`() =
        runBlocking {
            assertEquals(
                ReleaseBlocked(OrderNotFound),
                payoutTarget.send(AggregateId("o-1"), "\"ship\"", CommandId("Window-e-9"), CorrelationId("Window/window-o-1")),
            )
        }

    @Test
    fun `triggers round-trip through their JSON serializers`() =
        runBlocking {
            val inputs = JsonTriggerSerializer(InputTrigger.serializer())
            val commands = JsonTriggerSerializer(CommandTrigger.serializer())
            val input = InputTrigger("window-o-1", "{}", "in-e-1")
            val command = CommandTrigger("window-o-1", "Order", "o-1", "\"ship\"", "Window-e-2")

            assertEquals(input, inputs.deserialize(inputs.serialize(input)))
            assertEquals(command, commands.deserialize(commands.serialize(command)))
        }

    @Test
    fun `a process executor completes a run, retries a failure and waits for notBefore`() =
        runBlocking {
            val queues = ManualQueues()
            val now = Instant.parse("2026-10-05T10:00:00Z")
            var fail = true
            val runs = mutableListOf<String>()
            val executor =
                processExecutor(queues.channel("internal", JsonTriggerSerializer(InputTrigger.serializer()), ordered = false), clock = { now }) {
                    runs += it.inputId
                    if (fail) error("boom")
                }
            executor.start()

            executor.dispatch(EventReactionId("a"), InputTrigger("p", "{}", "a"))
            assertIs<ReactionOutcome.Retry>(queues.deliver("internal").single())
            fail = false
            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = false)), queues.deliver("internal"))

            executor.dispatch(EventReactionId("later"), InputTrigger("p", "{}", "later"), notBefore = Instant.parse("2026-10-05T10:05:00Z"))
            assertIs<ReactionOutcome.Wait>(queues.deliver("internal").single())
            assertEquals(listOf("a", "a"), runs)
        }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.ProcessRuntimeTest'`
Expected: compilation FAILS (unresolved `ProcessManagerQueues`, `target`, triggers, `processExecutor`).

- [ ] **Step 3: Implement it**

Create `kotmod/src/main/kotlin/io/kotmod/process/ProcessRuntime.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CommandResult
import io.kotmod.CorrelationId
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.RetrySignal
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Instant

/**
 * Provides the queues a [ProcessManager]'s channels run on. A process manager asks for a channel per name; names are
 * stable across restarts. `kotmod-db-scheduler` provides `DbSchedulerProcessManagerQueues`; any queue that implements
 * an event reaction sink and source works.
 */
interface ProcessManagerQueues {
    /** Returns the queue for channel [name]. An [ordered] channel's sink must support ordering. */
    fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ProcessChannel<T>
}

/** One queue for a process manager channel: where reactions are published, and where they are delivered from. */
class ProcessChannel<T : EventReactionTrigger>(
    val sink: EventReactionTriggerSink<T>,
    val source: EventReactionTriggerSource<T>,
)

/** Delivers [input] (JSON) to process [processId], recognised by [inputId] if delivered again. */
@Serializable
internal data class InputTrigger(
    val processId: String,
    val input: String,
    val inputId: String,
    @Transient override val timeout: Duration? = null,
) : EventReactionTrigger

/** Runs [command] (JSON) against aggregate [targetType]/[targetId] with command id [commandId], for process [processId]. */
@Serializable
internal data class CommandTrigger(
    val processId: String,
    val targetType: String,
    val targetId: String,
    val command: String,
    val commandId: String,
    @Transient override val timeout: Duration? = null,
) : EventReactionTrigger

internal class JsonTriggerSerializer<T : EventReactionTrigger>(
    private val serializer: KSerializer<T>,
) : EventReactionTriggerSerializer<T> {
    override suspend fun serialize(trigger: T): String = Json.encodeToString(serializer, trigger)

    override suspend fun deserialize(serializedTrigger: String): T = Json.decodeFromString(serializer, serializedTrigger)
}

/** An aggregate a process manager may send commands to, with how its rejections come back as inputs. Build it with [target]. */
class ProcessTarget<I : Any> internal constructor(
    internal val type: AggregateType,
    internal val send: suspend (targetId: AggregateId, command: String, commandId: CommandId, correlationId: CorrelationId) -> I?,
)

/**
 * Lets a process manager send commands to [manager]'s aggregates. When one of them rejects a command, [onRejected] turns
 * the command and its typed rejection into an input in the process's own language, which is delivered back to the
 * process instance that asked.
 */
fun <C : Any, R : Any, I : Any> target(
    manager: AggregateManager<*, C, *, R>,
    onRejected: (command: C, rejection: R) -> I,
): ProcessTarget<I> =
    ProcessTarget(manager.kind.type) { targetId, json, commandId, correlationId ->
        val command = Json.decodeFromString(manager.kind.commandSerializer, json)
        when (val result = manager.handle(targetId, command, commandId, correlationId)) {
            is CommandResult.Accepted -> null
            is CommandResult.Rejected -> onRejected(command, result.rejection)
        }
    }

private val log = LoggerFactory.getLogger(ProcessManager::class.java)

/**
 * An executor for one process manager channel: [run] does the work; any exception is retried with capped backoff and
 * never given up, so an input or command is never silently dropped.
 */
internal fun <T : EventReactionTrigger> processExecutor(
    channel: ProcessChannel<T>,
    clock: () -> Instant,
    run: suspend (T) -> Unit,
): EventReactionExecutor<T, Unit> {
    val backoff = BackoffStrategy()
    return EventReactionExecutor(
        sink = channel.sink,
        source = channel.source,
        createExecutionContext = { _, _ -> },
        execute = { _, _, trigger, _, _ ->
            run(trigger)
            EventReactionExecutionResult.EventReactionExecutionCompleted
        },
        failureRetryHandler = { id, _, _, retryCount, _, ex ->
            log.error("Process manager reaction ${id.value} failed and will be retried [ totalRetries=$retryCount ]", ex)
            RetrySignal.Retry(backoff.calculateBackoff(retryCount))
        },
        timeoutRetryHandler = { _, _, _, retryCount, _ -> RetrySignal.Retry(backoff.calculateBackoff(retryCount)) },
        onCompletion = { _, _, _, _, _, _ -> },
        clock = clock,
    )
}
```

`ProcessManager` doesn't exist until Task 4. Until then, use `LoggerFactory.getLogger("io.kotmod.process.ProcessManager")` (a string). Keep it as the string; no change is needed in Task 4.

`AggregateManager<*, C, *, R>` is a star projection with a bounded first parameter. If the compiler rejects calling `handle` on it, declare the function with explicit type parameters instead: `fun <S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any, I : Any> target(manager: AggregateManager<S, C, E, R>, onRejected: (C, R) -> I)`. Record it in your report; callers' code doesn't change.

- [ ] **Step 4: Run the tests**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.*'`
Expected: PASS. No `w:` lines.

- [ ] **Step 5: Commit**

```bash
git add -A kotmod
git commit -m "Add process manager queues, targets and channel executors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The ProcessManager facade

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/process/ProcessManager.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/process/ProcessManagerTest.kt`

**Interfaces:**
- Consumes:
  - Tasks 1–3;
  - `DomainEventPoller` (internal, `io.kotmod.outbox`), `DomainEventPollingBackend`, `PersistedEvent`, `EventLogPosition`, `ReactionOrdering`, `stampFor` (internal), `claimOrderedSource` (internal), `EventReaction`, `EventReactionId`;
  - `PublicEventContract.subscribe`, `PublicEventEnvelope`, `PublicDomainEvent`;
  - `PostgresDomainPersistenceBackend`, `PostgresDomainPollingBackend`, `JdbcContext`;
  - test support `persistedEvent`, `StubPersistenceBackend`, `StubRepository`.
- Produces: `class ProcessManager<S, I, E>`, with:
  - a public constructor `(type, repository, jdbc, initial, inputSerializer, eventSerialization, translate, targets, queues, inputOrdering = Unordered, getPosition, savePosition, isLeader, pollInterval = 500.milliseconds, batchSize = 100, clock = { Clock.System.now() })`;
  - an internal constructor taking `persistence: DomainPersistenceBackend<DomainEvent>` and `polling: DomainEventPollingBackend` instead of `jdbc`;
  - `fun <P : PublicDomainEvent> subscribeTo(name: String, contract: PublicEventContract<*, P>, translate: (PublicEventEnvelope<P>) -> Pair<AggregateId, I>?)`;
  - `fun start()`, `suspend fun stop()`;
  - internal `suspend fun tickForTest()`, `fun startExecutorsForTest()`.

- [ ] **Step 1: Write the failing tests**

Create `kotmod/src/test/kotlin/io/kotmod/process/ProcessManagerTest.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.support.ClosedWindow
import io.kotmod.support.NoOrder
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.Order
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.Window
import io.kotmod.support.WindowInput
import io.kotmod.support.persistedEvent
import io.kotmod.support.testOrders
import io.kotmod.support.windowEventSerialization
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Instant

class ProcessManagerTest {
    private val windowType = AggregateType("Window")
    private val closeAt = Instant.parse("2026-10-05T10:30:00Z")
    private var now = Instant.parse("2026-10-05T10:00:00Z")
    private val queues = ManualQueues()
    private val processBackend = StubPersistenceBackend<DomainEvent>()
    private val windows = StubRepository<Window>()
    private val orderRepository = StubRepository<Order>()
    private val orderBackend = StubPersistenceBackend<OrderEvent>()
    private val orders = AggregateManager(testOrders, orderRepository, orderBackend, NoOrder)
    private val log = EventLog()
    private var position = EventLogPosition.START
    private val translated = mutableListOf<EventId>()

    /** The event log the poller reads: foreign events added by tests, plus the process's own stream copied from its backend. */
    private inner class EventLog : DomainEventPollingBackend {
        val events = mutableListOf<PersistedEvent>()
        private var copied = 0

        fun add(event: PersistedEvent) {
            val offset = events.size + 1L
            events += event.copy(position = EventLogPosition(offset, offset))
        }

        fun copyProcessStream() {
            val serialization = ProcessEventSerialization(windowEventSerialization())
            processBackend.events.drop(copied).forEach { pending ->
                add(persistedEvent(globalOffset = 0).copy(metadata = pending.metadata, serialized = serialization.serialize(pending.event)))
            }
            copied = processBackend.events.size
        }

        override fun readEventsAfter(
            position: EventLogPosition,
            limit: Int,
        ): List<PersistedEvent> = events.filter { it.position > position }.take(limit)
    }

    private fun manager(
        targets: List<ProcessTarget<WindowInput>> = listOf(target(orders) { _, rejection -> ReleaseBlocked(rejection) }),
        inputOrdering: ReactionOrdering = ReactionOrdering.Unordered,
    ) = ProcessManager(
        type = windowType,
        repository = windows,
        persistence = processBackend,
        polling = log,
        initial = NoWindow,
        inputSerializer = WindowInput.serializer(),
        eventSerialization = windowEventSerialization(),
        translate = { event ->
            translated += event.metadata.eventId
            if (event.metadata.aggregateType == AggregateType("Order")) {
                AggregateId("window-${event.metadata.aggregateId.value}") to Opened(event.metadata.aggregateId.value, closeAt.epochSeconds)
            } else {
                null
            }
        },
        targets = targets,
        queues = queues,
        inputOrdering = inputOrdering,
        getPosition = { position },
        savePosition = { position = it },
        isLeader = { true },
        clock = { now },
    ).also { it.startExecutorsForTest() }

    private suspend fun ProcessManager<Window, WindowInput, *>.openWindowFor(orderId: String) {
        log.add(persistedEvent(globalOffset = 0, eventId = "e-$orderId", aggregateType = "Order", aggregateId = orderId))
        tickForTest()
        queues.deliver("inputs")
        log.copyProcessStream()
        tickForTest()
    }

    private suspend fun ProcessManager<Window, WindowInput, *>.elapse() {
        now = closeAt
        queues.deliver("internal")
        log.copyProcessStream()
        tickForTest()
    }

    @Test
    fun `the manager asks for its three channels`() {
        manager()
        assertEquals(listOf("inputs" to false, "internal" to false, "commands" to false), queues.channels)
    }

    @Test
    fun `a translated event starts the process and its timeout is scheduled with notBefore`() =
        runBlocking {
            val pm = manager()

            pm.openWindowFor("o-1")

            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
            val scheduled = queues.pending("internal").single()
            assertTrue(scheduled.id.value.startsWith("sched-"))
            assertEquals(closeAt, scheduled.notBefore)
        }

    @Test
    fun `a timeout delivered early waits, then runs and requests the command`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")

            assertIs<ReactionOutcome.Wait>(queues.deliver("internal").single())
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])

            pm.elapse()

            assertEquals(ClosedWindow("o-1"), windows.store[AggregateId("window-o-1")])
            val command = queues.pending("commands").single()
            assertTrue(command.id.value.startsWith("cmd-"))
            assertEquals("\"ship\"", (command.trigger as CommandTrigger).command)
        }

    @Test
    fun `an accepted command changes the target and returns nothing to the process`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("o-1"))
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            queues.deliver("commands")

            assertEquals(ShippedOrder("o-1"), orderRepository.store[AggregateId("o-1")])
            assertTrue(queues.pending("internal").none { it.id.value.startsWith("rejected-") })
        }

    @Test
    fun `a rejected command comes back to the process as its own input`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            queues.deliver("commands")
            queues.deliver("internal")

            assertEquals(ClosedWindow("o-1", blocked = OrderNotFound), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `a redelivered command gets the same answer and its feedback is applied once`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()
            val command = queues.pending("commands").single()

            queues.deliver("commands")
            queues.redeliver(command)
            queues.deliver("commands")

            assertEquals(1, orderBackend.commands.size)
            assertEquals(1, queues.pending("internal").count { it.id.value.startsWith("rejected-") })
            queues.deliver("internal")
            assertEquals(ClosedWindow("o-1", blocked = OrderNotFound), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `the process's own events are never translated`() =
        runBlocking {
            val pm = manager()
            pm.openWindowFor("o-1")
            pm.elapse()

            assertEquals(listOf(EventId("e-o-1")), translated)
        }

    @Test
    fun `a command for an unregistered aggregate type is retried, and nothing is recorded`() =
        runBlocking {
            val pm = manager(targets = emptyList())
            pm.openWindowFor("o-1")
            now = closeAt

            assertIs<ReactionOutcome.Retry>(queues.deliver("internal").single())
            assertEquals(OpenWindow("o-1"), windows.store[AggregateId("window-o-1")])
        }

    @Test
    fun `ordered inputs are stamped with their source aggregate`() =
        runBlocking {
            val pm = manager(inputOrdering = ReactionOrdering.PerAggregate())
            log.add(persistedEvent(globalOffset = 0, eventId = "e-1", aggregateType = "Order", aggregateId = "o-1"))

            pm.tickForTest()

            assertEquals("Order/o-1", queues.pending("inputs").single().ordering?.key)
            assertEquals(listOf("inputs" to true, "internal" to false, "commands" to false), queues.channels)
        }

    @Test
    fun `two targets for the same aggregate type are refused`() {
        assertFailsWith<IllegalArgumentException> {
            manager(targets = listOf(target(orders) { _, r -> ReleaseBlocked(r) }, target(orders) { _, r -> ReleaseBlocked(r) }))
        }
    }
}
```

The `persistedEvent` fixture (internal, `kotmod` test source set) builds a `PersistedEvent` with the given `eventId`, `aggregateType` and `aggregateId`; `EventLog.add` renumbers positions. If `PersistedEvent` has no `copy` because it isn't a data class, construct it directly with the same fields.

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.ProcessManagerTest'`
Expected: compilation FAILS (unresolved `ProcessManager`).

- [ ] **Step 3: Implement it**

Create `kotmod/src/main/kotlin/io/kotmod/process/ProcessManager.kt`:

```kotlin
package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.DomainPersistenceBackend
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.PublicEventEnvelope
import io.kotmod.Repository
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.stampFor
import io.kotmod.jdbc.JdbcContext
import io.kotmod.outbox.DomainEventPoller
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

/**
 * A long-running workflow: it reacts to events (translated into its own inputs), keeps its own state per process
 * instance, records its own facts, requests commands to aggregates of this context, and schedules inputs to itself,
 * never doing anything synchronously.
 *
 * - [translate] is the anti-corruption layer for this context's events: it turns an event into the process id and
 *   input it is for, or `null` to ignore it. [subscribeTo] does the same for another context's public events.
 * - Each instance is decided by its state ([ProcessState.handle]), or by [initial] for an instance that doesn't exist
 *   yet. A transition is recorded in one transaction: the state, the process's own events, the commands it requests
 *   and the inputs it schedules.
 * - Requested commands are run against [targets] through their `AggregateManager.handle`; a rejection comes back as an
 *   input, via the target's mapping.
 * - Scheduled inputs (timeouts) are delivered at their time.
 *
 * It reads the event log with its own poller (only while [isLeader]), and runs its work on channels from [queues]:
 * `inputs`, `internal` (scheduled inputs and rejection feedback), `commands`, and one per [subscribeTo]. Failures are
 * retried with capped backoff and never given up. Start it before the queue's scheduler, and stop it after.
 *
 * @param type the process manager's aggregate type; its instances and events are recorded under it.
 * @param repository stores each instance's state, in the same transaction as its events.
 * @param inputOrdering whether inputs from one source aggregate are delivered in that aggregate's order.
 */
class ProcessManager<S : ProcessState<S, I, E>, I : Any, E : DomainEvent> internal constructor(
    private val type: AggregateType,
    repository: Repository<S>,
    persistence: DomainPersistenceBackend<DomainEvent>,
    polling: DomainEventPollingBackend,
    initial: ProcessInitialState<S, I, E>,
    private val inputSerializer: KSerializer<I>,
    eventSerialization: DataSerializationContext<E>,
    private val translate: (PersistedEvent) -> Pair<AggregateId, I>?,
    targets: List<ProcessTarget<I>>,
    private val queues: ProcessManagerQueues,
    private val inputOrdering: ReactionOrdering,
    getPosition: () -> EventLogPosition,
    savePosition: (EventLogPosition) -> Unit,
    isLeader: () -> Boolean,
    pollInterval: Duration,
    batchSize: Int,
    private val clock: () -> Instant,
) {
    constructor(
        type: AggregateType,
        repository: Repository<S>,
        jdbc: JdbcContext,
        initial: ProcessInitialState<S, I, E>,
        inputSerializer: KSerializer<I>,
        eventSerialization: DataSerializationContext<E>,
        translate: (PersistedEvent) -> Pair<AggregateId, I>?,
        targets: List<ProcessTarget<I>>,
        queues: ProcessManagerQueues,
        inputOrdering: ReactionOrdering = ReactionOrdering.Unordered,
        getPosition: () -> EventLogPosition,
        savePosition: (EventLogPosition) -> Unit,
        isLeader: () -> Boolean,
        pollInterval: Duration = 500.milliseconds,
        batchSize: Int = 100,
        clock: () -> Instant = { Clock.System.now() },
    ) : this(
        type,
        repository,
        PostgresDomainPersistenceBackend(jdbc, ProcessEventSerialization(eventSerialization)),
        PostgresDomainPollingBackend(jdbc),
        initial,
        inputSerializer,
        eventSerialization,
        translate,
        targets,
        queues,
        inputOrdering,
        getPosition,
        savePosition,
        isLeader,
        pollInterval,
        batchSize,
        clock,
    )

    private val targetsByType: Map<AggregateType, ProcessTarget<I>> =
        targets.associateBy { it.type }.also { byType ->
            require(byType.size == targets.size) { "A process manager can have only one target per aggregate type" }
        }

    private val instances = ProcessInstances(type, repository, persistence, initial, inputSerializer, targetsByType.keys)
    private val streamSerialization = ProcessEventSerialization(eventSerialization)
    private val inputTriggers = JsonTriggerSerializer(InputTrigger.serializer())
    private val ordered = inputOrdering != ReactionOrdering.Unordered

    private val inputs = processExecutor(queues.channel("inputs", inputTriggers, ordered), clock, ::deliverInput)
    private val internal = processExecutor(queues.channel("internal", inputTriggers, ordered = false), clock, ::deliverInput)
    private val commands =
        processExecutor(queues.channel("commands", JsonTriggerSerializer(CommandTrigger.serializer()), ordered = false), clock, ::runCommand)
    private val contractExecutors = mutableListOf<EventReactionExecutor<InputTrigger, Unit>>()
    private var started = false

    init {
        require(!ordered || inputs.supportsOrdering) { "Ordered process inputs need a queue that supports ordering" }
        if (ordered) inputs.claimOrderedSource(this)
    }

    private val poller =
        DomainEventPoller(
            backend = polling,
            getPosition = getPosition,
            savePosition = savePosition,
            isLeader = isLeader,
            pollInterval = pollInterval,
            batchSize = batchSize,
            loggerName = "ProcessManager(${type.value})",
            handleEvent = ::route,
        )

    /**
     * Delivers another context's public events to this process manager: [translate] turns each one into the process id
     * and input it is for, or `null` to ignore it. [name] names the channel and must stay the same across restarts.
     * Must be called before [start].
     */
    fun <P : PublicDomainEvent> subscribeTo(
        name: String,
        contract: PublicEventContract<*, P>,
        translate: (PublicEventEnvelope<P>) -> Pair<AggregateId, I>?,
    ) {
        check(!started) { "subscribeTo() must be called before start()" }
        val executor = processExecutor(queues.channel("contract-$name", inputTriggers, ordered), clock, ::deliverInput)
        contractExecutors += executor
        contract.subscribe(executor, inputOrdering) { envelope ->
            val (processId, input) = translate(envelope) ?: return@subscribe emptyList()
            val inputId = "in-${envelope.metadata.eventId.value}"
            listOf(EventReaction(EventReactionId(inputId), InputTrigger(processId.value, encode(input), inputId)))
        }
    }

    /** Starts the channels' executors and the poller. */
    fun start() {
        startExecutorsForTest()
        poller.start()
    }

    /** Stops the poller, then the channels' executors. */
    suspend fun stop() {
        poller.stop()
        (listOf(inputs, internal, commands) + contractExecutors).forEach { it.stop() }
    }

    internal fun startExecutorsForTest() {
        started = true
        (listOf(inputs, internal, commands) + contractExecutors).forEach { it.start() }
    }

    internal suspend fun tickForTest() = poller.tickForTest()

    private fun encode(input: I): String = Json.encodeToString(inputSerializer, input)

    private suspend fun route(event: PersistedEvent) {
        val eventId = event.metadata.eventId.value
        if (event.metadata.aggregateType == type) {
            val processId = event.metadata.aggregateId.value
            when (event.serialized.type) {
                ProcessEventSerialization.COMMAND_REQUESTED -> {
                    val requested = streamSerialization.deserialize(event.serialized) as CommandRequested
                    commands.dispatch(
                        EventReactionId("cmd-$eventId"),
                        CommandTrigger(processId, requested.targetType, requested.targetId, requested.command, "${type.value}-$eventId"),
                    )
                }
                ProcessEventSerialization.INPUT_SCHEDULED -> {
                    val scheduled = streamSerialization.deserialize(event.serialized) as InputScheduled
                    val inputId = "sched-$eventId"
                    internal.dispatch(EventReactionId(inputId), InputTrigger(processId, scheduled.input, inputId), notBefore = Instant.parse(scheduled.at))
                }
                else -> Unit // the process's own facts are never fed back to it
            }
            return
        }
        val (processId, input) = translate(event) ?: return
        val inputId = "in-$eventId"
        inputs.dispatch(EventReactionId(inputId), InputTrigger(processId.value, encode(input), inputId), inputOrdering.stampFor(event.metadata, 0))
    }

    private suspend fun deliverInput(trigger: InputTrigger) {
        instances.deliver(AggregateId(trigger.processId), Json.decodeFromString(inputSerializer, trigger.input), trigger.inputId)
    }

    private suspend fun runCommand(trigger: CommandTrigger) {
        val target =
            checkNotNull(targetsByType[AggregateType(trigger.targetType)]) {
                "No target for aggregate type ${trigger.targetType}; register it with target(manager) { … }"
            }
        val feedback =
            target.send(
                AggregateId(trigger.targetId),
                trigger.command,
                CommandId(trigger.commandId),
                CorrelationId("${type.value}/${trigger.processId}"),
            ) ?: return
        val inputId = "rejected-${trigger.commandId}"
        internal.dispatch(EventReactionId(inputId), InputTrigger(trigger.processId, encode(feedback), inputId))
    }
}
```

Notes:
- `inputs`, `internal`, `commands` and the init block must be initialised before `poller` and before anything uses them. Kotlin initialises in declaration order, as written.
- `stampFor`, `claimOrderedSource`, `DomainEventPoller` and `tickForTest` are `internal` to the `kotmod` module, so they can be used from `io.kotmod.process`.
- If the contract's `subscribe` block parameter isn't `(PublicEventEnvelope<P>) -> List<EventReaction<T>>`, adapt it, keeping the behaviour.

- [ ] **Step 4: Run the tests**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.process.*'`
Expected: PASS (10 `ProcessManagerTest` tests plus the earlier ones). No `w:` lines.

Run: `./gradlew :kotmod:test :kotmod:integrationTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A kotmod
git commit -m "Add the ProcessManager facade: translation, channels and command feedback

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: db-scheduler queues and the end-to-end test

**Files:**
- Create: `kotmod-db-scheduler/src/main/kotlin/io/kotmod/event/reaction/dbscheduler/DbSchedulerProcessManagerQueues.kt`
- Test: `kotmod-db-scheduler/src/integrationTest/kotlin/io/kotmod/event/reaction/dbscheduler/ProcessManagerIntegrationTest.kt`

**Interfaces:**
- Consumes: `ProcessManagerQueues`, `ProcessChannel`, `ProcessManager` (public constructor), `target`; `DbSchedulerEventReactions(taskName, triggerSerializer, unsubscribedRetryDelay, jdbc)`; test fixtures (`testOrders`, `NoOrder`, the order states and commands, window process, `windowEventSerialization`, `orderEventSerialization`), `IntegrationTest`, `eventually`, `testScheduler`.
- Produces: `class DbSchedulerProcessManagerQueues(name: String, jdbc: JdbcContext? = null, unsubscribedRetryDelay: Duration = 5.seconds) : ProcessManagerQueues` with `val tasks: List<Task<*>>` and `fun bind(client: SchedulerClient)`.

- [ ] **Step 1: Write the failing integration test**

Create `ProcessManagerIntegrationTest.kt`:

```kotlin
package io.kotmod.event.reaction.dbscheduler

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.EventLogPosition
import io.kotmod.Repository
import io.kotmod.jdbc.JdbcContext
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.process.ProcessManager
import io.kotmod.process.target
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.support.CancelOrder
import io.kotmod.support.CancelledOrder
import io.kotmod.support.ClosedWindow
import io.kotmod.support.NoOrder
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.Order
import io.kotmod.support.OrderNotPending
import io.kotmod.support.OrderRejection
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShippedOrder
import io.kotmod.support.Window
import io.kotmod.support.WindowInput
import io.kotmod.support.testOrders
import io.kotmod.support.windowEventSerialization
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.BeforeEach
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class ProcessManagerIntegrationTest : IntegrationTest() {
    private class OrderTable(
        private val jdbc: JdbcContext,
    ) : Repository<Order> {
        override fun get(id: AggregateId): Order? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, name, reason FROM pm_test_order WHERE id = ?").use { ps ->
                    ps.setString(1, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) {
                            null
                        } else {
                            when (rs.getString(1)) {
                                "PENDING" -> PendingOrder(rs.getString(2))
                                "SHIPPED" -> ShippedOrder(rs.getString(2))
                                else -> CancelledOrder(rs.getString(2), rs.getString(3))
                            }
                        }
                    }
                }
            }

        override fun save(
            id: AggregateId,
            state: Order,
        ) {
            val (status, name, reason) =
                when (state) {
                    is PendingOrder -> Triple("PENDING", state.name, null)
                    is ShippedOrder -> Triple("SHIPPED", state.name, null)
                    is CancelledOrder -> Triple("CANCELLED", state.name, state.reason)
                }
            jdbc.withConnection { conn ->
                conn
                    .prepareStatement(
                        "INSERT INTO pm_test_order (id, status, name, reason) VALUES (?, ?, ?, ?) " +
                            "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, name = EXCLUDED.name, reason = EXCLUDED.reason",
                    ).use { ps ->
                        ps.setString(1, id.value)
                        ps.setString(2, status)
                        ps.setString(3, name)
                        ps.setString(4, reason)
                        ps.executeUpdate()
                    }
            }
        }
    }

    private class WindowTable(
        private val jdbc: JdbcContext,
    ) : Repository<Window> {
        override fun get(id: AggregateId): Window? =
            jdbc.withConnection { conn ->
                conn.prepareStatement("SELECT status, order_id, blocked FROM pm_test_window WHERE id = ?").use { ps ->
                    ps.setString(1, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) {
                            null
                        } else if (rs.getString(1) == "OPEN") {
                            OpenWindow(rs.getString(2))
                        } else {
                            ClosedWindow(rs.getString(2), rs.getString(3)?.let { Json.decodeFromString(OrderRejection.serializer(), it) })
                        }
                    }
                }
            }

        override fun save(
            id: AggregateId,
            state: Window,
        ) {
            val (status, orderId, blocked) =
                when (state) {
                    is OpenWindow -> Triple("OPEN", state.orderId, null)
                    is ClosedWindow -> Triple("CLOSED", state.orderId, state.blocked?.let { Json.encodeToString(OrderRejection.serializer(), it) })
                }
            jdbc.withConnection { conn ->
                conn
                    .prepareStatement(
                        "INSERT INTO pm_test_window (id, status, order_id, blocked) VALUES (?, ?, ?, ?) " +
                            "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, order_id = EXCLUDED.order_id, blocked = EXCLUDED.blocked",
                    ).use { ps ->
                        ps.setString(1, id.value)
                        ps.setString(2, status)
                        ps.setString(3, orderId)
                        ps.setString(4, blocked)
                        ps.executeUpdate()
                    }
            }
        }
    }

    @BeforeEach
    fun createTables() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS pm_test_order (id TEXT PRIMARY KEY, status TEXT NOT NULL, name TEXT NOT NULL, reason TEXT)")
                stmt.execute("CREATE TABLE IF NOT EXISTS pm_test_window (id TEXT PRIMARY KEY, status TEXT NOT NULL, order_id TEXT NOT NULL, blocked TEXT)")
                stmt.execute("TRUNCATE pm_test_order, pm_test_window")
            }
        }
    }

    private val orders by lazy {
        AggregateManager(testOrders, OrderTable(jdbc), PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()), NoOrder)
    }

    /** Runs a window process: every OrderPlaced opens a window that closes [closeAfter] later. */
    private suspend fun runningWindows(
        closeAfter: Duration,
        clock: () -> Instant = { Clock.System.now() },
        block: suspend () -> Unit,
    ) {
        val queues = DbSchedulerProcessManagerQueues("windows", jdbc)
        val offsets = PostgresOffsetManager(jdbc)
        val windows =
            ProcessManager(
                type = AggregateType("Window"),
                repository = WindowTable(jdbc),
                jdbc = jdbc,
                initial = NoWindow,
                inputSerializer = WindowInput.serializer(),
                eventSerialization = windowEventSerialization(),
                translate = { event ->
                    if (event.metadata.aggregateType == testOrders.type && event.serialized.type.endsWith("OrderPlaced")) {
                        val orderId = event.metadata.aggregateId.value
                        AggregateId("window-$orderId") to Opened(orderId, (Clock.System.now() + closeAfter).epochSeconds)
                    } else {
                        null
                    }
                },
                targets = listOf(target(orders) { _, rejection -> ReleaseBlocked(rejection) }),
                queues = queues,
                inputOrdering = ReactionOrdering.PerAggregate(),
                getPosition = { offsets.getPosition("windows") ?: EventLogPosition.START },
                savePosition = { offsets.savePosition("windows", it) },
                isLeader = { true },
                clock = clock,
            )
        val scheduler = testScheduler(dataSource, *queues.tasks.toTypedArray())
        queues.bind(scheduler)
        windows.start()
        scheduler.start()
        try {
            block()
        } finally {
            scheduler.stop()
            windows.stop()
        }
    }

    private fun window(orderId: String) = WindowTable(jdbc).get(AggregateId("window-$orderId"))

    private fun order(orderId: String) = OrderTable(jdbc).get(AggregateId(orderId))

    @Test
    fun `a placed order opens a window that closes on time and ships the order`() =
        runBlocking {
            runningWindows(closeAfter = 3.seconds) {
                orders.handle(AggregateId("o-1"), PlaceOrder("o-1"))

                eventually { window("o-1") == OpenWindow("o-1") }
                delay(1_000)
                assertEquals(PendingOrder("o-1"), order("o-1"), "shipped before the window closed")
                eventually { order("o-1") == ShippedOrder("o-1") }
                eventually { window("o-1") == ClosedWindow("o-1") }
            }
        }

    @Test
    fun `a refused command comes back to the process as its own input`() =
        runBlocking {
            runningWindows(closeAfter = 2.seconds) {
                orders.handle(AggregateId("o-2"), PlaceOrder("o-2"))
                orders.handle(AggregateId("o-2"), CancelOrder("changed mind"))

                eventually { window("o-2") == ClosedWindow("o-2", blocked = OrderNotPending("CancelledOrder")) }
                assertEquals(CancelledOrder("o-2", "changed mind"), order("o-2"))
            }
        }

    @Test
    fun `a timeout that arrives before the process's clock says it is due waits and runs later`() =
        runBlocking {
            // The process manager's clock lags 2 seconds behind db-scheduler's, so the timeout arrives "early".
            runningWindows(closeAfter = 1.seconds, clock = { Clock.System.now() - 2.seconds }) {
                orders.handle(AggregateId("o-3"), PlaceOrder("o-3"))

                eventually { window("o-3") == OpenWindow("o-3") }
                delay(1_500)
                assertEquals(OpenWindow("o-3"), window("o-3"), "ran before its notBefore by the process's clock")
                eventually { window("o-3") == ClosedWindow("o-3") }
            }
        }
}
```

Adapt to the real signatures if they differ: `PostgresOffsetManager.getPosition` may return a non-null `EventLogPosition`, in which case drop the `?: EventLogPosition.START`, and the event-type check may need the serialized type name used by `orderEventSerialization()`. Check `OrderPlaced`'s serialized type with a quick look at the fixture, and match it exactly rather than with `endsWith`.

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :kotmod-db-scheduler:integrationTest --tests '*ProcessManagerIntegrationTest*'`
Expected: compilation FAILS (unresolved `DbSchedulerProcessManagerQueues`).

- [ ] **Step 3: Implement the queues**

Create `DbSchedulerProcessManagerQueues.kt`:

```kotlin
package io.kotmod.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.jdbc.JdbcContext
import io.kotmod.process.ProcessChannel
import io.kotmod.process.ProcessManagerQueues
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/**
 * Runs a [io.kotmod.process.ProcessManager]'s channels on db-scheduler: one task per channel, named `<name>-<channel>`.
 *
 * ```
 * val queues = DbSchedulerProcessManagerQueues("refund-window", jdbc)
 * val refundWindow = ProcessManager(…, queues = queues)        // and any subscribeTo(…) calls
 * val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
 * queues.bind(scheduler)
 * refundWindow.start()
 * scheduler.start()
 * ```
 *
 * [jdbc] is needed only when the process manager's inputs are ordered.
 */
class DbSchedulerProcessManagerQueues(
    private val name: String,
    private val jdbc: JdbcContext? = null,
    private val unsubscribedRetryDelay: Duration = 5.seconds,
) : ProcessManagerQueues {
    private val reactions = mutableListOf<DbSchedulerEventReactions<*>>()

    @Volatile
    private var client: SchedulerClient? = null

    /** The tasks to register with the app's `Scheduler`, once the process manager (and its subscriptions) are built. */
    val tasks: List<Task<*>> get() = reactions.flatMap { it.tasks }

    /** Publishes through [client] (usually the app's `Scheduler`). Call it before starting the process manager. */
    fun bind(client: SchedulerClient) {
        this.client = client
    }

    override fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ProcessChannel<T> {
        require(!ordered || jdbc != null) { "Ordered process inputs need DbSchedulerProcessManagerQueues to be created with a JdbcContext" }
        val channelReactions =
            DbSchedulerEventReactions("${this.name}-$name", triggerSerializer, unsubscribedRetryDelay, jdbc = if (ordered) jdbc else null)
        reactions += channelReactions
        val sink =
            object : EventReactionTriggerSink<T> {
                override val supportsOrdering: Boolean = channelReactions.supportsOrdering

                override suspend fun publish(
                    id: EventReactionId,
                    trigger: T,
                    ordering: DispatchOrdering?,
                    notBefore: Instant?,
                ) {
                    val bound = checkNotNull(client) { "Call bind(scheduler) on DbSchedulerProcessManagerQueues before starting" }
                    channelReactions.sink(bound).publish(id, trigger, ordering, notBefore)
                }
            }
        return ProcessChannel(sink, channelReactions.source)
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `./gradlew :kotmod-db-scheduler:integrationTest --tests '*ProcessManagerIntegrationTest*'`
Expected: PASS (3 tests). Run the class 3 times (`--rerun-tasks`) to check for timing flakiness. If one is flaky, lengthen the delays rather than weakening the assertions, and report it.

Run: `./gradlew :kotmod:test :kotmod:integrationTest :kotmod-db-scheduler:test :kotmod-db-scheduler:integrationTest :kotmod-sqldelight:integrationTest :examples:integrationTest`
Expected: PASS, with no `w:` lines.

- [ ] **Step 5: Commit**

```bash
git add -A kotmod-db-scheduler
git commit -m "Run process managers on db-scheduler

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The process managers guide

**Files:**
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt` (a compiled process manager example)
- Modify: `README.md` (new `### Process managers` guide; Contents; Core concepts)

**Interfaces:**
- Consumes: everything public from Tasks 1–5; the examples' `Orders`, `OrderCommand`, `CancelOrder`, `OrderRejection`, `OrderPlaced`, `OrderShipped`, `orders` construction style, and `serialization`.
- Produces: nothing used later.

- [ ] **Step 1: Write the compiled example**

In `ReadmeExamples.kt`, add a section `// Guide: Process managers`, using only kotmod's public API. It holds a dispatch deadline process: when an order is placed, it waits two days, and if the order hasn't shipped by then it cancels the order and records that the deadline was missed. Write it exactly like this, adding the needed imports (`io.kotmod.process.*`, `io.kotmod.PersistedEvent`, `io.kotmod.EventLogPosition`, `kotlin.time.Duration.Companion.days`, `com.github.kagkarlsson.scheduler.Scheduler`, `io.kotmod.event.reaction.dbscheduler.DbSchedulerProcessManagerQueues`, and others as needed):

```kotlin
@Serializable
sealed interface DispatchDeadlineInput

@Serializable
data class OrderWasPlaced(
    val orderId: String,
    val placedAt: Instant,
) : DispatchDeadlineInput

@Serializable
data object OrderWasShipped : DispatchDeadlineInput

@Serializable
data object DeadlinePassed : DispatchDeadlineInput

@Serializable
data class CancellationRefused(
    val rejection: OrderRejection,
) : DispatchDeadlineInput

@Serializable
sealed interface DispatchDeadlineEvent : DomainEvent

@Serializable
data class DispatchDeadlineMissed(
    val orderId: String,
) : DispatchDeadlineEvent

typealias DispatchDeadlineOutcome = ProcessOutcome<DispatchDeadline, DispatchDeadlineEvent, DispatchDeadlineInput>

sealed interface DispatchDeadline : ProcessState<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent>

object NoDispatchDeadline : ProcessInitialState<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent> {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            is OrderWasPlaced ->
                transition(
                    AwaitingDispatch(input.orderId),
                    schedule = listOf(schedule(DeadlinePassed, at = input.placedAt + 2.days)),
                )
            OrderWasShipped, DeadlinePassed, is CancellationRefused -> ignore()
        }
}

data class AwaitingDispatch(
    val orderId: String,
) : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            OrderWasShipped -> transition(Dispatched)
            DeadlinePassed ->
                transition(
                    Missed,
                    events = listOf(DispatchDeadlineMissed(orderId)),
                    commands = listOf(Orders.command(AggregateId(orderId), CancelOrder("not shipped within 2 days"))),
                )
            is OrderWasPlaced, is CancellationRefused -> ignore()
        }
}

data object Dispatched : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome = ignore()
}

data object Missed : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            // The order shipped just before the cancellation reached it: nothing to undo.
            is CancellationRefused -> ignore()
            is OrderWasPlaced, OrderWasShipped, DeadlinePassed -> ignore()
        }
}

fun translateOrderEvent(
    event: PersistedEvent,
    serialization: DataSerializationContext<OrderEvent>,
): Pair<AggregateId, DispatchDeadlineInput>? {
    if (event.metadata.aggregateType != Orders.type) return null
    val deadline = AggregateId("deadline-${event.metadata.aggregateId.value}")
    return when (serialization.deserialize(event.serialized)) {
        is OrderPlaced -> deadline to OrderWasPlaced(event.metadata.aggregateId.value, event.metadata.timestamp)
        is OrderShipped -> deadline to OrderWasShipped
        else -> null
    }
}

fun dispatchDeadlines(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    deadlines: Repository<DispatchDeadline>,
    deadlineEvents: DataSerializationContext<DispatchDeadlineEvent>,
    queues: DbSchedulerProcessManagerQueues,
): ProcessManager<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent> {
    val offsets = PostgresOffsetManager(jdbc)
    return ProcessManager(
        type = AggregateType("DispatchDeadline"),
        repository = deadlines,
        jdbc = jdbc,
        initial = NoDispatchDeadline,
        inputSerializer = DispatchDeadlineInput.serializer(),
        eventSerialization = deadlineEvents,
        translate = { event -> translateOrderEvent(event, serialization) },
        targets = listOf(target(orders) { _, rejection -> CancellationRefused(rejection) }),
        queues = queues,
        getPosition = { offsets.getPosition("dispatch-deadlines") },
        savePosition = { offsets.savePosition("dispatch-deadlines", it) },
        isLeader = { true },
    )
}

fun startDispatchDeadlines(
    dataSource: DataSource,
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    deadlines: Repository<DispatchDeadline>,
    deadlineEvents: DataSerializationContext<DispatchDeadlineEvent>,
): Scheduler {
    val queues = DbSchedulerProcessManagerQueues("dispatch-deadlines", jdbc)
    val process = dispatchDeadlines(jdbc, serialization, orders, deadlines, deadlineEvents, queues)
    val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
    queues.bind(scheduler)
    process.start()
    scheduler.start()
    return scheduler
}
```

`Instant` here is `kotlin.time.Instant`, and `event.metadata.timestamp` is already one. If `kotlin.time.Instant` isn't `@Serializable`-supported by kotlinx.serialization 1.11 in `OrderWasPlaced`, store `placedAtEpochSeconds: Long` instead, and use `Instant.fromEpochSeconds(input.placedAtEpochSeconds) + 2.days`. If `PostgresOffsetManager.getPosition` returns a nullable value, follow the existing outbox examples in this file exactly.

Run: `./gradlew :examples:integrationTest --rerun-tasks`
Expected: PASS (the example compiles; the existing 4 tests pass).

- [ ] **Step 2: Write the guide**

Add a guide `### Process managers` after "Publishing events to other contexts", and add it to Contents. Use the README's plain voice, wrapped at ~110 columns. Copy every snippet verbatim from `ReadmeExamples.kt`. Cover, in order:

1. **What it is.** Long-running workflows that react to events from several aggregates, keep their own state, and request commands, never doing anything synchronously. An aggregate receives commands and emits events; a process manager receives events and emits commands. kotmod calls them process managers, not sagas.
2. **Inputs, the anti-corruption layer.** A process manager sees only its own input type. `translate` turns this context's events into `(processId, input)` or `null`, and `subscribeTo(name, contract) { … }` does the same for other contexts' public events. Snippet: the inputs, plus `translateOrderEvent`.
3. **States own their inputs.** `ProcessState`, `ProcessInitialState`, `transition(...)` and `ignore()`. Inputs can't be rejected, because they're facts. An ignored input for a process that doesn't exist yet creates nothing. Snippet: the states.
4. **Timeouts.** `schedule(input, at)` delivers an input to the same instance at a time. There's no cancellation: a state that has moved on ignores it.
5. **Commands to other aggregates.** `Orders.command(id, command)` only accepts that kind's commands. kotmod runs it through the target's `AggregateManager.handle` with a command id derived from the request, so a redelivery gets the same answer. `target(orders) { command, rejection -> input }` turns a typed rejection back into an input.
6. **Facts the process owns.** `transition(events = …)` records them in the process's own stream. To tell other contexts, publish them through a `PublicEventContract`, as with any events.
7. **Wiring with db-scheduler.** Snippet: `dispatchDeadlines` and `startDispatchDeadlines`. Mention the lifecycle (start the process manager before the scheduler, stop it after) and the channels: inputs (optionally ordered per source aggregate with `inputOrdering`), internal (timeouts and rejection feedback), commands, and one per `subscribeTo`.
8. **When things fail.** Input delivery and commands are retried with capped backoff and never given up. A command for an aggregate type that isn't a target fails loudly, and nothing is recorded.

In **Core concepts**, add a bullet after **Public event**:

> - **Process manager** — a long-running workflow that reacts to events, keeps its own state, and asks for
>   commands to be run and inputs to be delivered to it later.

- [ ] **Step 3: Check drift**

Compare every README Kotlin snippet in the new guide with `ReadmeExamples.kt`. They must be identical.

Run: `./gradlew :examples:integrationTest`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add README.md examples
git commit -m "Document process managers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

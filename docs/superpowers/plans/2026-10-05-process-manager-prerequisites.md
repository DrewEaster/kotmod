# Process Manager Prerequisites Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `AggregateKind` (an aggregate type plus its command and rejection serializers, from which `AggregateManager` is built) and delayed reactions (`notBefore`), which process managers build on in the next cycle.

**Architecture:** `AggregateKind<C, R>` is an open class in the core. `AggregateManager` takes a kind in place of `aggregateType` and `rejectionSerializer`. For delayed reactions:
- `EventReaction`, `EventReactionExecutor.dispatch` and `EventReactionTriggerSink.publish` gain `notBefore`.
- Sources pass it back on delivery.
- The executor puts early deliveries back with a new `ReactionOutcome.Wait`, which doesn't count as a retry.
- db-scheduler schedules the row at `notBefore` and stores it in the task data.
- Ordered reactions with `notBefore` are refused.

**Tech Stack:** Kotlin 2.4.20 (JVM toolchain 25), kotlinx.serialization 1.11.0, kotlinx.coroutines 1.11.0, db-scheduler, Postgres 17 via Testcontainers, JUnit 5 / kotlin.test / MockK, Gradle.

**Spec:** `docs/superpowers/specs/2026-10-05-process-manager-prerequisites-design.md`

## Global Constraints

- `open class AggregateKind<C : Any, R : Any>(val type: AggregateType, val commandSerializer: KSerializer<C>, val rejectionSerializer: KSerializer<R>)` in package `io.kotmod`.
- `AggregateManager<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any>(kind: AggregateKind<C, R>, repository: Repository<S>, backend: DomainPersistenceBackend<E>, initial: InitialState<S, C, E, R>, maxConflictRetries: Int = 5)`. Its behaviour doesn't change.
- `notBefore` is `kotlin.time.Instant?` everywhere in the public API.
- `EventReaction(id, trigger, notBefore: Instant? = null)`.
- `EventReactionExecutor.dispatch(id, trigger, ordering: DispatchOrdering? = null, notBefore: Instant? = null)`.
- `EventReactionTriggerSink.publish(id, trigger, ordering: DispatchOrdering?, notBefore: Instant?)`.
- `EventReactionTriggerSource.subscribe(block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, Instant?) -> ReactionOutcome)`.
- `ReactionOutcome.Wait(delay: Duration)` means deliver again after about `delay`, **without** incrementing the retry count.
- `dispatch` with both an ordering stamp and `notBefore` throws `IllegalArgumentException`.
- Executor guard: when `notBefore` is in the future, return `ReactionOutcome.Wait(notBefore - now)` without calling `createExecutionContext` or `execute`.
- db-scheduler: schedule the instance at `notBefore` (now when `null`); store `notBeforeEpochMillis: Long? = null` in `ReactionTaskData` (old rows decode as `null`); `Wait` reschedules with the retry count unchanged and the delay capped like retries.
- Publishing an id that is already pending still does nothing.
- Version stays `0.2.0`.
- README Kotlin snippets must be byte-identical to `examples/src/integrationTest/kotlin/io/kotmod/readme/{Quickstart.kt,QuickstartTest.kt,ReadmeExamples.kt}`, apart from indentation inside test functions.
- Commit messages follow the repo style (imperative sentence, no prefix) and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. No new compiler warnings.

## Review Focus

1. **A delayed reaction delivered early by a queue that can't schedule.** Expected: it doesn't run, its retry count doesn't grow, and it runs once due. Pinned in Task 2 (executor guard tests).
2. **A delayed reaction from an ordered outbox or contract.** Expected: a loud `IllegalArgumentException` naming the reaction, never a silently unordered or silently dropped reaction. Pinned in Task 2.
3. **Rows written before `notBefore` existed** (db-scheduler task data without the field). Expected: they decode as not delayed and run as before. Pinned in Task 2.
4. **A duplicate dispatch of a pending delayed reaction, possibly with a different `notBefore`.** Expected: one row, and the first schedule wins. Pinned in Task 2 (integration).
5. **Comparing an event's aggregate type after the switch to kinds.** Expected: `event.metadata.aggregateType != Orders.type` works where `orderType` was used. Pinned in Task 1 (the examples compile and the quickstart test passes).

---

## File Structure

- **Modify** `kotmod/src/main/kotlin/io/kotmod/Commands.kt`: `AggregateKind`.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`: built from a kind.
- **Modify** `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`: a test kind.
- **Modify** the `AggregateManager` construction sites: `kotmod/src/test/kotlin/io/kotmod/AggregateManagerHandleTest.kt:47`, `kotmod/src/testFixtures/kotlin/io/kotmod/jdbc/JdbcContextContract.kt:164`, `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/AggregateManagerIntegrationTest.kt:85`, `kotmod-sqldelight/src/integrationTest/kotlin/io/kotmod/sqldelight/SqlDelightJdbcContextIntegrationTest.kt:35`, `examples/.../QuickstartTest.kt:64-72`.
- **Modify** `kotmod/src/main/kotlin/io/kotmod/event/reaction/{EventReaction.kt,Ordering.kt}`, `kotmod/src/main/kotlin/io/kotmod/outbox/AggregateEventOutbox.kt`, `kotmod/src/main/kotlin/io/kotmod/contract/PublicEventContract.kt`: delayed reactions.
- **Modify** `kotmod-db-scheduler/src/main/kotlin/io/kotmod/event/reaction/dbscheduler/{DbSchedulerTriggerSink.kt,DbSchedulerTriggerSource.kt,ReactionTask.kt,ReactionTaskData.kt,TaskRowOutcome.kt}`.
- **Create** `kotmod/src/test/kotlin/io/kotmod/event/reaction/DelayedReactionsTest.kt`; **modify** the test fakes in `EventReactionExecutorTest.kt`, `OrderedSourceGuardTest.kt`, `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/support/RecordingExecutor.kt`, and the db-scheduler tests.
- **Modify** `README.md`, `examples/src/integrationTest/kotlin/io/kotmod/readme/{Quickstart.kt,QuickstartTest.kt,ReadmeExamples.kt}`.

---

### Task 1: Aggregate kinds

**Files:**
- Modify: `kotmod/src/main/kotlin/io/kotmod/Commands.kt`
- Modify: `kotmod/src/main/kotlin/io/kotmod/AggregateManager.kt`
- Modify: `kotmod/src/testFixtures/kotlin/io/kotmod/support/TestAggregate.kt`
- Modify: `kotmod/src/test/kotlin/io/kotmod/AggregateManagerHandleTest.kt`, `kotmod/src/testFixtures/kotlin/io/kotmod/jdbc/JdbcContextContract.kt`, `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/AggregateManagerIntegrationTest.kt`, `kotmod-sqldelight/src/integrationTest/kotlin/io/kotmod/sqldelight/SqlDelightJdbcContextIntegrationTest.kt`
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt`, `QuickstartTest.kt`
- Modify: `README.md` (Quickstart step 3 and the step 5 outbox snippet)

**Interfaces:**
- Consumes: the existing `AggregateManager`, `AggregateState` and `InitialState`.
- Produces:
  - `io.kotmod.AggregateKind<C, R>` with `type`, `commandSerializer` and `rejectionSerializer`.
  - The `AggregateManager(kind, repository, backend, initial, maxConflictRetries = 5)` constructor.
  - Fixture `fun testOrderKind(type: String = "Order"): AggregateKind<OrderCommand, OrderRejection>`.
  - Example `object Orders : AggregateKind<OrderCommand, OrderRejection>`.

- [ ] **Step 1: Write the failing test**

In `AggregateManagerHandleTest.kt`, change `manager()` to:

```kotlin
    private fun manager(maxConflictRetries: Int = 5) =
        AggregateManager(testOrderKind(), repo, backend, NoOrder, maxConflictRetries = maxConflictRetries)
```

Add `import io.kotmod.support.testOrderKind`, and remove the `OrderRejection` import if nothing else uses it. Add this test:

```kotlin
    @Test
    fun `the kind's type names the aggregate in the event log and the command history`() =
        runTest {
            val invoices = AggregateManager(testOrderKind("Invoice"), repo, backend, NoOrder)

            invoices.handle(id, PlaceOrder("book"), commandId = CommandId("c-1"))

            assertEquals(AggregateType("Invoice"), backend.events.single().metadata.aggregateType)
            assertEquals(HandledCommand.Accepted, backend.commands[StubPersistenceBackend.CommandKey(AggregateType("Invoice"), id, CommandId("c-1"))])
        }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.AggregateManagerHandleTest'`
Expected: compilation FAILS: unresolved `testOrderKind`, and an `AggregateManager` constructor mismatch.

- [ ] **Step 3: Add AggregateKind and build AggregateManager from it**

In `Commands.kt`, add `import kotlinx.serialization.KSerializer` and, after `CommandResult`:

```kotlin
/**
 * Names one kind of aggregate, such as orders, together with how its commands and rejections are serialized.
 * Build an [AggregateManager] from it. Declare one per aggregate type, usually as an `object`:
 *
 * ```
 * object Orders : AggregateKind<OrderCommand, OrderRejection>(
 *     type = AggregateType("Order"),
 *     commandSerializer = OrderCommand.serializer(),
 *     rejectionSerializer = OrderRejection.serializer(),
 * )
 * ```
 *
 * @param type the aggregate type its events and commands are recorded under.
 * @param commandSerializer serializes its commands, so other parts of the app can request them as data.
 * @param rejectionSerializer serializes its rejections, which are recorded so a repeated command id gets the
 *   same answer.
 */
open class AggregateKind<C : Any, R : Any>(
    val type: AggregateType,
    val commandSerializer: KSerializer<C>,
    val rejectionSerializer: KSerializer<R>,
)
```

In `AggregateManager.kt`:
- Replace the constructor parameters `private val aggregateType: AggregateType` and `private val rejectionSerializer: KSerializer<R>` with a first parameter `private val kind: AggregateKind<C, R>`. The order becomes `kind, repository, backend, initial, maxConflictRetries`.
- Inside the class, add `private val aggregateType: AggregateType get() = kind.type`, and replace each use of `rejectionSerializer` with `kind.rejectionSerializer`. That keeps every other line unchanged.
- Remove the now-unused `KSerializer` import.
- In the KDoc, replace `@param rejectionSerializer …` with `@param kind names the aggregate type and how its commands and rejections are serialized.`

- [ ] **Step 4: Add the test kind and update every construction site**

In `TestAggregate.kt`, add:

```kotlin
/** Test commands are never serialized (DecideWith holds a lambda), so the test kind's command serializer refuses. */
object TestOrderCommandSerializer : KSerializer<OrderCommand> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("io.kotmod.support.TestOrderCommand", PrimitiveKind.STRING)

    override fun serialize(
        encoder: Encoder,
        value: OrderCommand,
    ): Unit = error("test commands are not serialized")

    override fun deserialize(decoder: Decoder): OrderCommand = error("test commands are not serialized")
}

fun testOrderKind(type: String = "Order"): AggregateKind<OrderCommand, OrderRejection> =
    AggregateKind(AggregateType(type), TestOrderCommandSerializer, OrderRejection.serializer())
```

The imports this needs are:
- `io.kotmod.AggregateKind`
- `io.kotmod.AggregateType`
- `kotlinx.serialization.KSerializer`
- `kotlinx.serialization.descriptors.PrimitiveKind`
- `kotlinx.serialization.descriptors.PrimitiveSerialDescriptor`
- `kotlinx.serialization.descriptors.SerialDescriptor`
- `kotlinx.serialization.encoding.Decoder`
- `kotlinx.serialization.encoding.Encoder`

Update the construction sites:
- **`JdbcContextContract.kt` `manager(type, jdbc)`:** `AggregateManager(kind = testOrderKind(type), repository = ProbeOrderRepository(jdbc), backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()), initial = NoOrder)`. Drop the `OrderRejection` import if it becomes unused.
- **`AggregateManagerIntegrationTest.kt`:** `AggregateManager(testOrderKind(), OrderTable(jdbc), PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()), NoOrder)`.
- **`SqlDelightJdbcContextIntegrationTest.kt` `statelessOrders`:** replace `aggregateType = AggregateType("Order"),` with `kind = io.kotmod.support.testOrderKind(),`, and delete the `rejectionSerializer = …` line.

- [ ] **Step 5: Run the core tests**

Run: `./gradlew :kotmod:test :kotmod:integrationTest :kotmod-sqldelight:integrationTest`
Expected: PASS, including the new `AggregateManagerHandleTest` case. No `w:` lines.

- [ ] **Step 6: Give the examples a kind**

In `Quickstart.kt`, after the last rejection (`CancellationReasonMissing`) and before `typealias OrderOutcome`, add the following, with the import `io.kotmod.AggregateKind` (and `io.kotmod.AggregateType` if not already imported):

```kotlin
object Orders : AggregateKind<OrderCommand, OrderRejection>(
    type = AggregateType("Order"),
    commandSerializer = OrderCommand.serializer(),
    rejectionSerializer = OrderRejection.serializer(),
)
```

In `QuickstartTest.kt`:
- Delete the line `val orderType = AggregateType("Order")` and the blank line after it.
- Change the manager construction to:

```kotlin
            val orders =
                AggregateManager(
                    kind = Orders,
                    repository = OrderRepository(jdbc),
                    backend = PostgresDomainPersistenceBackend(jdbc, serialization),
                    initial = NoOrder,
                )
```

- Change the outbox filter to `if (event.metadata.aggregateType != Orders.type) {`.
- Remove the `AggregateType` import if it becomes unused.

Run: `grep -n 'orderType\|aggregateType =\|initial = NoOrder,$' examples/src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt` and check that the only match is the `initial = NoOrder,` line of the new construction (no `orderType`, no `aggregateType =`, no `rejectionSerializer =` left in `QuickstartTest.kt`)
Expected: no output.

Run: `./gradlew :examples:integrationTest --rerun-tasks`
Expected: PASS (4 tests).

- [ ] **Step 7: Update the README to match**

In Quickstart step 3:
- Change the sentence before the first snippet to: "…tell kotmod how to serialize your events, name the aggregate with a **kind**, and create an `AggregateManager` for orders:".
- In the snippet, delete `val orderType = AggregateType("Order")` and its blank line, and replace the `AggregateManager(...)` call with the one from `QuickstartTest.kt`.
- Directly before that snippet, add a paragraph and a snippet copied from `Quickstart.kt`:

> An `AggregateKind` names the aggregate type and says how its commands and rejections are serialized:
>
> (the `object Orders` block)

In the step 5 outbox snippet, change `event.metadata.aggregateType != orderType` to `event.metadata.aggregateType != Orders.type`, as in `QuickstartTest.kt`.

Run: `grep -n 'orderType\|aggregateType = ' README.md`
Expected: no output. (`rejectionSerializer =` legitimately appears in the `Orders` kind snippet; the upgrade section's prose is rewritten in Task 3.)

- [ ] **Step 8: Commit**

```bash
git add -A kotmod kotmod-sqldelight examples README.md
git commit -m "Build aggregate managers from an aggregate kind

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Delayed reactions

**Files:**
- Modify: `kotmod/src/main/kotlin/io/kotmod/event/reaction/EventReaction.kt` (`EventReaction`, sink and source interfaces, `EventReactionExecutor`)
- Modify: `kotmod/src/main/kotlin/io/kotmod/event/reaction/Ordering.kt` (`ReactionOutcome`)
- Modify: `kotmod/src/main/kotlin/io/kotmod/outbox/AggregateEventOutbox.kt:68`, `kotmod/src/main/kotlin/io/kotmod/contract/PublicEventContract.kt:62`
- Modify: `kotmod-db-scheduler/src/main/kotlin/io/kotmod/event/reaction/dbscheduler/{DbSchedulerTriggerSink.kt,DbSchedulerTriggerSource.kt,ReactionTask.kt,ReactionTaskData.kt,TaskRowOutcome.kt}`
- Create: `kotmod/src/test/kotlin/io/kotmod/event/reaction/DelayedReactionsTest.kt`
- Modify (test fakes): `kotmod/src/test/kotlin/io/kotmod/event/reaction/EventReactionExecutorTest.kt`, `OrderedSourceGuardTest.kt`, `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/support/RecordingExecutor.kt`
- Modify (db-scheduler tests): `DbSchedulerTriggerSinkTest.kt`, `TaskRowOutcomeTest.kt`, `ReactionTaskDataTest.kt`, `DbSchedulerEventReactionsIntegrationTest.kt`

**Interfaces:**
- Consumes: the existing executor, outbox and contract; the db-scheduler `ReactionTaskData`, `outcomeAfterExecution`, `MAX_RETRY_DELAY`.
- Produces: everything in Global Constraints about `notBefore` and `ReactionOutcome.Wait`, plus an `EventReactionExecutor` constructor parameter `clock: () -> Instant = { Clock.System.now() }` (last, after `defaultBackoffStrategy`).

- [ ] **Step 1: Write the failing core tests**

Create `kotmod/src/test/kotlin/io/kotmod/event/reaction/DelayedReactionsTest.kt`:

```kotlin
package io.kotmod.event.reaction

import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.PublicDomainEvent
import io.kotmod.SerializedEvent
import io.kotmod.contract.PublicEventContract
import io.kotmod.outbox.AggregateEventOutbox
import io.kotmod.support.persistedEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class DelayedReactionsTest {
    private data class FakeTrigger(
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private data class Published(
        val id: EventReactionId,
        val ordering: DispatchOrdering?,
        val notBefore: Instant?,
    )

    private val now = Instant.parse("2026-10-05T10:00:00Z")
    private val published = mutableListOf<Published>()
    private val contextsCreated = AtomicInteger()
    private val executions = AtomicInteger()
    private lateinit var deliver: suspend (EventReactionId, EventReactionExecutionId, FakeTrigger, RetryCount, Instant?) -> ReactionOutcome

    private fun executor(supportsOrdering: Boolean = false) =
        EventReactionExecutor<FakeTrigger, Unit>(
            sink =
                object : EventReactionTriggerSink<FakeTrigger> {
                    override val supportsOrdering = supportsOrdering

                    override suspend fun publish(
                        id: EventReactionId,
                        trigger: FakeTrigger,
                        ordering: DispatchOrdering?,
                        notBefore: Instant?,
                    ) {
                        published += Published(id, ordering, notBefore)
                    }
                },
            source =
                object : EventReactionTriggerSource<FakeTrigger> {
                    override fun subscribe(
                        block: suspend (EventReactionId, EventReactionExecutionId, FakeTrigger, RetryCount, Instant?) -> ReactionOutcome,
                    ): Cancellable {
                        deliver = block
                        return object : Cancellable {
                            override fun cancel() {}
                        }
                    }
                },
            createExecutionContext = { _, _ -> contextsCreated.incrementAndGet() },
            execute = { _, _, _, _, _ ->
                executions.incrementAndGet()
                EventReactionExecutionResult.EventReactionExecutionCompleted
            },
            failureRetryHandler = { _, _, _, _, _, _ -> RetrySignal.Retry(1.seconds) },
            timeoutRetryHandler = { _, _, _, _, _ -> RetrySignal.Retry(1.seconds) },
            onCompletion = { _, _, _, _, _, _ -> },
            clock = { now },
        )

    private class OneEventBackend(
        private val event: PersistedEvent,
    ) : DomainEventPollingBackend {
        override fun readEventsAfter(
            position: EventLogPosition,
            limit: Int,
        ): List<PersistedEvent> = if (position == EventLogPosition.START) listOf(event) else emptyList()
    }

    @Test
    fun `dispatch hands notBefore to the sink`() =
        runBlocking {
            executor().dispatch(EventReactionId("r-1"), FakeTrigger(), notBefore = now + 60.seconds)

            assertEquals(listOf(Published(EventReactionId("r-1"), null, now + 60.seconds)), published)
        }

    @Test
    fun `dispatch refuses a reaction that is both ordered and delayed`() =
        runBlocking {
            val failure =
                assertFailsWith<IllegalArgumentException> {
                    executor(supportsOrdering = true).dispatch(
                        EventReactionId("r-1"),
                        FakeTrigger(),
                        ordering = DispatchOrdering("Order/o-1", 1, 0, OnGiveUp.ContinueWithNext),
                        notBefore = now + 60.seconds,
                    )
                }

            assertEquals(true, failure.message!!.contains("r-1"))
            assertEquals(emptyList(), published)
        }

    @Test
    fun `a delivery before notBefore waits without running and without counting a retry`() =
        runBlocking {
            executor().start()

            val outcome = deliver(EventReactionId("r-1"), EventReactionExecutionId("x-1"), FakeTrigger(), 0, now + 90.seconds)

            assertEquals(ReactionOutcome.Wait(90.seconds), outcome)
            assertEquals(0, contextsCreated.get())
            assertEquals(0, executions.get())
        }

    @Test
    fun `a delivery at or after notBefore runs normally`() =
        runBlocking {
            executor().start()

            assertEquals(
                ReactionOutcome.Finished(gaveUp = false),
                deliver(EventReactionId("r-1"), EventReactionExecutionId("x-1"), FakeTrigger(), 0, now),
            )
            assertEquals(1, executions.get())
        }

    @Test
    fun `an outbox passes a reaction's notBefore to dispatch`() =
        runBlocking {
            val outbox =
                AggregateEventOutbox(
                    backend = OneEventBackend(persistedEvent(globalOffset = 1)),
                    executor = executor(),
                    eventToReactions = { listOf(EventReaction(EventReactionId("remind"), FakeTrigger(), notBefore = now + 7.seconds)) },
                    getPosition = { EventLogPosition.START },
                    savePosition = {},
                    isLeader = { true },
                )

            outbox.tickForTest()

            assertEquals(listOf(Published(EventReactionId("remind"), null, now + 7.seconds)), published)
        }

    @Test
    fun `an ordered outbox fails loudly on a delayed reaction`() =
        runBlocking {
            val outbox =
                AggregateEventOutbox(
                    backend = OneEventBackend(persistedEvent(globalOffset = 1)),
                    executor = executor(supportsOrdering = true),
                    eventToReactions = { listOf(EventReaction(EventReactionId("remind"), FakeTrigger(), notBefore = now + 7.seconds)) },
                    getPosition = { EventLogPosition.START },
                    savePosition = {},
                    isLeader = { true },
                    ordering = ReactionOrdering.PerAggregate(),
                )

            assertFailsWith<IllegalArgumentException> { outbox.tickForTest() }
            assertEquals(emptyList(), published)
        }

    private data class Internal(
        val id: String,
    ) : DomainEvent

    private data class Public(
        val id: String,
    ) : PublicDomainEvent

    @Test
    fun `a contract subscription passes a reaction's notBefore to dispatch`() =
        runBlocking {
            val contract =
                PublicEventContract<Internal, Public>(
                    backend = OneEventBackend(persistedEvent(globalOffset = 1)),
                    serialization =
                        object : DataSerializationContext<Internal> {
                            override fun serialize(event: Internal) = SerializedEvent("Internal", 1, event.id)

                            override fun deserialize(serialized: SerializedEvent) = Internal(serialized.payload)
                        },
                    internalToPublic = { Public(it.id) },
                    getPosition = { EventLogPosition.START },
                    savePosition = {},
                    isLeader = { true },
                )
            contract.subscribe(executor()) { listOf(EventReaction(EventReactionId("later"), FakeTrigger(), notBefore = now + 3.seconds)) }

            contract.tickForTest()

            assertEquals(listOf(Published(EventReactionId("later"), null, now + 3.seconds)), published)
        }
}
```

If `PublicEventContract`'s constructor or `subscribe` signature differs from the snippet (check `OrderedSourceGuardTest.contract()` for the exact call), mirror that file's construction. The behaviour under test must stay the same.

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.event.reaction.DelayedReactionsTest'`
Expected: compilation FAILS: no `notBefore` parameters, no `clock`, no `ReactionOutcome.Wait`.

- [ ] **Step 3: Implement delayed reactions in the core**

In `Ordering.kt`, add to `ReactionOutcome`:

```kotlin
    /** The reaction isn't due yet: deliver it again after about [delay], without counting a retry. */
    data class Wait(
        val delay: Duration,
    ) : ReactionOutcome
```

In `EventReaction.kt`, add the imports `kotlin.time.Clock` and `kotlin.time.Instant`, then make these changes.

`EventReaction`:

```kotlin
/**
 * A reaction to dispatch for an event. [id] must be deterministic for a given (event, reaction kind) —
 * typically the event's id plus a label, e.g. `EventReactionId("charge-${eventId}")` — so that
 * re-dispatching the same event after a crash is recognised as a duplicate. A random id would create a
 * second reaction.
 *
 * [notBefore], if set, delays the reaction: it doesn't run before that time. Delayed reactions can't be ordered.
 */
data class EventReaction<T : EventReactionTrigger>(
    val id: EventReactionId,
    val trigger: T,
    val notBefore: Instant? = null,
)
```

`EventReactionTriggerSink.publish`:

```kotlin
    /**
     * Queues reaction [id] with [trigger]. Publishing an id that is already queued must not queue it twice.
     * An ordering stamp is only passed to sinks that support ordering; such sinks must run reactions with the
     * same key one at a time, in (sequence, ordinal) order. [notBefore], if not null, is the earliest time the
     * reaction may run: a sink that can schedule should hold the reaction back until then, and every sink must
     * carry it to the source, which passes it back on delivery. A reaction never has both an ordering stamp and
     * a [notBefore].
     */
    suspend fun publish(
        id: EventReactionId,
        trigger: T,
        ordering: DispatchOrdering?,
        notBefore: Instant?,
    )
```

`EventReactionTriggerSource.subscribe`:

```kotlin
    /**
     * Starts delivering reactions to [block], which runs one attempt and returns a [ReactionOutcome]:
     * [ReactionOutcome.Retry] if the reaction should run again (counting a retry), [ReactionOutcome.Wait] if it
     * isn't due yet (deliver again later without counting a retry), or [ReactionOutcome.Finished] once it is done.
     * The last argument is the reaction's `notBefore`, as published. Returns a handle that stops delivery.
     */
    fun subscribe(block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, Instant?) -> ReactionOutcome): Cancellable
```

`EventReactionExecutor`:
- Add `private val clock: () -> Instant = { Clock.System.now() },` as the last constructor parameter.
- Add to its KDoc: "A delivery before its `notBefore` is put back with [ReactionOutcome.Wait] without creating an execution context or calling [execute]."
- Replace `dispatch` with:

```kotlin
    /** Queues reaction [id] with [trigger] for execution, stamped with [ordering] if given, not before [notBefore] if given. */
    suspend fun dispatch(
        id: EventReactionId,
        trigger: T,
        ordering: DispatchOrdering? = null,
        notBefore: Instant? = null,
    ) {
        require(ordering == null || sink.supportsOrdering) { "This executor's sink does not support ordering" }
        require(ordering == null || notBefore == null) {
            "Delayed reactions can't be ordered: reaction ${id.value} has notBefore $notBefore but its subscription is ordered"
        }
        sink.publish(id, trigger, ordering, notBefore)
    }
```

- In `start()`, change the subscription lambda header to `source.subscribe { id, executionId, trigger, retryCount, notBefore ->`. Make these its first lines, before `createExecutionContext`:

```kotlin
                val now = clock()
                if (notBefore != null && now < notBefore) {
                    log.debug("Event reaction {} isn't due until {}; waiting", id.value, notBefore)
                    return@subscribe ReactionOutcome.Wait(notBefore - now)
                }
```

In `AggregateEventOutbox.kt`, change the dispatch loop to:

```kotlin
                reactions.forEachIndexed { ordinal, (id, trigger, notBefore) ->
                    log.debug(…unchanged…)
                    executor.dispatch(id, trigger, ordering.stampFor(envelope.metadata, ordinal), notBefore)
                }
```

In `PublicEventContract.kt` `fanOut`, change the dispatch to `executor.dispatch(reaction.id, reaction.trigger, ordering.stampFor(envelope.metadata, firstOrdinal + index), reaction.notBefore)`.

- [ ] **Step 4: Update the core test fakes**

- **`EventReactionExecutorTest.kt`:**
  - Add the `kotlin.time.Instant` import.
  - The `subscribed` property type and the fake source's `subscribe` parameter get a fifth parameter, `Instant?`.
  - The fake sink's `publish` gets `notBefore: Instant?`.
  - `runReaction` calls `subscribed(EventReactionId("r-1"), EventReactionExecutionId("x-1"), trigger, 0, null)`.
- **`OrderedSourceGuardTest.kt`:** the fake sink's `publish` gets `notBefore: Instant?`.
- **`RecordingExecutor.kt`:** the fake sink's `publish` gets `notBefore: Instant?` (still calling `onDispatch(id, trigger)`). The fake source's `subscribe` parameter gets a fifth `Instant?`.

Run: `./gradlew :kotmod:test`
Expected: PASS, including the 7 `DelayedReactionsTest` tests.

- [ ] **Step 5: Write the failing db-scheduler unit tests**

In `ReactionTaskDataTest.kt`, add:

```kotlin
    @Test
    fun `task data written before notBefore existed decodes as not delayed`() {
        assertEquals(ReactionTaskData(trigger = "t", retryCount = 0), ReactionTaskData.decode("""{"trigger":"t","retryCount":0}"""))
    }

    @Test
    fun `notBefore round-trips`() {
        val data = ReactionTaskData(trigger = "t", retryCount = 0, notBeforeEpochMillis = 1_760_000_000_000)
        assertEquals(data, ReactionTaskData.decode(data.encode()))
    }
```

In `TaskRowOutcomeTest.kt`, add:

```kotlin
    @Test
    fun `wait reschedules after the delay without incrementing retryCount`() {
        assertEquals(
            TaskRowOutcome.Reschedule(at = Instant.parse("2026-10-03T10:01:30Z"), taskData = data.encode()),
            outcomeAfterExecution(ReactionOutcome.Wait(90.seconds), data, now),
        )
    }
```

In `DbSchedulerTriggerSinkTest.kt`:
- Change every existing `sink.publish(a, b, c)` call to pass a fourth argument, `null`.
- Add:

```kotlin
    @Test
    fun `a delayed reaction is scheduled at notBefore and carries it in its task data`() {
        val instance = slot<TaskInstance<String>>()
        val notBefore = kotlin.time.Instant.parse("2026-10-10T09:00:00Z")
        every { client.scheduleIfNotExists(capture(instance), any<Instant>()) } returns true

        runBlocking { sink.publish(EventReactionId("remind-e-1"), FakeTrigger("remind"), null, notBefore) }

        verify(exactly = 1) { client.scheduleIfNotExists(any<TaskInstance<String>>(), Instant.parse("2026-10-10T09:00:00Z")) }
        assertEquals(notBefore.toEpochMilliseconds(), ReactionTaskData.decode(instance.captured.data).notBeforeEpochMillis)
    }
```

Run: `./gradlew :kotmod-db-scheduler:test`
Expected: compilation FAILS: no `notBeforeEpochMillis`, `publish` arity mismatch, and an unhandled `Wait`.

- [ ] **Step 6: Implement it in db-scheduler**

- **`ReactionTaskData.kt`:**
  - Add `val notBeforeEpochMillis: Long? = null,` as the last property.
  - Extend the KDoc: "…and, for delayed reactions, the earliest time they may run."
- **`DbSchedulerTriggerSink.publish`:**
  - It gains `notBefore: kotlin.time.Instant?`.
  - Build the task data with `notBeforeEpochMillis = notBefore?.toEpochMilliseconds()`.
  - Schedule with `client.scheduleIfNotExists(TaskInstance(taskName, instanceId, taskData), notBefore?.toJavaInstant() ?: clock())`, importing `kotlin.time.toJavaInstant`.
  - Extend the class KDoc: "A delayed reaction is scheduled at its `notBefore`."
- **`DbSchedulerTriggerSource.kt`:** `ReactionHandler<T>` becomes `suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, kotlin.time.Instant?) -> ReactionOutcome`.
- **`ReactionTask.kt`:** the `handler(...)` call gains a fifth argument, `data.notBeforeEpochMillis?.let { kotlin.time.Instant.fromEpochMilliseconds(it) }`.
- **`TaskRowOutcome.kt` `outcomeAfterExecution`:** add a branch, and change the function KDoc's first sentence to "Removes the row if the reaction finished, reschedules it after the requested delay with the retry count incremented for a retry, or unchanged for a wait."

```kotlin
        is ReactionOutcome.Wait ->
            TaskRowOutcome.Reschedule(
                at = now.plus(result.delay.coerceAtMost(MAX_RETRY_DELAY).toJavaDuration()),
                taskData = data.encode(),
            )
```

Run: `./gradlew :kotmod-db-scheduler:test`
Expected: PASS.

- [ ] **Step 7: Write the db-scheduler integration test**

In `DbSchedulerEventReactionsIntegrationTest.kt`, add the following (imports: `kotlin.time.Clock`, `kotlin.time.Duration.Companion.seconds`):

```kotlin
    @Test
    fun `a delayed reaction does not run before notBefore, then runs once with retryCount 0`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)
            val notBefore = Clock.System.now() + 3.seconds

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("later"), TestTrigger("first"), notBefore = notBefore)
                // A second dispatch of the pending reaction, with a different time, is ignored.
                executor.dispatch(EventReactionId("later"), TestTrigger("second"), notBefore = Clock.System.now())
                delay(1_500)
                assertTrue(recorder.attempts.isEmpty(), "ran before notBefore")
                eventually { recorder.completions.isNotEmpty() }
            }

            assertEquals(listOf(TestTrigger("first")), recorder.attempts.map { it.trigger })
            assertEquals(0, recorder.attempts.single().retryCount)
            assertTrue(Clock.System.now() >= notBefore)
        }
```

If `eventually`'s default timeout is shorter than about 5 seconds, pass a longer one, or check the helper's signature in `io.kotmod.postgres.support`.

Run: `./gradlew :kotmod-db-scheduler:integrationTest --tests '*DbSchedulerEventReactionsIntegrationTest*'`
Expected: PASS.

- [ ] **Step 8: Run every suite**

Run: `./gradlew :kotmod:test :kotmod:integrationTest :kotmod-sqldelight:integrationTest :kotmod-db-scheduler:test :kotmod-db-scheduler:integrationTest :examples:integrationTest`
Expected: PASS, with no `w:` lines. The examples don't implement a sink or source, so they compile unchanged.

- [ ] **Step 9: Commit**

```bash
git add -A kotmod kotmod-db-scheduler
git commit -m "Let reactions be delayed until a given time

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Document delayed reactions and the new queue contract

**Files:**
- Modify: `examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt` (a compiled review-reminder example)
- Modify: `README.md` ("The outbox and event reactions", "Using another queue (e.g. Google Pub/Sub)", "Upgrading from 0.1.0")

**Interfaces:**
- Consumes: Task 1's `AggregateKind` and `Orders`; Task 2's `EventReaction(id, trigger, notBefore)`, the sink and source signatures, and `ReactionOutcome.Wait`.
- Produces: nothing used later.

- [ ] **Step 1: Add a compiled review-reminder example**

In `ReadmeExamples.kt`, add a section `// Guide: Delayed reactions`. Mirror the trigger, serializer and outbox style already in the file, and use only types that exist there (reuse `OrderNotification`'s serializer style). It should be a function returning an `AggregateEventOutbox<OrderNotification>` that, for each `OrderShipped` event, returns:

```kotlin
EventReaction(
    id = EventReactionId("review-reminder-${event.metadata.eventId.value}"),
    trigger = SendReviewReminder(orderId = event.metadata.aggregateId.value),
    notBefore = event.metadata.timestamp + 7.days,
)
```

Add a new `@Serializable data class SendReviewReminder(val orderId: String, override val timeout: Duration? = null) : OrderNotification` next to `SendOrderConfirmation` in `Quickstart.kt`. Update any exhaustive `when` over `OrderNotification` in the examples, such as the quickstart executor's `execute`, so they still compile. If that `when` sits in a README snippet, update the README snippet identically.

Run: `./gradlew :examples:integrationTest --rerun-tasks`
Expected: PASS.

- [ ] **Step 2: Document delayed reactions**

In "The outbox and event reactions", after the retry-handler table and `BackoffStrategy` paragraph, add a subsection:

> #### Delayed reactions
>
> Give a reaction a `notBefore` and it doesn't run before that time. For example, ask for a review a week
> after an order ships:
>
> (the snippet from `ReadmeExamples.kt`)
>
> - With db-scheduler, a delayed reaction waits in `scheduled_tasks` until it is due, at no extra cost.
> - If a queue delivers a reaction early, the executor puts it back until it is due. It doesn't run, and it
>   doesn't count as a retry.
> - Delayed reactions can't be ordered: a delayed reaction would hold back every later reaction of its
>   aggregate. An ordered outbox or subscription that returns one fails with an `IllegalArgumentException`
>   naming it.

Add the subsection to the Contents list if the list goes to that depth. It currently lists `###` guides only, so check and follow the existing depth.

- [ ] **Step 3: Update "Using another queue"**

Replace the two interface bullets with:

> - **`EventReactionTriggerSink`** — `publish(id, trigger, ordering, notBefore)` queues a reaction. Publishing
>   an id that is already queued should not queue it twice; if your queue can't guarantee that, rely on your
>   reactions being idempotent (they must be anyway, since delivery is at-least-once). Carry `notBefore` with
>   the message, and if your queue can delay delivery, don't deliver before it.
> - **`EventReactionTriggerSource`** — `subscribe(block)` starts delivering queued reactions. For each
>   delivery, call `block` with the reaction id, a fresh execution id, the trigger, the retry count and the
>   reaction's `notBefore`, and act on what it returns:
>   - `ReactionOutcome.Finished` — the reaction is done (succeeded, cancelled or gave up): remove it from
>     the queue.
>   - `ReactionOutcome.Retry(delay)` — deliver it again after about `delay`, counting a retry.
>   - `ReactionOutcome.Wait(delay)` — it isn't due yet: deliver it again after about `delay` without
>     counting a retry.
>   - An exception — deliver it again later.

In the Pub/Sub sketch:
- Its sink's `publish` gains `notBefore` and puts it in a message attribute.
- Its source passes the attribute back to `block` as the fifth argument.
- It treats `Wait` like `Retry` but without incrementing its retry count. If its retry count comes from Pub/Sub's delivery attempt, say so in a comment: this sketch can't tell the difference, so a dead-letter policy should allow for waits.

The sketch is not compiled. Keep its existing caveat. After the sketch's list of gaps, add:

> **Delays on Google Cloud.** Pub/Sub can't hold a message back until a time. A sink can instead hand a
> delayed reaction to **Cloud Tasks** with a schedule time, and have the task publish it to Pub/Sub when it's
> due. Cloud Tasks limits how far ahead a task can be scheduled (about 30 days); for longer delays, the
> executor's check covers you, because a reaction that arrives early is simply scheduled again.

- [ ] **Step 4: Update the upgrade section**

In "Upgrading from 0.1.0":
- In step 3, change "Pass the `InitialState` object as `initial`, and your rejection type's serializer as `rejectionSerializer`, to `AggregateManager`" to "Declare an `AggregateKind` for the aggregate, as `Orders` in [the quickstart](#3-wire-up-persistence), and build `AggregateManager` from it and the `InitialState` object". Keep the rest of the step, including the type-argument sentence.
- Add a final step:

> 6. If you implemented your own queue, add the `notBefore` parameter to your sink's `publish` and carry it
>    to delivery, pass it to `block` as the fifth argument, and handle `ReactionOutcome.Wait` by delivering
>    again after the delay without counting a retry.

Check that the anchor `#3-wire-up-persistence` matches the step 3 heading slug.

- [ ] **Step 5: Check for drift**

Run: `grep -n 'publish(id, trigger, ordering)\|retry count, and act\|as .rejectionSerializer.' README.md`
Expected: no output.

Compare each README Kotlin snippet touched in Tasks 1 and 3 with its source in the example files; they must be identical apart from indentation inside test functions.

Run: `./gradlew :examples:integrationTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add README.md examples
git commit -m "Document delayed reactions and the updated queue contract

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

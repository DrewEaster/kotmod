# db-scheduler Event Reactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide a durable, db-scheduler-backed `EventReactionTriggerSink` / `EventReactionTriggerSource` so event reactions dispatched by `AggregateEventOutbox` and `PublicEventContract` are persisted in Postgres and executed with retries by `EventReactionExecutor`.

**Architecture:** A public `DbSchedulerEventReactions<T>` per executor exposes a db-scheduler custom `Task<String>` (registered by the app on its own `Scheduler`), a source (holds the executor's subscribed handler) and a sink factory (schedules via `scheduleIfNotExists`). Task data is a kotlinx-serialized JSON string `{trigger, retryCount}`; the task's execute handler calls the subscribed handler inside `runBlocking` and maps `RetrySignal.Retry?` to remove / reschedule-with-incremented-count. Failures outside the handler go to a capped exponential-backoff `FailureHandler`.

**Tech Stack:** Kotlin 2.4.20 (JVM 25), kotlinx-coroutines 1.11.0, kotlinx-serialization-json 1.11.0, db-scheduler 16.12.0, MockK 1.14.11, Testcontainers 2.0.5 + Postgres 17, Gradle.

**Spec:** `docs/superpowers/specs/2026-10-03-db-scheduler-event-reactions-design.md`

## Preconditions

- Work happens on branch `db-scheduler-event-reactions`.
- At the time of writing, the working tree contains **uncommitted earlier work** (offset manager, tenant removal, integration test setup in `build.gradle.kts` and `src/integrationTest`, renames). `build.gradle.kts` and `src/integrationTest/.../IntegrationTest.kt` are untracked, so committing them in a task would also commit that earlier work. **Before Task 1, confirm with the user that the earlier work has been committed** (or that they accept it landing in this plan's commits). Every commit step below lists exact paths — never use `git add -A` / `git add .`.

## Global Constraints

- db-scheduler version: `com.github.kagkarlsson:db-scheduler:16.12.0`, declared as an `api` dependency.
- All new main code lives in package `com.dreweaster.ddd.event.reaction.dbscheduler`.
- The library never creates, starts or stops a db-scheduler `Scheduler`; the app owns it.
- One db-scheduler task name per `DbSchedulerEventReactions` instance; task instance id is exactly `EventReactionId.value`.
- Task data stored in db-scheduler is always a `String`: `{"trigger":"<app-serialized trigger>","retryCount":<int>}`.
- Each execution gets a fresh `EventReactionExecutionId(randomId())` (existing `com.dreweaster.ddd.randomId`).
- `RetrySignal.Retry(delay)` → reschedule at `now + delay` with `retryCount + 1`; `null` → remove the row.
- No subscribed handler → reschedule at `now + unsubscribedRetryDelay` (default `5.seconds`) with task data unchanged, logging a warning.
- Failures outside the handler → exponential backoff starting at `10.seconds`, capped at `1.hours`, unlimited attempts.
- `scheduled_tasks` is not added to `DddSchema`.
- `EventReactionExecutor`, the sink/source interfaces, `AggregateEventOutbox` and `PublicEventContract` are not changed (except `BackoffStrategy`'s cap fix and KDoc).
- Run unit tests with `./gradlew test`; integration tests with `./gradlew integrationTest` (needs Docker; in sandboxed shells run with sandbox disabled).

## Review Focus

1. A trigger whose envelope decodes but which the app's serializer rejects — expect the executor never to be invoked and the row kept with growing `consecutiveFailures` (test in Task 6).
2. `createExecutionContext` throwing (it runs outside the executor's `runCatching`) — expect the db-scheduler failure path, not a consumed retry; no attempt recorded (test in Task 6).
3. The app's trigger serializer throwing during `publish` — expect nothing scheduled and the exception propagated to the poller (test in Task 4).
4. An executor stopped and started again — expect re-subscription to work and a stale `Cancellable` from the old subscription not to unsubscribe the new handler (test in Task 4).
5. `RetrySignal.Retry(Duration.ZERO)` — expect an immediate reschedule with `retryCount + 1`, not a removal (test in Task 3).

---

## File Structure

**Create (main):** `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/`
- `ReactionTaskData.kt` — internal serializable task payload with `encode()` / `decode()`.
- `ReactionOutcome.kt` — internal pure outcome model and the two outcome functions.
- `CappedExponentialBackoffFailureHandler.kt` — internal db-scheduler `FailureHandler<String>` with a cap.
- `DbSchedulerTriggerSource.kt` — internal source holding the subscribed handler.
- `DbSchedulerTriggerSink.kt` — internal sink calling `scheduleIfNotExists`.
- `ReactionTask.kt` — internal factory building the db-scheduler custom task.
- `DbSchedulerEventReactions.kt` — the public entry point.

**Modify (main):**
- `build.gradle.kts` — `java-library` plugin, db-scheduler `api` dependency.
- `src/main/kotlin/com/dreweaster/ddd/event/reaction/EventReaction.kt` — `BackoffStrategy` cap fix; KDoc on `EventReaction`.

**Create (unit tests):** `src/test/kotlin/com/dreweaster/ddd/event/reaction/`
- `BackoffStrategyTest.kt`
- `dbscheduler/ReactionTaskDataTest.kt`
- `dbscheduler/ReactionOutcomeTest.kt`
- `dbscheduler/CappedExponentialBackoffFailureHandlerTest.kt`
- `dbscheduler/DbSchedulerTriggerSourceTest.kt`
- `dbscheduler/DbSchedulerTriggerSinkTest.kt`

**Create / modify (integration tests):**
- Create `src/integrationTest/resources/db-scheduler/postgresql_tables.sql` — db-scheduler 16.12.0 DDL.
- Modify `src/integrationTest/kotlin/com/dreweaster/ddd/postgres/support/IntegrationTest.kt` — apply the DDL, truncate `scheduled_tasks`.
- Create `src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTestSupport.kt` — test trigger, serializer, scheduler/executor builders, `eventually`.
- Create `src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt`.

---

### Task 1: Fix `BackoffStrategy` cap

**Files:**
- Modify: `src/main/kotlin/com/dreweaster/ddd/event/reaction/EventReaction.kt` (the `BackoffStrategy` class)
- Test: `src/test/kotlin/com/dreweaster/ddd/event/reaction/BackoffStrategyTest.kt`

**Interfaces:**
- Consumes: nothing new.
- Produces: `BackoffStrategy(maximumDuration: Duration = 600.seconds).calculateBackoff(retryCount: RetryCount): Duration` — doubles from 1s, never exceeds `maximumDuration`, treats negative counts as 0.

- [ ] **Step 1: Write the failing test**

```kotlin
package com.dreweaster.ddd.event.reaction

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class BackoffStrategyTest {
    private val strategy = BackoffStrategy(maximumDuration = 600.seconds)

    @Test
    fun `doubles from one second`() {
        assertEquals(1.seconds, strategy.calculateBackoff(0))
        assertEquals(2.seconds, strategy.calculateBackoff(1))
        assertEquals(512.seconds, strategy.calculateBackoff(9))
    }

    @Test
    fun `caps at maximumDuration`() {
        assertEquals(600.seconds, strategy.calculateBackoff(10))
        assertEquals(600.seconds, strategy.calculateBackoff(100))
        assertEquals(600.seconds, strategy.calculateBackoff(Int.MAX_VALUE))
    }

    @Test
    fun `negative retry count is treated as zero`() {
        assertEquals(1.seconds, strategy.calculateBackoff(-1))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests 'com.dreweaster.ddd.event.reaction.BackoffStrategyTest'`
Expected: FAIL — `caps at maximumDuration` (e.g. 1024s returned for retry 10) and `negative retry count…`.

- [ ] **Step 3: Write minimal implementation**

Replace the body of `BackoffStrategy` in `EventReaction.kt`:

```kotlin
class BackoffStrategy(
    private val maximumDuration: Duration = 600.seconds,
) {
    fun calculateBackoff(retryCount: RetryCount): Duration {
        // 2^30 seconds is decades — far beyond any sensible cap — and keeps the shift from overflowing.
        val exponent = retryCount.coerceIn(0, 30)
        return minOf((1L shl exponent).seconds, maximumDuration)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL, all tests pass (existing 64 + 3 new).

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/com/dreweaster/ddd/event/reaction/EventReaction.kt src/test/kotlin/com/dreweaster/ddd/event/reaction/BackoffStrategyTest.kt
git commit -m "Cap BackoffStrategy at maximumDuration"
```

---

### Task 2: db-scheduler dependency and `ReactionTaskData`

**Files:**
- Modify: `build.gradle.kts`
- Create: `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionTaskData.kt`
- Test: `src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionTaskDataTest.kt`

**Interfaces:**
- Consumes: nothing.
- Produces: `internal data class ReactionTaskData(val trigger: String, val retryCount: Int)` with `fun encode(): String` and `companion fun decode(taskData: String): ReactionTaskData` (throws `IllegalArgumentException` — kotlinx `SerializationException` — on invalid input).

- [ ] **Step 1: Add the dependency**

In `build.gradle.kts`, add `` `java-library` `` as the first entry of the `plugins { }` block (gives the `api` configuration explicitly):

```kotlin
plugins {
    `java-library`
    kotlin("jvm")
    kotlin("plugin.serialization")
    id("app.cash.sqldelight") version "2.4.0"
}
```

and in `dependencies { }`, after the `implementation(...)` lines:

```kotlin
    api("com.github.kagkarlsson:db-scheduler:16.12.0")
```

- [ ] **Step 2: Write the failing test**

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ReactionTaskDataTest {
    @Test
    fun `encodes to the documented JSON shape`() {
        assertEquals("""{"trigger":"a","retryCount":0}""", ReactionTaskData("a", 0).encode())
    }

    @Test
    fun `round-trips triggers containing quotes, newlines and unicode`() {
        val data = ReactionTaskData(trigger = "{\"name\":\"x\"}\nline two ☃", retryCount = 7)
        assertEquals(data, ReactionTaskData.decode(data.encode()))
    }

    @Test
    fun `decode rejects garbage`() {
        assertFailsWith<IllegalArgumentException> { ReactionTaskData.decode("not json") }
        assertFailsWith<IllegalArgumentException> { ReactionTaskData.decode("""{"trigger":"a"}""") }
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew test --tests 'com.dreweaster.ddd.event.reaction.dbscheduler.ReactionTaskDataTest'`
Expected: FAIL — compilation error, `Unresolved reference 'ReactionTaskData'`.

- [ ] **Step 4: Write minimal implementation**

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * The payload stored in db-scheduler's `task_data` column. Always a plain [String] as far as
 * db-scheduler is concerned, so it works with whichever db-scheduler serializer the app configures.
 */
@Serializable
internal data class ReactionTaskData(
    val trigger: String,
    val retryCount: Int,
) {
    fun encode(): String = Json.encodeToString(serializer(), this)

    companion object {
        fun decode(taskData: String): ReactionTaskData = Json.decodeFromString(serializer(), taskData)
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add build.gradle.kts src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionTaskData.kt src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionTaskDataTest.kt
git commit -m "Add db-scheduler dependency and reaction task payload"
```

---

### Task 3: Outcome logic and capped failure handler

**Files:**
- Create: `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionOutcome.kt`
- Create: `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/CappedExponentialBackoffFailureHandler.kt`
- Test: `src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionOutcomeTest.kt`
- Test: `src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/CappedExponentialBackoffFailureHandlerTest.kt`

**Interfaces:**
- Consumes: `ReactionTaskData` (Task 2), `RetrySignal.Retry` (existing).
- Produces:
  - `internal sealed interface ReactionOutcome { data object Remove; data class Reschedule(val at: java.time.Instant, val taskData: String) }`
  - `internal fun outcomeAfterExecution(result: RetrySignal.Retry?, data: ReactionTaskData, now: java.time.Instant): ReactionOutcome`
  - `internal fun outcomeWhenUnsubscribed(rawTaskData: String, now: java.time.Instant, delay: kotlin.time.Duration): ReactionOutcome`
  - `internal class CappedExponentialBackoffFailureHandler(initialDelay: Duration, maximumDelay: Duration) : FailureHandler<String>` with `internal fun delayFor(previousConsecutiveFailures: Int): Duration`

- [ ] **Step 1: Write the failing tests**

`ReactionOutcomeTest.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.RetrySignal
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class ReactionOutcomeTest {
    private val now = Instant.parse("2026-10-03T10:00:00Z")
    private val data = ReactionTaskData(trigger = "t", retryCount = 2)

    @Test
    fun `null result removes the row`() {
        assertEquals(ReactionOutcome.Remove, outcomeAfterExecution(null, data, now))
    }

    @Test
    fun `retry reschedules after the delay with retryCount incremented`() {
        assertEquals(
            ReactionOutcome.Reschedule(
                at = Instant.parse("2026-10-03T10:00:30Z"),
                taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode(),
            ),
            outcomeAfterExecution(RetrySignal.Retry(30.seconds), data, now),
        )
    }

    @Test
    fun `zero-delay retry reschedules immediately and still increments retryCount`() {
        assertEquals(
            ReactionOutcome.Reschedule(at = now, taskData = ReactionTaskData(trigger = "t", retryCount = 3).encode()),
            outcomeAfterExecution(RetrySignal.Retry(Duration.ZERO), data, now),
        )
    }

    @Test
    fun `unsubscribed reschedules after the delay with task data unchanged`() {
        assertEquals(
            ReactionOutcome.Reschedule(at = Instant.parse("2026-10-03T10:00:05Z"), taskData = "raw, possibly undecodable"),
            outcomeWhenUnsubscribed("raw, possibly undecodable", now, 5.seconds),
        )
    }
}
```

`CappedExponentialBackoffFailureHandlerTest.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

class CappedExponentialBackoffFailureHandlerTest {
    private val handler = CappedExponentialBackoffFailureHandler(initialDelay = 10.seconds, maximumDelay = 1.hours)

    @Test
    fun `doubles from the initial delay`() {
        assertEquals(10.seconds, handler.delayFor(0))
        assertEquals(20.seconds, handler.delayFor(1))
        assertEquals(2560.seconds, handler.delayFor(8))
    }

    @Test
    fun `caps at the maximum delay`() {
        assertEquals(1.hours, handler.delayFor(9))
        assertEquals(1.hours, handler.delayFor(1_000))
        assertEquals(1.hours, handler.delayFor(Int.MAX_VALUE))
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests 'com.dreweaster.ddd.event.reaction.dbscheduler.*'`
Expected: FAIL — compilation errors, unresolved `ReactionOutcome`, `outcomeAfterExecution`, `CappedExponentialBackoffFailureHandler`.

- [ ] **Step 3: Write minimal implementation**

`ReactionOutcome.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.RetrySignal
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/** What the db-scheduler task should do with its row once an execution has finished. */
internal sealed interface ReactionOutcome {
    data object Remove : ReactionOutcome

    data class Reschedule(
        val at: Instant,
        val taskData: String,
    ) : ReactionOutcome
}

internal fun outcomeAfterExecution(
    result: RetrySignal.Retry?,
    data: ReactionTaskData,
    now: Instant,
): ReactionOutcome =
    when (result) {
        null -> ReactionOutcome.Remove
        else ->
            ReactionOutcome.Reschedule(
                at = now.plus(result.delay.toJavaDuration()),
                taskData = data.copy(retryCount = data.retryCount + 1).encode(),
            )
    }

internal fun outcomeWhenUnsubscribed(
    rawTaskData: String,
    now: Instant,
    delay: Duration,
): ReactionOutcome = ReactionOutcome.Reschedule(at = now.plus(delay.toJavaDuration()), taskData = rawTaskData)
```

`CappedExponentialBackoffFailureHandler.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.task.ExecutionComplete
import com.github.kagkarlsson.scheduler.task.ExecutionOperations
import com.github.kagkarlsson.scheduler.task.FailureHandler
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Handles failures that happen *outside* the subscribed reaction handler (undecodable task data,
 * a trigger the app's serializer rejects, a handler that throws). Retries indefinitely with
 * exponential backoff capped at [maximumDelay], so rows recover once a fix is deployed.
 * db-scheduler's own ExponentialBackoffFailureHandler has no cap, hence this class.
 */
internal class CappedExponentialBackoffFailureHandler(
    private val initialDelay: Duration,
    private val maximumDelay: Duration,
) : FailureHandler<String> {
    private val log = LoggerFactory.getLogger(CappedExponentialBackoffFailureHandler::class.java)

    override fun onFailure(
        executionComplete: ExecutionComplete,
        executionOperations: ExecutionOperations<String>,
    ) {
        val execution = executionComplete.execution
        val delay = delayFor(execution.consecutiveFailures)
        log.error(
            "Event reaction task {} instance {} failed outside its handler; retrying in {} [consecutiveFailures={}]",
            execution.taskName,
            execution.id,
            delay,
            execution.consecutiveFailures + 1,
            executionComplete.cause.orElse(null),
        )
        executionOperations.reschedule(executionComplete, executionComplete.timeDone.plus(delay.toJavaDuration()))
    }

    internal fun delayFor(previousConsecutiveFailures: Int): Duration {
        val exponent = previousConsecutiveFailures.coerceIn(0, 30)
        return minOf(initialDelay * (1 shl exponent), maximumDelay)
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionOutcome.kt src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/CappedExponentialBackoffFailureHandler.kt src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionOutcomeTest.kt src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/CappedExponentialBackoffFailureHandlerTest.kt
git commit -m "Add db-scheduler reaction outcome logic and capped failure handler"
```

---

### Task 4: Trigger source and sink

**Files:**
- Create: `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSource.kt`
- Create: `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSink.kt`
- Test: `src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSourceTest.kt`
- Test: `src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSinkTest.kt`

**Interfaces:**
- Consumes: `ReactionTaskData` (Task 2); existing `EventReactionTriggerSource`, `EventReactionTriggerSink`, `EventReactionTriggerSerializer`, `Cancellable`, `RetryCount`, `EventReactionId`, `EventReactionExecutionId`, `RetrySignal`.
- Produces:
  - `internal typealias ReactionHandler<T> = suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> RetrySignal.Retry?`
  - `internal class DbSchedulerTriggerSource<T : EventReactionTrigger> : EventReactionTriggerSource<T>` with `val handler: ReactionHandler<T>?` (volatile, read by the task).
  - `internal class DbSchedulerTriggerSink<T : EventReactionTrigger>(taskName: String, triggerSerializer: EventReactionTriggerSerializer<T>, client: SchedulerClient, clock: () -> java.time.Instant = java.time.Instant::now) : EventReactionTriggerSink<T>`

- [ ] **Step 1: Write the failing tests**

`DbSchedulerTriggerSourceTest.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.time.Duration

class DbSchedulerTriggerSourceTest {
    private data class FakeTrigger(
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private val source = DbSchedulerTriggerSource<FakeTrigger>()
    private val first: ReactionHandler<FakeTrigger> = { _, _, _, _ -> null }
    private val second: ReactionHandler<FakeTrigger> = { _, _, _, _ -> null }

    @Test
    fun `subscribe stores the handler`() {
        source.subscribe(first)
        assertSame(first, source.handler)
    }

    @Test
    fun `second subscribe while subscribed throws`() {
        source.subscribe(first)
        assertFailsWith<IllegalStateException> { source.subscribe(second) }
        assertSame(first, source.handler)
    }

    @Test
    fun `cancel clears the handler and allows re-subscription`() {
        source.subscribe(first).cancel()
        assertNull(source.handler)

        source.subscribe(second)
        assertSame(second, source.handler)
    }

    @Test
    fun `stale cancel from an earlier subscription does not clear a newer handler`() {
        val stale = source.subscribe(first)
        stale.cancel()
        source.subscribe(second)

        stale.cancel()

        assertSame(second, source.handler)
    }
}
```

`DbSchedulerTriggerSinkTest.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.TaskInstance
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration

class DbSchedulerTriggerSinkTest {
    private data class FakeTrigger(
        val name: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private object FakeTriggerSerializer : EventReactionTriggerSerializer<FakeTrigger> {
        override suspend fun serialize(trigger: FakeTrigger): String {
            require(trigger.name != "unserializable") { "cannot serialize" }
            return trigger.name
        }

        override suspend fun deserialize(serializedTrigger: String) = FakeTrigger(serializedTrigger)
    }

    private val now = Instant.parse("2026-10-03T10:00:00Z")
    private val client: SchedulerClient = mockk()
    private val sink = DbSchedulerTriggerSink("billing-reactions", FakeTriggerSerializer, client, clock = { now })

    @Test
    fun `publish schedules the reaction if not already scheduled`() {
        val instance = slot<TaskInstance<String>>()
        every { client.scheduleIfNotExists(capture(instance), any<Instant>()) } returns true

        runBlocking { sink.publish(EventReactionId("charge-e-1"), FakeTrigger("charge")) }

        verify(exactly = 1) { client.scheduleIfNotExists(any<TaskInstance<String>>(), now) }
        assertEquals("billing-reactions", instance.captured.taskName)
        assertEquals("charge-e-1", instance.captured.id)
        assertEquals(ReactionTaskData(trigger = "charge", retryCount = 0), ReactionTaskData.decode(instance.captured.data))
    }

    @Test
    fun `publish of an already-scheduled reaction is not an error`() {
        every { client.scheduleIfNotExists(any<TaskInstance<String>>(), any<Instant>()) } returns false

        runBlocking { sink.publish(EventReactionId("charge-e-1"), FakeTrigger("charge")) }
    }

    @Test
    fun `serializer failure propagates and nothing is scheduled`() {
        assertFailsWith<IllegalArgumentException> {
            runBlocking { sink.publish(EventReactionId("charge-e-1"), FakeTrigger("unserializable")) }
        }
        verify(exactly = 0) { client.scheduleIfNotExists(any<TaskInstance<String>>(), any<Instant>()) }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests 'com.dreweaster.ddd.event.reaction.dbscheduler.*'`
Expected: FAIL — compilation errors, unresolved `DbSchedulerTriggerSource`, `ReactionHandler`, `DbSchedulerTriggerSink`.

- [ ] **Step 3: Write minimal implementation**

`DbSchedulerTriggerSource.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.Cancellable
import com.dreweaster.ddd.event.reaction.EventReactionExecutionId
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSource
import com.dreweaster.ddd.event.reaction.RetryCount
import com.dreweaster.ddd.event.reaction.RetrySignal

internal typealias ReactionHandler<T> = suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> RetrySignal.Retry?

/**
 * Holds the handler of the single [com.dreweaster.ddd.event.reaction.EventReactionExecutor] subscribed
 * to this task. The db-scheduler task reads [handler] on every execution.
 */
internal class DbSchedulerTriggerSource<T : EventReactionTrigger> : EventReactionTriggerSource<T> {
    @Volatile
    var handler: ReactionHandler<T>? = null
        private set

    override fun subscribe(block: ReactionHandler<T>): Cancellable {
        synchronized(this) {
            check(handler == null) { "An event reaction executor is already subscribed to this source" }
            handler = block
        }
        return object : Cancellable {
            override fun cancel() {
                synchronized(this@DbSchedulerTriggerSource) {
                    if (handler === block) handler = null
                }
            }
        }
    }
}
```

`DbSchedulerTriggerSink.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSink
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.TaskInstance
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.time.Instant

internal class DbSchedulerTriggerSink<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    private val client: SchedulerClient,
    private val clock: () -> Instant = Instant::now,
) : EventReactionTriggerSink<T> {
    private val log = LoggerFactory.getLogger(DbSchedulerTriggerSink::class.java)

    override suspend fun publish(
        id: EventReactionId,
        trigger: T,
    ) {
        val taskData = ReactionTaskData(trigger = triggerSerializer.serialize(trigger), retryCount = 0).encode()
        val scheduled =
            withContext(Dispatchers.IO) {
                client.scheduleIfNotExists(TaskInstance(taskName, id.value, taskData), clock())
            }
        if (!scheduled) {
            log.debug("Event reaction {} already scheduled for task {}; ignoring duplicate dispatch", id.value, taskName)
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSource.kt src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSink.kt src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSourceTest.kt src/test/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTriggerSinkTest.kt
git commit -m "Add db-scheduler trigger source and sink"
```

---

### Task 5: The db-scheduler task, public entry point, and core integration tests

**Files:**
- Create: `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionTask.kt`
- Create: `src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactions.kt`
- Modify: `src/main/kotlin/com/dreweaster/ddd/event/reaction/EventReaction.kt` (KDoc on `EventReaction`)
- Create: `src/integrationTest/resources/db-scheduler/postgresql_tables.sql`
- Modify: `src/integrationTest/kotlin/com/dreweaster/ddd/postgres/support/IntegrationTest.kt`
- Create: `src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTestSupport.kt`
- Test: `src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt`

**Interfaces:**
- Consumes: `ReactionTaskData` (Task 2); `ReactionOutcome`, `outcomeAfterExecution`, `outcomeWhenUnsubscribed`, `CappedExponentialBackoffFailureHandler` (Task 3); `DbSchedulerTriggerSource`, `DbSchedulerTriggerSink`, `ReactionHandler` (Task 4); existing `randomId()` from `com.dreweaster.ddd`.
- Produces (public):
  ```kotlin
  class DbSchedulerEventReactions<T : EventReactionTrigger>(
      taskName: String,
      triggerSerializer: EventReactionTriggerSerializer<T>,
      unsubscribedRetryDelay: Duration = 5.seconds,
  ) {
      val task: Task<String>
      val source: EventReactionTriggerSource<T>
      fun sink(client: SchedulerClient): EventReactionTriggerSink<T>
  }
  ```
- Produces (integration test support, used by Task 6 and 7): `TestTrigger`, `TestTriggerSerializer`, `Attempt`, `ReactionRecorder`, `testScheduler(...)`, `testExecutor(...)`, `running(...)`, `eventually(...)` — exact code below.

- [ ] **Step 1: Add the db-scheduler DDL test resource**

Create `src/integrationTest/resources/db-scheduler/postgresql_tables.sql` (copied from db-scheduler v16.12.0, `db-scheduler/src/test/resources/postgresql_tables.sql`):

```sql
create table scheduled_tasks (
  task_name text not null,
  task_instance text not null,
  task_data bytea,
  execution_time timestamp with time zone not null,
  picked BOOLEAN not null,
  picked_by text,
  last_success timestamp with time zone,
  last_failure timestamp with time zone,
  consecutive_failures INT,
  last_heartbeat timestamp with time zone,
  version BIGINT not null,
  priority SMALLINT,
  PRIMARY KEY (task_name, task_instance)
);

CREATE INDEX execution_time_idx ON scheduled_tasks (execution_time);
CREATE INDEX last_heartbeat_idx ON scheduled_tasks (last_heartbeat);
CREATE INDEX priority_execution_time_idx on scheduled_tasks (priority desc, execution_time asc);
```

- [ ] **Step 2: Apply it in the integration test base class**

In `IntegrationTest.kt`, change the truncate statement to include `scheduled_tasks`:

```kotlin
                stmt.execute(
                    "TRUNCATE ddd_aggregate_root, ddd_domain_event, ddd_command_history, ddd_consumer_offset, " +
                        "scheduled_tasks RESTART IDENTITY",
                )
```

and replace the `init` block of `SharedPostgres` with:

```kotlin
    init {
        val dbSchedulerDdl =
            checkNotNull(SharedPostgres::class.java.getResource("/db-scheduler/postgresql_tables.sql")) {
                "db-scheduler DDL resource missing"
            }.readText()
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(DddSchema.ddl)
                stmt.execute(dbSchedulerDdl)
            }
        }
    }
```

Also update the class KDoc's second sentence to: "[DddSchema.ddl] and db-scheduler's `scheduled_tasks` DDL are applied once, and every table is truncated before each test."

- [ ] **Step 3: Write the integration test support**

`DbSchedulerTestSupport.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionCompletionResult
import com.dreweaster.ddd.event.reaction.EventReactionExecutionId
import com.dreweaster.ddd.event.reaction.EventReactionExecutionResult
import com.dreweaster.ddd.event.reaction.EventReactionExecutor
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.event.reaction.RetrySignal
import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

data class TestTrigger(
    val name: String,
    override val timeout: Duration? = null,
) : EventReactionTrigger

/** Serializes a trigger as its name. Names starting with "poison" are rejected on deserialize. */
object TestTriggerSerializer : EventReactionTriggerSerializer<TestTrigger> {
    override suspend fun serialize(trigger: TestTrigger): String = trigger.name

    override suspend fun deserialize(serializedTrigger: String): TestTrigger {
        require(!serializedTrigger.startsWith("poison")) { "cannot deserialize $serializedTrigger" }
        return TestTrigger(serializedTrigger)
    }
}

data class Attempt(
    val id: EventReactionId,
    val executionId: EventReactionExecutionId,
    val trigger: TestTrigger,
    val retryCount: Int,
)

/** Thread-safe record of what an executor saw. */
class ReactionRecorder {
    val attempts = CopyOnWriteArrayList<Attempt>()
    val completions = CopyOnWriteArrayList<Pair<EventReactionId, EventReactionCompletionResult>>()
}

fun testScheduler(
    dataSource: DataSource,
    vararg tasks: Task<*>,
): Scheduler =
    Scheduler
        .create(dataSource, *tasks)
        .pollingInterval(java.time.Duration.ofMillis(100))
        .enableImmediateExecution()
        .threads(4)
        .shutdownMaxWait(java.time.Duration.ofSeconds(2))
        .build()

fun testExecutor(
    reactions: DbSchedulerEventReactions<TestTrigger>,
    client: SchedulerClient,
    recorder: ReactionRecorder,
    execute: (Attempt) -> EventReactionExecutionResult = { EventReactionExecutionResult.EventReactionExecutionCompleted },
    failureRetryHandler: (Attempt, Throwable) -> RetrySignal = { _, _ -> RetrySignal.Retry(100.milliseconds) },
    createExecutionContext: (EventReactionId, TestTrigger) -> Unit = { _, _ -> },
): EventReactionExecutor<TestTrigger, Unit> =
    EventReactionExecutor(
        sink = reactions.sink(client),
        source = reactions.source,
        createExecutionContext = { id, trigger -> createExecutionContext(id, trigger) },
        execute = { id, executionId, trigger, retryCount, _ ->
            val attempt = Attempt(id, executionId, trigger, retryCount)
            recorder.attempts += attempt
            execute(attempt)
        },
        failureRetryHandler = { id, executionId, trigger, retryCount, _, ex ->
            failureRetryHandler(Attempt(id, executionId, trigger, retryCount), ex)
        },
        timeoutRetryHandler = { _, _, _, _, _ -> RetrySignal.Retry(100.milliseconds) },
        onCompletion = { id, _, _, _, _, result -> recorder.completions += id to result },
    )

/** Starts executors before the scheduler and stops them after it — the documented lifecycle order. */
suspend fun <R> running(
    scheduler: Scheduler,
    vararg executors: EventReactionExecutor<*, *>,
    block: suspend () -> R,
): R {
    executors.forEach { it.start() }
    scheduler.start()
    try {
        return block()
    } finally {
        scheduler.stop()
        executors.forEach { it.stop() }
    }
}

suspend fun eventually(
    timeout: Duration = 10.seconds,
    condition: () -> Boolean,
) {
    withTimeout(timeout) {
        while (!condition()) delay(50)
    }
}
```

- [ ] **Step 4: Write the failing integration tests (spec tests 1–4)**

`DbSchedulerEventReactionsIntegrationTest.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionCompletionResult
import com.dreweaster.ddd.event.reaction.EventReactionExecutionResult
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.RetrySignal
import com.dreweaster.ddd.postgres.support.IntegrationTest
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DbSchedulerEventReactionsIntegrationTest : IntegrationTest() {
    private fun rowExists(
        reactions: DbSchedulerEventReactions<TestTrigger>,
        scheduler: com.github.kagkarlsson.scheduler.Scheduler,
        id: String,
    ) = scheduler.getScheduledExecution(reactions.task.instanceId(id)).isPresent

    @Test
    fun `dispatched reaction executes once, completes and its row is removed`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("hello"))
                eventually { recorder.completions.isNotEmpty() && !rowExists(reactions, scheduler, "r-1") }
            }

            assertEquals(1, recorder.attempts.size)
            assertEquals(TestTrigger("hello"), recorder.attempts.single().trigger)
            assertEquals(0, recorder.attempts.single().retryCount)
            assertEquals(
                listOf(EventReactionId("r-1") to EventReactionCompletionResult.EventReactionCompleted),
                recorder.completions.toList(),
            )
        }

    @Test
    fun `duplicate dispatch while pending results in one row and one execution`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)

            // Scheduler not started yet, so the first dispatch is still pending when the second arrives.
            executor.dispatch(EventReactionId("r-1"), TestTrigger("first"))
            executor.dispatch(EventReactionId("r-1"), TestTrigger("second"))
            assertEquals(1, scheduler.getScheduledExecutionsForTask("test-reactions").size)

            running(scheduler, executor) {
                eventually { recorder.completions.isNotEmpty() }
                delay(500) // give a hypothetical duplicate time to show up
            }

            assertEquals(listOf(TestTrigger("first")), recorder.attempts.map { it.trigger })
        }

    @Test
    fun `retries carry an increasing retryCount and fresh execution ids until success`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor =
                testExecutor(
                    reactions,
                    scheduler,
                    recorder,
                    execute = { attempt ->
                        if (attempt.retryCount < 2) {
                            EventReactionExecutionResult.EventReactionFailed(RuntimeException("attempt ${attempt.retryCount}"))
                        } else {
                            EventReactionExecutionResult.EventReactionExecutionCompleted
                        }
                    },
                )

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("flaky"))
                eventually { recorder.completions.isNotEmpty() && !rowExists(reactions, scheduler, "r-1") }
            }

            assertEquals(listOf(0, 1, 2), recorder.attempts.map { it.retryCount })
            assertEquals(3, recorder.attempts.map { it.executionId }.toSet().size)
            assertEquals(EventReactionCompletionResult.EventReactionCompleted, recorder.completions.single().second)
        }

    @Test
    fun `DoNotRetry passes the failure to onCompletion and removes the row`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val failure = EventReactionCompletionResult.EventReactionFailed("boom", allowManualRetry = false)
            val executor =
                testExecutor(
                    reactions,
                    scheduler,
                    recorder,
                    execute = { EventReactionExecutionResult.EventReactionFailed(RuntimeException("boom")) },
                    failureRetryHandler = { _, _ -> RetrySignal.DoNotRetry(failure) },
                )

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("doomed"))
                eventually { recorder.completions.isNotEmpty() && !rowExists(reactions, scheduler, "r-1") }
            }

            assertEquals(1, recorder.attempts.size)
            assertEquals(listOf(EventReactionId("r-1") to failure), recorder.completions.toList())
            assertTrue(scheduler.getScheduledExecutionsForTask("test-reactions").isEmpty())
        }
}
```

- [ ] **Step 5: Run integration tests to verify they fail**

Run: `./gradlew integrationTest --tests 'com.dreweaster.ddd.event.reaction.dbscheduler.*'`
Expected: FAIL — compilation error, `Unresolved reference 'DbSchedulerEventReactions'`.

- [ ] **Step 6: Implement the task factory**

`ReactionTask.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionExecutionId
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.randomId
import com.github.kagkarlsson.scheduler.task.CompletionHandler
import com.github.kagkarlsson.scheduler.task.helper.CustomTask
import com.github.kagkarlsson.scheduler.task.helper.Tasks
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

private val log = LoggerFactory.getLogger("com.dreweaster.ddd.event.reaction.dbscheduler.ReactionTask")

internal fun <T : EventReactionTrigger> reactionTask(
    taskName: String,
    triggerSerializer: EventReactionTriggerSerializer<T>,
    source: DbSchedulerTriggerSource<T>,
    unsubscribedRetryDelay: Duration,
): CustomTask<String> =
    Tasks
        .custom(taskName, String::class.java)
        .onFailure(CappedExponentialBackoffFailureHandler(initialDelay = 10.seconds, maximumDelay = 1.hours))
        .execute { instance, _ ->
            val handler = source.handler
            val outcome =
                if (handler == null) {
                    log.warn(
                        "No event reaction executor subscribed to task {}; rescheduling {} in {}. " +
                            "Start executors before the db-scheduler Scheduler and stop them after it.",
                        taskName,
                        instance.id,
                        unsubscribedRetryDelay,
                    )
                    outcomeWhenUnsubscribed(instance.data, Instant.now(), unsubscribedRetryDelay)
                } else {
                    // Decoding and deserialization failures throw, handing the row to the failure handler.
                    val data = ReactionTaskData.decode(instance.data)
                    val executionId = EventReactionExecutionId(randomId())
                    log.debug(
                        "Executing event reaction {} [task={}, executionId={}, retryCount={}]",
                        instance.id,
                        taskName,
                        executionId.value,
                        data.retryCount,
                    )
                    val result =
                        runBlocking {
                            handler(
                                EventReactionId(instance.id),
                                executionId,
                                triggerSerializer.deserialize(data.trigger),
                                data.retryCount,
                            )
                        }
                    outcomeAfterExecution(result, data, Instant.now())
                }
            outcome.toCompletionHandler()
        }

private fun ReactionOutcome.toCompletionHandler(): CompletionHandler<String> =
    CompletionHandler { executionComplete, executionOperations ->
        when (this) {
            ReactionOutcome.Remove -> executionOperations.remove()
            is ReactionOutcome.Reschedule -> executionOperations.reschedule(executionComplete, at, taskData)
        }
    }
```

- [ ] **Step 7: Implement the public entry point**

`DbSchedulerEventReactions.kt`:

```kotlin
package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSink
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSource
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.Task
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Durable event reactions backed by db-scheduler. Create one per
 * [com.dreweaster.ddd.event.reaction.EventReactionExecutor]; each instance is one db-scheduler
 * task named [taskName], and each reaction is a task instance whose id is its
 * [com.dreweaster.ddd.event.reaction.EventReactionId].
 *
 * The app owns the db-scheduler `Scheduler`:
 * ```
 * val reactions = DbSchedulerEventReactions("billing-reactions", BillingTriggerSerializer)
 * val scheduler = Scheduler.create(dataSource, reactions.task).threads(10).build()
 * val executor = EventReactionExecutor(sink = reactions.sink(scheduler), source = reactions.source, ...)
 *
 * executor.start()   // executors start before the scheduler...
 * scheduler.start()
 * ...
 * scheduler.stop()
 * executor.stop()    // ...and stop after it
 * ```
 * If the scheduler runs a reaction while no executor is subscribed, the reaction is rescheduled
 * [unsubscribedRetryDelay] later (without consuming a retry) and a warning is logged.
 *
 * Delivery is at-least-once: reaction ids must be deterministic per (event, reaction kind) so that
 * duplicate dispatches are absorbed while pending, and handlers must be idempotent.
 * The app must create db-scheduler's `scheduled_tasks` table itself.
 */
class DbSchedulerEventReactions<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    unsubscribedRetryDelay: Duration = 5.seconds,
) {
    private val triggerSource = DbSchedulerTriggerSource<T>()

    /** Register this with the app's db-scheduler `Scheduler`. */
    val task: Task<String> = reactionTask(taskName, triggerSerializer, triggerSource, unsubscribedRetryDelay)

    val source: EventReactionTriggerSource<T> get() = triggerSource

    fun sink(client: SchedulerClient): EventReactionTriggerSink<T> = DbSchedulerTriggerSink(taskName, triggerSerializer, client)
}
```

- [ ] **Step 8: Document the reaction id rule on `EventReaction`**

In `EventReaction.kt`, add KDoc directly above `data class EventReaction`:

```kotlin
/**
 * A reaction to dispatch for an event. [id] must be deterministic for a given (event, reaction kind) —
 * typically built from the event's `eventId` plus a label, e.g. `EventReactionId("charge-${eventId}")` —
 * so that re-dispatching the same event after a crash is recognised as a duplicate. A random id would
 * create a second reaction.
 */
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL; unit tests all pass; integration tests = previous 21 + 4 new, all pass.

- [ ] **Step 10: Commit**

```bash
git add src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/ReactionTask.kt src/main/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactions.kt src/main/kotlin/com/dreweaster/ddd/event/reaction/EventReaction.kt src/integrationTest/resources/db-scheduler/postgresql_tables.sql src/integrationTest/kotlin/com/dreweaster/ddd/postgres/support/IntegrationTest.kt src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerTestSupport.kt src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt
git commit -m "Add DbSchedulerEventReactions backed by a db-scheduler custom task"
```

---

### Task 6: Safety net, failure paths and multi-executor routing

**Files:**
- Test: `src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt` (add tests)

**Interfaces:**
- Consumes: `DbSchedulerEventReactions`, `testScheduler`, `testExecutor`, `running`, `eventually`, `ReactionRecorder`, `TestTrigger`, `TestTriggerSerializer` (Task 5).
- Produces: nothing new. These tests should pass against Task 5's implementation; if one fails, fix the implementation in `ReactionTask.kt` / `DbSchedulerTriggerSource.kt`, not the test.

- [ ] **Step 1: Add the tests**

Add these imports to the test file if not present:

```kotlin
import com.github.kagkarlsson.scheduler.task.TaskInstance
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds
```

Add these test methods inside `DbSchedulerEventReactionsIntegrationTest`:

```kotlin
    @Test
    fun `reaction picked up before the executor subscribes is rescheduled and runs once it does`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer, unsubscribedRetryDelay = 300.milliseconds)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)
            val dispatchedAt = Instant.now()
            executor.dispatch(EventReactionId("r-1"), TestTrigger("early"))

            scheduler.start() // wrong order on purpose: no executor subscribed yet
            try {
                eventually {
                    val execution = scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get()
                    execution.executionTime.isAfter(dispatchedAt.plusMillis(250))
                }
                val execution = scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get()
                assertEquals(0, execution.consecutiveFailures)
                assertTrue(recorder.attempts.isEmpty())

                executor.start()
                eventually { recorder.completions.isNotEmpty() }
            } finally {
                scheduler.stop()
                executor.stop()
            }

            assertEquals(listOf(0), recorder.attempts.map { it.retryCount })
        }

    @Test
    fun `undecodable task data or an undeserializable trigger is retried by the failure handler without reaching the executor`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)

            // Garbage envelope, written directly with db-scheduler's own API.
            scheduler.schedule(TaskInstance("test-reactions", "garbage", "not json"), Instant.now())
            // Valid envelope, but TestTriggerSerializer rejects triggers starting with "poison".
            executor.dispatch(EventReactionId("poisoned"), TestTrigger("poison-pill"))

            running(scheduler, executor) {
                eventually {
                    listOf("garbage", "poisoned").all { id ->
                        scheduler.getScheduledExecution(reactions.task.instanceId(id)).get().consecutiveFailures >= 1
                    }
                }
            }

            assertTrue(recorder.attempts.isEmpty())
            assertTrue(recorder.completions.isEmpty())
        }

    @Test
    fun `createExecutionContext throwing goes to the failure handler without consuming a retry`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor =
                testExecutor(
                    reactions,
                    scheduler,
                    recorder,
                    createExecutionContext = { _, _ -> error("context unavailable") },
                )

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("needs-context"))
                eventually { scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get().consecutiveFailures >= 1 }
            }

            val data = scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get().data as String
            assertTrue(data.contains("\"retryCount\":0"), "retry count should be untouched, was $data")
            assertTrue(recorder.attempts.isEmpty())
        }

    @Test
    fun `two executors on one scheduler each receive only their own reactions`() =
        runBlocking {
            val billing = DbSchedulerEventReactions("billing-reactions", TestTriggerSerializer)
            val notifications = DbSchedulerEventReactions("notification-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, billing.task, notifications.task)
            val billingRecorder = ReactionRecorder()
            val notificationRecorder = ReactionRecorder()
            val billingExecutor = testExecutor(billing, scheduler, billingRecorder)
            val notificationExecutor = testExecutor(notifications, scheduler, notificationRecorder)

            running(scheduler, billingExecutor, notificationExecutor) {
                billingExecutor.dispatch(EventReactionId("charge-e-1"), TestTrigger("charge"))
                notificationExecutor.dispatch(EventReactionId("confirm-e-1"), TestTrigger("confirm"))
                // Same id under a different task name is a different reaction.
                notificationExecutor.dispatch(EventReactionId("charge-e-1"), TestTrigger("charge-receipt"))
                eventually { billingRecorder.completions.size == 1 && notificationRecorder.completions.size == 2 }
            }

            assertEquals(listOf(TestTrigger("charge")), billingRecorder.attempts.map { it.trigger })
            assertEquals(
                setOf(TestTrigger("confirm"), TestTrigger("charge-receipt")),
                notificationRecorder.attempts.map { it.trigger }.toSet(),
            )
        }
```

- [ ] **Step 2: Run the integration tests**

Run: `./gradlew integrationTest --tests 'com.dreweaster.ddd.event.reaction.dbscheduler.*'`
Expected: PASS (8 tests). If `reaction picked up before the executor subscribes…` fails, check `ReactionTask.kt` reads `source.handler` per execution rather than capturing it once.

- [ ] **Step 3: Commit**

```bash
git add src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt
git commit -m "Cover db-scheduler safety net, failure paths and multi-executor routing"
```

---

### Task 7: End-to-end outbox delivery through db-scheduler

**Files:**
- Test: `src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt` (add test)

**Interfaces:**
- Consumes: Task 5 support; existing `AggregateEventOutbox(backend, executor, eventToReactions, getOffset, saveOffset, isLeader, pollInterval, batchSize)`, `PostgresDomainPersistenceBackend(driver, serialization)`, `PostgresDomainPollingBackend(driver)`, `PostgresOffsetManager(driver)`, `orderEventSerialization()` and `driver`/`dataSource` from `IntegrationTest`.
- Produces: nothing new.

- [ ] **Step 1: Add the test**

Add these imports to the test file if not present:

```kotlin
import com.dreweaster.ddd.AggregateId
import com.dreweaster.ddd.AggregateType
import com.dreweaster.ddd.CommandId
import com.dreweaster.ddd.EventId
import com.dreweaster.ddd.EventMetadata
import com.dreweaster.ddd.PendingEvent
import com.dreweaster.ddd.event.reaction.EventReaction
import com.dreweaster.ddd.outbox.AggregateEventOutbox
import com.dreweaster.ddd.postgres.PostgresDomainPersistenceBackend
import com.dreweaster.ddd.postgres.PostgresDomainPollingBackend
import com.dreweaster.ddd.postgres.PostgresOffsetManager
import com.dreweaster.ddd.postgres.support.orderEventSerialization
import com.dreweaster.ddd.support.OrderPlaced
```

Add this test method:

```kotlin
    @Test
    fun `outbox delivers reactions end to end through db-scheduler and saves its offset`() =
        runBlocking {
            val persistence = PostgresDomainPersistenceBackend(driver, orderEventSerialization())
            val offsets = PostgresOffsetManager(driver)
            listOf("e-1", "e-2").forEachIndexed { index, eventId ->
                persistence.appendEvents(
                    listOf(
                        PendingEvent(
                            metadata =
                                EventMetadata(
                                    eventId = EventId(eventId),
                                    aggregateType = AggregateType("Order"),
                                    aggregateId = AggregateId("o-$index"),
                                    causationId = CommandId("cmd-$eventId"),
                                    correlationId = null,
                                    timestamp = kotlin.time.Instant.parse("2026-10-03T10:00:00Z"),
                                ),
                            event = OrderPlaced("widgets-$index"),
                        ),
                    ),
                )
            }

            val reactions = DbSchedulerEventReactions("order-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)
            val outbox =
                AggregateEventOutbox(
                    backend = PostgresDomainPollingBackend(driver),
                    executor = executor,
                    eventToReactions = { event ->
                        listOf(
                            EventReaction(
                                id = EventReactionId("charge-${event.metadata.eventId.value}"),
                                trigger = TestTrigger("charge-${event.metadata.aggregateId.value}"),
                            ),
                        )
                    },
                    getOffset = { offsets.getOffset("order-outbox") },
                    saveOffset = { offsets.saveOffset("order-outbox", it) },
                    isLeader = { true },
                    pollInterval = 50.milliseconds,
                )

            running(scheduler, executor) {
                outbox.start()
                try {
                    eventually { recorder.completions.size == 2 }
                } finally {
                    outbox.stop()
                }
            }

            assertEquals(
                setOf(EventReactionId("charge-e-1"), EventReactionId("charge-e-2")),
                recorder.completions.map { it.first }.toSet(),
            )
            assertEquals(
                setOf(TestTrigger("charge-o-0"), TestTrigger("charge-o-1")),
                recorder.attempts.map { it.trigger }.toSet(),
            )
            assertEquals(2L, offsets.getOffset("order-outbox"))
            assertTrue(scheduler.getScheduledExecutionsForTask("order-reactions").isEmpty())
        }
```

- [ ] **Step 2: Run the full suite**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL; unit tests all pass; integration tests = 21 pre-existing + 9 new, all pass.

- [ ] **Step 3: Commit**

```bash
git add src/integrationTest/kotlin/com/dreweaster/ddd/event/reaction/dbscheduler/DbSchedulerEventReactionsIntegrationTest.kt
git commit -m "Add end-to-end outbox delivery test through db-scheduler"
```

# README Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two-line `README.md` with documentation that takes a new user from installation to a working aggregate with durable event reactions, then explains every part of kotmod — with all Kotlin examples compiled and the quickstart run as a test.

**Architecture:** The README's code lives first in the `integrationTest` source set, package `io.kotmod.readme`: `Quickstart.kt` (domain, repository, trigger and serializer declarations), `QuickstartTest.kt` (the quickstart wiring, run end-to-end against the Testcontainer Postgres) and `ReadmeExamples.kt` (guide snippets that must compile, with a test for the migration example). README code blocks are copied from those files, and a throwaway checker script confirms every README Kotlin line exists in them.

**Tech Stack:** Kotlin 2.4.20, kotlinx-serialization 1.11.0, SQLDelight JDBC driver 2.4.0, db-scheduler 16.12.0, Postgres 17 via Testcontainers 2.0.5, GitHub-flavoured Markdown with Mermaid.

**Spec:** `docs/superpowers/specs/2026-10-04-readme-documentation-design.md`

## Global Constraints

- Primary reader: public open-source users; explain concepts briefly before using them.
- Pitch leads with exactly two points: **domain events without event sourcing** and **transactional outbox built in**.
- One `README.md`, ~500–800 lines, with a table of contents; guides written as self-contained sections.
- Installation uses placeholder coordinates `implementation("io.kotmod:kotmod:<version>")` with a "publishing coming soon" note; requirements: JVM 25 toolchain (as built), Kotlin, Postgres.
- All examples use package `io.kotmod` APIs and the orders running example (`Order`, `OrderEvent`, `OrderNotification`, billing).
- Every README ```` ```kotlin ```` block must be copied from `Quickstart.kt`, `QuickstartTest.kt` or `ReadmeExamples.kt`, except blocks immediately preceded by `<!-- not-compiled -->` (only the Gradle dependency block).
- Example source files carry the comment `// Keep in sync with README.md.`
- Licence: Apache 2.0 (per `LICENSE`).
- No library code changes; if an example exposes a library bug, stop and record it as a ruling instead of changing library code.
- Tests: `./gradlew integrationTest` (needs Docker; run with the sandbox disabled in sandboxed shells).

## Review Focus

1. README snippets drifting from the compiled sources — expect every README Kotlin line to exist, in order, in an example source file (checker script in Tasks 3–5).
2. A reader without db-scheduler's `scheduled_tasks` table — expect the quickstart to say where the DDL comes from, with a pinned link to db-scheduler 16.12.0's `postgresql_tables.sql` (grep check in Task 3).
3. A reader whose reactions seem to "never run" — expect the README to mention db-scheduler's default 10-second polling and `enableImmediateExecution()` (grep check in Task 3).
4. A reader whose `Repository` opens its own connection — expect the README to say it must borrow the driver's connection to join the transaction, showing `withConnection` (grep check in Task 4).
5. Broken table-of-contents links — expect every TOC anchor to match a heading (anchor check in Task 5).

---

## File Structure

- Create `src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt` — orders domain (state, events), `OrderRepository`, `withConnection`, `OrderNotification` trigger + serializer, default `sendConfirmation`.
- Create `src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt` — quickstart steps 3–5 verbatim inside a test, plus the `orders` table DDL.
- Create `src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt` — guide snippets (cancel with command id, conflict retry, `EventProducer`, migrations, multi-executor scheduler, cancelling a reaction, public contract).
- Create `src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamplesTest.kt` — checks the migration example reads old payloads.
- Modify `README.md` — the documentation.

Throwaway checker (not committed), saved to the session scratchpad as `readme_check.py` in Task 3 and reused in Tasks 4–5.

---

### Task 1: Quickstart example code, run as a test

**Files:**
- Create: `src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt`
- Test: `src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt`

**Interfaces:**
- Consumes: `io.kotmod.postgres.support.IntegrationTest` (`protected val dataSource: DataSource`), `io.kotmod.event.reaction.dbscheduler.eventually(timeout, condition)` from existing integration test support.
- Produces (package `io.kotmod.readme`, used by Tasks 2–5): `Order`, `PendingOrder(item)`, `ShippedOrder(item)`, `CancelledOrder(item, reason)`; `OrderEvent`, `OrderPlaced(item)`, `OrderShipped(item)`, `OrderCancelled(item, reason)`; `class OrderRepository(driver: JdbcDriver) : Repository<Order>`; `fun <R> JdbcDriver.withConnection(block: (Connection) -> R): R`; `OrderNotification`, `SendOrderConfirmation(orderId, timeout = null)`; `object OrderNotificationSerializer : EventReactionTriggerSerializer<OrderNotification>`; `fun sendConfirmation(orderId: String)`.

- [ ] **Step 1: Write the failing test**

`QuickstartTest.kt`:

```kotlin
package io.kotmod.readme

// Keep in sync with README.md (Quickstart, steps 1, 3, 4 and 5).

import app.cash.sqldelight.TransacterImpl
import app.cash.sqldelight.driver.jdbc.asJdbcDriver
import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionCompletionResult
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.RetrySignal
import io.kotmod.event.reaction.dbscheduler.DbSchedulerEventReactions
import io.kotmod.event.reaction.dbscheduler.eventually
import io.kotmod.outbox.AggregateEventOutbox
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class QuickstartTest : IntegrationTest() {
    @BeforeEach
    fun createOrdersTable() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(ORDERS_TABLE_DDL)
                stmt.execute("TRUNCATE orders")
            }
        }
    }

    @Test
    fun `quickstart places and ships an order and sends one confirmation`() =
        runBlocking {
            val sentConfirmations = CopyOnWriteArrayList<String>()

            fun sendConfirmation(orderId: String) {
                sentConfirmations += orderId
            }

            val driver = dataSource.asJdbcDriver()

            val serialization =
                jsonDataSerializationContext<OrderEvent> {
                    +OrderPlaced.serializer().toEventSerializer()
                    +OrderShipped.serializer().toEventSerializer()
                    +OrderCancelled.serializer().toEventSerializer()
                }

            val orders =
                AggregateManager(
                    aggregateType = AggregateType("Order"),
                    repository = OrderRepository(driver),
                    backend = PostgresDomainPersistenceBackend(driver, serialization),
                    transacter = object : TransacterImpl(driver) {},
                )

            val orderId = AggregateId("order-1")

            orders.create(orderId) {
                PendingOrder("book") to listOf(OrderPlaced("book"))
            }

            val shipped =
                orders.execute<PendingOrder>(orderId) { order ->
                    ShippedOrder(order.item) to listOf(OrderShipped(order.item))
                }

            val notifications = DbSchedulerEventReactions("order-notifications", OrderNotificationSerializer)

            val scheduler =
                Scheduler
                    .create(dataSource, notifications.task)
                    .threads(4)
                    .enableImmediateExecution()
                    .build()

            val executor =
                EventReactionExecutor<OrderNotification, Unit>(
                    sink = notifications.sink(scheduler),
                    source = notifications.source,
                    createExecutionContext = { _, _ -> },
                    execute = { _, _, trigger, _, _ ->
                        when (trigger) {
                            is SendOrderConfirmation -> sendConfirmation(trigger.orderId)
                        }
                        EventReactionExecutionResult.EventReactionExecutionCompleted
                    },
                    failureRetryHandler = { _, _, _, retryCount, _, ex ->
                        if (retryCount < 5) {
                            RetrySignal.Retry(BackoffStrategy().calculateBackoff(retryCount))
                        } else {
                            RetrySignal.DoNotRetry(
                                EventReactionCompletionResult.EventReactionFailed(ex.message ?: "failed", allowManualRetry = true),
                            )
                        }
                    },
                    timeoutRetryHandler = { _, _, _, retryCount, _ ->
                        RetrySignal.Retry(BackoffStrategy().calculateBackoff(retryCount))
                    },
                    onCompletion = { id, _, _, _, _, result ->
                        println("Reaction ${id.value} finished: $result")
                    },
                )

            val offsets = PostgresOffsetManager(driver)

            val outbox =
                AggregateEventOutbox<OrderNotification>(
                    backend = PostgresDomainPollingBackend(driver),
                    executor = executor,
                    eventToReactions = { event ->
                        when (serialization.deserialize(event.serialized)) {
                            is OrderPlaced ->
                                listOf(
                                    EventReaction(
                                        id = EventReactionId("confirmation-${event.metadata.eventId.value}"),
                                        trigger = SendOrderConfirmation(orderId = event.metadata.aggregateId.value),
                                    ),
                                )
                            else -> emptyList()
                        }
                    },
                    getOffset = { offsets.getOffset("order-notifications") },
                    saveOffset = { offsets.saveOffset("order-notifications", it) },
                    isLeader = { true },
                )

            executor.start()
            scheduler.start()
            outbox.start()

            try {
                eventually(15.seconds) { sentConfirmations.isNotEmpty() }
                delay(500) // give a duplicate time to show up
            } finally {
                outbox.stop()
                scheduler.stop()
                executor.stop()
            }

            assertEquals(ShippedOrder("book"), shipped)
            assertEquals(listOf("order-1"), sentConfirmations.toList())
        }

    private companion object {
        const val ORDERS_TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS orders (
                id     TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                item   TEXT NOT NULL,
                reason TEXT
            )
        """
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew integrationTest --tests 'io.kotmod.readme.QuickstartTest'`
Expected: FAIL — compilation errors, `Unresolved reference 'OrderEvent'` (and the other `Quickstart.kt` declarations).

- [ ] **Step 3: Write the example declarations**

`Quickstart.kt`:

```kotlin
package io.kotmod.readme

// Keep in sync with README.md (Quickstart, steps 2, 3 and 5).

import app.cash.sqldelight.driver.jdbc.JdbcDriver
import io.kotmod.AggregateId
import io.kotmod.DomainEvent
import io.kotmod.Repository
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.sql.Connection
import kotlin.time.Duration

sealed interface Order

data class PendingOrder(
    val item: String,
) : Order

data class ShippedOrder(
    val item: String,
) : Order

data class CancelledOrder(
    val item: String,
    val reason: String,
) : Order

@Serializable
sealed interface OrderEvent : DomainEvent

@Serializable
data class OrderPlaced(
    val item: String,
) : OrderEvent

@Serializable
data class OrderShipped(
    val item: String,
) : OrderEvent

@Serializable
data class OrderCancelled(
    val item: String,
    val reason: String,
) : OrderEvent

class OrderRepository(
    private val driver: JdbcDriver,
) : Repository<Order> {
    override fun get(id: AggregateId): Order? =
        driver.withConnection { conn ->
            conn.prepareStatement("SELECT status, item, reason FROM orders WHERE id = ?").use { ps ->
                ps.setString(1, id.value)
                ps.executeQuery().use { rs ->
                    if (!rs.next()) {
                        null
                    } else {
                        val item = rs.getString("item")
                        when (rs.getString("status")) {
                            "PENDING" -> PendingOrder(item)
                            "SHIPPED" -> ShippedOrder(item)
                            else -> CancelledOrder(item, rs.getString("reason"))
                        }
                    }
                }
            }
        }

    override fun save(
        id: AggregateId,
        state: Order,
    ) {
        val (status, item, reason) =
            when (state) {
                is PendingOrder -> Triple("PENDING", state.item, null)
                is ShippedOrder -> Triple("SHIPPED", state.item, null)
                is CancelledOrder -> Triple("CANCELLED", state.item, state.reason)
            }
        driver.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO orders (id, status, item, reason) VALUES (?, ?, ?, ?) " +
                        "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, item = EXCLUDED.item, reason = EXCLUDED.reason",
                ).use { ps ->
                    ps.setString(1, id.value)
                    ps.setString(2, status)
                    ps.setString(3, item)
                    ps.setString(4, reason)
                    ps.executeUpdate()
                }
        }
    }
}

// Borrows the driver's connection, which is the transaction's connection inside AggregateManager.
fun <R> JdbcDriver.withConnection(block: (Connection) -> R): R {
    val (connection, close) = connectionAndClose()
    try {
        return block(connection)
    } finally {
        close()
    }
}

@Serializable
sealed interface OrderNotification : EventReactionTrigger

@Serializable
data class SendOrderConfirmation(
    val orderId: String,
    override val timeout: Duration? = null,
) : OrderNotification

object OrderNotificationSerializer : EventReactionTriggerSerializer<OrderNotification> {
    override suspend fun serialize(trigger: OrderNotification): String = Json.encodeToString(OrderNotification.serializer(), trigger)

    override suspend fun deserialize(serializedTrigger: String): OrderNotification =
        Json.decodeFromString(OrderNotification.serializer(), serializedTrigger)
}

fun sendConfirmation(orderId: String) {
    println("Sending confirmation for order $orderId")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew integrationTest --tests 'io.kotmod.readme.QuickstartTest'`
Expected: PASS (1 test). If compilation fails on `AggregateEventOutbox`/`EventReaction` type inference or on the narrowed `execute`, fix the example (not the library) with the smallest change, record a ruling, and use the fixed form in the README.

- [ ] **Step 5: Run the full suites**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL; 86 unit tests, 31 integration tests, all passing.

- [ ] **Step 6: Commit**

```bash
git add src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt
git commit -m "Add runnable quickstart example for the README"
```

---

### Task 2: Guide examples

**Files:**
- Create: `src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt`
- Test: `src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamplesTest.kt`

**Interfaces:**
- Consumes: Task 1's `Order*`, `OrderEvent*`, `OrderNotification`, `OrderNotificationSerializer`.
- Produces (used by Task 4): `cancelOrder(...)`, `retryOnConflict(...)`, `AuditEvent`, `OrderViewed(viewer)`, `auditLog(driver)`, `recordView(...)`, `orderEventSerialization`, `BillingTrigger`, `ChargeCustomer(orderId, timeout = null)`, `BillingTriggerSerializer`, `sharedScheduler(...)`, `cancelPendingConfirmation(...)`, `OrderPublicEvent`, `OrderPlacedV1(item)`, `orderContract(...)`.

- [ ] **Step 1: Write the failing test**

`ReadmeExamplesTest.kt`:

```kotlin
package io.kotmod.readme

import io.kotmod.SerializedEvent
import kotlin.test.Test
import kotlin.test.assertEquals

class ReadmeExamplesTest {
    @Test
    fun `migration example reads events stored under the old class name`() {
        val old = SerializedEvent(type = "com.example.orders.OrderDispatched", version = 1, payload = """{"item":"book"}""")
        assertEquals(OrderShipped("book"), orderEventSerialization.deserialize(old))
    }

    @Test
    fun `migration example fills in the reason for old cancellations`() {
        val old = SerializedEvent(type = OrderCancelled::class.qualifiedName!!, version = 1, payload = """{"item":"book"}""")
        assertEquals(OrderCancelled("book", "not recorded"), orderEventSerialization.deserialize(old))
    }

    @Test
    fun `migration example writes new events at the latest version`() {
        val serialized = orderEventSerialization.serialize(OrderCancelled("book", "changed mind"))
        assertEquals(2, serialized.version)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew integrationTest --tests 'io.kotmod.readme.ReadmeExamplesTest'`
Expected: FAIL — compilation error, `Unresolved reference 'orderEventSerialization'`.

- [ ] **Step 3: Write the guide examples**

`ReadmeExamples.kt`:

```kotlin
package io.kotmod.readme

// Keep in sync with README.md (Guides). These must compile; QuickstartTest runs the quickstart.

import app.cash.sqldelight.TransacterImpl
import app.cash.sqldelight.driver.jdbc.JdbcDriver
import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.EventId
import io.kotmod.EventProducer
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.PublicDomainEvent
import io.kotmod.UnexpectedAggregateStateException
import io.kotmod.contract.PublicEventContract
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.dbscheduler.DbSchedulerEventReactions
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import javax.sql.DataSource
import kotlin.time.Duration

// Guide: Aggregates and commands

suspend fun cancelOrder(
    orders: AggregateManager<Order, OrderEvent>,
    orderId: AggregateId,
    reason: String,
    requestId: String,
): Order =
    try {
        orders.execute<PendingOrder>(orderId, commandId = CommandId(requestId)) { order ->
            CancelledOrder(order.item, reason) to listOf(OrderCancelled(order.item, reason))
        }
    } catch (e: UnexpectedAggregateStateException) {
        throw IllegalStateException("Only pending orders can be cancelled", e)
    }

suspend fun <T> retryOnConflict(
    attempts: Int = 3,
    command: suspend () -> T,
): T {
    repeat(attempts - 1) {
        try {
            return command()
        } catch (e: OptimisticConcurrencyException) {
            // Someone else changed the aggregate first: run the command again against the latest state.
        }
    }
    return command()
}

// Guide: Event-only aggregates

@Serializable
sealed interface AuditEvent : DomainEvent

@Serializable
data class OrderViewed(
    val viewer: String,
) : AuditEvent

fun auditLog(driver: JdbcDriver): EventProducer<AuditEvent> =
    EventProducer(
        aggregateType = AggregateType("OrderAuditLog"),
        backend =
            PostgresDomainPersistenceBackend(
                driver,
                jsonDataSerializationContext<AuditEvent> { +OrderViewed.serializer().toEventSerializer() },
            ),
        transacter = object : TransacterImpl(driver) {},
    )

suspend fun recordView(
    auditLog: EventProducer<AuditEvent>,
    orderId: AggregateId,
    viewer: String,
    requestId: String,
) {
    auditLog.emit(orderId, listOf(OrderViewed(viewer)), commandId = CommandId(requestId))
}

// Guide: Event serialization and schema migrations

val orderEventSerialization =
    jsonDataSerializationContext<OrderEvent> {
        +OrderPlaced.serializer().toEventSerializer()
        // OrderShipped used to be called OrderDispatched, in another package.
        +OrderShipped.serializer().toEventSerializer(initialClassName = "com.example.orders.OrderDispatched") {
            migrateClassName(OrderShipped::class.qualifiedName!!)
        }
        // OrderCancelled gained a `reason` field; older events get a default.
        +OrderCancelled.serializer().toEventSerializer {
            migrateFormat { json -> JsonObject(json + ("reason" to JsonPrimitive("not recorded"))) }
        }
    }

// Guide: Durable reactions with db-scheduler

@Serializable
sealed interface BillingTrigger : EventReactionTrigger

@Serializable
data class ChargeCustomer(
    val orderId: String,
    override val timeout: Duration? = null,
) : BillingTrigger

object BillingTriggerSerializer : EventReactionTriggerSerializer<BillingTrigger> {
    override suspend fun serialize(trigger: BillingTrigger): String = Json.encodeToString(BillingTrigger.serializer(), trigger)

    override suspend fun deserialize(serializedTrigger: String): BillingTrigger =
        Json.decodeFromString(BillingTrigger.serializer(), serializedTrigger)
}

fun sharedScheduler(
    dataSource: DataSource,
    billing: DbSchedulerEventReactions<BillingTrigger>,
    notifications: DbSchedulerEventReactions<OrderNotification>,
): Scheduler =
    Scheduler
        .create(dataSource, billing.task, notifications.task)
        .threads(10)
        .enableImmediateExecution()
        .build()

fun cancelPendingConfirmation(
    scheduler: Scheduler,
    notifications: DbSchedulerEventReactions<OrderNotification>,
    eventId: EventId,
) {
    scheduler.cancel(notifications.task.instanceId("confirmation-${eventId.value}"))
}

// Guide: Publishing events to other contexts

@Serializable
sealed interface OrderPublicEvent : PublicDomainEvent

@Serializable
data class OrderPlacedV1(
    val item: String,
) : OrderPublicEvent

fun orderContract(
    driver: JdbcDriver,
    serialization: DataSerializationContext<OrderEvent>,
    offsets: PostgresOffsetManager,
    billingExecutor: EventReactionExecutor<BillingTrigger, *>,
): PublicEventContract<OrderEvent, OrderPublicEvent> {
    val contract =
        PublicEventContract<OrderEvent, OrderPublicEvent>(
            backend = PostgresDomainPollingBackend(driver),
            serialization = serialization,
            internalToPublic = { event ->
                when (event) {
                    is OrderPlaced -> OrderPlacedV1(event.item)
                    else -> null
                }
            },
            getOffset = { offsets.getOffset("order-contract") },
            saveOffset = { offsets.saveOffset("order-contract", it) },
            isLeader = { true },
        )

    contract.subscribe(billingExecutor) { envelope ->
        when (envelope.event) {
            is OrderPlacedV1 ->
                listOf(
                    EventReaction<BillingTrigger>(
                        id = EventReactionId("charge-${envelope.metadata.eventId.value}"),
                        trigger = ChargeCustomer(orderId = envelope.metadata.aggregateId.value),
                    ),
                )
        }
    }
    return contract
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew integrationTest --tests 'io.kotmod.readme.*'`
Expected: PASS (4 tests: 1 quickstart + 3 examples). Fix compile errors in the example (not the library) with a recorded ruling.

- [ ] **Step 5: Run the full suites**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL; 86 unit tests, 34 integration tests.

- [ ] **Step 6: Commit**

```bash
git add src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamplesTest.kt
git commit -m "Add compiled guide examples for the README"
```

---

### Task 3: README — introduction, installation, quickstart, core concepts

**Files:**
- Modify: `README.md` (replace entirely)

**Interfaces:**
- Consumes: code from Task 1 (`Quickstart.kt`, `QuickstartTest.kt`).
- Produces: README headings used by the TOC in Task 5: `## Why kotmod`, `## Installation`, `## Quickstart`, `## Core concepts`, `## Guides`, `## Running in production`, `## Status and contributing`.

- [ ] **Step 1: Save the snippet checker (not committed)**

Save to the session scratchpad as `readme_check.py`:

```python
"""Checks every ```kotlin block in README.md against the compiled example sources.

A block passes if its non-blank lines, stripped of indentation, appear in the same order in one
source file. Blocks immediately preceded by <!-- not-compiled --> are skipped. Also checks that
every TOC link (#anchor) matches a heading.
"""
import re, sys
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
readme = (root / "README.md").read_text()
sources = {p.name: [l.strip() for l in p.read_text().splitlines() if l.strip()]
           for p in (root / "src/integrationTest/kotlin/io/kotmod/readme").glob("*.kt")}

def in_order(needle, hay):
    i = 0
    for line in hay:
        if i < len(needle) and line == needle[i]:
            i += 1
    return i == len(needle)

failures = 0
for m in re.finditer(r"(<!-- not-compiled -->\s*)?```kotlin\n(.*?)```", readme, re.S):
    if m.group(1):
        continue
    block = [l.strip() for l in m.group(2).splitlines() if l.strip()]
    if not any(in_order(block, src) for src in sources.values()):
        failures += 1
        line_no = readme[: m.start()].count("\n") + 1
        print(f"README.md:{line_no}: kotlin block not found in example sources:\n  " + "\n  ".join(block[:4]))

def slug(h):
    s = re.sub(r"[^\w\- ]", "", h.strip().lower())
    return s.replace(" ", "-")

anchors = {slug(h) for h in re.findall(r"^#{1,6} (.+)$", readme, re.M)}
for a in re.findall(r"\]\(#([^)]+)\)", readme):
    if a not in anchors:
        failures += 1
        print(f"broken anchor: #{a}")

print("OK" if failures == 0 else f"{failures} problem(s)")
sys.exit(1 if failures else 0)
```

- [ ] **Step 2: Write the first half of the README**

Replace `README.md` with these sections, in order. Prose is new; every ```` ```kotlin ```` block is copied from the named source.

1. `# kotmod` + one-paragraph pitch: a Kotlin library for building DDD aggregates on Postgres; bold the two lead points (domain events without event sourcing; transactional outbox built in).
2. `## Contents` — placeholder heading; the full TOC is written in Task 5.
3. `## Why kotmod` — 6–8 lines: aggregates keep plain state in your own tables and kotmod records their events in the same transaction; this avoids the dual-write problem (state saved, message lost — or the reverse); events then reliably drive reactions and public events; what kotmod is not (not event sourcing, not a message broker, not a framework — a library you wire into your app).
4. `## Installation` — `<!-- not-compiled -->` then a ```` ```kotlin ```` Gradle block with `implementation("io.kotmod:kotmod:<version>")`; note publishing to Maven Central is coming; requirements (JVM 25 toolchain, Kotlin, Postgres; db-scheduler 16.12.0 comes in as an API dependency).
5. `## Quickstart` — intro sentence (orders domain: place an order, ship it, send a confirmation email when it's placed), then five numbered subsections:
   1. *Create the tables* — the `orders` table SQL from `QuickstartTest.ORDERS_TABLE_DDL` (as a ```` ```sql ```` block); run `DddSchema.ddl` (from `io.kotmod.postgres`) in your migrations; create db-scheduler's `scheduled_tasks` table from https://github.com/kagkarlsson/db-scheduler/blob/v16.12.0/db-scheduler/src/test/resources/postgresql_tables.sql .
   2. *Define state and events* — from `Quickstart.kt`: `Order` hierarchy, `OrderEvent` hierarchy; explain state vs events in one sentence each.
   3. *Wire up persistence* — say "given a `javax.sql.DataSource` (e.g. HikariCP)"; from `QuickstartTest.kt`: `driver`, `serialization`, `orders`; then `OrderRepository` and `withConnection` from `Quickstart.kt`, with one sentence on why the repository borrows the driver's connection.
   4. *Run commands* — from `QuickstartTest.kt`: `orderId`, `orders.create`, `orders.execute<PendingOrder>`; note these are `suspend` calls.
   5. *React to events* — from `Quickstart.kt`: `OrderNotification`, `SendOrderConfirmation`, `OrderNotificationSerializer`, `sendConfirmation`; from `QuickstartTest.kt`: `notifications`, `scheduler`, `executor`, `offsets`, `outbox`, and the three `start()` calls. Mention db-scheduler polls every 10 seconds by default and `enableImmediateExecution()` runs newly dispatched reactions straight away. End with what the reader sees (`Sending confirmation for order order-1`) and the shutdown order (`outbox.stop()`, `scheduler.stop()`, `executor.stop()`).
6. `## Core concepts` — a ```` ```mermaid ```` flowchart: Command → AggregateManager → (one transaction) state in your tables + events in `ddd_domain_event` → AggregateEventOutbox → EventReactionExecutor → db-scheduler; and `ddd_domain_event` → PublicEventContract → subscribers. Follow with a short glossary: aggregate, command, domain event, event reaction, trigger, public event.

- [ ] **Step 3: Run the checks**

Run: `python3 <scratchpad>/readme_check.py .` — Expected: `OK` (anchors not yet present except headings; no TOC links yet).
Run: `grep -c "postgresql_tables.sql" README.md` — Expected: ≥ 1 (Review Focus 2).
Run: `grep -c "enableImmediateExecution" README.md` and `grep -c "10 seconds" README.md` — Expected: ≥ 1 each (Review Focus 3).

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "Write README introduction, installation, quickstart and core concepts"
```

---

### Task 4: README — guides

**Files:**
- Modify: `README.md` (append `## Guides` with seven `###` subsections after `## Core concepts`)

**Interfaces:**
- Consumes: Task 2's `ReadmeExamples.kt` declarations; Task 1's declarations.
- Produces: `### ` guide headings for the TOC: `Aggregates and commands`, `Event-only aggregates`, `Event serialization and schema migrations`, `Postgres setup`, `The outbox and event reactions`, `Durable reactions with db-scheduler`, `Publishing events to other contexts`.

- [ ] **Step 1: Write the guides**

Each guide: one sentence on when to use it → code copied from the named source → the behaviours below as short prose or bullets → the key classes in backticks.

1. **Aggregates and commands** — code: `cancelOrder` and `retryOnConflict` from `ReadmeExamples.kt`. Cover: read / command / write phases with no database access while the block runs; `create` vs `execute` vs `execute<T>`; `CommandId` makes retries idempotent (no id → random → not idempotent); `CorrelationId` ties a flow together; `OptimisticConcurrencyException` and retrying; `AggregateNotFoundException`, `AggregateAlreadyExistsException`, `UnexpectedAggregateStateException`; the repository must borrow the driver's connection via `withConnection` so it joins the transaction (Review Focus 4).
2. **Event-only aggregates** — code: `AuditEvent`, `OrderViewed`, `auditLog`, `recordView`. Cover: when `EventProducer` fits (event log + idempotent commands, no stored state); `emit` creates bookkeeping on first use; optimistic concurrency still applies.
3. **Event serialization and schema migrations** — code: `orderEventSerialization`. Cover: registration with `+X.serializer().toEventSerializer()`; each event is stored with its class name and a version; `migrateFormat` adds a version with a JSON transformation; `migrateClassName` records a rename/move and requires `initialClassName` to be the original name; old events migrate when read, new events are written at the latest version; you can implement `DataSerializationContext` yourself.
4. **Postgres setup** — no new Kotlin (reference quickstart). Cover: tables in `DddSchema.ddl` (`ddd_aggregate_root`, `ddd_domain_event`, `ddd_command_history`, `ddd_consumer_offset`) and copying it into Flyway/Liquibase; `scheduled_tasks` is the app's table (db-scheduler DDL link); `asJdbcDriver()` on any `DataSource`; `TransacterImpl(driver)` or your SQLDelight-generated database as the `Transacter` (same driver); `PostgresDomainPollingBackend`; `PostgresOffsetManager` with one consumer name per poller.
5. **The outbox and event reactions** — code: the `executor` and `outbox` blocks from `QuickstartTest.kt` may be referenced, not repeated. Cover: outbox polls → maps → dispatches → saves offset after each event; executor parameters (`execute`, `failureRetryHandler`, `timeoutRetryHandler`, `onCompletion`, `createExecutionContext`, `defaultTimeout`, `BackoffStrategy`); a table mapping `EventReactionExecutionResult` (Completed / Cancelled / Failed / TimedOut) to what happens (`onCompletion` / retry handler → `RetrySignal.Retry` or `DoNotRetry`) and the resulting `EventReactionCompletionResult`; one paragraph on implementing your own `EventReactionTriggerSink`/`Source`.
6. **Durable reactions with db-scheduler** — code: `BillingTrigger`, `ChargeCustomer`, `BillingTriggerSerializer`, `sharedScheduler`, `cancelPendingConfirmation`. Cover: app owns the `Scheduler`; one task name per executor; several executors on one scheduler; retries become reschedules with an incremented retry count; a fresh execution id per attempt; misordered startup is rescheduled with a warning; unreadable stored data retries with backoff (10s → 1h); remove a reaction for good with `scheduler.cancel(...)`.
7. **Publishing events to other contexts** — code: `OrderPublicEvent`, `OrderPlacedV1`, `orderContract`. Cover: why public events are separate; `internalToPublic` returning `null` keeps an event private; subscribers receive `PublicEventEnvelope` (event + metadata); several subscribers/executors; `subscribe` before `start`; its own consumer name for offsets.

- [ ] **Step 2: Run the checks**

Run: `python3 <scratchpad>/readme_check.py .` — Expected: `OK`.
Run: `grep -c "withConnection" README.md` — Expected: ≥ 2 (Review Focus 4).

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "Write README guides"
```

---

### Task 5: README — production, status, table of contents

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: headings from Tasks 3–4.
- Produces: the final README.

- [ ] **Step 1: Write `## Running in production`**

Cover, in this order:
- **At-least-once delivery** — events are never lost; a reaction can run more than once (crash between dispatch and offset save; shutdown interrupt); make `execute` and `onCompletion` idempotent.
- **Deterministic reaction ids** — build from `eventId` + a label (as in the quickstart); a random id creates duplicates.
- **Lifecycle order** — startup: executors → `scheduler.start()` → outbox/contracts; shutdown in reverse; misordering is survivable (rescheduled with a warning) but noisy.
- **Leader election** — outbox and contracts poll only while `isLeader()` returns true; run one active poller per consumer name (e.g. a Postgres advisory lock or your platform's leader election); db-scheduler is cluster-safe on every node.
- **Failure behaviour** — a table: reaction fails → your retry handler; reaction times out → your timeout handler; stored data unreadable → retried with backoff 10s–1h; node crashes mid-reaction → db-scheduler revives it; database down during dispatch → outbox batch halts and retries next poll.
- **Tuning** — `pollInterval` (default 500ms) and `batchSize` (default 100) on the outbox/contract; `Scheduler.threads(n)` bounds how many reactions run at once.

- [ ] **Step 2: Write `## Status and contributing`**

Pre-1.0, APIs may change; issues and PRs welcome; `./gradlew test` for unit tests and `./gradlew integrationTest` for integration tests (needs Docker); licensed under Apache 2.0 (link `LICENSE`).

- [ ] **Step 3: Write the table of contents**

Replace the `## Contents` placeholder with links to every `##` section and each `###` guide, e.g. `- [Quickstart](#quickstart)`, `- [Aggregates and commands](#aggregates-and-commands)`.

- [ ] **Step 4: Run the checks**

Run: `python3 <scratchpad>/readme_check.py .` — Expected: `OK` (snippets and every TOC anchor; Review Focus 1 and 5).
Run: `wc -l README.md` — Expected: roughly 500–800.
Run: `./gradlew integrationTest --tests 'io.kotmod.readme.*'` — Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Finish README with production guidance, status and contents"
```

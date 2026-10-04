# Skipped-Events Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make outboxes and public contracts deliver every committed event under concurrent writes by tracking a `(transaction id, offset)` position and only reading past transactions that have finished.

**Architecture:** `ddd_domain_event` records each event's writing transaction (`transaction_id XID8 DEFAULT pg_current_xact_id()`). `PostgresDomainPollingBackend` reads events after an `EventLogPosition(transactionId, globalOffset)`, ordered by `(transaction_id, global_offset)`, and only those with `transaction_id < pg_snapshot_xmin(pg_current_snapshot())`. The poller, `AggregateEventOutbox`, `PublicEventContract` and `PostgresOffsetManager` move from `Long` offsets to `EventLogPosition`.

**Tech Stack:** Kotlin 2.4.20, PostgreSQL 13+ (tests on 17 via Testcontainers 2.0.5), plain JDBC through `JdbcContext`, MockK, kotlinx-coroutines.

**Spec:** `docs/superpowers/specs/2026-10-04-skipped-events-fix-design.md`

## Global Constraints

- Requires PostgreSQL 13+ (`xid8`, `pg_current_xact_id()`, `pg_current_snapshot()`, `pg_snapshot_xmin()`).
- `ddd_domain_event.transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id()`; index `idx_ddd_domain_event_position ON ddd_domain_event (transaction_id, global_offset)`.
- `ddd_consumer_offset` columns: `consumer_name VARCHAR(255) PRIMARY KEY, last_transaction_id BIGINT NOT NULL, last_offset BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL`.
- `EventLogPosition(transactionId: Long, globalOffset: Long)`, `Comparable`, `EventLogPosition.START = EventLogPosition(0, 0)`, in package `io.kotmod`.
- API after: `PersistedEvent.position`, `readEventsAfter(position: EventLogPosition, limit: Int)`, `getPosition`/`savePosition` on `AggregateEventOutbox` and `PublicEventContract`, `PostgresOffsetManager.getPosition(name)` / `savePosition(name, position)`. `INITIAL_OFFSET` and `PersistedEvent.globalOffset` are removed.
- Delivery stays at-least-once; the poller saves its position after each event; kotmod's insert statement is unchanged.
- Suites: `./gradlew test integrationTest` (needs Docker; run with the sandbox disabled in sandboxed shells).

## Review Focus

1. A long-running transaction in **another database on the same Postgres server** — `pg_snapshot_xmin` is server-wide, so it delays delivery too; expect the README to say so (grep check in Task 3).
2. A consumer with no saved position — expect it to start at `EventLogPosition.START` and read every event (test in Task 2).
3. One transaction writing more events than the poller's batch size — expect every event read exactly once across batches, since they share a transaction id (test in Task 2).
4. A rolled-back transaction leaving a hole in `global_offset` — expect polling to continue past it (test in Task 2).
5. Resuming after a restart from a saved position that is mid-way through one transaction's events — expect the remaining events of that transaction, and nothing earlier, to be read (covered by the batch test in Task 2, which resumes after each batch).

---

## File Structure

- Create `kotmod/src/main/kotlin/io/kotmod/EventLogPosition.kt` (Task 1).
- Create `kotmod/src/test/kotlin/io/kotmod/EventLogPositionTest.kt` (Task 1).
- Create `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PollingVisibilityIntegrationTest.kt` (Task 2).
- Modify (Task 2): `kotmod/src/main/kotlin/io/kotmod/DomainBackend.kt`, `postgres/DddSchema.kt`, `postgres/PostgresDomainBackend.kt`, `postgres/PostgresOffsetManager.kt`, `outbox/DomainEventPoller.kt`, `outbox/AggregateEventOutbox.kt`, `contract/PublicEventContract.kt`; tests `kotmod/src/test/kotlin/io/kotmod/support/PersistedEventFixtures.kt`, `outbox/AggregateEventOutboxTest.kt`, `contract/PublicEventContractTest.kt`; integration tests `postgres/PostgresOffsetManagerIntegrationTest.kt`, `postgres/PostgresDomainBackendIntegrationTest.kt`, `outbox/AggregateEventOutboxIntegrationTest.kt`, `contract/PublicEventContractIntegrationTest.kt`, `kotmod-db-scheduler/.../DbSchedulerEventReactionsIntegrationTest.kt`; examples `QuickstartTest.kt`, `ReadmeExamples.kt`.
- Modify `README.md` (Task 3).

---

### Task 1: `EventLogPosition`

**Files:**
- Create: `kotmod/src/main/kotlin/io/kotmod/EventLogPosition.kt`
- Test: `kotmod/src/test/kotlin/io/kotmod/EventLogPositionTest.kt`

**Interfaces:**
- Produces: `data class EventLogPosition(val transactionId: Long, val globalOffset: Long) : Comparable<EventLogPosition>` with `companion object { val START: EventLogPosition }`.

- [ ] **Step 1: Write the failing test**

```kotlin
package io.kotmod

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EventLogPositionTest {
    @Test
    fun `positions order by transaction id first, then offset`() {
        assertTrue(EventLogPosition(1, 99) < EventLogPosition(2, 1))
        assertTrue(EventLogPosition(2, 1) < EventLogPosition(2, 2))
        assertEquals(0, EventLogPosition(3, 4).compareTo(EventLogPosition(3, 4)))
    }

    @Test
    fun `START is before every real position`() {
        assertEquals(EventLogPosition(0, 0), EventLogPosition.START)
        assertTrue(EventLogPosition.START < EventLogPosition(1, 1))
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.EventLogPositionTest'`
Expected: FAIL — `Unresolved reference 'EventLogPosition'`.

- [ ] **Step 3: Implement**

```kotlin
package io.kotmod

/**
 * Where an event sits in the event log: the id of the transaction that wrote it, then its global offset.
 * Consumers save the position of the last event they handled and resume after it. Ordering by transaction
 * first is what lets a consumer never skip an event committed by a slower, concurrent transaction.
 */
data class EventLogPosition(
    val transactionId: Long,
    val globalOffset: Long,
) : Comparable<EventLogPosition> {
    override fun compareTo(other: EventLogPosition): Int =
        compareValuesBy(this, other, EventLogPosition::transactionId, EventLogPosition::globalOffset)

    companion object {
        /** The position before every event; a consumer that has saved nothing starts here. */
        val START = EventLogPosition(0, 0)
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.EventLogPositionTest'`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add kotmod/src/main/kotlin/io/kotmod/EventLogPosition.kt kotmod/src/test/kotlin/io/kotmod/EventLogPositionTest.kt
git commit -m "Add EventLogPosition"
```

---

### Task 2: Read by transaction-aware position

**Files:** see File Structure (Task 2).

**Interfaces:**
- Consumes: `EventLogPosition` (Task 1).
- Produces: `PersistedEvent.position`, `DomainEventPollingBackend.readEventsAfter(position: EventLogPosition, limit: Int)`, `AggregateEventOutbox(…, getPosition: () -> EventLogPosition, savePosition: (EventLogPosition) -> Unit, …)`, `PublicEventContract(…, getPosition, savePosition, …)`, `PostgresOffsetManager.getPosition(consumerName): EventLogPosition`, `savePosition(consumerName, position)`.

- [ ] **Step 1: Reproduce the bug with the current API**

Create `kotmod/src/integrationTest/kotlin/io/kotmod/postgres/PollingVisibilityIntegrationTest.kt`:

```kotlin
package io.kotmod.postgres

import io.kotmod.postgres.support.IntegrationTest
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals

class PollingVisibilityIntegrationTest : IntegrationTest() {
    /** A consumer that reads the event log the way the poller does and records what it saw. */
    private inner class Consumer {
        private var offset = -1L
        val seen = mutableListOf<String>()

        fun poll(limit: Int = 100) {
            for (event in PostgresDomainPollingBackend(jdbc).readEventsAfter(offset, limit)) {
                seen += event.metadata.eventId.value
                offset = event.globalOffset
            }
        }
    }

    private fun openTransaction(): Connection = dataSource.connection.apply { autoCommit = false }

    private fun assignTransactionId(conn: Connection) {
        conn.createStatement().use { it.executeQuery("SELECT pg_current_xact_id()").close() }
    }

    private fun insertEvent(
        conn: Connection,
        eventId: String,
    ) {
        conn
            .prepareStatement(
                "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, causation_id, event_id, " +
                    "event_type, event_version, event_payload, event_timestamp) " +
                    "VALUES ('Order', ?, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
            ).use { ps ->
                ps.setString(1, "agg-$eventId")
                ps.setString(2, eventId)
                ps.executeUpdate()
            }
    }

    @Test
    fun `an event committed after a later one is still delivered`() {
        val consumer = Consumer()
        val a = openTransaction()
        val b = openTransaction()
        try {
            insertEvent(a, "e-1") // offset 1, still in flight
            insertEvent(b, "e-2") // offset 2
            b.commit()
            consumer.poll()
            a.commit()
            consumer.poll()
        } finally {
            a.close()
            b.close()
        }

        assertEquals(listOf("e-1", "e-2"), consumer.seen)
    }

    @Test
    fun `an event with a lower offset from a later transaction is still delivered`() {
        val consumer = Consumer()
        val a = openTransaction()
        val b = openTransaction()
        try {
            assignTransactionId(a)
            assignTransactionId(b) // b's transaction id is later than a's
            insertEvent(b, "e-b") // offset 1
            insertEvent(a, "e-a") // offset 2
            a.commit()
            consumer.poll()
            b.commit()
            consumer.poll()
        } finally {
            a.close()
            b.close()
        }

        assertEquals(setOf("e-a", "e-b"), consumer.seen.toSet())
        assertEquals(2, consumer.seen.size)
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.postgres.PollingVisibilityIntegrationTest'`
Expected: FAIL — both tests: the first sees only `[e-2]`, the second only `[e-a]` (the bug).

- [ ] **Step 3: Change the schema**

In `DddSchema.kt`, in `CREATE TABLE ddd_domain_event`, add after `global_offset BIGSERIAL PRIMARY KEY,`:

```sql
            transaction_id    XID8         NOT NULL DEFAULT pg_current_xact_id(),
```

after the existing `idx_ddd_domain_event_aggregate` index add:

```sql
        CREATE INDEX idx_ddd_domain_event_position ON ddd_domain_event (transaction_id, global_offset);
```

and replace the `ddd_consumer_offset` table with:

```sql
        CREATE TABLE ddd_consumer_offset (
            consumer_name       VARCHAR(255) PRIMARY KEY,
            last_transaction_id BIGINT       NOT NULL,
            last_offset         BIGINT       NOT NULL,
            updated_at          TIMESTAMPTZ  NOT NULL
        );
```

Update `DddSchema`'s KDoc to mention PostgreSQL 13+.

- [ ] **Step 4: Change the API and the read query**

`DomainBackend.kt`:
- `PersistedEvent`: replace `val globalOffset: Long,` with `val position: EventLogPosition,` and its KDoc line `@property globalOffset …` with `@property position where the event sits in the log; consumers resume after it.`
- `DomainEventPollingBackend`:

```kotlin
    /**
     * Returns up to [limit] events after [position], in log order, from transactions that have finished.
     * Events from transactions still in progress (and anything after them) are held back until those
     * transactions end, so no event is ever skipped.
     */
    fun readEventsAfter(
        position: EventLogPosition,
        limit: Int,
    ): List<PersistedEvent>
```

`PostgresDomainBackend.kt` — `PostgresDomainPollingBackend.readEventsAfter`:

```kotlin
    override fun readEventsAfter(
        position: EventLogPosition,
        limit: Int,
    ): List<PersistedEvent> =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT global_offset, transaction_id::text::bigint AS transaction_id_value, " +
                        "aggregate_type, aggregate_id, causation_id, correlation_id, event_id, event_type, " +
                        "event_version, event_payload, event_timestamp " +
                        "FROM ddd_domain_event " +
                        "WHERE (transaction_id, global_offset) > (?::text::xid8, ?) " +
                        "AND transaction_id < pg_snapshot_xmin(pg_current_snapshot()) " +
                        "ORDER BY transaction_id, global_offset " +
                        "LIMIT ?",
                ).use { ps ->
                    ps.setLong(1, position.transactionId)
                    ps.setLong(2, position.globalOffset)
                    ps.setInt(3, limit)
                    ps.executeQuery().use { rs ->
                        val result = mutableListOf<PersistedEvent>()
                        while (rs.next()) result += rs.toPublishedEvent()
                        result
                    }
                }
        }
```

and in `toPublishedEvent()` replace `globalOffset = getLong("global_offset"),` with:

```kotlin
            position = EventLogPosition(getLong("transaction_id_value"), getLong("global_offset")),
```

Update the class KDoc: "reading `ddd_domain_event` in `(transaction_id, global_offset)` order, only past transactions that have finished".

`outbox/DomainEventPoller.kt`: rename constructor parameters `getOffset: () -> Long` → `getPosition: () -> EventLogPosition` and `saveOffset: (Long) -> Unit` → `savePosition: (EventLogPosition) -> Unit`; in `tick()` use `val position = withContext(Dispatchers.IO) { getPosition() }`, `backend.readEventsAfter(position, batchSize)`, log `envelope.position`, and `savePosition(envelope.position)`.

`outbox/AggregateEventOutbox.kt` and `contract/PublicEventContract.kt`: rename the same two constructor parameters to `getPosition: () -> EventLogPosition` and `savePosition: (EventLogPosition) -> Unit`, pass them to `DomainEventPoller(getPosition = getPosition, savePosition = savePosition, …)`, replace `envelope.globalOffset` with `envelope.position` (in `PublicEventContract`, `fanOut`'s `globalOffset: Long` parameter becomes `position: EventLogPosition`, and log lines say `[position={}]`), and update KDoc "the offset returned by [getOffset]" → "the position returned by [getPosition]".

`postgres/PostgresOffsetManager.kt` — replace the two methods and delete the companion object:

```kotlin
    /** Returns the position last saved for [consumerName], or [EventLogPosition.START] if it has never saved one. */
    fun getPosition(consumerName: String): EventLogPosition =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT last_transaction_id, last_offset FROM ddd_consumer_offset WHERE consumer_name = ?",
                ).use { ps ->
                    ps.setString(1, consumerName)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) EventLogPosition(rs.getLong(1), rs.getLong(2)) else EventLogPosition.START
                    }
                }
        }

    /** Saves [position] as the last position processed by [consumerName], replacing any previous value. */
    fun savePosition(
        consumerName: String,
        position: EventLogPosition,
    ) {
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO ddd_consumer_offset (consumer_name, last_transaction_id, last_offset, updated_at) " +
                        "VALUES (?, ?, ?, now()) " +
                        "ON CONFLICT (consumer_name) DO UPDATE SET last_transaction_id = EXCLUDED.last_transaction_id, " +
                        "last_offset = EXCLUDED.last_offset, updated_at = EXCLUDED.updated_at",
                ).use { ps ->
                    ps.setString(1, consumerName)
                    ps.setLong(2, position.transactionId)
                    ps.setLong(3, position.globalOffset)
                    ps.executeUpdate()
                }
        }
    }
```

Update its class KDoc example to `getPosition = { offsets.getPosition("orders-outbox") }, savePosition = { offsets.savePosition("orders-outbox", it) }`. Add `import io.kotmod.EventLogPosition` wherever it is used.

- [ ] **Step 5: Convert the reproduction test to the new API and add the remaining cases**

In `PollingVisibilityIntegrationTest`, replace the `Consumer` class with:

```kotlin
    private inner class Consumer(
        var position: EventLogPosition = EventLogPosition.START,
    ) {
        val seen = mutableListOf<String>()

        fun poll(limit: Int = 100): Int {
            val events = PostgresDomainPollingBackend(jdbc).readEventsAfter(position, limit)
            for (event in events) {
                seen += event.metadata.eventId.value
                position = event.position
            }
            return events.size
        }
    }
```

(import `io.kotmod.EventLogPosition`), strengthen the first test by asserting `assertEquals(emptyList(), consumer.seen)` right after the first `consumer.poll()` (A is still in flight, so nothing may be read), change the second test's final assertion to `assertEquals(listOf("e-a", "e-b"), consumer.seen)`, and add:

```kotlin
    @Test
    fun `a rolled-back transaction does not block delivery`() {
        val consumer = Consumer()
        openTransaction().use { a ->
            insertEvent(a, "e-rolled-back")
            a.rollback()
        }
        openTransaction().use { b ->
            insertEvent(b, "e-2")
            b.commit()
        }

        consumer.poll()

        assertEquals(listOf("e-2"), consumer.seen)
    }

    @Test
    fun `a consumer with no saved position reads every event`() {
        openTransaction().use { a ->
            insertEvent(a, "e-1")
            insertEvent(a, "e-2")
            a.commit()
        }

        val consumer = Consumer()
        consumer.poll()

        assertEquals(listOf("e-1", "e-2"), consumer.seen)
    }

    @Test
    fun `one transaction's events are read exactly once across batches and restarts`() {
        openTransaction().use { a ->
            (1..5).forEach { insertEvent(a, "e-$it") }
            a.commit()
        }

        val seen = mutableListOf<String>()
        var saved = EventLogPosition.START
        while (true) {
            // A fresh consumer each batch, resuming from the saved position, as after a restart.
            val consumer = Consumer(saved)
            if (consumer.poll(limit = 2) == 0) break
            seen += consumer.seen
            saved = consumer.position
        }

        assertEquals((1..5).map { "e-$it" }, seen)
    }
```

- [ ] **Step 6: Run the visibility tests**

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.postgres.PollingVisibilityIntegrationTest'`
Expected: PASS (5 tests). (Other test sources do not compile yet — if Gradle refuses to run because of them, do Step 7 first and run this afterwards.)

- [ ] **Step 7: Update the remaining tests and examples to the new API**

Unit tests (`kotmod/src/test`):
- `support/PersistedEventFixtures.kt`: `persistedEvent(globalOffset: Long, …)` keeps its parameter but builds `position = EventLogPosition(transactionId = 1, globalOffset = globalOffset)`; `RecordingOffsets` becomes position-based: `RecordingOffsets(initial: EventLogPosition)`, `var current: EventLogPosition`, `val saved = mutableListOf<EventLogPosition>()`, `fun get(): EventLogPosition`, `fun save(position: EventLogPosition)`.
- `outbox/AggregateEventOutboxTest.kt` and `contract/PublicEventContractTest.kt`: `RecordingOffsets(initial = 9L)` → `RecordingOffsets(initial = EventLogPosition(1, 9))`; `readEventsAfter(9L, any())` → `readEventsAfter(EventLogPosition(1, 9), any())`; `getOffset = offsets::get` → `getPosition = offsets::get`; `saveOffset = offsets::save` → `savePosition = offsets::save`; assertions on `offsets.saved` compare `offsets.saved.map { it.globalOffset }`; `it.globalOffset` → `it.position.globalOffset`.

Integration tests:
- `PostgresOffsetManagerIntegrationTest.kt` — replace the test bodies with:

```kotlin
    @Test
    fun `getPosition returns START for an unknown consumer`() {
        assertEquals(EventLogPosition.START, offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `savePosition then getPosition round-trips`() {
        offsets.savePosition("orders-outbox", EventLogPosition(7, 10))
        assertEquals(EventLogPosition(7, 10), offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `savePosition overwrites the previous position`() {
        offsets.savePosition("orders-outbox", EventLogPosition(7, 10))
        offsets.savePosition("orders-outbox", EventLogPosition(9, 42))
        assertEquals(EventLogPosition(9, 42), offsets.getPosition("orders-outbox"))
    }

    @Test
    fun `positions are independent per consumer`() {
        offsets.savePosition("orders-outbox", EventLogPosition(9, 42))
        offsets.savePosition("public-contract", EventLogPosition(3, 3))

        assertEquals(EventLogPosition(9, 42), offsets.getPosition("orders-outbox"))
        assertEquals(EventLogPosition(3, 3), offsets.getPosition("public-contract"))
        assertEquals(EventLogPosition.START, offsets.getPosition("someone-else"))
    }
```

- `PostgresDomainBackendIntegrationTest.kt` (`readEventsAfter` test): `readEventsAfter(lastOffset = -1, …)` → `readEventsAfter(EventLogPosition.START, …)`; `all.map { it.globalOffset }` → `all.map { it.position.globalOffset }`; `readEventsAfter(lastOffset = 1, …)` → `readEventsAfter(all[0].position, …)`; `readEventsAfter(lastOffset = 3, …)` → `readEventsAfter(all[2].position, …)`.
- `AggregateEventOutboxIntegrationTest.kt`, `PublicEventContractIntegrationTest.kt`, `DbSchedulerEventReactionsIntegrationTest.kt`: `getOffset = { offsets.getOffset(X) }` → `getPosition = { offsets.getPosition(X) }`; `saveOffset = { offsets.saveOffset(X, it) }` → `savePosition = { offsets.savePosition(X, it) }`; comparisons `offsets.getOffset(X) < n` / `assertEquals(nL, offsets.getOffset(X))` → use `offsets.getPosition(X).globalOffset`; `PostgresOffsetManager.INITIAL_OFFSET` → `EventLogPosition.START` (compare whole positions). In the outbox test `outbox resumes from the persisted offset`, replace `offsets.saveOffset(CONSUMER, 1)` with
  `offsets.savePosition(CONSUMER, PostgresDomainPollingBackend(jdbc).readEventsAfter(EventLogPosition.START, 1).single().position)` and rename the test to `` `outbox resumes from the persisted position` ``.
- Examples (`examples/src/integrationTest/kotlin/io/kotmod/readme/`): in `QuickstartTest.kt` and `ReadmeExamples.kt`, `getOffset = { offsets.getOffset(X) }` → `getPosition = { offsets.getPosition(X) }` and `saveOffset = { offsets.saveOffset(X, it) }` → `savePosition = { offsets.savePosition(X, it) }`.

- [ ] **Step 8: Add the end-to-end outbox test**

Add to `AggregateEventOutboxIntegrationTest` (imports `io.kotmod.EventLogPosition`, `java.sql.Connection`):

```kotlin
    @Test
    fun `outbox delivers an event committed after a later one`() =
        runBlocking {
            fun insert(
                conn: Connection,
                eventId: String,
            ) = conn
                .prepareStatement(
                    "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, causation_id, event_id, " +
                        "event_type, event_version, event_payload, event_timestamp) " +
                        "VALUES ('Order', ?, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
                ).use { ps ->
                    ps.setString(1, "agg-$eventId")
                    ps.setString(2, eventId)
                    ps.executeUpdate()
                }

            val dispatched = CopyOnWriteArrayList<EventReactionId>()
            val position = AtomicReference(EventLogPosition.START)
            val outbox =
                AggregateEventOutbox(
                    backend = PostgresDomainPollingBackend(jdbc),
                    executor = recordingExecutor<FakeTrigger> { id, _ -> dispatched += id },
                    eventToReactions = { event ->
                        listOf(EventReaction(EventReactionId("reaction-${event.metadata.eventId.value}"), FakeTrigger(event.metadata.eventId.value)))
                    },
                    getPosition = { position.get() },
                    savePosition = { position.set(it) },
                    isLeader = { true },
                    pollInterval = 50.milliseconds,
                )

            val a = dataSource.connection.apply { autoCommit = false }
            try {
                insert(a, "e-1") // in flight
                dataSource.connection.use { b -> insert(b, "e-2") } // auto-commit
                outbox.start()
                delay(300)
                assertEquals(emptyList(), dispatched.toList()) // e-2 is held back behind e-1's transaction
                a.commit()
                eventually { dispatched.size == 2 }
            } finally {
                outbox.stop()
                a.close()
            }

            assertEquals(listOf(EventReactionId("reaction-e-1"), EventReactionId("reaction-e-2")), dispatched.toList())
        }
```

(imports: `io.kotmod.postgres.support.eventually`, `kotlinx.coroutines.delay`, `java.util.concurrent.atomic.AtomicReference`.)

- [ ] **Step 9: Run everything**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL; all tests pass.
Run: `grep -rnE "globalOffset: Long|INITIAL_OFFSET|getOffset|saveOffset|lastOffset" --include='*.kt' kotmod kotmod-db-scheduler kotmod-sqldelight examples | grep -v /build/`
Expected: no output.

- [ ] **Step 10: Commit**

```bash
git add -A kotmod kotmod-db-scheduler examples
git commit -m "Track a transaction-aware position so concurrent commits are never skipped"
```

---

### Task 3: Documentation

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 2's API names.

- [ ] **Step 1: Confirm the snippet checker fails**

Run the README snippet checker from the previous plan (session scratchpad `readme_check2.py`, sources `examples/src/integrationTest/kotlin/io/kotmod/readme`): `python3 <scratchpad>/readme_check2.py .`
Expected: FAIL — the quickstart outbox block and `orderContract` still use `getOffset`/`saveOffset`.

- [ ] **Step 2: Update the README**

- Quickstart step 5 and the public-contract guide: re-copy the `outbox` and `orderContract` blocks (now `getPosition`/`savePosition`). In the sentence introducing `PostgresOffsetManager`, say it "remembers how far the outbox has read".
- Requirements: "PostgreSQL 13 or later."
- Outbox guide: replace "the batch stops and the next poll starts again from the last saved offset. (Under concurrent writes there is one case where an event can be missed; see [Running in production](#running-in-production).)" with "the batch stops and the next poll starts again from the last saved position, so no event is skipped — including events committed late by slower, concurrent transactions."
- Running in production: delete the whole "**Known limitation: concurrent writes can cause missed events.**" paragraph; in "Delivery is at-least-once" say events are never skipped; add a paragraph: "**Long transactions delay delivery.** An outbox only reads past transactions that have finished, so it never skips an event that a slower transaction commits late. The flip side: while any transaction on the same Postgres server is open — even in another database — later events wait for it. Keep transactions short." Change "The database is down while dispatching … resumes from the last saved offset" to "…saved position".
- Postgres setup: in the schema table, `ddd_consumer_offset` holds "How far each outbox or contract has read (a transaction id and offset)".

- [ ] **Step 3: Run the checks**

Run: `python3 <scratchpad>/readme_check2.py .` — Expected: `OK`.
Run: `grep -n "Known limitation\|getOffset\|saveOffset" README.md` — Expected: no output.
Run: `grep -n "another database" README.md` — Expected: one match (Review Focus 1).
Run: `./gradlew test integrationTest` — Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "Document transaction-aware positions and remove the skipped-events limitation"
```

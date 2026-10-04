# Ordered Event Reactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an outbox or contract subscription opt into strict per-aggregate ordering of its reactions, using new per-aggregate event sequence numbers, source-side reordering in the shared poller, and a check-on-pickup mechanism in the db-scheduler sink.

**Architecture:** (1) Events get `aggregate_sequence`, assigned under the aggregate row lock in `saveMeta`. (2) `DomainEventPoller` asks the polling backend, per event, whether earlier events of the same aggregate are still ahead (handle them first) or whether this event was already handled early (skip it). (3) Ordered subscriptions stamp each dispatched reaction with a `DispatchOrdering`; sources report a `ReactionOutcome`. (4) The db-scheduler sink gives ordered reactions sortable instance ids; on pickup a reaction runs only if no earlier reaction of its aggregate is pending in `scheduled_tasks`.

**Tech Stack:** Kotlin 2.4.20, PostgreSQL 13+ (17 in tests), plain JDBC via `JdbcContext`, db-scheduler 16.12.0, kotlinx-serialization, MockK, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-10-04-ordered-reactions-design.md`

## Global Constraints

- Branch builds on `skipped-events-fix` (transaction-aware `EventLogPosition`).
- Sequence numbers: contiguous from 1 per aggregate; assigned in `saveMeta` via `UPDATE … last_sequence = last_sequence + n … RETURNING last_sequence` (create: `INSERT … RETURNING`); unique index `(aggregate_type, aggregate_id, aggregate_sequence)`.
- `EventMetadata.sequence: Long`; `DomainPersistenceBackend.saveMeta(type, id, expectedVersion, eventCount: Int): Long`.
- Ordering key: `"<aggregateType>/<aggregateId>"` of the source event.
- `ReactionOrdering { Unordered; PerAggregate(onGiveUp = ContinueWithNext) }`, `OnGiveUp { ContinueWithNext, BlockAggregate }`, `DispatchOrdering(key, sequence, ordinal, onGiveUp)`, `ReactionOutcome { Retry(delay); Finished(gaveUp) }`.
- `EventReactionTriggerSink.supportsOrdering: Boolean` (default `false`); ordered subscriptions on a non-ordering sink fail with `IllegalArgumentException` at construction / `subscribe`.
- Ordered instance id: `<len(key)>:<key>#<sequence 19-digit zero-padded>#<ordinal 4-digit zero-padded>#<reaction id>`. String comparisons in SQL use `COLLATE "C"`.
- `DbSchedulerEventReactions(taskName, triggerSerializer, unsubscribedRetryDelay = 5.seconds, jdbc: JdbcContext? = null, orderedRecheckDelay = 2.seconds, tableName = "scheduled_tasks")`; `supportsOrdering = jdbc != null`.
- Unordered behaviour unchanged everywhere.
- Suites: `./gradlew test integrationTest` (Docker; sandbox disabled).

## Review Focus

1. Aggregate ids containing `#`, `:` or `/` — expect ordering keys and instance-id ranges never to mix two aggregates (test in Task 4).
2. Postgres's default linguistic collation ignoring punctuation in comparisons — expect the pickup check to compare in `"C"` collation (covered by Task 4's integration tests on the default collation, plus Review Focus 1's test).
3. A blocked reaction must not be nudged back to life — expect the nudge to skip parked rows (test in Task 4).
4. Two executors handling the same aggregate — expect them not to wait on each other (test in Task 4).
5. An unordered subscription on the same poller as ordered ones (contract with mixed subscribers) — expect unordered reactions dispatched with no ordering stamp (test in Task 3).

---

## File Structure

- Task 1 (sequence numbers): `kotmod/src/main/kotlin/io/kotmod/{EventMetadata.kt, DomainBackend.kt, AggregateManager.kt, EventProducer.kt, postgres/DddSchema.kt, postgres/PostgresDomainBackend.kt}`; tests `support/StubPersistenceBackend.kt`, `support/PersistedEventFixtures.kt`, `EventMetadataTest.kt`, `AggregateManagerCreateTest.kt`, integration `postgres/PostgresDomainBackendIntegrationTest.kt` (+ any test constructing `EventMetadata` or calling `saveMeta`).
- Task 2 (source ordering): `DomainBackend.kt` (`SequenceCheck`, `checkSequence`), `postgres/PostgresDomainBackend.kt`, `outbox/DomainEventPoller.kt`; integration test `outbox/SourceOrderingIntegrationTest.kt`.
- Task 3 (reaction API): create `kotmod/src/main/kotlin/io/kotmod/event/reaction/Ordering.kt`; modify `event/reaction/EventReaction.kt`, `outbox/AggregateEventOutbox.kt`, `contract/PublicEventContract.kt`; tests `event/reaction/EventReactionExecutorTest.kt`, `outbox/AggregateEventOutboxTest.kt`, `contract/PublicEventContractTest.kt`, integration `postgres/support/RecordingExecutor.kt`; db-scheduler `DbSchedulerTriggerSource.kt`, `DbSchedulerTriggerSink.kt`, `ReactionTask.kt`, rename `ReactionOutcome.kt` → `TaskRowOutcome.kt` (+ its test).
- Task 4 (db-scheduler ordering): `kotmod-db-scheduler/src/main/kotlin/io/kotmod/event/reaction/dbscheduler/{OrderedIds.kt (new), OrderedQueries.kt (new), ReactionTaskData.kt, TaskRowOutcome.kt, ReactionTask.kt, DbSchedulerTriggerSink.kt, DbSchedulerEventReactions.kt}`; tests `OrderedIdsTest.kt`, `TaskRowOutcomeTest.kt`, integration `OrderedReactionsIntegrationTest.kt`.
- Task 5: `README.md`.

---

### Task 1: Per-aggregate sequence numbers

**Interfaces:**
- Produces: `EventMetadata(…, sequence: Long)` (last parameter, required); `DomainPersistenceBackend.saveMeta(type, id, expectedVersion: Long?, eventCount: Int): Long` returning the aggregate's new `last_sequence`; DB columns `ddd_aggregate_root.last_sequence`, `ddd_domain_event.aggregate_sequence`.

- [ ] **Step 1: Write the failing unit test**

Add to `AggregateManagerCreateTest`:

```kotlin
    @Test
    fun `events are numbered 1, 2, 3 within their aggregate across commands`() =
        runTest {
            orders.create(AggregateId("o-1")) { PendingOrder("book") to listOf(OrderPlaced("book"), OrderPlaced("pen")) }
            orders.execute(AggregateId("o-1")) { (it as PendingOrder).let { order -> order to listOf(OrderPlaced(order.name)) } }

            assertEquals(listOf(1L, 2L, 3L), backend.events.map { it.metadata.sequence })
        }
```

In `StubPersistenceBackend`: change `saveMeta` to the new signature and keep a per-aggregate counter:

```kotlin
    private val lastSequences = mutableMapOf<Key, Long>()

    override fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
        eventCount: Int,
    ): Long {
        recordWrite("saveMeta")
        // (existing version / existence checks unchanged)
        val key = Key(type, id)
        val last = (lastSequences[key] ?: 0L) + eventCount
        lastSequences[key] = last
        return last
    }
```

(keep the existing body that validates `expectedVersion` and updates `metas`; add the counter lines at the end and `return last`).

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :kotmod:test --tests 'io.kotmod.AggregateManagerCreateTest'`
Expected: FAIL — compilation: `'saveMeta' overrides nothing` / `Unresolved reference 'sequence'`.

- [ ] **Step 3: Implement**

`EventMetadata.kt`: add `val sequence: Long,` as the last property, with KDoc `@property sequence this event's number within its aggregate: 1, 2, 3, … in history order, with no gaps.`

`DomainBackend.kt`:

```kotlin
    /**
     * Records a change to aggregate [type]/[id] that raised [eventCount] events, and returns the aggregate's
     * new last event sequence number (the events are numbered `result - eventCount + 1 … result`). With
     * [expectedVersion] `null` the aggregate is created (throwing [AggregateAlreadyExistsException] if it
     * exists); otherwise its version is advanced from [expectedVersion] (throwing
     * [OptimisticConcurrencyException] if the stored version differs).
     */
    fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
        eventCount: Int,
    ): Long
```

`AggregateManager.kt` (both write phases) and `EventProducer.kt`:

```kotlin
                val lastSequence = backend.saveMeta(aggregateType, id, expectedVersion = /* as today */, eventCount = events.size)
                …
                    backend.appendEvents(wrapPending(…, events = events, firstSequence = lastSequence - events.size + 1))
```

and `wrapPending` / `wrap` gain `firstSequence: Long`, setting `sequence = firstSequence + index` via `events.mapIndexed { index, event -> … }`.

`DddSchema.kt`: in `ddd_aggregate_root` add `last_sequence     BIGINT       NOT NULL,` after `aggregate_version`; in `ddd_domain_event` add `aggregate_sequence BIGINT      NOT NULL,` after `aggregate_id`; add after the existing indexes:

```sql
        CREATE UNIQUE INDEX idx_ddd_domain_event_sequence
            ON ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence);
```

`PostgresDomainBackend.kt` — `saveMeta`:

```kotlin
    override fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
        eventCount: Int,
    ): Long =
        jdbc.withConnection { conn ->
            val now = OffsetDateTime.now(ZoneOffset.UTC)
            if (expectedVersion == null) {
                try {
                    conn
                        .prepareStatement(
                            "INSERT INTO ddd_aggregate_root " +
                                "(aggregate_type, aggregate_id, aggregate_version, last_sequence, created_at, updated_at) " +
                                "VALUES (?, ?, 1, ?, ?, ?) RETURNING last_sequence",
                        ).use { ps ->
                            ps.setString(1, type.value)
                            ps.setString(2, id.value)
                            ps.setLong(3, eventCount.toLong())
                            ps.setObject(4, now)
                            ps.setObject(5, now)
                            ps.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
                        }
                } catch (e: SQLException) {
                    if (e.sqlState == UNIQUE_VIOLATION) throw AggregateAlreadyExistsException(type, id)
                    throw e
                }
            } else {
                conn
                    .prepareStatement(
                        "UPDATE ddd_aggregate_root " +
                            "SET aggregate_version = ?, last_sequence = last_sequence + ?, updated_at = ? " +
                            "WHERE aggregate_type = ? AND aggregate_id = ? AND aggregate_version = ? " +
                            "RETURNING last_sequence",
                    ).use { ps ->
                        ps.setLong(1, expectedVersion + 1)
                        ps.setLong(2, eventCount.toLong())
                        ps.setObject(3, now)
                        ps.setString(4, type.value)
                        ps.setString(5, id.value)
                        ps.setLong(6, expectedVersion)
                        ps.executeQuery().use { rs ->
                            if (!rs.next()) throw OptimisticConcurrencyException(type, id, expectedVersion)
                            rs.getLong(1)
                        }
                    }
            }
        }
```

`appendEvents`: add `aggregate_sequence` to the column list and bind `metadata.sequence` (shift later parameter indexes by one). `PostgresDomainPollingBackend`: select `aggregate_sequence` and set `sequence = getLong("aggregate_sequence")` in `EventMetadata`.

Update every other construction of `EventMetadata(…)` (fixtures, `EventMetadataTest`, integration tests) to pass `sequence = …` (use 1, 2, … in order within an aggregate), and every direct `saveMeta(…)` call in integration tests to pass `eventCount = …` (the number of events that test then appends for that aggregate; 0 if none).

- [ ] **Step 4: Run the unit tests**

Run: `./gradlew :kotmod:test`
Expected: PASS.

- [ ] **Step 5: Add integration tests**

Add to `PostgresDomainBackendIntegrationTest`:

```kotlin
    @Test
    fun `saveMeta numbers events contiguously within each aggregate`() =
        runTest {
            val order = AggregateType("Order")
            assertEquals(2L, backend.saveMeta(order, AggregateId("o-1"), expectedVersion = null, eventCount = 2))
            assertEquals(3L, backend.saveMeta(order, AggregateId("o-1"), expectedVersion = 1, eventCount = 1))
            assertEquals(3L, backend.saveMeta(order, AggregateId("o-1"), expectedVersion = 2, eventCount = 0))
            assertEquals(1L, backend.saveMeta(order, AggregateId("o-2"), expectedVersion = null, eventCount = 1))
        }

    @Test
    fun `a duplicate sequence within an aggregate is rejected`() =
        runTest {
            fun pending(eventId: String) =
                PendingEvent(
                    EventMetadata(EventId(eventId), AggregateType("Order"), AggregateId("o-1"), CommandId("c"), null,
                        kotlin.time.Instant.parse("2026-10-04T10:00:00Z"), sequence = 1),
                    OrderPlaced("x"),
                )
            backend.appendEvents(listOf(pending("e-1")))
            assertFailsWith<java.sql.SQLException> { backend.appendEvents(listOf(pending("e-2"))) }
        }
```

and an end-to-end check that sequences come back through polling: create via `AggregateManager` with a stub repository is not available here, so instead extend the existing `readEventsAfter` test's `PendingEvent`s to carry `sequence` 1, 2 (for o-1) and 1 (for o-2) and assert `all.map { it.metadata.sequence } == listOf(1L, 2L, 1L)`.

- [ ] **Step 6: Run everything**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add -A kotmod kotmod-db-scheduler examples
git commit -m "Number events within their aggregate"
```

---

### Task 2: Source-side ordering in the shared poller

**Interfaces:**
- Consumes: `EventMetadata.sequence` (Task 1).
- Produces:

```kotlin
sealed interface SequenceCheck {
    data object InOrder : SequenceCheck
    data object AlreadyHandled : SequenceCheck
    data class HandleEarlierFirst(val earlier: List<PersistedEvent>) : SequenceCheck
    data object WaitForEarlier : SequenceCheck
}
// on DomainEventPollingBackend, with a default so other backends keep working:
fun checkSequence(event: PersistedEvent, position: EventLogPosition): SequenceCheck = SequenceCheck.InOrder
```

- [ ] **Step 1: Write the failing integration test**

Create `kotmod/src/integrationTest/kotlin/io/kotmod/outbox/SourceOrderingIntegrationTest.kt`. It inserts events with raw SQL in hand-held transactions, runs an `AggregateEventOutbox` with an in-memory position, and records dispatch order with `recordingExecutor`:

```kotlin
package io.kotmod.outbox

import io.kotmod.EventLogPosition
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.postgres.support.recordingExecutor
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class SourceOrderingIntegrationTest : IntegrationTest() {
    private data class Seen(
        val label: String,
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private fun open(): Connection = dataSource.connection.apply { autoCommit = false }

    private fun assignTransactionId(conn: Connection) {
        conn.createStatement().use { it.executeQuery("SELECT pg_current_xact_id()").close() }
    }

    private fun insert(
        conn: Connection,
        aggregateId: String,
        sequence: Long,
    ) {
        conn
            .prepareStatement(
                "INSERT INTO ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence, causation_id, " +
                    "event_id, event_type, event_version, event_payload, event_timestamp) " +
                    "VALUES ('Order', ?, ?, 'cmd', ?, 'OrderPlaced', 1, '{}', now())",
            ).use { ps ->
                ps.setString(1, aggregateId)
                ps.setLong(2, sequence)
                ps.setString(3, "$aggregateId#$sequence")
                ps.executeUpdate()
            }
    }

    private fun outbox(dispatched: MutableList<String>): AggregateEventOutbox<Seen> {
        val position = AtomicReference(EventLogPosition.START)
        return AggregateEventOutbox(
            backend = PostgresDomainPollingBackend(jdbc),
            executor = recordingExecutor<Seen> { _, trigger -> dispatched += trigger.label },
            eventToReactions = { event ->
                val label = "${event.metadata.aggregateId.value}#${event.metadata.sequence}"
                listOf(EventReaction(EventReactionId("r-$label"), Seen(label)))
            },
            getPosition = { position.get() },
            savePosition = { position.set(it) },
            isLeader = { true },
            pollInterval = 50.milliseconds,
        )
    }

    @Test
    fun `an inverted pair is dispatched in sequence order, each once`() =
        runBlocking {
            dataSource.connection.use { insert(it, "A", 1) }
            val t1 = open()
            insert(t1, "B", 1) // t1 gets the earlier transaction id
            open().use { t2 ->
                insert(t2, "A", 2)
                t2.commit()
            }
            insert(t1, "A", 3) // later in A's history, earlier transaction id
            t1.commit()
            t1.close()

            val dispatched = CopyOnWriteArrayList<String>()
            val outbox = outbox(dispatched)
            outbox.start()
            try {
                eventually { dispatched.size >= 4 }
                delay(300)
            } finally {
                outbox.stop()
            }

            val forA = dispatched.filter { it.startsWith("A#") }
            assertEquals(listOf("A#1", "A#2", "A#3"), forA)
            assertEquals(4, dispatched.size)
        }

    @Test
    fun `the poller waits for an earlier event that is not yet readable`() =
        runBlocking {
            val t1 = open()
            insert(t1, "B", 1) // t1: earliest transaction id
            val blocker = open()
            assignTransactionId(blocker) // an unrelated transaction, id between t1 and t2, left open
            open().use { t2 ->
                insert(t2, "A", 1)
                t2.commit()
            }
            insert(t1, "A", 2)
            t1.commit()
            t1.close()

            val dispatched = CopyOnWriteArrayList<String>()
            val outbox = outbox(dispatched)
            outbox.start()
            try {
                delay(500)
                assertEquals(false, dispatched.contains("A#2"), "A#2 must wait for A#1, which is held back")
                blocker.rollback()
                blocker.close()
                eventually { dispatched.size >= 3 }
            } finally {
                outbox.stop()
            }

            assertEquals(listOf("A#1", "A#2"), dispatched.filter { it.startsWith("A#") })
        }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.outbox.SourceOrderingIntegrationTest'`
Expected: FAIL — the first test dispatches `A#1, A#3, A#2`; the second dispatches `A#2` before `A#1` is readable.

- [ ] **Step 3: Implement**

`DomainBackend.kt`: add `SequenceCheck` (above) and the default `checkSequence` on `DomainEventPollingBackend`, with KDoc explaining the four outcomes.

`PostgresDomainBackend.kt` — `PostgresDomainPollingBackend.checkSequence`:

```kotlin
    override fun checkSequence(
        event: PersistedEvent,
        position: EventLogPosition,
    ): SequenceCheck =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT $EVENT_COLUMNS, " +
                        "transaction_id < pg_snapshot_xmin(pg_current_snapshot()) AS readable " +
                        "FROM ddd_domain_event " +
                        "WHERE aggregate_type = ? AND aggregate_id = ? AND (" +
                        "(aggregate_sequence < ? AND (transaction_id, global_offset) > (?::text::xid8, ?)) OR " +
                        "(aggregate_sequence > ? AND (transaction_id, global_offset) <= (?::text::xid8, ?))) " +
                        "ORDER BY aggregate_sequence",
                ).use { ps ->
                    val m = event.metadata
                    ps.setString(1, m.aggregateType.value)
                    ps.setString(2, m.aggregateId.value)
                    ps.setLong(3, m.sequence)
                    ps.setLong(4, position.transactionId)
                    ps.setLong(5, position.globalOffset)
                    ps.setLong(6, m.sequence)
                    ps.setLong(7, position.transactionId)
                    ps.setLong(8, position.globalOffset)
                    ps.executeQuery().use { rs ->
                        val earlier = mutableListOf<PersistedEvent>()
                        var allReadable = true
                        var laterPassed = false
                        while (rs.next()) {
                            val found = rs.toPublishedEvent()
                            if (found.metadata.sequence > m.sequence) {
                                laterPassed = true
                            } else {
                                earlier += found
                                if (!rs.getBoolean("readable")) allReadable = false
                            }
                        }
                        when {
                            laterPassed -> SequenceCheck.AlreadyHandled
                            earlier.isEmpty() -> SequenceCheck.InOrder
                            !allReadable -> SequenceCheck.WaitForEarlier
                            else -> SequenceCheck.HandleEarlierFirst(earlier)
                        }
                    }
                }
        }
```

Extract the existing SELECT column list into a private `EVENT_COLUMNS` constant shared with `readEventsAfter` (`global_offset, transaction_id::text::bigint AS transaction_id_value, aggregate_type, aggregate_id, aggregate_sequence, causation_id, correlation_id, event_id, event_type, event_version, event_payload, event_timestamp`).

`DomainEventPoller.tick()`:

```kotlin
    private suspend fun tick() {
        if (!isLeader()) return

        var position = withContext(Dispatchers.IO) { getPosition() }
        val rows = withContext(Dispatchers.IO) { backend.readEventsAfter(position, batchSize) }

        for (envelope in rows) {
            when (val check = withContext(Dispatchers.IO) { backend.checkSequence(envelope, position) }) {
                SequenceCheck.WaitForEarlier -> return // an earlier event of this aggregate isn't readable yet
                SequenceCheck.AlreadyHandled -> Unit // handled early, when a later event pulled it forward
                SequenceCheck.InOrder -> handle(envelope)
                is SequenceCheck.HandleEarlierFirst -> {
                    check.earlier.forEach { handle(it) }
                    handle(envelope)
                }
            }
            position = envelope.position
            withContext(Dispatchers.IO) { savePosition(position) }
        }
    }

    private suspend fun handle(envelope: PersistedEvent) {
        log.debug("Handling DDD event {} [position={}]", envelope.metadata.eventId.value, envelope.position)
        handleEvent(envelope)
    }
```

Update the poller's class KDoc: "delivers each aggregate's events in sequence order (pulling an earlier event forward when the log has it later, and skipping it when reached)".

Update `kotmod/src/test/kotlin/io/kotmod/outbox/AggregateEventOutboxTest.kt` and `contract/PublicEventContractTest.kt` mocks: their `backend` mocks now also need `every { backend.checkSequence(any(), any()) } returns SequenceCheck.InOrder` (add to the setup next to `readEventsAfter`).

- [ ] **Step 4: Run it to verify it passes**

Run: `./gradlew :kotmod:integrationTest --tests 'io.kotmod.outbox.SourceOrderingIntegrationTest'`
Expected: PASS (2 tests).

- [ ] **Step 5: Run everything and commit**

Run: `./gradlew test integrationTest` — Expected: BUILD SUCCESSFUL.

```bash
git add -A kotmod
git commit -m "Deliver each aggregate's events in sequence order from the poller"
```

---

### Task 3: Ordering in the reaction API

**Interfaces:**
- Consumes: `EventMetadata.sequence`.
- Produces (core, `io.kotmod.event.reaction`): `ReactionOrdering`, `OnGiveUp`, `DispatchOrdering`, `ReactionOutcome` (see Global Constraints); `EventReactionTriggerSink.supportsOrdering: Boolean` (default `false`) and `publish(id, trigger, ordering: DispatchOrdering?)`; `EventReactionTriggerSource.subscribe(block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> ReactionOutcome)`; `EventReactionExecutor.dispatch(id, trigger, ordering: DispatchOrdering? = null)` and `val supportsOrdering: Boolean`; `AggregateEventOutbox(…, ordering: ReactionOrdering = ReactionOrdering.Unordered)`; `PublicEventContract.subscribe(executor, ordering = ReactionOrdering.Unordered, block)`; internal helper `fun ReactionOrdering.stampFor(metadata: EventMetadata, ordinal: Int): DispatchOrdering?`.

- [ ] **Step 1: Write the failing tests**

`EventReactionExecutorTest` — change the stub source's `subscribed` type to `suspend (…) -> ReactionOutcome`, and add:

```kotlin
    @Test
    fun `a completed reaction finishes without giving up`() =
        runBlocking {
            startExecutor { EventReactionExecutionResult.EventReactionExecutionCompleted }
            assertEquals(ReactionOutcome.Finished(gaveUp = false), runReaction())
        }

    @Test
    fun `a failed reaction that is not retried finishes as given up`() =
        runBlocking {
            startExecutor { EventReactionExecutionResult.EventReactionFailed(RuntimeException("boom")) }
            assertEquals(ReactionOutcome.Finished(gaveUp = true), runReaction())
        }
```

(the stub's `failureRetryHandler` already returns `DoNotRetry(EventReactionFailed(…))`; the timeout test's assertion becomes `assertEquals(ReactionOutcome.Retry(1.seconds), result)`).

`AggregateEventOutboxTest` — add:

```kotlin
    @Test
    fun `an ordered outbox stamps each reaction with the source aggregate, sequence and ordinal`() {
        every { executor.supportsOrdering } returns true
        givenEvents(persistedEvent(globalOffset = 10, aggregateId = "o-1", sequence = 4))
        val outbox =
            AggregateEventOutbox(
                backend = backend,
                executor = executor,
                eventToReactions = { listOf(EventReaction(EventReactionId("a"), FakeTrigger("a")), EventReaction(EventReactionId("b"), FakeTrigger("b"))) },
                getPosition = offsets::get,
                savePosition = offsets::save,
                isLeader = { true },
                ordering = ReactionOrdering.PerAggregate(OnGiveUp.BlockAggregate),
            )
        kotlinx.coroutines.runBlocking { outbox.tickForTest() }

        coVerifyOrder {
            executor.dispatch(EventReactionId("a"), FakeTrigger("a"), DispatchOrdering("Order/o-1", 4, 0, OnGiveUp.BlockAggregate))
            executor.dispatch(EventReactionId("b"), FakeTrigger("b"), DispatchOrdering("Order/o-1", 4, 1, OnGiveUp.BlockAggregate))
        }
    }

    @Test
    fun `an ordered outbox on an executor that cannot order fails at construction`() {
        every { executor.supportsOrdering } returns false
        assertFailsWith<IllegalArgumentException> {
            AggregateEventOutbox(
                backend = backend,
                executor = executor,
                eventToReactions = { emptyList() },
                getPosition = offsets::get,
                savePosition = offsets::save,
                isLeader = { true },
                ordering = ReactionOrdering.PerAggregate(),
            )
        }
    }
```

(`persistedEvent` fixture gains a `sequence: Long = 1` parameter passed to `EventMetadata`.) Existing outbox/contract tests verify `executor.dispatch(id, trigger, null)` for unordered subscriptions (update their `coVerify` calls to include `null`/`any()` as the third argument).

`PublicEventContractTest` — add (Review Focus 5):

```kotlin
    @Test
    fun `a contract stamps only its ordered subscriptions`() {
        every { executorA.supportsOrdering } returns true
        givenEvents(persistedEvent(globalOffset = 10, aggregateId = "o-1", sequence = 2, eventType = "Opened", eventPayload = "Opened(id=doc-1)"))
        val contract = newContract()
        contract.subscribe(executorA, ordering = ReactionOrdering.PerAggregate()) { listOf(EventReaction(EventReactionId("A"), FakeTrigger("A"))) }
        contract.subscribe(executorB) { listOf(EventReaction(EventReactionId("B"), FakeTrigger("B"))) }
        kotlinx.coroutines.runBlocking { contract.tickForTest() }

        coVerify { executorA.dispatch(EventReactionId("A"), FakeTrigger("A"), DispatchOrdering("Order/o-1", 2, 0, OnGiveUp.ContinueWithNext)) }
        coVerify { executorB.dispatch(EventReactionId("B"), FakeTrigger("B"), null) }
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod:test`
Expected: FAIL — compilation (`ReactionOutcome`, `ReactionOrdering`, `DispatchOrdering` unresolved).

- [ ] **Step 3: Implement**

Create `kotmod/src/main/kotlin/io/kotmod/event/reaction/Ordering.kt`:

```kotlin
package io.kotmod.event.reaction

import io.kotmod.EventMetadata
import kotlin.time.Duration

/** Whether a subscription's reactions run in their aggregate's history order. */
sealed interface ReactionOrdering {
    /** Reactions may run in any order and in parallel (the default). */
    data object Unordered : ReactionOrdering

    /**
     * Reactions for the same source aggregate run one at a time, in the order of the events that caused them.
     * [onGiveUp] decides what happens to later reactions when one gives up for good.
     */
    data class PerAggregate(
        val onGiveUp: OnGiveUp = OnGiveUp.ContinueWithNext,
    ) : ReactionOrdering
}

/** What an ordered subscription does when a reaction gives up for good. */
enum class OnGiveUp {
    /** Complete it as failed and move on to the aggregate's next reaction. */
    ContinueWithNext,

    /** Hold back the aggregate's later reactions until the failed one is retried or skipped by an operator. */
    BlockAggregate,
}

/** The ordering stamp kotmod attaches to a reaction dispatched by an ordered subscription. */
data class DispatchOrdering(
    val key: String,
    val sequence: Long,
    val ordinal: Int,
    val onGiveUp: OnGiveUp,
)

/** What a source learns after one attempt at a reaction. */
sealed interface ReactionOutcome {
    /** Run the reaction again after [delay]. */
    data class Retry(
        val delay: Duration,
    ) : ReactionOutcome

    /** The reaction is done; [gaveUp] is true when it ended as a failure. */
    data class Finished(
        val gaveUp: Boolean,
    ) : ReactionOutcome
}

internal fun ReactionOrdering.stampFor(
    metadata: EventMetadata,
    ordinal: Int,
): DispatchOrdering? =
    when (this) {
        ReactionOrdering.Unordered -> null
        is ReactionOrdering.PerAggregate ->
            DispatchOrdering("${metadata.aggregateType.value}/${metadata.aggregateId.value}", metadata.sequence, ordinal, onGiveUp)
    }
```

`EventReaction.kt`:
- `EventReactionTriggerSink`: add `val supportsOrdering: Boolean get() = false` and change `publish(id, trigger)` to `publish(id: EventReactionId, trigger: T, ordering: DispatchOrdering?)`, KDoc: "An ordering stamp is only passed to sinks that support ordering; such sinks must run reactions with the same key one at a time, in (sequence, ordinal) order."
- `EventReactionTriggerSource.subscribe` block returns `ReactionOutcome`; KDoc updated.
- `EventReactionExecutor`: `val supportsOrdering: Boolean get() = sink.supportsOrdering`; `suspend fun dispatch(id: EventReactionId, trigger: T, ordering: DispatchOrdering? = null) { require(ordering == null || sink.supportsOrdering) { "This executor's sink does not support ordering" }; sink.publish(id, trigger, ordering) }`.
- In `start()`, change return values: completed → `ReactionOutcome.Finished(gaveUp = false)` (replacing `null`); completion-handler failure → `ReactionOutcome.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))`; cancelled → `ReactionOutcome.Finished(gaveUp = false)`; `RetrySignal.Retry` from a retry handler → `ReactionOutcome.Retry(retryHandlingResult.delay)`; `DoNotRetry` → after `onCompletion`, `ReactionOutcome.Finished(gaveUp = retryHandlingResult.completionResult is EventReactionCompletionResult.EventReactionFailed)`; retry-handler failure → `ReactionOutcome.Retry(defaultBackoffStrategy.calculateBackoff(retryCount))`.

`AggregateEventOutbox.kt`: add `private val ordering: ReactionOrdering = ReactionOrdering.Unordered` (last constructor parameter) and an `init { require(ordering == ReactionOrdering.Unordered || executor.supportsOrdering) { "An ordered outbox needs an executor whose sink supports ordering" } }`; dispatch with `reactions.forEachIndexed { ordinal, (id, trigger) -> executor.dispatch(id, trigger, ordering.stampFor(envelope.metadata, ordinal)) }`.

`PublicEventContract.kt`: `Subscription` gains `val ordering: ReactionOrdering`; `subscribe(executor, ordering: ReactionOrdering = ReactionOrdering.Unordered, block)` with the same `require`; `fanOut` dispatches with `ordering.stampFor(envelope.metadata, ordinal)`.

Integration support `postgres/support/RecordingExecutor.kt`: sink `publish(id, trigger, ordering)` (ignores ordering); source `subscribe` block type returns `ReactionOutcome`.

db-scheduler module (behaviour for unordered reactions unchanged):
- Rename `ReactionOutcome.kt` → `TaskRowOutcome.kt`, the sealed interface `ReactionOutcome` → `TaskRowOutcome` (and its test file/class `ReactionOutcomeTest` → `TaskRowOutcomeTest`, references updated).
- `outcomeAfterExecution(result: io.kotmod.event.reaction.ReactionOutcome, data, now)`: `Finished` → `TaskRowOutcome.Remove`; `Retry(delay)` → reschedule as today. Update `TaskRowOutcomeTest` accordingly (`null` → `ReactionOutcome.Finished(false)`, `RetrySignal.Retry(d)` → `ReactionOutcome.Retry(d)`).
- `DbSchedulerTriggerSource`: `ReactionHandler<T>` returns `ReactionOutcome`.
- `DbSchedulerTriggerSink.publish(id, trigger, ordering)`: for now `require(ordering == null)` (Task 4 adds ordering).
- `DbSchedulerTriggerSinkTest`/`DbSchedulerTriggerSourceTest`: pass `null` as `ordering`; handler lambdas return `ReactionOutcome.Finished(false)`.

- [ ] **Step 4: Run everything**

Run: `./gradlew test integrationTest`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add -A kotmod kotmod-db-scheduler examples
git commit -m "Add ordering to the reaction API"
```

---

### Task 4: Ordered reactions on db-scheduler

**Interfaces:**
- Consumes: Task 3 API.
- Produces: `DbSchedulerEventReactions(taskName, triggerSerializer, unsubscribedRetryDelay = 5.seconds, jdbc: JdbcContext? = null, orderedRecheckDelay: Duration = 2.seconds, tableName: String = "scheduled_tasks")`; `supportsOrdering`; `blockedReactions(client: SchedulerClient): List<BlockedReaction>`, `retryBlocked(client, id: EventReactionId)`, `skipBlocked(client, id)`; `data class BlockedReaction(val key: String, val reactionId: EventReactionId, val sequence: Long)`.

- [ ] **Step 1: Write the failing unit tests**

`OrderedIdsTest.kt` (db-scheduler `src/test`):

```kotlin
package io.kotmod.event.reaction.dbscheduler

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OrderedIdsTest {
    @Test
    fun `ids sort by sequence then ordinal within a key`() {
        val ids =
            listOf(
                orderedInstanceId("Order/o-1", 10, 0, "r-c"),
                orderedInstanceId("Order/o-1", 2, 1, "r-b"),
                orderedInstanceId("Order/o-1", 2, 0, "r-a"),
            )
        assertEquals(listOf("r-a", "r-b", "r-c"), ids.sorted().map { it.substringAfterLast('#') })
    }

    @Test
    fun `key prefixes never overlap even when ids contain separators`() {
        val a = orderedKeyPrefix("Order/a")
        val tricky = orderedInstanceId("Order/a#0", 1, 0, "r")
        assertTrue(!tricky.startsWith(a), "different keys must never share a prefix")
    }
}
```

`TaskRowOutcomeTest` additions:

```kotlin
    @Test
    fun `an ordered reaction that gives up with BlockAggregate is parked and flagged`() {
        val ordered = data.copy(ordering = OrderingStamp("Order/o-1", 3, 0, "BlockAggregate", "r-1"))
        val outcome = outcomeAfterExecution(ReactionOutcome.Finished(gaveUp = true), ordered, now)
        outcome as TaskRowOutcome.Reschedule
        assertTrue(outcome.at.isAfter(now.plusSeconds(PARKED_DELAY.inWholeSeconds - 1)))
        assertEquals(true, ReactionTaskData.decode(outcome.taskData).blocked)
    }

    @Test
    fun `an ordered reaction that gives up with ContinueWithNext is removed and nudges the next`() {
        val ordered = data.copy(ordering = OrderingStamp("Order/o-1", 3, 0, "ContinueWithNext", "r-1"))
        assertEquals(TaskRowOutcome.Remove(nudgeKey = "Order/o-1"), outcomeAfterExecution(ReactionOutcome.Finished(true), ordered, now))
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `./gradlew :kotmod-db-scheduler:test`
Expected: FAIL — unresolved `orderedInstanceId`, `OrderingStamp`, `PARKED_DELAY`.

- [ ] **Step 3: Implement the pure parts**

`OrderedIds.kt`:

```kotlin
package io.kotmod.event.reaction.dbscheduler

/** `<len(key)>:<key>#` — the length prefix keeps different keys' ranges apart whatever characters they contain. */
internal fun orderedKeyPrefix(key: String): String = "${key.length}:$key#"

/** A db-scheduler instance id that sorts by key, then sequence, then ordinal. */
internal fun orderedInstanceId(
    key: String,
    sequence: Long,
    ordinal: Int,
    reactionId: String,
): String = orderedKeyPrefix(key) + sequence.toString().padStart(19, '0') + "#" + ordinal.toString().padStart(4, '0') + "#" + reactionId
```

`ReactionTaskData.kt`:

```kotlin
@Serializable
internal data class OrderingStamp(
    val key: String,
    val sequence: Long,
    val ordinal: Int,
    val onGiveUp: String,
    val reactionId: String,
)

@Serializable
internal data class ReactionTaskData(
    val trigger: String,
    val retryCount: Int,
    val ordering: OrderingStamp? = null,
    val blocked: Boolean = false,
) { /* encode/decode unchanged */ }
```

`TaskRowOutcome.kt`:

```kotlin
internal val PARKED_DELAY: Duration = 36500.days

internal sealed interface TaskRowOutcome {
    /** Remove the row; for ordered reactions, nudge the next pending reaction of [nudgeKey]. */
    data class Remove(val nudgeKey: String? = null) : TaskRowOutcome

    data class Reschedule(val at: Instant, val taskData: String) : TaskRowOutcome
}

internal fun outcomeAfterExecution(
    result: ReactionOutcome,
    data: ReactionTaskData,
    now: Instant,
): TaskRowOutcome =
    when (result) {
        is ReactionOutcome.Retry ->
            TaskRowOutcome.Reschedule(now.plus(result.delay.coerceAtMost(MAX_RETRY_DELAY).toJavaDuration()), data.copy(retryCount = data.retryCount + 1).encode())
        is ReactionOutcome.Finished -> {
            val ordering = data.ordering
            if (ordering != null && result.gaveUp && ordering.onGiveUp == OnGiveUp.BlockAggregate.name) {
                TaskRowOutcome.Reschedule(now.plus(PARKED_DELAY.toJavaDuration()), data.copy(blocked = true).encode())
            } else {
                TaskRowOutcome.Remove(nudgeKey = ordering?.key)
            }
        }
    }

internal fun outcomeWhenWaiting(rawTaskData: String, now: Instant, delay: Duration): TaskRowOutcome =
    TaskRowOutcome.Reschedule(now.plus(delay.toJavaDuration()), rawTaskData)
```

(`outcomeWhenUnsubscribed` stays, returning `TaskRowOutcome.Reschedule`; existing `TaskRowOutcomeTest` cases expecting `Remove` now expect `Remove(nudgeKey = null)`.)

- [ ] **Step 4: Run the unit tests**

Run: `./gradlew :kotmod-db-scheduler:test`
Expected: PASS.

- [ ] **Step 5: Write the failing integration tests**

Create `kotmod-db-scheduler/src/integrationTest/kotlin/io/kotmod/event/reaction/dbscheduler/OrderedReactionsIntegrationTest.kt`. Use a local executor builder that dispatches with an explicit `DispatchOrdering` (the poller's stamping is covered in Task 3) and records start/end per reaction:

```kotlin
package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionCompletionResult
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.RetrySignal
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class OrderedReactionsIntegrationTest : IntegrationTest() {
    private fun reactions(name: String = "ordered") =
        DbSchedulerEventReactions(name, TestTriggerSerializer, jdbc = jdbc, orderedRecheckDelay = 200.milliseconds)

    private fun ordering(
        key: String,
        sequence: Long,
        onGiveUp: OnGiveUp = OnGiveUp.ContinueWithNext,
    ) = DispatchOrdering(key, sequence, 0, onGiveUp)

    private class Log {
        val events = CopyOnWriteArrayList<String>() // "start:<name>" / "end:<name>"
    }

    private fun executor(
        reactions: DbSchedulerEventReactions<TestTrigger>,
        scheduler: com.github.kagkarlsson.scheduler.Scheduler,
        log: Log,
        execute: suspend (TestTrigger, Int) -> EventReactionExecutionResult = { _, _ ->
            delay(50)
            EventReactionExecutionResult.EventReactionExecutionCompleted
        },
        giveUp: Boolean = false,
    ) = EventReactionExecutor<TestTrigger, Unit>(
        sink = reactions.sink(scheduler),
        source = reactions.source,
        createExecutionContext = { _, _ -> },
        execute = { _, _, trigger, retryCount, _ ->
            log.events += "start:${trigger.name}"
            try {
                execute(trigger, retryCount)
            } finally {
                log.events += "end:${trigger.name}"
            }
        },
        failureRetryHandler = { _, _, _, _, _, _ ->
            if (giveUp) RetrySignal.DoNotRetry(EventReactionCompletionResult.EventReactionFailed("gave up", allowManualRetry = true))
            else RetrySignal.Retry(100.milliseconds)
        },
        timeoutRetryHandler = { _, _, _, _, _ -> RetrySignal.Retry(100.milliseconds) },
        onCompletion = { _, _, _, _, _, _ -> },
    )

    private fun neverOverlap(log: Log): Boolean {
        var running = 0
        for (e in log.events) {
            running += if (e.startsWith("start:")) 1 else -1
            if (running > 1) return false
        }
        return true
    }

    @Test
    fun `reactions for one aggregate run one at a time in sequence order`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log)
            (1..8L).forEach { executor.dispatch(EventReactionId("r-$it"), TestTrigger("A$it"), ordering("Order/a", it)) }

            running(scheduler, executor) { eventually(20.seconds) { log.events.count { it.startsWith("end:") } == 8 } }

            assertTrue(neverOverlap(log), log.events.toString())
            assertEquals((1..8).map { "A$it" }, log.events.filter { it.startsWith("start:") }.map { it.removePrefix("start:") })
        }

    @Test
    fun `different aggregates run in parallel`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, execute = { _, _ ->
                delay(500)
                EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-a"), TestTrigger("A1"), ordering("Order/a", 1))
            executor.dispatch(EventReactionId("r-b"), TestTrigger("B1"), ordering("Order/b", 1))

            running(scheduler, executor) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `a retrying head holds back later reactions`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, execute = { trigger, retryCount ->
                if (trigger.name == "A1" && retryCount < 2) EventReactionExecutionResult.EventReactionFailed(RuntimeException("flaky"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2))

            running(scheduler, executor) { eventually(15.seconds) { log.events.contains("end:A2") } }

            val starts = log.events.filter { it.startsWith("start:") }
            assertEquals(listOf("start:A1", "start:A1", "start:A1", "start:A2"), starts)
        }

    @Test
    fun `ContinueWithNext moves on after a give-up`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, giveUp = true, execute = { trigger, _ ->
                if (trigger.name == "A1") EventReactionExecutionResult.EventReactionFailed(RuntimeException("poison"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.ContinueWithNext))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.ContinueWithNext))

            running(scheduler, executor) { eventually(10.seconds) { log.events.contains("end:A2") } }
        }

    @Test
    fun `BlockAggregate parks the aggregate until retried or skipped, and is never nudged`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val failures = AtomicInteger()
            val executor = executor(reactions, scheduler, log, giveUp = true, execute = { trigger, _ ->
                if (trigger.name == "A1" && failures.getAndIncrement() == 0) EventReactionExecutionResult.EventReactionFailed(RuntimeException("poison"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.BlockAggregate))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.BlockAggregate))

            running(scheduler, executor) {
                eventually { reactions.blockedReactions(scheduler).isNotEmpty() }
                delay(1000)
                assertTrue("start:A2" !in log.events, "A2 must wait behind the blocked A1")
                assertEquals(listOf(EventReactionId("r-1")), reactions.blockedReactions(scheduler).map { it.reactionId })

                reactions.retryBlocked(scheduler, EventReactionId("r-1"))
                eventually(10.seconds) { log.events.contains("end:A2") }
            }
            assertEquals(listOf("start:A1", "start:A1", "start:A2"), log.events.filter { it.startsWith("start:") })
        }

    @Test
    fun `skipBlocked releases the aggregate without running the blocked reaction again`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, giveUp = true, execute = { trigger, _ ->
                if (trigger.name == "A1") EventReactionExecutionResult.EventReactionFailed(RuntimeException("poison"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.BlockAggregate))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.BlockAggregate))

            running(scheduler, executor) {
                eventually { reactions.blockedReactions(scheduler).isNotEmpty() }
                reactions.skipBlocked(scheduler, EventReactionId("r-1"))
                eventually(10.seconds) { log.events.contains("end:A2") }
            }
            assertEquals(1, log.events.count { it == "start:A1" })
        }

    @Test
    fun `duplicate ordered dispatch is absorbed`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log)
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))

            running(scheduler, executor) {
                eventually { log.events.contains("end:A1") }
                delay(500)
            }
            assertEquals(1, log.events.count { it == "start:A1" })
        }

    @Test
    fun `aggregate ids with separators never share ordering`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, execute = { _, _ ->
                delay(400)
                EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-a"), TestTrigger("X"), ordering("Order/a", 2))
            executor.dispatch(EventReactionId("r-b"), TestTrigger("Y"), ordering("Order/a#0000000000000000001", 1))

            running(scheduler, executor) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            // Different aggregates: both start before either ends.
            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `two executors handling the same aggregate do not wait on each other`() =
        runBlocking {
            val first = reactions("first")
            val second = reactions("second")
            val scheduler = testScheduler(dataSource, *(first.tasks + second.tasks).toTypedArray())
            val log = Log()
            val slow: suspend (TestTrigger, Int) -> EventReactionExecutionResult = { _, _ ->
                delay(500)
                EventReactionExecutionResult.EventReactionExecutionCompleted
            }
            val e1 = executor(first, scheduler, log, execute = slow)
            val e2 = executor(second, scheduler, log, execute = slow)
            e1.dispatch(EventReactionId("r-1"), TestTrigger("first"), ordering("Order/a", 1))
            e2.dispatch(EventReactionId("r-1"), TestTrigger("second"), ordering("Order/a", 1))

            running(scheduler, e1, e2) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }
}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `./gradlew :kotmod-db-scheduler:integrationTest --tests '*OrderedReactionsIntegrationTest*'`
Expected: FAIL — compilation (`tasks`, `jdbc` parameter, helpers unresolved).

- [ ] **Step 7: Implement the db-scheduler side**

`OrderedQueries.kt`:

```kotlin
package io.kotmod.event.reaction.dbscheduler

import io.kotmod.jdbc.JdbcContext

/** SQL against db-scheduler's own table for ordered reactions. Comparisons use the "C" collation. */
internal class OrderedQueries(
    private val jdbc: JdbcContext,
    private val tableName: String,
) {
    fun earlierPending(
        taskName: String,
        key: String,
        ownInstanceId: String,
    ): Boolean =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT EXISTS (SELECT 1 FROM $tableName WHERE task_name = ? AND starts_with(task_instance, ?) " +
                        "AND task_instance COLLATE \"C\" < ? COLLATE \"C\")",
                ).use { ps ->
                    ps.setString(1, taskName)
                    ps.setString(2, orderedKeyPrefix(key))
                    ps.setString(3, ownInstanceId)
                    ps.executeQuery().use { rs -> rs.next(); rs.getBoolean(1) }
                }
        }

    /** The next pending, not-parked reaction of [key], if any. */
    fun nextPending(
        taskName: String,
        key: String,
    ): String? =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT task_instance FROM $tableName WHERE task_name = ? AND starts_with(task_instance, ?) " +
                        "AND NOT picked AND execution_time < now() + interval '50 years' " +
                        "ORDER BY task_instance COLLATE \"C\" LIMIT 1",
                ).use { ps ->
                    ps.setString(1, taskName)
                    ps.setString(2, orderedKeyPrefix(key))
                    ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
                }
        }
}
```

`DbSchedulerTriggerSink.publish(id, trigger, ordering)`:

```kotlin
        val stamp = ordering?.let { OrderingStamp(it.key, it.sequence, it.ordinal, it.onGiveUp.name, id.value) }
        val instanceId = stamp?.let { orderedInstanceId(it.key, it.sequence, it.ordinal, id.value) } ?: id.value
        val taskData = ReactionTaskData(trigger = triggerSerializer.serialize(trigger), retryCount = 0, ordering = stamp).encode()
        // scheduleIfNotExists(TaskInstance(taskName, instanceId, taskData), clock()) as today
```

with `override val supportsOrdering: Boolean` passed from `DbSchedulerEventReactions` (true when `jdbc != null`).

`ReactionTask.kt` — `reactionTask(…, orderedQueries: OrderedQueries?, orderedRecheckDelay: Duration)`; inside `.execute { instance, context ->`:

```kotlin
            val handler = source.handler
            val outcome =
                if (handler == null) {
                    // (unsubscribed warning as today)
                    outcomeWhenUnsubscribed(instance.data, Instant.now(), unsubscribedRetryDelay)
                } else {
                    val data = ReactionTaskData.decode(instance.data)
                    val ordering = data.ordering
                    if (ordering != null && (data.blocked || checkNotNull(orderedQueries).earlierPending(taskName, ordering.key, instance.id))) {
                        outcomeWhenWaiting(instance.data, Instant.now(), if (data.blocked) PARKED_DELAY else orderedRecheckDelay)
                    } else {
                        val reactionId = EventReactionId(ordering?.reactionId ?: instance.id)
                        // (executionId, debug log, runBlocking { handler(reactionId, …) } as today)
                        outcomeAfterExecution(result, data, Instant.now())
                    }
                }
            outcome.toCompletionHandler(context.schedulerClient)
```

and `toCompletionHandler(client)`:

```kotlin
private fun TaskRowOutcome.toCompletionHandler(
    client: SchedulerClient,
    taskName: String,
    orderedQueries: OrderedQueries?,
): CompletionHandler<String> =
    CompletionHandler { executionComplete, executionOperations ->
        when (this) {
            is TaskRowOutcome.Remove -> {
                executionOperations.remove()
                if (nudgeKey != null && orderedQueries != null) {
                    runCatching {
                        orderedQueries.nextPending(taskName, nudgeKey)?.let { next ->
                            client.reschedule(TaskInstanceId.of(taskName, next), Instant.now())
                        }
                    }.onFailure { log.warn("Could not nudge the next reaction of {}", nudgeKey, it) }
                }
            }
            is TaskRowOutcome.Reschedule -> executionOperations.reschedule(executionComplete, at, taskData)
        }
    }
```

(pass `taskName` and `orderedQueries` through; `TaskInstanceId.of` is db-scheduler's factory — if it does not exist in 16.12.0, use `TaskInstance<String>(taskName, next)` which implements `TaskInstanceId`.)

`DbSchedulerEventReactions.kt`:

```kotlin
class DbSchedulerEventReactions<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    unsubscribedRetryDelay: Duration = 5.seconds,
    jdbc: JdbcContext? = null,
    orderedRecheckDelay: Duration = 2.seconds,
    tableName: String = "scheduled_tasks",
) {
    private val orderedQueries = jdbc?.let { OrderedQueries(it, tableName) }
    private val triggerSource = DbSchedulerTriggerSource<T>()

    /** Whether this can run ordered reactions (requires [jdbc]). */
    val supportsOrdering: Boolean get() = orderedQueries != null

    val task: Task<String> = reactionTask(taskName, triggerSerializer, triggerSource, unsubscribedRetryDelay, orderedQueries, orderedRecheckDelay)

    /** The tasks to register with the app's `Scheduler`. */
    val tasks: List<Task<*>> get() = listOf(task)

    val source: EventReactionTriggerSource<T> get() = triggerSource

    fun sink(client: SchedulerClient): EventReactionTriggerSink<T> =
        DbSchedulerTriggerSink(taskName, triggerSerializer, client, supportsOrdering = supportsOrdering)

    /** Ordered reactions that gave up with [OnGiveUp.BlockAggregate] and are holding back their aggregate. */
    fun blockedReactions(client: SchedulerClient): List<BlockedReaction> =
        client
            .getScheduledExecutionsForTask(taskName, String::class.java)
            .mapNotNull { execution ->
                val data = ReactionTaskData.decode(execution.data)
                val ordering = data.ordering
                if (data.blocked && ordering != null) BlockedReaction(ordering.key, EventReactionId(ordering.reactionId), ordering.sequence) else null
            }

    /** Runs a blocked reaction again now, with its retry count reset. */
    fun retryBlocked(client: SchedulerClient, id: EventReactionId) {
        val execution = blockedExecution(client, id)
        val data = ReactionTaskData.decode(execution.data)
        client.reschedule(execution.taskInstance, Instant.now(), data.copy(blocked = false, retryCount = 0).encode())
    }

    /** Drops a blocked reaction so its aggregate's next reaction can run. */
    fun skipBlocked(client: SchedulerClient, id: EventReactionId) {
        val execution = blockedExecution(client, id)
        val key = checkNotNull(ReactionTaskData.decode(execution.data).ordering).key
        client.cancel(execution.taskInstance)
        orderedQueries?.nextPending(taskName, key)?.let { client.reschedule(TaskInstance<String>(taskName, it), Instant.now()) }
    }

    private fun blockedExecution(client: SchedulerClient, id: EventReactionId) =
        client.getScheduledExecutionsForTask(taskName, String::class.java).single { execution ->
            val data = ReactionTaskData.decode(execution.data)
            data.blocked && data.ordering?.reactionId == id.value
        }
}

/** An ordered reaction holding back its aggregate after giving up. */
data class BlockedReaction(
    val key: String,
    val reactionId: EventReactionId,
    val sequence: Long,
)
```

Update `DbSchedulerTestSupport.testScheduler` callers and the README examples/tests to keep using `reactions.task` or switch to `reactions.tasks` (both are valid).

- [ ] **Step 8: Run the ordered tests and everything**

Run: `./gradlew :kotmod-db-scheduler:integrationTest --tests '*OrderedReactionsIntegrationTest*'` — Expected: PASS (9 tests).
Run: `./gradlew test integrationTest` — Expected: BUILD SUCCESSFUL.

- [ ] **Step 9: Commit**

```bash
git add -A kotmod-db-scheduler kotmod examples
git commit -m "Run ordered reactions one at a time per aggregate on db-scheduler"
```

---

### Task 5: Documentation

**Files:** `README.md`, `examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt`

- [ ] **Step 1: Add a compiled example**

In `ReadmeExamples.kt` add (imports `io.kotmod.event.reaction.ReactionOrdering`, `io.kotmod.event.reaction.OnGiveUp`, `io.kotmod.jdbc.JdbcContext`):

```kotlin
// Guide: Ordered reactions

fun orderedNotifications(jdbc: JdbcContext): DbSchedulerEventReactions<OrderNotification> =
    DbSchedulerEventReactions("order-notifications", OrderNotificationSerializer, jdbc = jdbc)

fun orderedOutbox(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    offsets: PostgresOffsetManager,
    executor: EventReactionExecutor<OrderNotification, *>,
): AggregateEventOutbox<OrderNotification> =
    AggregateEventOutbox(
        backend = PostgresDomainPollingBackend(jdbc),
        executor = executor,
        eventToReactions = { event ->
            when (serialization.deserialize(event.serialized)) {
                is OrderPlaced -> listOf(EventReaction(EventReactionId("confirmation-${event.metadata.eventId.value}"), SendOrderConfirmation(event.metadata.aggregateId.value)))
                else -> emptyList()
            }
        },
        getPosition = { offsets.getPosition("order-notifications") },
        savePosition = { offsets.savePosition("order-notifications", it) },
        isLeader = { true },
        ordering = ReactionOrdering.PerAggregate(onGiveUp = OnGiveUp.BlockAggregate),
    )
```

Run `./gradlew :examples:integrationTest` — Expected: PASS.

- [ ] **Step 2: Confirm the README checker still passes, then write the guide**

Run `python3 <scratchpad>/readme_check2.py .` — Expected: OK (no README change yet).

Add to the README guides (and the table of contents) a section `### Ordered reactions` after "Durable reactions with db-scheduler", covering: when to use ordering (projections and anything that must apply an aggregate's changes in order); the `ordering = ReactionOrdering.PerAggregate(…)` flag on `AggregateEventOutbox` and `PublicEventContract.subscribe`; the two `OnGiveUp` policies; that db-scheduler needs `jdbc` to support ordering (and an ordered subscription on a sink without ordering fails at construction); the `orderedOutbox` example copied from `ReadmeExamples.kt`; the operational helpers `blockedReactions`, `retryBlocked`, `skipBlocked`; trade-offs (waiting reactions re-check every `orderedRecheckDelay`; finished reactions nudge the next); scoping (ordering is per executor and aggregate; executors sharing an aggregate don't wait on each other; subscriptions sharing an executor share ordering and need distinct reaction ids). In the aggregates guide, mention `event.metadata.sequence` (1, 2, 3… per aggregate). In the Postgres schema table, note `ddd_domain_event` "ordered by `(transaction_id, global_offset)`, with each event's `aggregate_sequence`". Fix the stale "ordered by `global_offset`" wording.

- [ ] **Step 3: Run the checks**

Run: `python3 <scratchpad>/readme_check2.py .` — Expected: OK.
Run: `./gradlew test integrationTest` — Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add README.md examples/src/integrationTest/kotlin/io/kotmod/readme/ReadmeExamples.kt
git commit -m "Document ordered reactions and event sequence numbers"
```

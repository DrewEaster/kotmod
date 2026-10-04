# Fix: events skipped under concurrent writes

**Date:** 2026-10-04
**Status:** Approved design, pending implementation plan

## Problem

`ddd_domain_event.global_offset` comes from a `BIGSERIAL` sequence, assigned when an event is inserted,
but transactions commit in any order. `PostgresDomainPollingBackend` reads `WHERE global_offset > ?` and
the poller (behind `AggregateEventOutbox` and `PublicEventContract`) saves the highest offset it read. If
a transaction holding offset 10 commits after a poll has read and saved offset 11, offset 10 is never
read and its reactions never run.

Filtering out events newer than in-flight transactions is not enough on its own: Postgres assigns a
transaction id at a transaction's first write, while offsets are assigned later at insert time, so a
transaction with a *later* id can hold a *lower* offset. Position must therefore be tracked by
(transaction id, offset), not offset alone.

## Goal

Every committed event is delivered to every outbox and public contract, under any concurrency
(including multi-aggregate outer transactions), with no timing heuristics. Writers stay fully concurrent.

## Decision

Approach A: record each event's writing transaction id; consumers track a `(transaction id, offset)`
position and only read events written by transactions older than the oldest transaction still in
progress. (Rejected: B, a global advisory lock serializing event commits — caps write throughput;
C, reader gap detection with a timeout — loses events from transactions slower than the timeout.)

## Schema

`ddd_domain_event` gains:

```sql
transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id()
```

and an index:

```sql
CREATE INDEX idx_ddd_domain_event_position ON ddd_domain_event (transaction_id, global_offset);
```

Postgres fills `transaction_id`; kotmod's insert statement is unchanged. All events of one transaction
share it.

`ddd_consumer_offset` stores a position:

```sql
CREATE TABLE ddd_consumer_offset (
    consumer_name       VARCHAR(255) PRIMARY KEY,
    last_transaction_id BIGINT       NOT NULL,
    last_offset         BIGINT       NOT NULL,
    updated_at          TIMESTAMPTZ  NOT NULL
);
```

Requires **PostgreSQL 13+** (`xid8`, `pg_current_xact_id()`, `pg_current_snapshot()`,
`pg_snapshot_xmin()`).

## Reading

`PostgresDomainPollingBackend.readEventsAfter(position, limit)`:

```sql
SELECT …, transaction_id::text::bigint AS transaction_id
FROM ddd_domain_event
WHERE (transaction_id, global_offset) > (?::text::xid8, ?)
  AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY transaction_id, global_offset
LIMIT ?
```

- **Visibility line:** only events from transactions older than the oldest in-progress transaction are
  read. Any transaction that could still commit an event has an id at or above the line, so it sorts
  after every position already saved.
- **Ordering:** by `(transaction_id, global_offset)`. Per-aggregate order is preserved for single-aggregate
  commands; with multi-aggregate outer transactions it can invert in rare cases (handled by the later
  ordering work).
- **Latency:** a long-running transaction delays delivery of later events until it finishes; it never
  causes loss.
- Transaction ids are read as `BIGINT` via a text cast (JDBC has no `xid8` type); Postgres 64-bit ids
  never wrap.

## API

New type in `io.kotmod`:

```kotlin
data class EventLogPosition(
    val transactionId: Long,
    val globalOffset: Long,
) : Comparable<EventLogPosition> {
    companion object {
        val START = EventLogPosition(0, 0)
    }
}
```

| Before | After |
|---|---|
| `PersistedEvent.globalOffset: Long` | `PersistedEvent.position: EventLogPosition` |
| `DomainEventPollingBackend.readEventsAfter(lastOffset: Long, limit: Int)` | `readEventsAfter(position: EventLogPosition, limit: Int)` |
| `AggregateEventOutbox(…, getOffset: () -> Long, saveOffset: (Long) -> Unit, …)` | `AggregateEventOutbox(…, getPosition: () -> EventLogPosition, savePosition: (EventLogPosition) -> Unit, …)` |
| `PublicEventContract(…, getOffset, saveOffset, …)` | `PublicEventContract(…, getPosition, savePosition, …)` |
| `PostgresOffsetManager.getOffset(name): Long`, `saveOffset(name, Long)`, `INITIAL_OFFSET` | `getPosition(name): EventLogPosition`, `savePosition(name, EventLogPosition)`, `EventLogPosition.START` |

Renames are deliberate so every caller is updated rather than silently keeping `Long` offsets. The poller
still saves its position after each event; reaction ids stay deterministic; delivery stays at-least-once.

## Testing

Deterministic integration tests using two manually controlled JDBC transactions inserting into
`ddd_domain_event`:

1. **Classic gap:** A inserts (offset 1) and stays open; B inserts (offset 2) and commits; a poll returns
   nothing; A commits; the next poll returns A's event then B's. (Current code returns offset 2 first and
   never sees offset 1.)
2. **Later transaction id, lower offset:** A and B start and are assigned ids (A first); B inserts (offset
   1); A inserts (offset 2) and commits; a poll returns A's event; B commits; the next poll returns B's
   event.
3. **Rolled-back transactions don't block:** a rolled-back insert leaves an offset gap; polling continues
   past it.
4. **End to end:** with the classic-gap interleaving, `AggregateEventOutbox` dispatches a reaction for
   every event.

Plus: `PostgresOffsetManager` round-trips positions; existing unit and integration tests updated to the
new names.

## Documentation

- Remove the README's "Known limitation: concurrent writes can cause missed events"; restore the
  no-skipping wording in the outbox guide and "Running in production".
- Requirements: PostgreSQL 13+.
- "Running in production": a long-running transaction delays, but never loses, later events.
- Quickstart and guides use `getPosition` / `savePosition`; snippets stay compiled and checked.
- KDoc for `EventLogPosition`, the read rule, and the changed parameters.

## Migration

`DddSchema.ddl` is updated in place; kotmod is unpublished, so no upgrade tooling is provided. For
reference, an existing database would need:

```sql
ALTER TABLE ddd_domain_event ADD COLUMN transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id();
CREATE INDEX idx_ddd_domain_event_position ON ddd_domain_event (transaction_id, global_offset);
ALTER TABLE ddd_consumer_offset ADD COLUMN last_transaction_id BIGINT NOT NULL DEFAULT 0;
```

(Existing rows all receive the migrating transaction's id, which sorts them before any new event;
consumers keep their saved offsets with transaction id 0.)

Note: with `last_transaction_id = 0` and an existing `last_offset`, the first read after migration uses
position `(0, last_offset)`; every pre-existing event has the migration's transaction id (> 0), so all of
them would be re-read once. That is acceptable under at-least-once delivery; to avoid it, set
`last_transaction_id` to the migration transaction's id for consumers that were fully caught up.

## Out of scope

- Per-aggregate sequence numbers and per-aggregate ordering (the next piece of work).
- Upgrade tooling for existing databases.

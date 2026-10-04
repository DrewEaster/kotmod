# Per-aggregate ordered event reactions

**Date:** 2026-10-04
**Status:** Approved design, pending implementation plan
**Builds on:** `2026-10-04-skipped-events-fix-design.md` (transaction-aware event log positions)

## Goal

Let a subscription — an `AggregateEventOutbox` or a `PublicEventContract` subscription — choose to have its
reactions executed **strictly in each aggregate's history order**: reactions for the same aggregate run one
at a time, in the order of the events that caused them, in every case (including multi-aggregate outer
transactions). Unordered subscriptions keep today's behaviour. The reaction API carries ordering
information so that any sink/source can honour it; kotmod implements it for db-scheduler, and a future
Pub/Sub sink can map it to native ordering keys.

## Decisions

- **Strict, always:** ordering is by true per-aggregate history, not log read order.
- **Per-aggregate sequence numbers** are added to events (they do not exist today).
- **Approach A — ordered at the source:** the shared poller delivers each aggregate's events in sequence
  order; ordered sinks only need per-key FIFO, one at a time.
- **Opt-in per subscription with a flag;** the ordering key is derived from the source event's aggregate.
- **Give-up policy per subscription:** continue with the next reaction, or block the aggregate.
- **db-scheduler mechanism: check on pickup** (not a per-key queue): an ordered reaction runs only when no
  earlier reaction for the same aggregate is pending; otherwise it waits and re-checks.
- Pub/Sub sink: out of scope; the API is designed so it only needs to map the ordering key.

## Why ordering can break today

`aggregate_version` counts commands, not events, and nothing records an event's place in its aggregate's
history. The outbox reads in `(transaction_id, global_offset)` order, which matches per-aggregate history for
single-aggregate commands (their first write locks the aggregate row, so later writers get later
transaction ids). It can differ in one case: an outer `jdbc.transaction { }` that wrote something else
first (getting an early transaction id), and then — after another transaction changed and committed
aggregate A — writes A. Its later A event carries the earlier transaction id and is read first.
Independently, db-scheduler runs reactions in parallel and retries reorder them.

## 1. Per-aggregate sequence numbers

**Schema** (in `DddSchema.ddl`):

```sql
-- ddd_aggregate_root
last_sequence      BIGINT NOT NULL            -- events written so far for this aggregate
-- ddd_domain_event
aggregate_sequence BIGINT NOT NULL            -- this event's number within its aggregate (1, 2, 3, …)
CREATE UNIQUE INDEX idx_ddd_domain_event_sequence
    ON ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence);
```

**Assignment:** the existing version-checked statement in `saveMeta` also advances the sequence and returns
it, under the same row lock:

```sql
UPDATE ddd_aggregate_root
SET aggregate_version = ?, last_sequence = last_sequence + ?, updated_at = ?
WHERE aggregate_type = ? AND aggregate_id = ? AND aggregate_version = ?
RETURNING last_sequence
```

(create: `INSERT … last_sequence = <event count> … RETURNING last_sequence`). A command emitting `n` events
that receives `L` numbers them `L-n+1 … L` in emission order.

**Guarantees:** contiguous per aggregate (rolled-back commands roll back their increment); unique (row lock
plus unique index); equal to true history order in every case, including the outer-transaction inversion.

**API:**
- `EventMetadata` gains `sequence: Long` (so `PersistedEvent`, `PublicEventEnvelope` and reaction mappings see
  it).
- `DomainPersistenceBackend.saveMeta(type, id, expectedVersion, eventCount: Int): Long` returns the aggregate's
  new `last_sequence`.
- `AggregateManager` and `EventProducer` build event metadata **inside** the write transaction, after
  `saveMeta`, so each event gets its sequence. `PendingEvent` metadata therefore includes the sequence.
- The polling backend reads `aggregate_sequence` into `EventMetadata.sequence`.

## 2. Source-side ordering in the shared poller

The `DomainEventPoller` (behind every outbox and contract) delivers each aggregate's events in sequence
order. For each event `e` read (in position order), before handling it:

1. **Already handled early?** If an event of the same aggregate with a **higher** sequence is at or before
   the saved position, `e` was pulled forward earlier: skip it and advance the position.
2. **Earlier events still ahead?** Find events of the same aggregate with a **lower** sequence whose position
   is after the saved position (committed, but later in the log) **and** whose sequence is above the highest
   sequence of the aggregate already at or before the saved position (anything at or below it was already
   handled, and pulling it forward again would re-deliver it after a newer event). If any is not yet readable (its
   transaction id is not below `pg_snapshot_xmin`), stop the batch and retry next poll. Otherwise handle them
   first, in sequence order, then `e`, then save `e`'s position.

Both checks are one indexed query on `(aggregate_type, aggregate_id, aggregate_sequence)`:

```sql
SELECT <event columns>, e.transaction_id < pg_snapshot_xmin(pg_current_snapshot()) AS readable, hp.highest_passed
FROM (SELECT max(aggregate_sequence) AS highest_passed FROM ddd_domain_event
      WHERE aggregate_type = ? AND aggregate_id = ? AND (transaction_id, global_offset) <= (?::text::xid8, ?)) hp
LEFT JOIN ddd_domain_event e
  ON e.aggregate_type = ? AND e.aggregate_id = ? AND e.aggregate_sequence < ?
 AND (e.transaction_id, e.global_offset) > (?::text::xid8, ?)
ORDER BY e.aggregate_sequence
```

It is one statement per event and returns only the earlier events still ahead (normally none, so one row
of NULLs) plus the scalar `highest_passed`. Its cost can grow with the aggregate's history (e.g. on replay,
when the saved position is far behind the aggregate's newest events), so it only runs for aggregates that have
had an out-of-order write. `ddd_aggregate_root` gains `last_transaction_id XID8 NOT NULL DEFAULT
pg_current_xact_id()` and `has_out_of_order_events BOOLEAN NOT NULL DEFAULT FALSE`; `saveMeta` sets
`last_transaction_id = pg_current_xact_id()` and, on update, `has_out_of_order_events = has_out_of_order_events
OR pg_current_xact_id() < last_transaction_id`. Under the row lock the last writer has committed, and while the
flag is false writers' ids rise with the sequence, so an inversion can only come from a writer whose id is below
the last writer's: any event that needs more than `InOrder` has its flagging write committed by the time the
poller checks it. The check therefore first reads the flag (a primary-key lookup) and returns `InOrder` when it
is false or the row is missing; the full query runs only for flagged aggregates, whose replay cost grows with
their history. Both checks derive from the event log, the flag and the saved position, so restarts are correct,
and a crash mid-way only repeats a dispatch (at-least-once). This runs for every subscription (cheap for
normal aggregates, harmless for unordered ones).

`DomainEventPollingBackend` gains the operation needed for this check (e.g.
`outOfOrderNeighbours(event, position): List<PersistedEvent>` with a readability flag); the plan fixes the
exact signature.

## 3. Ordering in the reaction API

```kotlin
sealed interface ReactionOrdering {
    data object Unordered : ReactionOrdering
    data class PerAggregate(val onGiveUp: OnGiveUp = OnGiveUp.ContinueWithNext) : ReactionOrdering
}
enum class OnGiveUp { ContinueWithNext, BlockAggregate }

data class DispatchOrdering(
    val key: String,          // "<aggregateType>/<aggregateId>" of the source event
    val sequence: Long,       // the source event's aggregate sequence
    val ordinal: Int,         // position among that event's reactions for this subscription
    val onGiveUp: OnGiveUp,
)
```

- `AggregateEventOutbox(…, ordering: ReactionOrdering = Unordered)`;
  `PublicEventContract.subscribe(executor, ordering: ReactionOrdering = Unordered) { … }`.
- For ordered subscriptions kotmod stamps each dispatched reaction with a `DispatchOrdering`; users'
  `eventToReactions` / `subscribe` blocks are unchanged.
- `EventReactionExecutor.dispatch(id, trigger, ordering: DispatchOrdering? = null)` →
  `EventReactionTriggerSink.publish(id, trigger, ordering: DispatchOrdering?)`.
- `EventReactionTriggerSink` declares `val supportsOrdering: Boolean`. An ordered outbox or subscription whose
  executor's sink does not support ordering throws `IllegalArgumentException` at construction / `subscribe`.
- The source callback returns `ReactionOutcome` instead of `RetrySignal.Retry?`:
  `Retry(delay: Duration)` or `Finished(gaveUp: Boolean)`. `EventReactionExecutor` produces it: completed →
  `Finished(false)`; cancelled → `Finished(false)`; `DoNotRetry` → `Finished(gaveUp = completion is
  EventReactionFailed)`; retries → `Retry`.

## 4. db-scheduler: check on pickup

Ordered reactions remain ordinary db-scheduler task instances of the existing task. Changes in
`kotmod-db-scheduler`:

- **Sortable instance id** for ordered reactions:
  `<len(key)>:<key>#<sequence, 19-digit zero-padded>#<ordinal, 4-digit zero-padded>#<reaction id>`.
  The length prefix makes key ranges unambiguous whatever characters ids contain. The mapping from reaction
  to instance id is deterministic, so duplicate dispatches are still absorbed by `scheduleIfNotExists`.
  Unordered reactions keep `reaction id` as the instance id.
- **Stored data** (`ReactionTaskData`) gains the ordering stamp (key, sequence, ordinal, onGiveUp) and a
  `blocked` flag; unordered data is unchanged.
- **Check on pickup:** before running an ordered reaction:

  ```sql
  SELECT EXISTS (SELECT 1 FROM scheduled_tasks
                 WHERE task_name = ? AND task_instance > '<len>:<key>#' AND task_instance < '<own id>')
  ```

  If an earlier reaction for the same aggregate is pending, reschedule after `orderedRecheckDelay`
  (default 2s) without touching the retry count or calling `execute`. Otherwise run as today. Rows only
  leave `scheduled_tasks` when a reaction completes, is cancelled or gives up, so at most one reaction per
  (task, aggregate) runs at a time, always the earliest.
- **Nudge:** when an ordered reaction finishes and its row is removed, reschedule the next pending reaction
  of the same aggregate (same range query) to now, so backlogs run back to back. Failure to nudge is harmless
  (the next reaction re-checks on its own).
- **Give up with `BlockAggregate`:** after `onCompletion` is told it failed, the row is rescheduled far into
  the future with `blocked = true`; later reactions keep waiting.
- **Operational helpers** on `DbSchedulerEventReactions`:
  `blockedReactions(client): List<BlockedReaction>` (key, reaction id, blocked at),
  `retryBlocked(client, id)` (reschedule now, retry count reset, `blocked = false`),
  `skipBlocked(client, id)` (cancel; the next reaction proceeds). A retried reaction may call `onCompletion`
  again (covered by at-least-once).
- **Settings:** `orderedRecheckDelay: Duration = 2.seconds`; optional `tableName: String = "scheduled_tasks"`
  for the check query. `supportsOrdering = true`.
- **Scope of ordering:** checks filter by `task_name`, so ordering is per (executor, aggregate). Separate
  executors never wait on each other. Ordered subscriptions of one contract sharing an executor share
  ordering for an aggregate (and a `BlockAggregate` affects both), and must use distinct reaction ids (as
  today); the contract numbers ordinals with one counter across its subscriptions, so anything dispatched
  later for an event sorts later. An ordered executor can be fed by only one source (one outbox or one
  contract): `EventReactionExecutor` rejects a second source when it is constructed or subscribes. Executors
  sharing one `DbSchedulerEventReactions` task name would share ordering across sources and are not
  supported; each ordered source needs its own task name.
- Existing behaviour retained: unsubscribed safety net, undecodable-data backoff, retry counts in task data,
  fresh execution id per attempt.

## Testing

- **Sequence numbers:** contiguous from 1 per aggregate; multi-event commands numbered in emission order;
  `EventProducer` numbered; concurrent commands on one aggregate never share a number; the outer-transaction
  inversion gets true-history numbers.
- **Source ordering:** reproduce the inversion with real transactions (outer transaction writes B, another
  transaction commits A#2, the outer one writes A#3): dispatch order is A#2 then A#3 and A#2 is dispatched
  exactly once; a not-yet-readable earlier event pauses the poller, which resumes when it becomes readable;
  restarting part-way gives the same result.
- **Ordered db-scheduler (4 threads, real Postgres):** many reactions for one aggregate never overlap and run
  in sequence order; different aggregates run in parallel; a retrying head holds back later reactions;
  `ContinueWithNext` proceeds after a give-up; `BlockAggregate` parks the aggregate and
  `retryBlocked`/`skipBlocked` release it; duplicate dispatch is still absorbed; the nudge starts the next
  reaction promptly; two executors with the same aggregate do not wait on each other.
- **Wiring:** an ordered outbox/subscription with a sink that does not support ordering fails at
  construction.
- Existing tests updated for `EventMetadata.sequence`, the `saveMeta` signature, and `ReactionOutcome`.

## Documentation

README: new guide section "Ordered reactions" (when to use them, the flag, give-up policies, the helpers,
polling/latency trade-offs, executor and reaction-id scoping); `EventMetadata.sequence` in the aggregates
guide; schema table updated; KDoc for all new types.

## Migration

Schema changes in place (unpublished). For reference, existing databases would need `last_sequence`
backfilled from event counts and `aggregate_sequence` backfilled per aggregate in log order.

## Out of scope

- A Pub/Sub sink (the API is ready for one).
- A per-key queue/runner mechanism for db-scheduler (possible later optimisation for heavy per-aggregate
  backlogs).
- Ordering keys other than the source aggregate.

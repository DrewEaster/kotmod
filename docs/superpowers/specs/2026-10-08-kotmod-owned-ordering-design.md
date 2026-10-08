# kotmod-owned ordering on a plain task scheduler

**Date:** 2026-10-08
**Status:** Approved design, pending implementation plan
**Release:** 0.4.0 (breaking; kotmod has no users yet, so no compatibility, aliases or migration)

## Goal

Move per-aggregate ordering out of the queue backend and into kotmod, on a Postgres table kotmod owns, so that:

- every backend is a plain "run this at time T" scheduler behind one small interface (db-scheduler now, Cloud Tasks
  next cycle, others later);
- ordering, blocked work, parked mappings and the operator tools are written and tested once, and behave the same on
  every backend;
- the ordering gap after a recovered parked mapping (on first-in-first-out queues) disappears;
- the db-scheduler module shrinks to an adapter, losing its sortable ids, raw SQL against `scheduled_tasks`,
  wait-and-recheck rows, nudges and 100-year parked rows.

## Decisions

- **Unordered work stays in the backend.** Its attempt count travels in the task payload. No table, no extra writes.
- **Ordered work lives in `ddd_reaction_row`**, one line per (policy, aggregate). Only the front item of each line is
  ever scheduled with the backend (one task per front item, named after it). Nothing waits or rechecks.
- **Unordered parked mappings also live in `ddd_reaction_row`**, so `parkedMappings`/`skipParked` work for every
  policy on every backend. They are rare, so the cost is negligible.
- **kotmod owns attempt counts.** Backends' own retry counters are not used.
- **Ordered attempts are counted at start**, so a crash counts as a failed attempt; a graceful shutdown gives the
  attempt back. Unordered work keeps counting failures only (documented limitation).
- **Every write pair (Postgres, then backend) is made safe by redoing the whole step idempotently**: the reactor saves
  its position last; a delivery returns `Done` only after scheduling the next item. A rare repair sweep covers tasks a
  backend loses.
- **Scope:** core + db-scheduler adapter. The interface is designed for Cloud Tasks' limits (hashed names, tasks can't
  be edited, 30-day horizon); the Cloud Tasks module is the next cycle. Pub/Sub doesn't meet the interface (no
  delayed delivery, no run-again-at) and is not a target.

## 1. Layers and the scheduler interface

1. **Reactor and event policies** (app code unchanged): reading the log, `on(...)` mapping, parked mappings, `handle`,
   `onFailure`, `onCompletion`.
2. **Ordering, in kotmod core** (new): `ddd_reaction_row`, the per-aggregate lock, advancing lines, attempt counting,
   blocked items, unordered parked mappings, the repair sweep, `ReactionOperations`.
3. **Scheduler** (one adapter per backend).

```kotlin
interface TaskScheduler {
    /** The queue named [name]; called once per queue, before start. */
    fun queue(name: String): TaskQueue
}

interface TaskQueue {
    /** Runs [payload] at [at], unless a task named [name] is already pending (then does nothing). */
    suspend fun schedule(name: String, payload: String, at: Instant)

    /** Delivers due tasks, at least once, until cancelled. */
    fun subscribe(handler: suspend (name: String, payload: String) -> TaskOutcome): Cancellable
}

sealed interface TaskOutcome {
    data object Done : TaskOutcome
    data class RunAgain(val at: Instant, val payload: String) : TaskOutcome
}
```

- **Contract:** `schedule` is idempotent per name while that task is pending (a backend may also refuse a recently
  finished name, as Cloud Tasks does; kotmod never relies on reusing a finished name). Delivery is at least once, not
  before `at`. `RunAgain` makes the task run again at `at` with the new payload (db-scheduler reschedules its row; a
  backend that can't edit tasks creates a follow-up task with a derived name and acknowledges the original). An
  exception from the handler means "deliver again later" with the backend's own backoff.
- **Names:** unordered work is named by its reaction id; an ordered front item by `<policy>/<aggregateKey>/<reactionId>`;
  a parked mapping by its reaction id `<policy>/<eventId>/mapping`. Adapters may transform names (Cloud Tasks will
  hash them); kotmod treats names as opaque identities.
- **Payloads** are kotmod JSON, opaque to the backend: unordered work carries the trigger, reaction id, attempt count
  and `notBefore`; an ordered front task and a parked-mapping task carry only what identifies their row.
- **One queue per policy**, named after it, as today. Process managers use the same interface for their `inputs`,
  `internal` and `commands` channels (`<processType>-<channel>`), so their ordered input channel gets the shared
  ordering.
- **db-scheduler adapter** (`DbSchedulerTaskScheduler`): one db-scheduler task per queue, `scheduleIfNotExists`,
  `RunAgain` → reschedule the same instance with new data, handler exception → db-scheduler's failure handling with
  a capped backoff. It keeps `tasks` (register with the `Scheduler`) and `bind(client)`.

## 2. How ordered work runs

### The table

`ddd_reaction_row`, added to the shipped schema (`DddSchema`) and the README's Postgres setup:

| Column | Meaning |
|---|---|
| `policy` | Queue name (policy name or process manager channel) |
| `reaction_id` | Unique within `policy` (primary key with `policy`) |
| `kind` | `ORDERED` (an ordered item) or `PARKED` (an unordered parked mapping) |
| `aggregate_key`, `sequence`, `ordinal` | Position in the aggregate's line (ordered rows; unique per policy) |
| `item` | JSON: a trigger, or a parked mapping (event id, aggregate type/id, source) |
| `attempts` | Attempts started (`ORDERED`) or failed (`PARKED`) |
| `blocked` | The policy gave up with `BlockAggregate` |
| `lease_until` | Set while the item is running |
| `updated_at` | Last change (for the repair sweep) |

An index on `(policy, aggregate_key, sequence, ordinal)` serves "front of the line".

### The lock

Every step that changes one aggregate's line runs in a short transaction that first takes
`pg_advisory_xact_lock` on a hash of `(policy, aggregate_key)`. `handle`, `onFailure`, `onCompletion` and `on(...)`
blocks never run while the lock is held.

### The reactor, per batch

1. For each ordered policy and affected aggregate, under its lock: insert the rows (`ON CONFLICT DO NOTHING` on
   `(policy, reaction_id)`) and note the line's front item. Failing mappings are inserted as a parked-mapping row in
   the event's place (an `ORDERED` row at `(sequence, 0)` whose item is the parked mapping) for ordered policies, or
   as a `PARKED` row for unordered ones.
2. After committing: schedule each noted front item, each new `PARKED` row's task, and the unordered triggers.
3. Save the position last.

Any failure before step 3 means the batch is read again; every step repeats harmlessly.

### Delivering an ordered front task

1. **Under the lock**, load the line's current front row:
   - not this task's item (a stale or repeated schedule) → make sure the real front item is scheduled (after
     commit), `Done`;
   - blocked → `Done`;
   - `lease_until` in the future (a concurrent duplicate delivery) → `RunAgain(at = lease_until)`;
   - otherwise `attempts += 1`, `lease_until = now + policy timeout + margin`, commit.
2. **Run it.** A trigger: `handle` with the policy's timeout and failure handling; `context.attempt = attempts - 1`.
   A parked mapping: read the event again and rerun the policy's current `on(...)` block.
3. **Finish:**
   - success, or `GiveUp` with `ContinueWithNext` → `onCompletion`; then under the lock delete the row (a recovered
     parked mapping is replaced by its trigger rows at the same sequence, ordinals from 0), find the new front, commit;
     schedule the new front; `Done`;
   - `Retry(delay)` → clear the lease; `RunAgain(now + delay)`;
   - `GiveUp` with `BlockAggregate` → `onCompletion`; set `blocked`, clear the lease; `Done` (nothing scheduled);
   - a failing parked mapping → clear the lease; `RunAgain` after capped backoff, forever;
   - `onFailure` or `onCompletion` throwing → retried after a backoff, as today;
   - graceful shutdown (cancellation) → `attempts -= 1`, clear the lease, then let the cancellation through.

Because the new front is scheduled before `Done`, a crash after the delete makes the backend deliver the old task
again, which finds its row gone and schedules the current front.

### Unordered parked mappings

A `PARKED` row has its own task, named by its reaction id. Delivered: rerun the mapping (counting failures in
`attempts`); on success schedule the event's triggers as ordinary unordered work, delete the row, `Done`; on failure
`RunAgain` after capped backoff, forever. If the policy no longer listens to the event's aggregate type, the row is
deleted with a warning (as today).

### Repair sweep

In the reactor's loop, on the leader, about every 10 minutes: schedule again every unblocked front item and every
parked-mapping row whose `updated_at` is older than the longest retry backoff plus a margin and whose lease has
expired. Scheduling is idempotent, so the sweep only matters when a backend lost a task.

### Operator tools

`ReactionOperations(jdbc, scheduler)` in core (usable from an admin endpoint without a running reactor):
`blockedReactions(policy)`, `retryBlocked(policy, reactionId)` (unblock, reset attempts, schedule),
`skipBlocked(policy, reactionId)` (delete, advance the line, schedule the new front), `parkedMappings(policy)`,
`skipParked(policy, eventId)` (delete; for ordered policies advance the line). All take the per-aggregate lock.

## 3. Public API, errors, testing, docs

### API changes (0.4.0)

- **Removed:** `ReactionQueues`, `ReactionChannel`, `EventReactionTriggerSink`/`Source`/`Serializer`,
  `EventReactionTrigger`, `DispatchOrdering`, `ReactionOutcome` and the rest of the old queue interface;
  `DbSchedulerQueues` and its helpers; the deprecated `Reactions`/`ReactionsDsl` aliases.
- **Added:** `TaskScheduler`, `TaskQueue`, `TaskOutcome`, `ReactionOperations`, `DbSchedulerTaskScheduler`.
- **Changed:** `EventReactor(jdbc, scheduler, isLeader, name = "reactor", …)` and `ProcessManager` take a
  `TaskScheduler`.

### Errors

| Failure | Behaviour |
|---|---|
| Reactor's schedule call fails | Position not saved; batch read again, idempotently |
| Crash after a row change, before scheduling | Backend delivers the old task again; it schedules the current front |
| Backend loses a task | Repair sweep schedules it again |
| Same task delivered twice at once | Lease turns the second delivery into `RunAgain` |
| Crash mid-run | Attempt already counted; runs again after the lease expires |
| Lock held by another step | Waits briefly; locked steps are short |

### Testing

- **Scheduler contract suite** every backend runs: idempotent `schedule` per name, `RunAgain` with new payload, at
  least once delivery, not before `at`, handler exception redelivers. db-scheduler runs it now.
- **In-memory test scheduler** for unit tests: duplicate delivery, crash injection between any two steps, losing tasks,
  strict first-in-first-out delivery.
- **Postgres scenarios:** reactor insert racing a finishing front item (both orders); concurrent duplicate delivery
  hitting the lease; crash after delete and before schedule; crash mid-run counts an attempt; graceful shutdown gives
  it back; a recovered parked mapping running before later work on the strict FIFO scheduler (the gap is closed);
  `BlockAggregate` with every operator tool; the sweep recovering a lost task; unordered parked mappings (park,
  recover, skip); process manager ordered inputs.
- **Existing end-to-end scenarios** rerun on db-scheduler.

### Docs

- "Using another queue" becomes "Using another scheduler": the interface, its contract and the contract suite; the
  Pub/Sub sketch is removed with a note on why Pub/Sub doesn't qualify; Cloud Tasks is mentioned as planned.
- "Ordered event policies" explains each aggregate's line and the table; the operator tools section uses
  `ReactionOperations`.
- Known limitations: drop the parked-mapping ordering gap; add "crashes don't count as attempts for unordered work".
- Short "Upgrading from 0.3" notes.

## Out of scope

- The Cloud Tasks module (next cycle; it must pass the contract suite).
- A delivery-count safety limit for unordered crash loops.
- Keeping all work (including unordered) in kotmod's table.

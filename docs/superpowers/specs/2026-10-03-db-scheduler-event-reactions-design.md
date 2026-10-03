# db-scheduler backed event reactions

**Date:** 2026-10-03
**Status:** Approved design, pending implementation plan

## Goal

Provide a concrete, durable implementation of `EventReactionTriggerSink` / `EventReactionTriggerSource`
backed by [db-scheduler](https://github.com/kagkarlsson/db-scheduler) 16.12.0, so that event reactions
dispatched by `AggregateEventOutbox` and `PublicEventContract` are persisted in Postgres and executed
(with retries) by `EventReactionExecutor`.

## Constraints and decisions

- **The app owns the `Scheduler`.** It creates, configures (threads, polling, serializer), starts and stops it.
  The library never creates or starts a scheduler. All logic that talks to db-scheduler lives in the library.
- **The library is domain-agnostic.** Trigger types, serializers, task names and reaction ids are supplied by the app.
- **One db-scheduler task name per `EventReactionExecutor`.** Each reaction is a task *instance* whose id is the
  `EventReactionId`. An app with several executors registers several tasks on the same `Scheduler`.
- **Retries are decided by our layer, executed by db-scheduler.** `RetrySignal.Retry(delay)` from the executor
  becomes a db-scheduler reschedule; db-scheduler never decides retry delays for reactions itself.
- **Permanently failed reactions are not stored by the library.** The row is removed; the app's `onCompletion`
  receives `EventReactionFailed` and decides whether to record or alert.
- **Delivery is at-least-once.** db-scheduler deletes completed rows, so a re-dispatch after completion (e.g. outbox
  crash between dispatch and offset save) runs the reaction again. `execute` and `onCompletion` must be idempotent.
  No dispatch log is reintroduced.
- **`scheduled_tasks` is the app's table**, not part of `DddSchema`. Apps create it from db-scheduler's own DDL.
- **Lifecycle order:** executors start before `scheduler.start()` and stop after `scheduler.stop()`. A safety net
  covers misordering (see Error handling).

## Components

All new code lives in `com.dreweaster.ddd.event.reaction.dbscheduler`. `com.github.kagkarlsson:db-scheduler:16.12.0`
is added as an `api` dependency because its `Task` and `SchedulerClient` types appear in public signatures.

### `DbSchedulerEventReactions<T : EventReactionTrigger>` (public)

```kotlin
class DbSchedulerEventReactions<T : EventReactionTrigger>(
    taskName: String,
    triggerSerializer: EventReactionTriggerSerializer<T>,
    unsubscribedRetryDelay: Duration = 5.seconds,
) {
    val task: Task<String>                                   // registered by the app when building its Scheduler
    val source: EventReactionTriggerSource<T>
    fun sink(client: SchedulerClient): EventReactionTriggerSink<T>
}
```

`sink` takes a `SchedulerClient` (which `Scheduler` implements) because `task` must exist before the scheduler is built.

### Internal pieces

- **`ReactionTaskData`** — stored payload `{"trigger": "<app-serialized trigger>", "retryCount": <int>}`, encoded to and
  from a `String` with kotlinx-serialization. db-scheduler only ever stores a `String`, so any app-configured
  db-scheduler serializer (Java serialization, Jackson, Gson) works.
- **`DbSchedulerTriggerSink`** — serializes the trigger, wraps it with `retryCount = 0`, and calls
  `client.scheduleIfNotExists(TaskInstance(taskName, id.value, data), now)` on `Dispatchers.IO`.
- **`DbSchedulerTriggerSource`** — holds the subscribed handler in a `@Volatile` field. `subscribe()` stores it and
  throws `IllegalStateException` if one is already set. The returned `Cancellable` clears it.
- **Custom task** — built with `Tasks.custom(taskName, String::class.java)` with an exponential-backoff
  `FailureHandler` (initial 10s, capped at 1h, unlimited attempts). Its execute handler bridges into coroutines with
  `runBlocking` and returns a `CompletionHandler` derived from the outcome logic below.
- **Outcome logic** — a small pure internal function mapping (subscribed?, handler result, now, data) to one of:
  remove; reschedule at time with data. Unit-tested without db-scheduler.

### Example wiring (app code)

```kotlin
val billingReactions = DbSchedulerEventReactions("billing-reactions", BillingTriggerSerializer)
val notificationReactions = DbSchedulerEventReactions("notification-reactions", NotificationTriggerSerializer)

val scheduler = Scheduler.create(dataSource, billingReactions.task, notificationReactions.task).threads(10).build()

val billingExecutor = EventReactionExecutor(
    sink = billingReactions.sink(scheduler),
    source = billingReactions.source,
    /* createExecutionContext, execute, failureRetryHandler, timeoutRetryHandler, onCompletion */
)
// notificationExecutor likewise

billingExecutor.start(); notificationExecutor.start()
scheduler.start()
// ... outbox / contract start
// shutdown: pollers stop, scheduler.stop(), then executors stop
```

### Reaction ids

The task instance id is exactly `EventReactionId.value`, chosen by the app in `eventToReactions` (outbox) or a
`subscribe` block (contract). Ids must be **deterministic per (event, reaction kind)** — typically built from
`event.metadata.eventId` plus a reaction label, e.g. `"charge-${eventId}"` — so that a re-dispatch of the same event
is absorbed by `scheduleIfNotExists` while the original is pending. KDoc on `EventReaction` and
`DbSchedulerEventReactions` states this rule.

### Execution ids

db-scheduler has no per-attempt id. Each execution gets a fresh `EventReactionExecutionId` from the library's existing
`randomId()` (nanoid), unique across retries, dead-execution revivals and post-completion re-dispatches. It is logged
alongside task name and instance id for correlation.

### Targeted fix: `BackoffStrategy`

`calculateBackoff` currently computes `1L shl retryCount.coerceAtMost(maxSeconds)`, which overflows and never caps.
It is fixed to double from 1s and cap the result at `maximumDuration`.

### Unchanged

`EventReactionExecutor`, the sink/source interfaces, `AggregateEventOutbox`, `PublicEventContract`.

## Data flow

### Dispatch

1. The poller maps an event to reactions and calls `executor.dispatch(id, trigger)` → `sink.publish(id, trigger)`.
2. The sink serializes the trigger, wraps it as `ReactionTaskData(trigger, retryCount = 0)`, encodes it.
3. `scheduleIfNotExists(TaskInstance(taskName, id.value, data), now)` on `Dispatchers.IO`. An existing row makes this
   a no-op (debug-logged).
4. The poller saves its offset after all of the event's reactions are dispatched (existing behaviour).

### Execution

1. A db-scheduler worker picks up the row and calls the custom task's execute handler.
2. No subscribed handler → warn, reschedule at `now + unsubscribedRetryDelay` with data unchanged.
3. Otherwise, inside `runBlocking`: decode `ReactionTaskData`, deserialize the trigger, generate an execution id, call
   `handler(EventReactionId(instanceId), executionId, trigger, retryCount)`.
4. The handler is `EventReactionExecutor`'s subscribe block (timeout, `execute`, completion and retry handlers) and
   returns `RetrySignal.Retry?`.

### Outcome

| Handler returns | Completion handler | Row |
|---|---|---|
| `null` (completed, cancelled, or `DoNotRetry` — executor has already called `onCompletion`) | `ops.remove()` | deleted |
| `Retry(delay)` | `ops.reschedule(complete, now + delay, data.copy(retryCount = retryCount + 1))` | same row, new time and count |

db-scheduler guarantees a row is executed by one node at a time; no extra locking is needed.

## Error handling

1. **Undecodable task data or trigger** — the task throws; the exponential-backoff `FailureHandler` retries
   indefinitely (10s → 1h cap) with an error log including task name and instance id. Rows recover once a fix is
   deployed; operators can remove a row with `SchedulerClient.cancel(...)`.
2. **Handler throws** (notably `createExecutionContext`, which `EventReactionExecutor` calls outside its
   `runCatching`) — same failure path; `retryCount` is not incremented, db-scheduler's `consecutiveFailures` is.
   The executor is not changed as part of this work.
3. **Crash or forced shutdown mid-execution** — the row becomes a dead execution and db-scheduler's default
   dead-execution handler revives it with the same `retryCount`. Graceful `scheduler.stop()` waits for running
   executions first; executions interrupted at the deadline cancel their `runBlocking`. The executor sees the
   resulting `CancellationException` as an `EventReactionFailed` and calls `failureRetryHandler` (which should
   return `Retry` for it — documented on `DbSchedulerEventReactions`); the interrupted `runBlocking` then throws,
   so db-scheduler's failure handler reschedules the row with the same `retryCount`.
4. **Failure in `remove` / `reschedule`** — the row is left picked, becomes a dead execution and is re-run;
   `onCompletion` may run more than once (covered by the idempotency rule).
5. **`publish` failure** (`scheduleIfNotExists` throws) — propagates to the poller, which halts the batch without
   saving the offset and retries next tick (existing behaviour).
6. **Threading** — handlers run in `runBlocking` on db-scheduler worker threads, so `Scheduler.threads(n)` bounds
   reaction concurrency. `execute` may switch dispatchers itself. Long executions are kept alive by db-scheduler
   heartbeats. The per-trigger timeout remains the executor's `withTimeout`.
7. **Unknown task names** (e.g. an executor removed from the app) — db-scheduler logs them as unresolved; the library
   does nothing.

## Testing

db-scheduler's Postgres DDL (`postgresql_tables.sql` for 16.12.0) is copied into `src/integrationTest/resources` and
applied by the `IntegrationTest` base class alongside `DddSchema`; `scheduled_tasks` is truncated before each test.

### Unit tests

- `BackoffStrategy` doubles and caps at `maximumDuration` for large retry counts.
- `ReactionTaskData` encode/decode round-trips, including quotes and newlines in the trigger.
- Outcome logic: `null` → remove; `Retry(d)` → reschedule at `now + d` with `retryCount + 1`; no subscriber →
  reschedule at `now + unsubscribedRetryDelay` with data unchanged.
- Sink calls `scheduleIfNotExists` with the expected task name, instance id, data and time (mocked `SchedulerClient`).
- Source: second `subscribe` throws; `cancel()` clears the handler.

### Integration tests (Testcontainer + real `Scheduler` with fast polling)

1. Happy path: dispatch → executes once → `onCompletion(EventReactionCompleted)` → row gone.
2. Duplicate dispatch while pending → one row, one execution.
3. Retry: execute fails twice then succeeds; handlers see `retryCount` 0, 1, 2 and distinct execution ids; row removed.
4. Give up: `DoNotRetry(EventReactionFailed)` → `onCompletion` receives the failure → row removed.
5. Startup-order safety net: scheduler started before the executor → reaction rescheduled with unchanged retry count
   → runs after `executor.start()`.
6. Undecodable data: garbage task data → row remains with `consecutiveFailures > 0`; executor never invoked.
7. Two executors on one scheduler: each reaction reaches only its own executor.
8. End to end: events appended via `PostgresDomainPersistenceBackend` → `AggregateEventOutbox` with
   `PostgresOffsetManager` and the db-scheduler sink → reactions executed → offset saved.

Existing outbox and contract integration tests keep their `recordingExecutor` stub.

## Out of scope

- Storing permanently failed reactions or a manual-retry API.
- A dispatch/dedup log beyond `scheduleIfNotExists`.
- Changes to `EventReactionExecutor` (including the `createExecutionContext` exception gap).
- Adding db-scheduler's table to `DddSchema`.

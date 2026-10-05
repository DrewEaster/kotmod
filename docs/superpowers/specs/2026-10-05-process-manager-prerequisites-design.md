# Process manager prerequisites: aggregate kinds and delayed reactions

**Date:** 2026-10-05
**Status:** Approved design, pending implementation plan
**Part of:** process managers, cycle 1 of 2 (cycle 2: `2026-10-05-process-managers-design.md`)

## Goal

Two core changes that process managers build on, each useful on its own:

1. **Aggregate kinds.** An `AggregateKind<C, R>` object names an aggregate type together with its command and
   rejection serializers. `AggregateManager` is built from it. Process managers will use kinds to address commands
   to other aggregates in a type-safe way (cycle 2).
2. **Delayed reactions.** A reaction can be dispatched with `notBefore`, so it doesn't run before a given time
   ("send a review reminder 7 days after delivery"). Process managers will use this for timeouts (cycle 2).

Both land in 0.2.0, which is not yet published.

## Decisions

- `AggregateManager(kind, repository, backend, initial, maxConflictRetries = 5)` replaces the `aggregateType` and
  `rejectionSerializer` constructor parameters. Every aggregate gets a kind object: one way to build an aggregate.
- The kind carries a command serializer now, although only process managers use it, so kinds don't change again in
  cycle 2. `AggregateKind.command(...)` (building a requested command) is cycle 2 and not part of this spec.
- `notBefore` is honoured natively by queues that can schedule (db-scheduler), and enforced for every queue by a
  guard in the executor: a reaction delivered early is put back without running and without using up a retry.
- `notBefore` combined with an ordering stamp is refused: a delayed reaction in an ordered lane would hold back
  every later reaction of its aggregate.

## 1. Aggregate kinds

```kotlin
// kotmod
open class AggregateKind<C : Any, R : Any>(
    val type: AggregateType,
    val commandSerializer: KSerializer<C>,
    val rejectionSerializer: KSerializer<R>,
)

class AggregateManager<S : AggregateState<S, C, E, R>, C : Any, E : DomainEvent, R : Any>(
    kind: AggregateKind<C, R>,
    repository: Repository<S>,
    backend: DomainPersistenceBackend<E>,
    initial: InitialState<S, C, E, R>,
    maxConflictRetries: Int = 5,
)
```

```kotlin
// the app
object Orders : AggregateKind<OrderCommand, OrderRejection>(
    type = AggregateType("Order"),
    commandSerializer = OrderCommand.serializer(),
    rejectionSerializer = OrderRejection.serializer(),
)

val orders = AggregateManager(Orders, OrderRepository(jdbc), PostgresDomainPersistenceBackend(jdbc, serialization), initial = NoOrder)
```

`AggregateManager` uses `kind.type` wherever it used `aggregateType`, and `kind.rejectionSerializer` wherever it used
`rejectionSerializer`. Behaviour is otherwise unchanged. Code that compares an event's aggregate type (for example the
quickstart outbox's `event.metadata.aggregateType != orderType`) can use `Orders.type`.

## 2. Delayed reactions

### API

- `EventReaction(id, trigger, notBefore: Instant? = null)`: an outbox's `eventToReactions` or a contract
  subscription can ask for a delay. (`kotlin.time.Instant`, as elsewhere in kotmod.)
- `EventReactionExecutor.dispatch(id, trigger, ordering = null, notBefore = null)`.
- `EventReactionTriggerSink.publish(id, trigger, ordering, notBefore: Instant?)`: the sink should not deliver the
  reaction before `notBefore`, if it can.
- `EventReactionTriggerSource.subscribe(block)`: `block` gains a fifth parameter, the reaction's `notBefore`
  (`null` if none), which the source must carry from publish to delivery.
- New `ReactionOutcome.Wait(delay: Duration)`: deliver again after about `delay`, **without** incrementing the retry
  count. Sources must handle it.

### Rules

- `dispatch` with both an ordering stamp and `notBefore` throws `IllegalArgumentException` ("delayed reactions can't
  be ordered"). An ordered outbox or contract subscription whose `eventToReactions`/block returns a reaction with
  `notBefore` therefore fails its batch and retries; the message names the reaction. Documented.
- **Executor guard:** when a delivery's `notBefore` is in the future, the executor returns
  `ReactionOutcome.Wait(notBefore - now)` without creating an execution context or calling `execute`. A delivery at or
  after `notBefore` runs normally.
- Publishing a reaction id that is already pending still does nothing (including when the second publish has a
  different `notBefore`).

### db-scheduler

- The sink schedules the task instance at `notBefore` (or now, when `null`), so the reaction sleeps in
  `scheduled_tasks` with no polling cost.
- `ReactionTaskData` stores `notBefore` (nullable; old rows without it decode as `null`), and the source passes it to
  `block`.
- `Wait(delay)` reschedules the row after `delay` (capped like retries) with the retry count unchanged.

### Other queues

The README's "Using another queue" section and its Pub/Sub sketch are updated:
- a sink should carry `notBefore` with the message and the source pass it back to `block`;
- a queue that can't delay delivery still works, because the guard puts early deliveries back (at the cost of
  repeated deliveries until the time is reached);
- for Google Cloud, the sink can hand delayed reactions to **Cloud Tasks** with a schedule time and have it publish to
  Pub/Sub when due; Cloud Tasks limits how far ahead a task can be scheduled (about 30 days), and the guard covers
  longer delays because an early delivery is simply scheduled again;
- the source must handle `ReactionOutcome.Wait` (nack/redeliver after the delay without counting a retry).

## Testing

- **Unit:** `AggregateManager` built from a kind uses its type and rejection serializer (existing tests, new
  construction); the executor guard returns `Wait` for a future `notBefore` without calling `createExecutionContext` or
  `execute`, and runs normally once due; `dispatch` refuses ordering plus `notBefore`; `EventReaction` carries
  `notBefore` through the outbox and contract subscription to `dispatch`; db-scheduler `TaskRowOutcome` maps `Wait` to
  a reschedule with an unchanged retry count; `ReactionTaskData` round-trips `notBefore` and decodes old data without it.
- **Integration (db-scheduler + Postgres):** a reaction dispatched with `notBefore` a few seconds ahead does not run
  before it and runs after it; its retry count is still 0 when it runs; a duplicate dispatch of a pending delayed
  reaction is ignored.
- **Examples:** the quickstart and guides compile with `Orders`.

## Documentation

- Quickstart step 3 and every `AggregateManager(...)` snippet use a kind object; step 2 or 3 introduces `Orders`
  briefly ("names the aggregate type and how its commands and rejections are serialized").
- "The outbox and event reactions": a short **Delayed reactions** subsection with a review-reminder example
  (`EventReaction(..., notBefore = deliveredAt + 7.days)`), the ordering restriction, and how the guard behaves.
- "Using another queue": the updated sink/source contract (fifth `block` parameter, `ReactionOutcome.Wait`) and the
  Pub/Sub + Cloud Tasks note.
- "Upgrading from 0.1.0": build `AggregateManager` from a kind; custom sinks add the `notBefore` parameter, custom
  sources pass `notBefore` to `block` and handle `ReactionOutcome.Wait`.

## Out of scope

- `AggregateKind.command(...)`, requested commands and everything else about process managers (cycle 2).
- A ready-made Pub/Sub or Cloud Tasks module.

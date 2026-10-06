# Use-case reactions

**Date:** 2026-10-06
**Status:** Approved design, pending implementation plan
**Release:** 0.3.0 (breaking; 0.2.0 is published)

## Goal

Make event reactions application code, not infrastructure configuration. Today an app writes its reaction logic as an
`execute` lambda (usually a central `when` over every trigger) passed to `EventReactionExecutor`, plus separate
failure/timeout/completion lambdas, plus an outbox's `eventToReactions` that filters and deserializes raw events and
builds reaction ids by hand. Instead, the app writes **use cases**: each one says which events it reacts to (typed, from
one or more sources), which triggers they produce, and how those triggers are handled, with its own retry policy. A
single runtime per context, the **reactor**, reads the event log once and runs every registered use case on one shared
queue infrastructure.

## Decisions

- **A use case owns the whole reaction:** the events it reacts to, the triggers they produce, and the handling.
- **Typed sources, one or more per use case**, registered with `on(source) { event, metadata -> … }`: a source is either
  one of this context's aggregate kinds (typed domain events) or another context's `PublicEventContract` (typed public
  events). One source is the common case; several are allowed.
- **Ordering is per aggregate instance only** (an aggregate instance's events are handled one at a time, in sequence
  order); nothing across aggregates or sources. A use case may not listen to the same aggregate type through two sources.
- **Retries, timeout and completion live on the use case**, with defaults (capped exponential backoff, never give up;
  60-second timeout; completion does nothing).
- **`AggregateKind<C, E, R>` carries its event serialization**, so `on(Orders)` delivers typed events.
- **Use cases replace today's reaction API** (one way to write a reaction): the executor, outbox and contract
  subscription become kotmod internals; the sink/source interfaces stay public for other queues.
- **One reader per context, one scheduler, one queue (task) per use case** — so each use case has its own ordering,
  timeout and failure policy, and a slow or failing use case never holds up another's handling.
- **A failing mapping is parked, not allowed to stall the reader:** if one use case's `on(...)` block throws for an
  event, that event is parked in that use case's own queue (carrying the event's ordering stamp), and the reader moves on.

## 1. The use case

```kotlin
@Serializable sealed interface FraudCheck
@Serializable data class CheckOrder(val orderId: String, val total: Long) : FraudCheck
@Serializable data class FlagCustomer(val customerId: String, val reason: String) : FraudCheck

class FraudChecks(private val fraud: FraudService) : Reactions<FraudCheck>(
    name = "fraud-checks",
    triggers = FraudCheck.serializer(),
) {
    init {
        on(Orders) { event, metadata ->
            when (event) {
                is OrderPlaced -> trigger(CheckOrder(metadata.aggregateId.value, event.total))
                else -> Unit
            }
        }
        on(payments) { event, _ ->
            if (event is PaymentDeclined) trigger(FlagCustomer(event.customerId, "declined"))
        }
    }

    override suspend fun handle(trigger: FraudCheck, context: ReactionContext) =
        when (trigger) {
            is CheckOrder -> fraud.score(trigger.orderId, trigger.total, idempotencyKey = context.reactionId)
            is FlagCustomer -> fraud.flag(trigger.customerId, trigger.reason)
        }

    override val ordering = ReactionOrdering.PerAggregate()
    override fun onFailure(trigger: FraudCheck, attempt: Int, error: Throwable) =
        if (attempt < 5) Retry(backoff(attempt)) else GiveUp
}
```

- `abstract class Reactions<T : Any>(val name: String, val triggers: KSerializer<T>)` (package `io.kotmod.reaction`).
  `name` is stable across restarts: it names the use case's queue.
- `on(kind: AggregateKind<*, E, *>) { event: E, metadata: EventMetadata -> … }` and
  `on(contract: PublicEventContract<*, P>) { event: P, metadata: EventMetadata -> … }`, called from `init`. Inside the
  block, `trigger(t)` and `trigger(t, notBefore = instant)` queue triggers; the block's return value is ignored.
- Triggers are plain `@Serializable` data (no `EventReactionTrigger` interface, no per-trigger timeout).
- `abstract suspend fun handle(trigger: T, context: ReactionContext)`: returning normally means done; throwing, or
  exceeding `timeout`, is a failure.
- `ReactionContext(reactionId: String, attempt: Int)`: `reactionId` is deterministic and stable across retries and
  redeliveries (use it as an idempotency key for external calls); `attempt` starts at 0.
- Overridable, with defaults: `open val ordering: ReactionOrdering = Unordered`, `open val timeout: Duration = 60.seconds`,
  `open fun onFailure(trigger, attempt, error): FailureDecision = Retry(backoff(attempt))` (`Retry(delay)` | `GiveUp`),
  `open suspend fun onCompletion(trigger, result: ReactionResult)` (`Completed` | `GaveUp(error)`), no-op by default.
  `backoff(attempt)` is the existing capped exponential backoff (1s, 2s, 4s… capped at 10 minutes).
- `ReactionOrdering.PerAggregate(onGiveUp = …)` keeps today's `OnGiveUp` choices (`ContinueWithNext`, `BlockAggregate`).
- Reaction ids are derived by kotmod: `<useCase>/<sourceEventId>/<n>` where `n` is the trigger's position in the block's
  output for that event — deterministic, so a redelivered or replayed event is recognised as a duplicate.
- Rules enforced at registration: a use case may not listen to the same aggregate type through two sources (an aggregate
  kind and a contract whose events come from that aggregate type count as the same type); a delayed trigger
  (`notBefore`) in an ordered use case is refused (when it is produced it is treated as a mapping failure and parked, see 3).

## 2. The reactor and wiring

```kotlin
val queues = DbSchedulerQueues(jdbc)
val reactor = EventReactor(jdbc, queues, isLeader = { election.isLeader })
reactor.register(OrderConfirmationEmails(email))
reactor.register(FraudChecks(fraud))
val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
queues.bind(scheduler)
reactor.start()
scheduler.start()
```

- `EventReactor(jdbc, queues, isLeader, name = "reactor", pollInterval = 500.milliseconds, batchSize = 100)`
  (package `io.kotmod.reaction`): one reader of the local event log with one saved position (consumer name = `name`),
  which starts at the log's head for a new reactor (existing start-at-head behaviour). `register(useCase)` before
  `start()`; duplicate use-case names are refused.
- For each event read, the reactor runs the `on(kind)` blocks of every use case whose sources include the event's aggregate
  type, deserializing with that kind's `eventSerialization`, and publishes the produced triggers to each use case's queue.
  kotmod's internal events are never visible (existing behaviour).
- `on(contract)` sources: registering the use case subscribes it, internally, to the contract's existing reader, feeding
  the same use-case queue. The publishing context still builds and starts its contract.
- Queues: one per use case, named after the use case, all on one shared `ReactionQueues` factory (renamed from
  `ProcessManagerQueues`), ordered when the use case's `ordering` is `PerAggregate`. The rule "one feeder per ordered
  queue" is relaxed to "an aggregate type reaches a use case through at most one source" — different aggregates never
  share an ordering key.
- `kotmod-db-scheduler` provides `DbSchedulerQueues(jdbc)` with `tasks`, `bind(client)` and the operator helpers for
  blocked ordered reactions (`blockedReactions(client, useCase)`, `retryBlocked`, `skipBlocked`). Process managers use the
  same factory.
- Removed from the public API (moved to internals or deleted): `EventReactionExecutor`, `AggregateEventOutbox`,
  `PublicEventContract.subscribe`, `EventReaction` (the app-built reaction), `DbSchedulerEventReactions`,
  `DbSchedulerProcessManagerQueues`; `ProcessManagerQueues` is renamed `ReactionQueues`.
- Kept public, unchanged, as the **queue SPI** for other queues: `EventReactionTriggerSink`, `EventReactionTriggerSource`,
  `EventReactionTriggerSerializer`, `EventReactionTrigger`, `EventReactionId`, `EventReactionExecutionId`, `RetryCount`,
  `DispatchOrdering`, `ReactionOutcome` (`Finished` / `Retry` / `Wait`) and `ReactionChannel` (renamed from
  `ProcessChannel`). App code never implements `EventReactionTrigger` any more: kotmod's internal trigger envelopes do;
  the SPI only moves them between publish and delivery.
- `AggregateKind<C : Any, E : DomainEvent, R : Any>(type, commandSerializer, eventSerialization, rejectionSerializer)`.
- `PublicEventContract` keeps its publishing role, its constructor and the `aggregateTypes` filter.
- `ProcessManager` keeps its API (`subscribeTo(name, contract)` etc.), apart from taking `ReactionQueues`.

## 3. Failure isolation: parked mappings

- **Handling failures** (`handle` throws or times out) are isolated to that reaction: it retries per `onFailure`. With
  ordering, only that aggregate instance's later reactions in that use case wait.
- **Mapping failures** (a use case's `on(...)` block throws for an event, the event can't be deserialized, or an ordered
  use case produces a delayed trigger): the reactor does not stop. It publishes one *parked mapping* item to that use case's
  queue — carrying the event id, the source, and the event's ordering stamp when the use case is ordered — and continues;
  other use cases still get their triggers for the event, and the reader moves on.
- A parked mapping, when run, re-reads the event by id (a new internal backend read), re-runs that use case's block, and on
  success publishes the produced triggers (with their normal deterministic ids) and completes; on failure it retries with
  capped backoff, never giving up, logging each failure. While it is pending, an ordered use case's later reactions for the
  same aggregate instance wait behind it; other aggregates and other use cases are unaffected.
- A crash anywhere is safe: the reader re-publishes the same deterministic ids for an event it re-reads, and a parked
  mapping's triggers have the same ids as if the mapping had succeeded inline.

## Testing

Unit tests on in-memory queues and Postgres + db-scheduler end-to-end tests covering at least:

1. Several use cases, one event: each gets its own triggers in its own queue; a use case listening to other types gets nothing.
2. One use case's mapping throws: other use cases still get triggers for the same event; the reader continues; the
   failure is parked in that use case only; after the code is fixed (simulated by a block that fails N times), the parked
   item produces the triggers exactly once.
3. Mapping throws in an ordered use case: that aggregate's later events wait behind the parked item; other aggregates in
   the use case flow; after the fix, the parked event's triggers and the later events' run in sequence order.
4. A mapping that always throws stays parked and retries, logged and visible; it is never dropped.
5. Handling throws: only that reaction retries; with ordering, only that aggregate waits, in that use case only.
6. Redelivery: the reader replayed from an earlier position, a parked item redelivered after success, and a trigger
   redelivered — no duplicate work (deterministic ids), and `handle` sees a stable `reactionId`.
7. A crash between publishing triggers and saving the reader's position: no loss, no duplicate triggers.
8. An ordered and an unordered use case on the same aggregate don't block each other.
9. A use case with a local source and a contract source: both typed; ordering keys never mix.
10. Refused at registration: the same aggregate type via two sources, duplicate names, registering after start. A delayed
    trigger produced by an ordered use case is parked (fails loudly) rather than stalling the reader.
11. Delayed triggers wait until `notBefore` and then run, including when produced by a parked mapping that succeeds later.
12. A use case added to a running context sees only events from then on.
13. `onFailure` returning `GiveUp`, `onCompletion` outcomes, and the timeout path.
14. A foreign context consuming another context's contract through `on(contract)` end to end.

## Pub/Sub

- Each use case's queue maps to one Pub/Sub subscription (a topic per use case, or a shared topic with a `useCase`
  attribute and a filtered subscription per use case).
- Ordering keys are `<aggregateType>/<aggregateId>`. Pub/Sub's ordered delivery holds back later messages with the same key
  while one fails, which is exactly what a parked mapping needs: it is a message with the event's ordering key, blocking that
  aggregate only.
- A parked mapping re-reads its event from the app's database, which the Pub/Sub consumer (kotmod code) can reach.
- Documented gaps: no deduplication by kotmod's id (use `context.reactionId` as an idempotency key), long delays via Cloud
  Tasks, and stuck ordering keys show up as redelivery or dead-letter rather than db-scheduler rows.
- The README's Pub/Sub sketch is updated to the queue SPI as it stands after this change.

## Documentation

- Quickstart step 5 rewritten around a use case and the reactor; guides for reactions (sources, triggers, handling, retries,
  ordering, delayed triggers, failure isolation), consuming another context's contract, db-scheduler wiring, operator
  helpers; process managers guide updated for `ReactionQueues`/`DbSchedulerQueues`.
- "Upgrading from 0.2.0": every rename/removal with before/after code (executor + outbox → use case + reactor;
  `AggregateKind` gains `E` and `eventSerialization`; `DbSchedulerEventReactions` / `DbSchedulerProcessManagerQueues` →
  `DbSchedulerQueues`; queue names change, so in-flight reactions under old task names must be drained before upgrading).

## Out of scope

- Backfilling history into a single new use case (it shares the reactor's position).
- Consuming another context that lives in a different service/database.
- A ready-made Pub/Sub module.

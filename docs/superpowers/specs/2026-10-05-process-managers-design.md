# Process managers

**Date:** 2026-10-05
**Status:** Approved design; implementation after the prerequisites are built
**Part of:** process managers, cycle 2 of 2
**Depends on:** `2026-10-05-process-manager-prerequisites-design.md` (aggregate kinds, delayed reactions) and
`2026-10-05-state-owned-commands-design.md` (state-owned commands, merged)

## Goal

Long-running workflows that react to events from several aggregates (and other contexts), keep their own state,
and request commands, never doing anything synchronously. A process manager is the mirror of an aggregate: an
aggregate receives commands and emits events; a process manager receives events (as inputs) and emits commands. It
can also schedule inputs to itself (timeouts), and it records its own facts as events. kotmod calls these process
managers, not sagas.

## Decisions

- **A process manager instance is an aggregate.** It is a pattern built on the core (state, version, deduplication,
  event stream), with a thin facade, `ProcessManager`.
- **Anti-corruption at the boundary.** A process manager only sees its own input type. Every event, from this context
  (`translate`) or from another context's public contract (`subscribeTo`), is translated to an input, or ignored.
- **States own their inputs**, as aggregate states own their commands: `ProcessState.handle(input)` and a
  `ProcessInitialState`. The first input for an unknown process id is decided by the initial state, which may start
  the process or ignore the input.
- **Inputs can't be rejected** (they are facts). A state either `transition`s or `ignore`s.
- **Intents are events in the process manager's own stream**, written atomically with its state: requested commands
  and scheduled inputs. Something else (reactions) carries them out later.
- **Commands only go to aggregates of this context**, addressed through `AggregateKind`s; kotmod runs them through the
  target's `AggregateManager.handle`. No `dispatchCommand` code.
- **Facts the process manager owns are its own events.** Downstream contexts receive them through a
  `PublicEventContract`, the outbound anti-corruption layer. (No `EventProducer` targets; they can be added later if a
  fact must live in another stream.)
- **Timeouts are scheduled inputs** to the same instance, delivered with `notBefore`. No cancellation: a state that has
  moved on ignores a stale timeout.
- **Typed rejection feedback:** each target is registered with a mapping `(command, rejection) -> input`; a rejected
  command comes back to the process manager as an input.
- **One poller per process manager**, separate channels (executors) for inputs, internal inputs and commands.
  Inputs may be ordered per source aggregate; internal inputs and commands are always unordered.

## 1. API

```kotlin
// Inputs: the process manager's own language
@Serializable sealed interface RefundWindowInput
@Serializable data class OrderWasConfirmed(val orderId: String, val at: Instant) : RefundWindowInput
@Serializable data object RefundWasIssued : RefundWindowInput
@Serializable data object RefundPeriodElapsed : RefundWindowInput
@Serializable data class PayoutBlocked(val rejection: PayoutRejection) : RefundWindowInput

// Facts the process manager owns
@Serializable sealed interface RefundWindowEvent : DomainEvent
@Serializable data object RefundPeriodExpired : RefundWindowEvent

typealias RefundWindowOutcome = ProcessOutcome<RefundWindowState, RefundWindowEvent, RefundWindowInput>

sealed interface RefundWindowState : ProcessState<RefundWindowState, RefundWindowInput, RefundWindowEvent>

object NoRefundWindow : ProcessInitialState<RefundWindowState, RefundWindowInput, RefundWindowEvent> {
    override suspend fun handle(input: RefundWindowInput): RefundWindowOutcome =
        when (input) {
            is OrderWasConfirmed ->
                transition(Open(input.orderId), schedule = listOf(schedule(RefundPeriodElapsed, at = input.at + 30.days)))
            RefundWasIssued, RefundPeriodElapsed, is PayoutBlocked -> ignore()
        }
}

data class Open(val orderId: String) : RefundWindowState {
    override suspend fun handle(input: RefundWindowInput): RefundWindowOutcome =
        when (input) {
            RefundPeriodElapsed ->
                transition(
                    Closed,
                    events = listOf(RefundPeriodExpired),
                    commands = listOf(Payouts.command(AggregateId(orderId), ReleaseFunds(orderId))),
                )
            RefundWasIssued -> transition(Closed)
            is OrderWasConfirmed, is PayoutBlocked -> ignore()
        }
}
```

kotmod types (package `io.kotmod.process`, except `AggregateKind.command` in `io.kotmod`):

- `ProcessState<S, I, E>` / `ProcessInitialState<S, I, E>`: `suspend fun handle(input: I): ProcessOutcome<S, E, I>`.
- `ProcessOutcome<S, E, I>`: built by `transition(state, events = emptyList(), commands = emptyList(),
  schedule = emptyList())` or `ignore()`.
- `RequestedCommand`: built by `AggregateKind<C, R>.command(id: AggregateId, command: C)`, so a command can only be
  addressed to a kind that accepts it.
- `ScheduledInput<I>`: built by `schedule(input, at: Instant)`.
- `ProcessManager`, built from: `type: AggregateType`; `repository: Repository<S>` (the app's table, as for
  aggregates); `jdbc`; `initial`; `inputSerializer: KSerializer<I>`; the serialization for its events; `translate:
  (PersistedEvent) -> Pair<AggregateId, I>?`; `targets`; `queues`; `inputOrdering: ReactionOrdering`; `getPosition`,
  `savePosition`, `isLeader`.
- `target(manager) { command: C, rejection: R -> input }`: registers an `AggregateManager` the process manager may
  send commands to, with its rejection mapping. A target can't be registered without one.
- `processManager.subscribeTo(contract) { publicEvent -> Pair<AggregateId, I>? }`: inputs from another context,
  translated in the block. Must be called before `start()`.
- `start()` / `stop()`: the poller and the channels' executors.

Testing a process manager's decisions is plain calls: `Open("o-1").handle(RefundPeriodElapsed)`.

## 2. How it runs

**Persistence reuses the core.** Internally an `AggregateManager` handles each instance, with the inputs as its
commands, so deduplication by input id, conflict retries, the outer-transaction rule and recorded answers come from
the core.

- `transition(...)` is accepted: in one transaction it saves the state and appends the app's events plus kotmod
  envelope events: `CommandRequested` (target aggregate type, target id, command JSON) and `InputScheduled` (input
  JSON, `at`).
- `ignore()` is recorded through the core's rejection record with an internal marker: it never creates an instance,
  changes state or bumps the version, but a redelivered input is recognised as handled.
- A requested command addressed to a kind that is not among `targets` fails at decision time, before anything is
  recorded; the input delivery is retried and logged (a wiring bug, never silently dropped).

**One poller per process manager** reads the event log once (own saved position, leader only):

| Event read | What happens |
|---|---|
| The process manager's own `CommandRequested` | Commands channel: reaction `cmd-<eventId>` |
| The process manager's own `InputScheduled` | Internal-inputs channel: reaction `in-<eventId>`, `notBefore = at` |
| The process manager's own domain events | Nothing (not fed back through `translate`); contracts and outboxes can still publish them |
| Any other event | `translate(event)`: inputs-channel reaction `in-<eventId>`, ordered per source aggregate if `inputOrdering` is `PerAggregate`; nothing on `null` |

Each `subscribeTo(contract)` gets its own input channel (an ordered executor takes one source), fed by the
contract's existing poller.

**Commands channel.** The reaction runs `manager.handle(targetId, command, commandId = CommandId("<pmType>-<eventId>"),
correlationId = CorrelationId("<pmType>/<processId>"))`:
- `Accepted`: done (the process manager learns about results from the target's events, if it translates them).
- `Rejected(r)`: the target's mapping produces an input, queued on the internal-inputs channel as reaction
  `rejected-<eventId>`.
- An exception: retried with capped backoff, never turned into a rejection.

Redelivery is safe: the command id is deterministic and the target records its answer.

**Internal-inputs channel:** timeouts and rejection feedback, unordered; each delivery goes through the same
deduplicated delivery path as other inputs.

## 3. Errors and ordering

| Failure | Behaviour |
|---|---|
| Delivering an input throws (state bug, unreadable data, unknown target kind) | Capped-backoff retry, no give-up, logged with the process id; visible as a stuck reaction |
| Delivering an input loses a race on the same instance | The core's automatic conflict retry |
| A command's `handle` throws | Capped-backoff retry, never a fake rejection |
| A target rejects a command | Typed mapping to an input, delivered back, deduplicated |
| `translate` throws, or an envelope can't be read | The poller stops its batch and retries from its saved position |

Inputs are optionally ordered per source aggregate (per channel); timeouts, rejection feedback and commands are
always unordered, so a long timeout never blocks anything.

## Code placement

- `ProcessManager` and its types: the core module, package `io.kotmod.process` (depends only on core abstractions and
  a queue factory).
- `DbSchedulerProcessManagerQueues` (one db-scheduler task per channel): `kotmod-db-scheduler`.

## Testing

- **Unit:** states decide by plain calls; delivery (transition, ignore, deduplication, unknown target kind); the
  poller's routing of envelopes, foreign events and the process manager's own events.
- **Postgres + db-scheduler integration**, the refund window end to end: an order confirmation starts it; the timeout
  fires only after `notBefore`; the command is accepted by the target; a rejection comes back as an input; a
  redelivered command gets the same answer; inputs from a contract subscription arrive; per-aggregate input ordering
  holds.

## Documentation

A new "Process managers" guide built on the refund-window example: translation as the anti-corruption layer; states
own their inputs; `ignore`; timeouts as scheduled inputs; commands to other aggregates via kinds; typed rejection
feedback; publishing the process manager's own facts through a contract; wiring with db-scheduler.

## Out of scope

- `EventProducer` targets, commands to other contexts, timeout cancellation.
- A shared poller across process managers.
- A ready-made JSON state table for process manager state (the app supplies a `Repository`).

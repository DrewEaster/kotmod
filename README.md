# kotmod

kotmod is a Kotlin library for building domain-driven design (DDD) aggregates on Postgres. It gives
you **domain events without event sourcing** — aggregates keep plain state in your own tables, and
kotmod records the events they raise in the same transaction — and a **transactional outbox built
in**, so those events reliably drive follow-up work in your own service and in others.

## Contents

- [Why kotmod](#why-kotmod)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Core concepts](#core-concepts)
- [Guides](#guides)
  - [Aggregates and commands](#aggregates-and-commands)
  - [Event-only aggregates](#event-only-aggregates)
  - [Event serialization and schema migrations](#event-serialization-and-schema-migrations)
  - [Postgres setup](#postgres-setup)
  - [Event policies](#event-policies)
  - [Running event policies on db-scheduler](#running-event-policies-on-db-scheduler)
  - [Ordered event policies](#ordered-event-policies)
  - [Using another scheduler](#using-another-scheduler)
  - [Publishing events to other contexts](#publishing-events-to-other-contexts)
  - [Consuming another context's events](#consuming-another-contexts-events)
  - [Process managers](#process-managers)
- [Running in production](#running-in-production)
- [Known limitations](#known-limitations)
- [Upgrading from 0.3](#upgrading-from-03)
- [Upgrading from 0.3.0](#upgrading-from-030)
- [Upgrading from 0.2.0](#upgrading-from-020)
- [Upgrading from 0.1.0](#upgrading-from-010)
- [Status and contributing](#status-and-contributing)

## Why kotmod

Most services want two things from their domain model: state they can query like any other table,
and events that tell the rest of the system what happened. Event sourcing gives you events but makes
state something you rebuild. Saving state and then publishing a message gives you both, but not
atomically: if the message fails after the commit (or the commit fails after the message), the two
disagree. This is the *dual-write problem*.

kotmod writes an aggregate's new state, its events and the command that caused them in **one database
transaction**. A reactor then reads those events in order and runs your *event policies* on them — durable, retried
follow-up work, written as plain application code — and contracts publish them to other bounded contexts.

Event policies run on whatever scheduler you choose. kotmod ships one built on
[db-scheduler](https://github.com/kagkarlsson/db-scheduler), which needs nothing but the Postgres database
you already have, and you can plug in another by implementing two small interfaces (see
[Using another scheduler](#using-another-scheduler)). kotmod keeps ordering, attempt counts and blocked work in its
own table, so they behave the same on every scheduler.

kotmod is not an event store, not a message broker and not a framework: it is a library you wire into
your own application, on the Postgres database you already have.

## Installation

<!-- not-compiled -->
```kotlin
plugins {
    kotlin("jvm") version "2.4.20"
    kotlin("plugin.serialization") version "2.4.20" // for @Serializable events and triggers
}

dependencies {
    implementation("io.github.dreweaster:kotmod:0.4.0")
    implementation("io.github.dreweaster:kotmod-db-scheduler:0.4.0") // optional: the ready-made scheduler
    // implementation("io.github.dreweaster:kotmod-sqldelight:0.4.0") // only if your app uses SQLDelight

    // Used directly by the code in this README:
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.postgresql:postgresql:42.7.13")
}
```

You also need a `javax.sql.DataSource` for your database, for example from HikariCP.

Requirements:

- A JVM 25 toolchain (kotmod is currently built and tested on it) and Kotlin.
- PostgreSQL 13 or later.

kotmod is split into modules:

- `kotmod` — aggregates, events, event policies, process managers and Postgres support, on plain JDBC with no other
  database library. This is the only module you need.
- `kotmod-db-scheduler` — optional. A ready-made scheduler for event policies and process managers on
  [db-scheduler](https://github.com/kagkarlsson/db-scheduler) 16.12.0, which it brings in. Leave it out if
  you run them on another scheduler (see [Using another scheduler](#using-another-scheduler)).
- `kotmod-sqldelight` — optional. Shares kotmod's transactions with SQLDelight.

## Quickstart

This walks through a tiny orders domain: you place an order, ship it, and send a confirmation email
whenever an order is placed. It uses kotmod's Postgres backends and runs follow-up work on db-scheduler,
kotmod's ready-made scheduler; you could swap in another scheduler without changing the rest.

Snippets leave out imports. The complete, compiled code is in
[`Quickstart.kt`](examples/src/integrationTest/kotlin/io/kotmod/readme/Quickstart.kt) and
[`QuickstartTest.kt`](examples/src/integrationTest/kotlin/io/kotmod/readme/QuickstartTest.kt). `Retry` and
`GiveUp` come from `io.kotmod.reaction`.

### 1. Create the tables

Your application owns the table that stores order state:

```sql
CREATE TABLE IF NOT EXISTS orders (
    id     TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    item   TEXT NOT NULL,
    reason TEXT
)
```

kotmod needs its own tables too. Copy the statements in `DddSchema.ddl` (in `io.kotmod.postgres`) into your
migrations; they create the event log, aggregate bookkeeping, handled-command history, consumer offsets and the rows
that hold ordered work and parked mappings. Event policies run on db-scheduler, which needs its `scheduled_tasks`
table: create it from db-scheduler's
[`postgresql_tables.sql`](https://github.com/kagkarlsson/db-scheduler/blob/v16.12.0/db-scheduler/src/test/resources/postgresql_tables.sql).

### 2. Define state, events, commands and rejections

An aggregate's **state** is whatever your application needs to make decisions. Here, an order is pending,
shipped or cancelled. Its **events** record what happened. They are stored as JSON, so they are
`@Serializable`:

```kotlin
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
```

A **command** asks the order to change. Commands are data, so they are `@Serializable`, and so is the
aggregate's **rejection** type: every way a command can be refused, in your domain's own words.

```kotlin
@Serializable
sealed interface OrderCommand

@Serializable
data class PlaceOrder(
    val item: String,
) : OrderCommand

@Serializable
data object ShipOrder : OrderCommand

@Serializable
data class CancelOrder(
    val reason: String,
) : OrderCommand

@Serializable
sealed interface OrderRejection

@Serializable
data object OrderAlreadyPlaced : OrderRejection

@Serializable
data object OrderNotFound : OrderRejection

@Serializable
data object OrderAlreadyShipped : OrderRejection

@Serializable
data object OrderAlreadyCancelled : OrderRejection

@Serializable
data object CancellationReasonMissing : OrderRejection
```

Each state owns its behaviour. It implements `handle` and decides every command it might receive:
`accept(newState, events…)` or `reject(rejection)`. `NoOrder` is the order before it exists: placing it is
accepted there, and everything else is rejected. Each `when` lists every command and has no `else`, so adding
a command doesn't compile until every state has decided what to do with it. Decisions are plain code that
you can unit-test without a database.

```kotlin
typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

sealed interface Order : AggregateState<Order, OrderCommand, OrderEvent, OrderRejection>

object NoOrder : InitialState<Order, OrderCommand, OrderEvent, OrderRejection> {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> accept(PendingOrder(command.item), OrderPlaced(command.item))
            ShipOrder, is CancelOrder -> reject(OrderNotFound)
        }
}

data class PendingOrder(
    val item: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder -> accept(ShippedOrder(item), OrderShipped(item))
            is CancelOrder -> cancel(command.reason)
        }

    private fun cancel(reason: String): OrderOutcome =
        if (reason.isBlank()) {
            reject(CancellationReasonMissing)
        } else {
            accept(CancelledOrder(item, reason), OrderCancelled(item, reason))
        }
}

data class ShippedOrder(
    val item: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyShipped)
        }
}

data class CancelledOrder(
    val item: String,
    val reason: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyCancelled)
        }
}
```

### 3. Wire up persistence

First name the aggregate. An `AggregateKind` names the aggregate type and says how its commands, events and
rejections are serialized:

```kotlin
object Orders : AggregateKind<OrderCommand, OrderEvent, OrderRejection>(
    type = AggregateType("Order"),
    commandSerializer = OrderCommand.serializer(),
    eventSerialization =
        jsonDataSerializationContext<OrderEvent> {
            +OrderPlaced.serializer().toEventSerializer()
            +OrderShipped.serializer().toEventSerializer()
            +OrderCancelled.serializer().toEventSerializer()
        },
    rejectionSerializer = OrderRejection.serializer(),
)
```

Then, given a `javax.sql.DataSource` for your database (for example from HikariCP), create a `JdbcContext` —
how kotmod reaches the database and runs transactions — and an `AggregateManager` for orders from the kind. Its
backend writes events with the kind's serialization:

```kotlin
val jdbc = DataSourceJdbcContext(dataSource)

val orders =
    AggregateManager(
        kind = Orders,
        repository = OrderRepository(jdbc),
        backend = PostgresDomainPersistenceBackend(jdbc, Orders.eventSerialization),
        initial = NoOrder,
    )
```

The `Repository` is yours: it loads and saves order state in the `orders` table. It borrows its connection
through `jdbc.withConnection`, so its writes join the same transaction as kotmod's:

```kotlin
class OrderRepository(
    private val jdbc: JdbcContext,
) : Repository<Order> {
    override fun get(id: AggregateId): Order? =
        jdbc.withConnection { conn ->
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
        jdbc.withConnection { conn ->
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
```

### 4. React to events

Follow-up work, such as sending an email when an order is placed, is an **event policy**. It says which events it
reacts to, what work they trigger, and how that work is done. A **trigger** is stored until its work runs, so it
must be serializable:

```kotlin
// This event policy does one kind of work. One that does several makes its trigger type a sealed interface, with one
// class per kind of work (see "Event policies" in the guides).
@Serializable
data class SendOrderConfirmation(
    val orderId: String,
)

class OrderNotifications(
    private val confirm: (orderId: String) -> Unit,
) : EventPolicy<SendOrderConfirmation>(
        name = "order-notifications",
        triggers = SendOrderConfirmation.serializer(),
    ) {
    init {
        on(Orders) { event, metadata ->
            when (event) {
                is OrderPlaced -> trigger(SendOrderConfirmation(metadata.aggregateId.value))
                is OrderShipped, is OrderCancelled -> Unit
            }
        }
    }

    override suspend fun handle(
        trigger: SendOrderConfirmation,
        context: ReactionContext,
    ) = confirm(trigger.orderId)

    override fun onFailure(
        trigger: SendOrderConfirmation,
        attempt: Int,
        error: Throwable,
    ): FailureDecision = if (attempt < 5) Retry(backoff(attempt)) else GiveUp

    override suspend fun onCompletion(
        trigger: SendOrderConfirmation,
        result: ReactionResult,
    ) {
        println("$trigger finished: $result")
    }
}

fun sendConfirmation(orderId: String) {
    println("Sending confirmation for order $orderId")
}
```

- `on(Orders) { event, metadata -> … }` reacts to order events, typed: `Orders` carries their serialization. The
  event log holds every aggregate type's events; this event policy only sees orders.
- `trigger(...)` queues work. kotmod builds its id from the event policy, the event and the trigger's position, so
  if the same event is read again while its work is pending, it is recognised as the same work.
- `handle` does the work. Returning means done. Throwing, or running past the event policy's `timeout` (60 seconds
  by default), is a failure, and `onFailure` decides: `Retry(delay)` or `GiveUp`. By default it retries with
  `backoff(attempt)` (1s, 2s, 4s… up to 10 minutes) and never gives up.
- `onCompletion` is told how the work ended: `ReactionResult.Completed` or `ReactionResult.GaveUp(error)`.

The **reactor** runs a context's event policies. It reads the event log once, hands each event to every event policy
that listens to it, and queues their triggers. They run on db-scheduler: `DbSchedulerTaskScheduler` gives each event
policy its own db-scheduler task, which you register with your db-scheduler `Scheduler`. db-scheduler polls for due
work every 10 seconds by default; `enableImmediateExecution()` runs new work straight away:

```kotlin
val scheduler = DbSchedulerTaskScheduler()

val reactor = EventReactor(jdbc, scheduler, isLeader = { true })
reactor.register(OrderNotifications(::sendConfirmation))

val dbScheduler =
    Scheduler
        .create(dataSource, *scheduler.tasks.toTypedArray())
        .threads(4)
        .enableImmediateExecution()
        .build()
scheduler.bind(dbScheduler)
```

Register every event policy before reading `scheduler.tasks`, and call `scheduler.bind(dbScheduler)` before starting
the reactor: queueing work needs it. Then start the reactor, then the db-scheduler `Scheduler`:

```kotlin
reactor.start()
dbScheduler.start()
```

A new reactor starts at the head of the event log: it sees events written after it first starts, not history. Start
it when your application starts, before it handles commands. To shut down, stop the db-scheduler `Scheduler`, then
the reactor: `dbScheduler.stop()`, then `reactor.stop()` (the reactor also handles its event policies' queues, so it
stops after the scheduler that delivers their work). The quickstart passes `isLeader = { true }` because it runs on
one node; see [Running in production](#running-in-production) for leader election and the full shutdown order. The
reactor saves its position under its name, `reactor` by default, so a second reactor on the same database needs its
own `name = "..."` (see **Reading events** in [Postgres setup](#postgres-setup)).

### 5. Run commands

Send the commands from step 2 through the aggregate manager. `handle` is a `suspend` function, and it
returns either `CommandResult.Accepted` with the new state or `CommandResult.Rejected` with one of your
rejections:

```kotlin
val orderId = AggregateId("order-1")

orders.handle(orderId, PlaceOrder("book"))

val result = orders.handle(orderId, ShipOrder)
```

An accepted command saves the order's state, appends its events to the event log and records the command,
all in one transaction. A rejected command changes nothing, but its rejection is recorded too.

Within a moment you'll see `Sending confirmation for order order-1`. Only `OrderPlaced` triggered work;
`OrderShipped` was read and ignored.

## Core concepts

```mermaid
flowchart LR
    C[Command] --> AM[AggregateManager]
    AM -->|one transaction| S[(State in your tables)]
    AM -->|one transaction| E[(Events in ddd_domain_event)]
    E --> R[EventReactor]
    R --> U[Event policies]
    U <--> D[(Your scheduler: db-scheduler, …)]
    U <--> RR[(Ordered work in ddd_reaction_row)]
    E --> P[PublicEventContract]
    P --> UO[Event policies in other contexts]
```

- **Aggregate** — a cluster of domain state changed only through commands, identified by an
  `AggregateType` and `AggregateId`.
- **Command** — a request to change an aggregate, as serializable data. The aggregate's current state
  decides it: accept it (new state and events) or reject it. It is idempotent when given a `CommandId`.
- **Rejection** — why an aggregate refused a command, as one of your own types. Rejections are recorded,
  so a repeated command id gets the same answer.
- **Domain event** — a fact recorded in the event log in the same transaction as the state change.
- **Event policy** — follow-up work written as application code: which events it reacts to, the triggers they
  produce, and how each trigger is handled, with its own retries, timeout and ordering.
- **Trigger** — the stored input of one piece of an event policy's work, as serializable data.
- **Reactor** — reads a context's event log once and queues every event policy's triggers, each event policy on its
  own queue.
- **Scheduler** — runs kotmod's work at a given time, with one queue per event policy and per process manager
  channel. kotmod ships one on db-scheduler;
  anything that implements `TaskScheduler` works. kotmod keeps ordered work, attempt counts and blocked work in its
  own table, `ddd_reaction_row`, not in the scheduler.
- **Public event** — a stable event published to other bounded contexts, mapped from internal domain
  events.
- **Process manager** — a long-running workflow that reacts to events, keeps its own state, and asks for
  commands to be run and inputs to be delivered to it later.

## Guides

Each guide builds on the quickstart's orders domain.

### Aggregates and commands

Use `AggregateManager` for anything whose state you store and change through commands.

Every command runs in three phases:

1. **Read** — if the command's id has already been handled, the recorded answer is returned and nothing
   else happens. Otherwise the aggregate's version and state are loaded.
2. **Decide** — the aggregate's current state (or the initial state, if the aggregate doesn't exist yet)
   decides the command. kotmod does no database work while it runs, so keep side effects out of it; put
   them in an [event policy](#event-policies) instead.
3. **Write** — in one transaction, the aggregate's version is advanced, your repository saves the new
   state, the events are appended and the command is recorded as handled. A rejected command writes only
   the rejection record.

**States own their commands.** Each state implements `AggregateState` and decides every command in
`handle`, returning `accept(newState, events…)` or `reject(rejection)`. An `InitialState` object (`NoOrder`)
decides commands for an aggregate that doesn't exist yet, and accepting there creates it. Write each `when`
without an `else`: then adding a command doesn't compile until every state, and the initial state, has
decided what to do with it.

Decisions are plain code, so you can unit-test them without a database:
`PendingOrder("book").handle(ShipOrder)` and `NoOrder.handle(PlaceOrder("book"))` return the `Outcome`. `handle` is a `suspend` function, so call it from a coroutine such as `runTest`.

Callers match on the result; a rejection is a value, never an exception:

```kotlin
suspend fun cancelOrder(
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    orderId: AggregateId,
    reason: String,
    requestId: String,
): String =
    when (val result = orders.handle(orderId, CancelOrder(reason), commandId = CommandId(requestId))) {
        is CommandResult.Accepted -> "Cancelled"
        is CommandResult.Rejected ->
            when (result.rejection) {
                OrderAlreadyShipped -> "Too late: the order has shipped"
                CancellationReasonMissing -> "Please give a reason"
                else -> "Can't cancel: ${result.rejection}"
            }
    }
```

**Why commands are data.** Every caller, whether an HTTP handler, an event policy or, later, a process
manager, runs a command the same way: through `AggregateManager.handle`. The rules for which state accepts a
command and how it is refused live in the states themselves, not in each caller.

**Event sequence numbers.** Every event carries `event.metadata.sequence`: its number within its
aggregate, counting 1, 2, 3… with no gaps. Event policies and public contracts can use it to tell which of an
aggregate's events came first.

**Idempotency.** Pass a `CommandId` you control — a request id, a message id. A repeated id returns the
recorded answer: an accepted command returns the aggregate's *current* state without running again, and a
rejected one returns the same rejection, even if the state would now allow the command. Without a command
id, kotmod generates a random one and the call is not idempotent. Pass a `CorrelationId` to tie together all
the events of one wider flow; it is stored with every event.

**Concurrency.** Each aggregate has a version. If someone else changes the aggregate between your read and
your write, `handle` reads again and decides again, up to `maxConflictRetries` times (5 by default), and
then throws `OptimisticConcurrencyException`. Deciding again is safe when decisions are pure. A state's
`handle` may suspend, but anything it calls out to is called once per attempt.

**Your repository joins the transaction.** `Repository.save` is called inside kotmod's transaction, so it
must borrow its connection from the same `JdbcContext` as the backend — as `OrderRepository` does with
`jdbc.withConnection` — rather than opening its own. A repository with its own connection would commit
state independently of the events.

#### Several aggregates in one transaction

**This is permitted, but not recommended.** In domain-driven design an aggregate is the boundary of
consistency: each command changes one aggregate in its own transaction. When a change to one aggregate
should lead to a change in another, the usual design is to react to the first aggregate's event and run
the second command asynchronously, with an [event policy](#event-policies). That keeps
aggregates independent, keeps transactions short and small, and lets each aggregate be changed without
locking the others.

kotmod still allows it, for pragmatism. Some teams have good reasons to change several aggregates
atomically — a migration, a legacy design, or an invariant they have chosen not to model as a single
aggregate — and kotmod would rather support that clearly than forbid it. If you do, wrap the commands in
`jdbc.transaction { }` and their state, events and command records commit together — or not at all:

```kotlin
suspend fun shipAndInvoice(
    jdbc: JdbcContext,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    invoices: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    orderId: AggregateId,
) {
    jdbc.transaction {
        // A rejection is a value: throw to roll the whole transaction back.
        val shipped = orders.handle(orderId, ShipOrder)
        if (shipped is CommandResult.Rejected) throw IllegalStateException("Can't ship: ${shipped.rejection}")
        val invoiced = invoices.handle(AggregateId("invoice-${orderId.value}"), PlaceOrder("invoice"))
        if (invoiced is CommandResult.Rejected) throw IllegalStateException("Can't invoice: ${invoiced.rejection}")
    }
}
```

- If any command fails — including a conflict — everything rolls back, including the other aggregates'
  events. Let the exception propagate: if you catch it and carry on, the transaction is still rolled back
  and kotmod throws `TransactionRolledBackException` rather than commit a partial result. Inside the block,
  `handle` does not retry conflicts; the exception propagates and the whole transaction rolls back.
- A rejection is a value. If you carry on, its record commits with everything else; to undo the other
  commands, throw, as `shipAndInvoice` does.
- Commands inside the block see each other's uncommitted writes.
- Run commands one after another, never in parallel, and don't switch threads inside the block (for
  example with `withContext`) — for kotmod commands or your own SQL; kotmod throws `IllegalStateException`
  if you do. The block keeps your coroutine context (name, tracing, MDC).
- Every kotmod class inside the block must use the same `JdbcContext`.
- Keep the block short: it holds a database transaction open.
- This is the only way an aggregate's events can reach the event log out of order. kotmod still delivers
  them in order, but such aggregates pay a slower check when replayed (see
  [Known limitations](#known-limitations)).

A conflict in a transaction is not retried for you. To retry, run the whole transaction again, for example
with this helper:

```kotlin
suspend fun <T> retryTransaction(
    jdbc: JdbcContext,
    attempts: Int = 3,
    block: suspend () -> T,
): T {
    repeat(attempts - 1) {
        try {
            return jdbc.transaction { block() }
        } catch (e: OptimisticConcurrencyException) {
            // Another writer got there first: run the whole transaction again.
        } catch (e: AggregateAlreadyExistsException) {
        } catch (e: CommandAlreadyRecordedException) {
        }
    }
    return jdbc.transaction { block() }
}
```

### Event-only aggregates

Use `EventProducer` when something needs an event log and idempotent commands but has no state worth
storing — an audit trail, for example:

```kotlin
@Serializable
sealed interface AuditEvent : DomainEvent

@Serializable
data class OrderViewed(
    val viewer: String,
) : AuditEvent

fun auditLog(jdbc: JdbcContext): EventProducer<AuditEvent> =
    EventProducer(
        aggregateType = AggregateType("OrderAuditLog"),
        backend =
            PostgresDomainPersistenceBackend(
                jdbc,
                jsonDataSerializationContext<AuditEvent> { +OrderViewed.serializer().toEventSerializer() },
            ),
    )

suspend fun recordView(
    auditLog: EventProducer<AuditEvent>,
    orderId: AggregateId,
    viewer: String,
    requestId: String,
) {
    auditLog.emit(orderId, listOf(OrderViewed(viewer)), commandId = CommandId(requestId))
}
```

`emit` creates the aggregate's bookkeeping the first time it is called for an id. Command ids and
optimistic concurrency work exactly as they do for `AggregateManager`. Its events go into the same event
log as everything else; an event policy only receives the aggregate types it listens to with `on(...)`, so they
never reach one that doesn't ask for them.

### Event serialization and schema migrations

Events live in the event log for a long time, so their classes will change. Each event is stored with
its class name and a schema version, and `jsonDataSerializationContext` knows how to bring old versions
up to date:

```kotlin
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
```

- Every event type starts at version 1. Each `migrateFormat` or `migrateClassName` adds a version, in the
  order the changes were made.
- `migrateFormat` transforms the previous version's JSON into the new shape.
- `migrateClassName` records a rename or move. Pass the event's *original* class name as
  `initialClassName`, and the current one to `migrateClassName`.
- Old events are migrated when they are read; new events are always written at the latest version.

If you don't want JSON, implement `DataSerializationContext` yourself.

### Postgres setup

kotmod's Postgres classes use plain JDBC through a `JdbcContext`. For a plain `DataSource`, use
`DataSourceJdbcContext(dataSource)`; SQLDelight users have an adapter (below).

**Schema.** `DddSchema.ddl` creates five tables; copy it into your Flyway or Liquibase migrations:

| Table | Holds |
|---|---|
| `ddd_aggregate_root` | Each aggregate's version, sequence counter and timestamps, and whether it has events out of order in the log |
| `ddd_domain_event` | The event log, ordered by `(transaction_id, global_offset)`, with each event's `aggregate_sequence` |
| `ddd_command_history` | Which commands each aggregate has handled |
| `ddd_consumer_offset` | How far the reactor, each contract and each process manager has read (a transaction id and offset) |
| `ddd_reaction_row` | Ordered event policies' and process managers' work, one line per aggregate, and parked mappings (see [Ordered event policies](#ordered-event-policies)) |

db-scheduler's `scheduled_tasks` table belongs to your application; create it from db-scheduler's
[`postgresql_tables.sql`](https://github.com/kagkarlsson/db-scheduler/blob/v16.12.0/db-scheduler/src/test/resources/postgresql_tables.sql).

**Transactions.** Pass the same `JdbcContext` to every kotmod class and to your repositories. Each command
runs in a transaction opened by its backend's `JdbcContext`; `jdbc.inTransaction { }` and
`jdbc.withConnection { }` let your own code join it, and `jdbc.transaction { }` spans several commands.

**Reading events.** `PostgresDomainPollingBackend` reads the event log for the reactor, public contracts and
process managers. `PostgresOffsetManager` stores how far each has read. The reactor saves its position under its
`name` (`reactor` by default); give every other poller its own consumer name. Two reactors on the same database
must have different names: with the same name they would share one saved position and skip each other's events.

**Starting positions.** A consumer with no saved position starts from the head of the event log, so deploying a
new reactor, contract or process manager does not replay history. The reactor fixes its starting position when
it starts, so a new one sees every event committed after `start()` returns. A contract or process manager fixes
it on its first read of its position (its first poll as leader), not when it is constructed. Either may also see
a few events committed just before, while an older transaction was still open, but never history from before
that. Pass `startFrom = StartFrom.Beginning` to `getPosition` for a contract or process manager that must see
history. An existing consumer keeps its saved position; to reset one deliberately, save a position yourself with
`savePosition`.

#### Using SQLDelight

Add `io.kotmod:kotmod-sqldelight` and build a `SqlDelightJdbcContext` from the same `JdbcDriver` as your
generated database. kotmod then runs its transactions through SQLDelight's, so your SQLDelight queries and
kotmod's writes share one transaction whichever side opens it:

- Inside `jdbc.transaction { }`, call your SQLDelight queries as usual — they join kotmod's transaction.
- Inside your own `database.transaction { }`, call kotmod from blocking code (SQLDelight's block can't
  suspend), e.g. `runBlocking { orders.handle(id, command) }`; commands join your transaction. Wrap
  several in `runBlocking { jdbc.transaction { … } }` to get kotmod's thread checks too. In a transaction
  SQLDelight opened, SQLDelight's rules apply: if a kotmod call fails and you catch it, SQLDelight rolls
  your transaction back when it ends.

Repositories implemented with SQLDelight queries need no changes: they already run inside the transaction.

### Event policies

An event policy is a class that extends `EventPolicy<T>`, where `T` is its trigger type (plain `@Serializable` data).
It owns the whole reaction, like `OrderNotifications` in [the quickstart](#4-react-to-events):

- **Sources.** In its `init` block, `on(kind) { event, metadata -> … }` reacts to one of this context's
  aggregate kinds, with the events typed (`AggregateKind` carries their serialization).
  `on(contract) { event, metadata -> … }` reacts to another context's public events (see
  [Consuming another context's events](#consuming-another-contexts-events)). One source is the common case;
  several are allowed, as long as each aggregate type reaches the event policy through only one of them. An event policy
  never sees events of types it doesn't listen to.
- **Triggers.** Inside the block, `trigger(t)` queues work and `trigger(t, notBefore = instant)` delays it. The
  block only decides what to do. Keep it free of I/O and deterministic: the same event must always produce the
  same triggers, in the same order, because their ids are numbered by position.
- **Handling.** `handle(trigger, context)` does the work. `context.reactionId` is the same on every retry and
  redelivery (`<policy>/<eventId>/<n>`), so pass it as the idempotency key of external calls; `context.attempt`
  counts retries from 0.
- **Failures.** `onFailure(trigger, attempt, error)` returns `Retry(delay)` or `GiveUp`. By default it retries
  with `backoff(attempt)` (1s, 2s, 4s… up to 10 minutes) and never gives up. Running past `timeout` (60 seconds
  by default) is a failure too, passed as a `ReactionTimeoutException`. If `onFailure` throws, the work is
  retried after a backoff.
- **Completion.** `onCompletion(trigger, result)` hears `ReactionResult.Completed` or
  `ReactionResult.GaveUp(error)`; it does nothing by default. If it throws, the work is retried after a backoff,
  so `handle` may run again.
- **Name.** `name` names the event policy's queue, so keep it stable across releases. With db-scheduler it is the
  task name, so it must be unique across everything that shares the `scheduled_tasks` table, including other
  contexts' event policies and process manager channels.

Each event policy has its own queue, so its ordering, timeout and failure handling are its own, and a slow or failing
event policy never holds up another. Delivery is at least once: make `handle` idempotent.

#### Several sources and kinds of work

An event policy can listen to several aggregates, and do several kinds of work. Make its trigger type a sealed
interface, with one class per kind of work, and call `on(...)` once per source. This one posts to a sales channel
when a customer registers and when an order is placed (`Customers` is the customer aggregate's kind, defined like
`Orders`):

```kotlin
@Serializable
sealed interface SalesFeedPost

@Serializable
data class NewCustomer(
    val customerId: String,
    val email: String,
) : SalesFeedPost

@Serializable
data class NewOrder(
    val orderId: String,
    val item: String,
) : SalesFeedPost

class SalesFeed(
    private val post: suspend (message: String) -> Unit,
) : EventPolicy<SalesFeedPost>(
        name = "sales-feed",
        triggers = SalesFeedPost.serializer(),
    ) {
    init {
        on(Customers) { event, metadata ->
            when (event) {
                is CustomerRegistered -> trigger(NewCustomer(metadata.aggregateId.value, event.email))
            }
        }
        on(Orders) { event, metadata ->
            if (event is OrderPlaced) trigger(NewOrder(metadata.aggregateId.value, event.item))
        }
    }

    override suspend fun handle(
        trigger: SalesFeedPost,
        context: ReactionContext,
    ) = when (trigger) {
        is NewCustomer -> post("New customer: ${trigger.email}")
        is NewOrder -> post("New order ${trigger.orderId}: ${trigger.item}")
    }
}
```

- Each `on(...)` block gets its own aggregate's events, typed, so `when (event)` covers that aggregate's events
  only.
- `handle` gets every trigger, from either source, and `when (trigger)` covers each kind of work.
- With [ordering](#ordered-event-policies), each aggregate instance's work runs in order: a customer's and an order's
  work are ordered separately, and never wait for each other.

#### Delayed triggers

Give a trigger a `notBefore` and it doesn't run before that time. For example, ask for a review a week after an
order ships:

```kotlin
@Serializable
data class SendReviewReminder(
    val orderId: String,
)

class ReviewReminders :
    EventPolicy<SendReviewReminder>(
        name = "review-reminders",
        triggers = SendReviewReminder.serializer(),
    ) {
    init {
        on(Orders) { event, metadata ->
            if (event is OrderShipped) {
                trigger(SendReviewReminder(metadata.aggregateId.value), notBefore = metadata.timestamp + 7.days)
            }
        }
    }

    override suspend fun handle(
        trigger: SendReviewReminder,
        context: ReactionContext,
    ) {
        println("Asking for a review of order ${trigger.orderId}")
    }
}
```

- With db-scheduler, delayed work waits in `scheduled_tasks` until it is due, at no extra cost.
- If a scheduler delivers work early, kotmod puts it back until it is due. It doesn't run, and it doesn't count
  as a retry.
- [Ordered event policies](#ordered-event-policies) can't produce delayed triggers: one would hold back every later
  reaction of its aggregate. If one does, its event is parked (below), so the mistake shows up loudly without
  stopping anything else.

#### When a mapping fails

Sometimes an event policy can't turn an event into triggers: its `on(...)` block throws, the event can't be
deserialized, or an ordered event policy produces a delayed trigger. The reactor doesn't stop. It **parks** the
event for that event policy, as an item with id `<policy>/<eventId>/mapping`, logs the error, and moves on. Other
event policies still get their triggers for the event.

Parked mappings live in kotmod's `ddd_reaction_row` table, whichever scheduler you use:

- For an [ordered event policy](#ordered-event-policies), the parked mapping takes the event's place in its
  aggregate's line, so the aggregate's later work in that event policy waits behind it. Other aggregates and other
  event policies are unaffected.
- For an unordered event policy, it is kept on its own row, with its own task.

A parked event is retried with capped backoff (1s, 2s, 4s… up to 10 minutes), forever, logging each failure; it
is never dropped while the event policy still listens to its aggregate type. Each retry reads the event again and
runs the event policy's current code, so deploying a fix is enough. The event's triggers are then queued with the
ids they would have had, and run: in an ordered event policy they take the parked mapping's place in the line, so
they still run before the aggregate's later work. If the fixed code no longer listens to the event's aggregate
type, the parked event is dropped with a warning.

If an event will never map — say its payload is beyond repair — an operator can drop its parked mapping with
`ReactionOperations` (see [Ordered event policies](#ordered-event-policies)). `parkedMappings(policy)` lists an
event policy's parked events (event id, reaction id and failed attempts), and `skipParked(policy, eventId)` drops one
without retrying it: that event's work in the event policy never runs, and with ordering the aggregate's later work
can then run. Both work for ordered and unordered event policies. `skipParked` refuses a parked mapping that is being
retried at that moment (with `IllegalStateException`); try again shortly.

### Running event policies on db-scheduler

This is kotmod's ready-made scheduler, in the optional `kotmod-db-scheduler` module. It needs nothing but your
Postgres database; to use a different one instead, see [Using another scheduler](#using-another-scheduler).

`DbSchedulerTaskScheduler` stores tasks in db-scheduler's `scheduled_tasks` table and runs them on your db-scheduler
`Scheduler`. Your application owns the `Scheduler` — its threads, polling and lifecycle. One
`DbSchedulerTaskScheduler` serves a whole context: each event policy, and each [process manager](#process-managers)
channel, becomes one db-scheduler task named after it, and each piece of work is an instance of that task. Register
every event policy before reading `scheduler.tasks`:

```kotlin
fun startReactions(
    dataSource: DataSource,
    jdbc: JdbcContext,
    election: PostgresLeaderElection,
): Pair<EventReactor, Scheduler> {
    val scheduler = DbSchedulerTaskScheduler()
    val reactor = EventReactor(jdbc, scheduler, isLeader = election::isLeader)
    reactor.register(OrderNotifications(::sendConfirmation))
    reactor.register(ReviewReminders())
    reactor.register(OrderStatusProjection(jdbc))
    val dbScheduler =
        Scheduler
            .create(dataSource, *scheduler.tasks.toTypedArray())
            .threads(10)
            .enableImmediateExecution()
            .build()
    scheduler.bind(dbScheduler)
    reactor.start()
    dbScheduler.start()
    return reactor to dbScheduler
}
```

How it behaves:

- **Duplicates.** Scheduling work that is already pending does nothing.
- **Retries.** A `Retry(delay)` reschedules the same task instance. kotmod counts the attempts itself (in the task's
  data for unordered work, in `ddd_reaction_row` for ordered work), so backoff keeps growing across restarts.
- **Startup order.** If the `Scheduler` runs work before the reactor has started, the work is pushed back 5
  seconds (`unsubscribedRetryDelay`, without counting an attempt) and a warning is logged.
- **Unreadable data.** If stored work can't be decoded — say a trigger class was renamed — it is retried until a
  fix is deployed. Unordered work is retried with db-scheduler's backoff, from 10 seconds up to 1 hour. Ordered
  work keeps its lease when decoding fails, so it is retried about every `timeout` plus 30 seconds, and each try
  counts an attempt. Keep old names readable with `@SerialName`.
- **Removing pending work.** To stop pending unordered work for good, cancel its task instance; its id is the
  reaction id, `<policy>/<eventId>/<n>`. Ordered work lives in `ddd_reaction_row`, not in its task (see
  [Ordered event policies](#ordered-event-policies)).

```kotlin
fun cancelPendingConfirmation(
    dbScheduler: Scheduler,
    eventId: EventId,
) {
    dbScheduler.cancel(TaskInstanceId.of("order-notifications", "order-notifications/${eventId.value}/0"))
}
```

### Ordered event policies

By default an event policy's work is unordered: two pieces of work from the same aggregate can run at the same time,
or finish in a different order from the events. That is fine for sending emails, but not for projections or
anything else that must apply an aggregate's changes in order. For those, override `ordering`:

- `ReactionOrdering.Unordered` is the default.
- `ReactionOrdering.PerAggregate(onGiveUp = …)` runs an aggregate's work in this event policy one at a time, in event
  order.

```kotlin
@Serializable
sealed interface OrderStatusChange

@Serializable
data class StatusChanged(
    val orderId: String,
    val status: String,
    val sequence: Long,
) : OrderStatusChange

class OrderStatusProjection(
    private val jdbc: JdbcContext,
) : EventPolicy<OrderStatusChange>(
        name = "order-status-projection",
        triggers = OrderStatusChange.serializer(),
    ) {
    init {
        on(Orders) { event, metadata ->
            val status =
                when (event) {
                    is OrderPlaced -> "placed"
                    is OrderShipped -> "shipped"
                    is OrderCancelled -> "cancelled"
                }
            trigger(StatusChanged(metadata.aggregateId.value, status, metadata.sequence))
        }
    }

    override val ordering = ReactionOrdering.PerAggregate(onGiveUp = OnGiveUp.BlockAggregate)

    override suspend fun handle(
        trigger: OrderStatusChange,
        context: ReactionContext,
    ) = when (trigger) {
        is StatusChanged -> saveStatus(trigger)
    }

    // Writes only if the stored status came from an earlier event, so a stale re-run can't overwrite a newer one.
    private fun saveStatus(change: StatusChanged) {
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO order_status (id, status, sequence) VALUES (?, ?, ?) " +
                        "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, sequence = EXCLUDED.sequence " +
                        "WHERE order_status.sequence < EXCLUDED.sequence",
                ).use { ps ->
                    ps.setString(1, change.orderId)
                    ps.setString(2, change.status)
                    ps.setLong(3, change.sequence)
                    ps.executeUpdate()
                }
        }
    }
}
```

The reactor reads each aggregate's events in sequence order (see `metadata.sequence`), even in the rare case
where the order in the log differs because a transaction changed several aggregates. For an aggregate that has
never been written out of order, checking this is a single primary-key lookup per event. Ordering is per
aggregate instance and per event policy: different aggregates never wait on each other, and neither do different
event policies. A policy with several sources gets ordering per aggregate of each; since an aggregate type
reaches a policy through only one source, their orders never mix.

**Each aggregate's line.** kotmod keeps an ordered event policy's work in its own table, `ddd_reaction_row`: one line
per aggregate (keyed `<aggregateType>/<aggregateId>`) and event policy, in event order. Only the front of each line
is scheduled, as one task. When it finishes, kotmod removes it and schedules the next one straight away. Nothing
waits and checks again, and ordering works the same on every scheduler. Each change to a line runs in a short
transaction that holds a Postgres advisory lock on that line; `handle`, `onFailure` and `onCompletion` never run
while it is held. One aggregate's work in an event policy therefore runs on at most one thread at a time; different
aggregates still run in parallel.

**Attempts and leases.** An ordered attempt is counted when it starts, so a crash while `handle` runs counts as a
failed attempt, and `context.attempt` shows it. A graceful shutdown that interrupts the attempt gives it back.
While an attempt runs, its work is leased for the event policy's `timeout` plus 30 seconds: a duplicate delivery
waits until the lease ends, and after a crash the work runs again once its lease has expired. `onFailure` and
`onCompletion` run inside the lease but outside the timeout, so keep them short.

**When work gives up.** Work gives up when `onFailure` returns `GiveUp`. The `OnGiveUp` setting says what happens
to the aggregate's later work in this event policy:

| Setting | Behaviour |
|---|---|
| `OnGiveUp.ContinueWithNext` (default) | The failed work is completed as given up and the next one runs |
| `OnGiveUp.BlockAggregate` | The aggregate's later work waits until an operator retries or skips the failed one |

Use `BlockAggregate` when running later work after a missed one would leave wrong data, such as a projection that
skipped an event. Other aggregates are not affected. To find and clear blocked work, use `ReactionOperations`
(in `io.kotmod.reaction`), built from your `JdbcContext` and your scheduler, with the event policy's name. It works the
same on every scheduler and doesn't need a running reactor, so you can call it from an admin endpoint or a script:

- `blockedReactions(policy)` lists each blocked reaction with its line's key, reaction id, sequence number and
  attempts.
- `retryBlocked(policy, id)` runs it again now, with its attempt count reset.
- `skipBlocked(policy, id)` drops it without running it, and schedules the aggregate's next work.
- `parkedMappings(policy)` and `skipParked(policy, eventId)` do the same for
  [parked mappings](#when-a-mapping-fails).

Each change takes the line's lock, so it is safe while the reactor and the scheduler run. A change to work that
isn't there any more (already retried, skipped or finished) throws `IllegalArgumentException`. The changes are
`suspend` functions; the lists are not.

```kotlin
suspend fun retryBlockedProjection(
    jdbc: JdbcContext,
    scheduler: TaskScheduler,
) {
    val operations = ReactionOperations(jdbc, scheduler)
    for (blocked in operations.blockedReactions("order-status-projection")) {
        println("${blocked.key} is held back by ${blocked.reactionId.value} at sequence ${blocked.sequence}")
        operations.retryBlocked("order-status-projection", blocked.reactionId)
    }
}
```

**Repair sweep.** Finishing ordered work and scheduling the next is two writes, one to Postgres and one to the
scheduler; after a crash between them, the old task is delivered again and schedules the line's current front. In
case a scheduler loses a task anyway, the reactor runs a repair sweep on the leader while it is reading normally:
after its first successful read, and then about every 10 minutes, it schedules again every line front and parked
mapping that isn't blocked or leased and has sat idle for 30 minutes. Scheduling is idempotent, so the sweep changes
nothing when no task was lost. The sweep runs after a successful read, so it doesn't run while the reader is stuck
on an event that keeps failing (say, while the scheduler is down). Process managers sweep their own queues the same
way.

**Delivery is still at least once, and a re-run can be out of order.** Work queued again after it finished runs
again, and it can run after (or at the same time as) the aggregate's later work: once work has finished its row is
gone, so its line no longer knows where it belonged. This happens if the reactor crashes after queueing work but
before saving its position, if two nodes briefly both poll (see
[Running in production](#running-in-production)), or if you move the reactor's position back. Idempotency alone
doesn't prevent the damage: re-applying an old status after a newer one is idempotent, and still overwrites the
newer status. Make ordered work that writes state guard on the event's sequence, as `OrderStatusProjection` does:
carry `metadata.sequence` in the trigger, and write only if the stored sequence is lower.

### Using another scheduler

db-scheduler is a convenient default, not a requirement. The reactor and process managers only need a
`TaskScheduler` (in `io.kotmod.scheduling`, in the core `kotmod` module), which runs named tasks at a time. kotmod
keeps ordering, attempt counts, blocked work and parked mappings itself, in `ddd_reaction_row`, so a scheduler
doesn't need to order anything or count retries. The interface, without its comments:

<!-- not-compiled -->
```kotlin
interface TaskScheduler {
    fun queue(name: String): TaskQueue
}

interface TaskQueue {
    suspend fun schedule(name: String, payload: String, at: Instant)

    fun subscribe(handler: suspend (name: String, payload: String) -> TaskOutcome): Cancellable
}

sealed interface TaskOutcome {
    data object Done : TaskOutcome

    data class RunAgain(val at: Instant, val payload: String) : TaskOutcome
}
```

What a scheduler must do:

- **Queues.** kotmod asks for one queue per event policy (named after it) and one per process manager channel
  (`<process type>-<channel>`), before starting. Asking for a name again returns a queue for the same tasks.
- **`schedule(name, payload, at)`** runs the task at `at` or later. Scheduling a name that is already pending in the
  queue does nothing. kotmod sometimes schedules a name again after its earlier task finished: an operator's
  `retryBlocked` and the repair sweep schedule a line's front under its usual name. A scheduler must accept that
  (db-scheduler does). One that refuses recently finished names, as Cloud Tasks does, must still make such a
  schedule succeed, for example by deriving a unique name.
- **`subscribe(handler)`** delivers due tasks, at least once each and never before their `at`, until the returned
  handle is cancelled. On `Done`, remove the task. On `RunAgain(at, payload)`, run it again at `at` or later with
  the new payload, under the same name (a scheduler that can't edit tasks can create a follow-up task and finish the
  original). If the handler throws, deliver the task again later, after your own backoff.
- **Names and payloads are opaque.** Payloads are kotmod's own JSON. An adapter may transform names (hash them, say)
  as long as one name always means one task.

Timeouts, `onFailure`, `onCompletion`, ordering, blocked work and parked mappings then work the same on your scheduler
as on db-scheduler. As with db-scheduler, every node can subscribe; only the reactor, contracts and process managers
need [one active poller](#running-in-production). Leave out the `kotmod-db-scheduler` dependency if you don't use it.

**Testing a scheduler.** `TaskSchedulerContract`, in `kotmod`'s test fixtures (`kotmod/src/testFixtures`), is the
JUnit test suite every scheduler must pass; `kotmod-db-scheduler` runs it. Extend it and implement `scheduler()`,
`start()` and `stop()`. The test fixtures aren't published to Maven Central, so copy the class (and the
`eventually` helper it uses) from the repository.

**Google Pub/Sub doesn't qualify.** It can't hold a message back until a chosen time, or run one again at a chosen
time, so it can't honour `at` or `RunAgain`. A ready-made Cloud Tasks scheduler is planned.

### Publishing events to other contexts

Internal domain events change as your model changes, so other contexts shouldn't depend on them directly.
Instead, publish **public events** — a deliberately stable contract — with a `PublicEventContract`:

```kotlin
@Serializable
sealed interface OrderPublicEvent : PublicDomainEvent

@Serializable
data class OrderPlacedV1(
    val item: String,
) : OrderPublicEvent

fun orderContract(
    jdbc: JdbcContext,
    offsets: PostgresOffsetManager,
): PublicEventContract<OrderEvent, OrderPublicEvent> =
    PublicEventContract(
        backend = PostgresDomainPollingBackend(jdbc),
        serialization = Orders.eventSerialization,
        internalToPublic = { event ->
            when (event) {
                is OrderPlaced -> OrderPlacedV1(event.item)
                else -> null
            }
        },
        getPosition = { offsets.getPosition("order-contract") },
        savePosition = { offsets.savePosition("order-contract", it) },
        isLeader = { true },
        aggregateTypes = setOf(Orders.type),
    )
```

- `internalToPublic` maps each internal event to a public one; returning `null` keeps it private.
- Event policies in other contexts react to the public events with `on(contract)`, each event with the original
  event's metadata (event id, aggregate id, sequence and so on); see
  [Consuming another context's events](#consuming-another-contexts-events).
- The publishing context builds and starts the contract. Register the event policies that listen to it before it
  starts.
- A contract reads the event log independently of the reactor, so give it its own consumer name.
- `aggregateTypes` lists the aggregate types the contract publishes; events of other types are skipped
  without being deserialized. The event log holds every aggregate's events, so with a filter, adding an
  aggregate or a [process manager](#process-managers) to the context never stalls this contract on an event
  type its `serialization` can't read. An event policy can combine a contract with other sources only when the
  contract has this filter.
- Without `aggregateTypes`, a contract deserializes **every** event in the log before mapping it, so its
  `serialization` must be able to read every event type your application writes, including the facts a
  process manager records. If you have several event families (orders and audit events, say), register them
  all in one `jsonDataSerializationContext<DomainEvent>` and use `DomainEvent` as the contract's internal type.

### Consuming another context's events

An event policy reacts to another context's public events with `on(contract)`, typed like any other source:

```kotlin
@Serializable
sealed interface BillingTrigger

@Serializable
data class ChargeCustomer(
    val orderId: String,
) : BillingTrigger

interface PaymentGateway {
    suspend fun charge(
        orderId: String,
        idempotencyKey: String,
    )
}

class CustomerBilling(
    orderEvents: PublicEventContract<*, OrderPublicEvent>,
    private val gateway: PaymentGateway,
) : EventPolicy<BillingTrigger>(
        name = "customer-billing",
        triggers = BillingTrigger.serializer(),
    ) {
    init {
        on(orderEvents) { event, metadata ->
            when (event) {
                is OrderPlacedV1 -> trigger(ChargeCustomer(metadata.aggregateId.value))
            }
        }
    }

    override suspend fun handle(
        trigger: BillingTrigger,
        context: ReactionContext,
    ) = when (trigger) {
        is ChargeCustomer -> gateway.charge(trigger.orderId, idempotencyKey = context.reactionId)
    }
}

fun startBilling(
    dataSource: DataSource,
    jdbc: JdbcContext,
    orderEvents: PublicEventContract<*, OrderPublicEvent>,
    gateway: PaymentGateway,
): Scheduler {
    val scheduler = DbSchedulerTaskScheduler()
    val reactor = EventReactor(jdbc, scheduler, isLeader = { true }, name = "billing-reactor")
    reactor.register(CustomerBilling(orderEvents, gateway))
    val dbScheduler = Scheduler.create(dataSource, *scheduler.tasks.toTypedArray()).enableImmediateExecution().build()
    scheduler.bind(dbScheduler)
    reactor.start()
    dbScheduler.start()
    return dbScheduler
}
```

- `startBilling` builds the billing context's own reactor, named `billing-reactor`. Every reactor on the same
  database needs its own name, because the name is where it saves its position; two reactors sharing `reactor`
  would share one position and skip events.
- Registering the event policy subscribes it to the contract's own reader, which queues its triggers in the
  policy's queue. Register it before the publishing context starts the contract.
- Reaction ids and ordering work as for local sources: ids are `<policy>/<eventId>/<n>` with the original
  event's id, and ordering is per original aggregate.
- If the event policy's block throws, the event is [parked](#when-a-mapping-fails), as for a local source, and the
  contract's reader moves on. If the contract itself can't read an event (its `serialization` or `internalToPublic`
  throws), the contract stops at that event, for everyone listening, until the publishing context fixes it.
- Both contexts share the database. Consuming a context that lives in another service or database is not
  supported.

### Process managers

An aggregate receives commands and emits events. A **process manager** is the mirror image: it receives events,
keeps its own state, and emits commands. Use one for a long-running workflow that spans several aggregates or
needs to wait ("if the order hasn't shipped within two days, cancel it"). A process manager never does anything
synchronously: it asks for commands to be run and for inputs to be delivered to it later. (kotmod says process
manager, not saga.)

The example below is a dispatch deadline: when an order is placed, wait two days; if it still hasn't shipped,
cancel it and record that the deadline was missed. If the customer cancels first, the deadline is abandoned.
A new process manager starts from the head of the event log as of its first poll as leader, so it sees events
written from then on (plus any committed just before by a transaction that was still open), not earlier ones;
to cover orders already in flight, pass `startFrom = StartFrom.Beginning` when it asks `PostgresOffsetManager`
for its position.

#### Inputs: the anti-corruption layer

A process manager only ever sees its own input type. Everything else is translated into it. `translate` turns
an event from this context into a `(processId, input)` pair, or `null` to ignore it, and
`subscribeTo(name, contract) { envelope -> … }` does the same for the public events of another context.
Inputs are facts, so they can't be rejected.

```kotlin
@Serializable
sealed interface DispatchDeadlineInput

@Serializable
data class OrderWasPlaced(
    val orderId: String,
    val placedAt: Instant,
) : DispatchDeadlineInput

@Serializable
data object OrderWasShipped : DispatchDeadlineInput

@Serializable
data object OrderWasCancelled : DispatchDeadlineInput

@Serializable
data object DeadlinePassed : DispatchDeadlineInput

@Serializable
data class CancellationRefused(
    val rejection: OrderRejection,
) : DispatchDeadlineInput
```

```kotlin
fun translateOrderEvent(
    event: PersistedEvent,
    serialization: DataSerializationContext<OrderEvent>,
): Pair<AggregateId, DispatchDeadlineInput>? {
    if (event.metadata.aggregateType != Orders.type) return null
    val deadline = AggregateId("deadline-${event.metadata.aggregateId.value}")
    return when (serialization.deserialize(event.serialized)) {
        is OrderPlaced -> deadline to OrderWasPlaced(event.metadata.aggregateId.value, event.metadata.timestamp)
        is OrderShipped -> deadline to OrderWasShipped
        is OrderCancelled -> deadline to OrderWasCancelled
    }
}
```

The process id (`deadline-<orderId>`) names an instance of the process manager's own aggregate type, not the
order.

`translate` receives every event in this context's log except this process manager's own. Facts recorded by
other process managers do reach it. Filter by aggregate type before deserializing, as `translateOrderEvent` does.

#### States own their inputs

A process is an aggregate whose commands are its inputs: each state decides what an input means. `handle`
returns `transition(newState, events, commands, schedule)` or `ignore()`. An input that is ignored for a process
that doesn't exist yet creates nothing.

```kotlin
@Serializable
sealed interface DispatchDeadlineEvent : DomainEvent

@Serializable
data class DispatchDeadlineMissed(
    val orderId: String,
) : DispatchDeadlineEvent

typealias DispatchDeadlineOutcome = ProcessOutcome<DispatchDeadline, DispatchDeadlineEvent, DispatchDeadlineInput>

sealed interface DispatchDeadline : ProcessState<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent>

object NoDispatchDeadline : ProcessInitialState<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent> {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            is OrderWasPlaced ->
                transition(
                    AwaitingDispatch(input.orderId),
                    schedule = listOf(schedule(DeadlinePassed, at = input.placedAt + 2.days)),
                )
            OrderWasShipped, OrderWasCancelled, DeadlinePassed, is CancellationRefused -> ignore()
        }
}

data class AwaitingDispatch(
    val orderId: String,
) : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            OrderWasShipped -> transition(Dispatched)
            OrderWasCancelled -> transition(Abandoned)
            DeadlinePassed ->
                transition(
                    Missed,
                    events = listOf(DispatchDeadlineMissed(orderId)),
                    commands = listOf(Orders.command(AggregateId(orderId), CancelOrder("not shipped within 2 days"))),
                )
            is OrderWasPlaced, is CancellationRefused -> ignore()
        }
}

data object Dispatched : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome = ignore()
}

data object Abandoned : DispatchDeadline {
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome = ignore()
}

data object Missed : DispatchDeadline {
    // A cancellation refused because the order has shipped: it shipped before the process heard about it.
    override suspend fun handle(input: DispatchDeadlineInput): DispatchDeadlineOutcome =
        when (input) {
            is CancellationRefused ->
                when (input.rejection) {
                    OrderAlreadyShipped -> transition(Dispatched)
                    else -> ignore()
                }
            OrderWasShipped, OrderWasCancelled, DeadlinePassed, is OrderWasPlaced -> ignore()
        }
}
```

#### Timeouts

`schedule(input, at)` delivers an input to the same process instance at the given time. There is no way to
cancel one: a state that has moved on simply ignores it, as `Dispatched` does with `DeadlinePassed` above. A
timeout delivered early waits until it is due.

#### When timeouts go stale

A timeout is never cancelled, so when one arrives the state decides whether it still means anything.

Usually the state has moved on. A state that no longer waits for the timeout ignores it, as `Dispatched` and
`Abandoned` ignore `DeadlinePassed` above. There is nothing to do beyond writing each state's `when`.

The harder case is a state that armed another timeout. Say an invoice's due date can be moved: the process
schedules `PaymentOverdue` for the new date, but the one for the old date is still queued. Both arrive while the
process is in `AwaitingPayment`, and the state can't tell them apart unless they say what they are about. So a
timeout carries what it is about, and the state remembers which one it is waiting for. A timeout that doesn't
match is stale and is ignored. This is cancellation without cancelling.

```kotlin
@Serializable
sealed interface PaymentReminderInput

@Serializable
data class InvoiceIssued(
    val invoiceId: String,
    val dueAt: Instant,
) : PaymentReminderInput

@Serializable
data class DueDateMoved(
    val until: Instant,
) : PaymentReminderInput

@Serializable
data object InvoicePaid : PaymentReminderInput

@Serializable
data class PaymentOverdue(
    val dueAt: Instant,
) : PaymentReminderInput
```

```kotlin
data class AwaitingPayment(
    val invoiceId: String,
    val dueAt: Instant,
) : PaymentReminder {
    override suspend fun handle(input: PaymentReminderInput): PaymentReminderOutcome =
        when (input) {
            is DueDateMoved ->
                transition(
                    copy(dueAt = input.until),
                    schedule = listOf(schedule(PaymentOverdue(input.until), at = input.until)),
                )
            is PaymentOverdue ->
                if (input.dueAt != dueAt) {
                    ignore()
                } else {
                    transition(Overdue, events = listOf(InvoiceWentOverdue(invoiceId)))
                }
            InvoicePaid -> transition(Paid)
            is InvoiceIssued -> ignore()
        }
}
```

If the due date moves from the 10th to the 20th, the timeout for the 10th arrives with `dueAt` of the 10th, which
is not the state's `dueAt`, and is ignored. The one for the 20th matches and moves the invoice to `Overdue`.

kotmod doesn't generate timeout ids for you. A decision can run again on a conflict retry, and a generated id
would be different each time, so the retry would schedule a different timeout. Take the value that identifies a
timeout from the domain instead: a due time, or a sequence number you keep in state.

#### Commands to other aggregates

`Orders.command(id, command)` only accepts commands of that aggregate kind, so a wrong command doesn't compile.
kotmod runs it through the target's `AggregateManager.handle` with a command id derived from the request, so a
redelivery gets the same answer instead of running twice. `target(orders) { command, rejection -> input }` says
how to turn a typed rejection back into an input for the process, like `CancellationRefused` above.

#### Facts the process owns

`transition(events = …)` records events in the process's own stream (here `DispatchDeadlineMissed`, which says
the deadline passed before the process saw a shipment; a cancellation refused because the order has shipped is
how the process learns otherwise). They are internal, like any domain events, and they sit in the log with
everything else, so a contract's `serialization` must be able to read them, unless the contract lists the
aggregate types it publishes (`aggregateTypes`) and leaves the process manager's out. To tell other contexts
about them, publish them through a `PublicEventContract`, as in
[Publishing events to other contexts](#publishing-events-to-other-contexts).

#### Wiring with db-scheduler

```kotlin
fun dispatchDeadlines(
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    deadlines: Repository<DispatchDeadline>,
    deadlineEvents: DataSerializationContext<DispatchDeadlineEvent>,
    scheduler: TaskScheduler,
): ProcessManager<DispatchDeadline, DispatchDeadlineInput, DispatchDeadlineEvent> {
    val offsets = PostgresOffsetManager(jdbc)
    return ProcessManager(
        type = AggregateType("DispatchDeadline"),
        repository = deadlines,
        jdbc = jdbc,
        initial = NoDispatchDeadline,
        inputSerializer = DispatchDeadlineInput.serializer(),
        inputOrdering = ReactionOrdering.PerAggregate(),
        eventSerialization = deadlineEvents,
        translate = { event -> translateOrderEvent(event, serialization) },
        targets = listOf(target(orders) { _, rejection -> CancellationRefused(rejection) }),
        scheduler = scheduler,
        getPosition = { offsets.getPosition("dispatch-deadlines") },
        savePosition = { offsets.savePosition("dispatch-deadlines", it) },
        isLeader = { true },
    )
}

fun startDispatchDeadlines(
    dataSource: DataSource,
    jdbc: JdbcContext,
    serialization: DataSerializationContext<OrderEvent>,
    orders: AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>,
    deadlines: Repository<DispatchDeadline>,
    deadlineEvents: DataSerializationContext<DispatchDeadlineEvent>,
): Scheduler {
    val scheduler = DbSchedulerTaskScheduler()
    val process = dispatchDeadlines(jdbc, serialization, orders, deadlines, deadlineEvents, scheduler)
    val dbScheduler = Scheduler.create(dataSource, *scheduler.tasks.toTypedArray()).enableImmediateExecution().build()
    scheduler.bind(dbScheduler)
    process.start()
    dbScheduler.start()
    return dbScheduler
}
```

- Start the process manager before the db-scheduler `Scheduler`, and stop it after. A stopped process manager
  can't be started again; build a new one.
- Read `scheduler.tasks` only after the process manager and all its `subscribeTo` calls are built. A channel
  created later wouldn't be registered with the scheduler.
- Per-aggregate ordering (`inputOrdering`) delivers an order's events in order, which this process relies on:
  an input the initial state ignores is gone, so out-of-order delivery could lose "shipped".
- There is one channel per kind of work, each a db-scheduler task named after the process type: inputs (ordered
  per source aggregate here, `DispatchDeadline-inputs`), internal (timeouts and rejection feedback,
  `DispatchDeadline-internal`), commands (`DispatchDeadline-commands`), and `DispatchDeadline-contract-<name>` for each
  `subscribeTo`. One `DbSchedulerTaskScheduler` serves every process manager and event policy of the context.
- Ordered inputs wait in their source aggregate's line in `ddd_reaction_row`, one line per channel, exactly as an
  [ordered event policy](#ordered-event-policies)'s work does.
- Inputs are stored as JSON with the input class's name. Renaming an input class breaks inputs that are already
  scheduled or in flight, so keep the old name with `@SerialName`.

#### When things fail

Input delivery and commands are retried with capped backoff and never given up on. A command for an aggregate
type that isn't one of the process manager's `targets` fails loudly when the process decides on it: nothing is
recorded, and it is retried until you fix the wiring.

- A `translate` that throws stops the poller at that event: its batch is retried on every poll, and nothing
  after it reaches the process manager. The poller also hands out the commands and timeouts the process asks
  for, so those wait too.
- With ordered inputs, an input that keeps failing holds back the later inputs from the same source aggregate
  until it succeeds.
- Timeouts wait in the scheduler until they are due. If a scheduler delivers one early (one that can only
  schedule a limited time ahead, say), kotmod schedules it again for its time without running it.

## Running in production

**Delivery is at-least-once.** Events are committed with the state change that produced them, and the
reactor only moves past an event once every event policy's triggers for it are queued, so no event is ever skipped.
Work can run more than once — for example if the process dies after queueing but before saving the reactor's
position, or if a shutdown interrupts running work. Make `handle` and `onCompletion` idempotent, and pass
`context.reactionId` as the idempotency key of external calls.

**Open transactions hold delivery back.** The reactor only reads past transactions that have finished, so it
never skips an event that a slower transaction commits late. The flip side: while any transaction that has
written something is still open on the same Postgres server — even in another database — later events wait
for it, and if it never finishes, **delivery stops for the reactor, every contract and every process manager**
with no error. Common culprits are connections left "idle in transaction", orphaned prepared transactions
(`pg_prepared_xacts`) and long batch jobs. Keep transactions short, set `idle_in_transaction_session_timeout`,
and monitor `pg_stat_activity` for old transactions with a `backend_xid`.

**Moving the database to a new server.** Event positions include Postgres transaction ids, which only make
sense on the server that issued them. `pg_upgrade` keeps them, so in-place upgrades are fine. After a
`pg_dump`/restore or a logical-replication migration, the new server's transaction ids start lower, and the
reactor, contracts and process managers stop with an error saying their saved position is "ahead of this
Postgres server's transaction counter" rather than silently skipping events. To resume after the move, with
nothing writing yet, set every row's `ddd_domain_event.transaction_id` to `'0'` and every
`ddd_consumer_offset.last_transaction_id` to `0` (keep `last_offset`). Each consumer then resumes exactly where
it left off, and new events sort after the migrated ones.

**Reaction ids are deterministic.** kotmod builds each reaction id from the event policy, the event and the
trigger's position in the block's output (`<policy>/<eventId>/<n>`), so an event read again while its work is
pending is recognised. Keep each `on(...)` block deterministic: the same event must produce the same triggers, in
the same order. A scheduler forgets an id once its work has run, so moving the reactor's position back runs finished
work again.

**Start and stop in order.** Register every event policy and build every process manager, then read
`scheduler.tasks`, build the db-scheduler `Scheduler` and call `scheduler.bind(dbScheduler)`. Start the reactor and
process managers, then the `Scheduler`, then the leader election, then any public contracts. To stop:

1. Stop the public contracts.
2. Stop the `Scheduler`, so no more work runs on this node.
3. Stop the reactor and process managers. They stop reading, then stop handling their queues; anything they
   queued in between waits in `scheduled_tasks` and `ddd_reaction_row` for the next node.
4. Stop the leader election, which hands the lock to another node.

Getting it wrong doesn't lose anything — work that arrives before its event policy is running is rescheduled with a
warning — but it adds noise and delay.

**Run one active poller per consumer.** The reactor, public contracts and process managers only poll while
`isLeader()` returns `true`. Run your application on as many nodes as you like, but make sure only one of them
polls for each. db-scheduler needs no such care: it is safe to run on every node, and each piece of work runs on
one node at a time.

**Leader election.** `PostgresLeaderElection` picks the polling node with a Postgres advisory lock. Each
node creates one with the same name; whichever takes the lock leads until its connection ends, and another
node takes over within a few seconds:

```kotlin
fun leaderElection(
    url: String,
    user: String,
    password: String,
): PostgresLeaderElection {
    val election = PostgresLeaderElection({ DriverManager.getConnection(url, user, password) }, "order-service")
    election.start()
    return election
}
```

Pass `isLeader = election::isLeader` to the reactor, and to each contract and process manager:

```kotlin
fun reactorWithLeaderElection(
    jdbc: JdbcContext,
    scheduler: TaskScheduler,
    election: PostgresLeaderElection,
): EventReactor = EventReactor(jdbc, scheduler, isLeader = election::isLeader)
```

- **One election per application** is the simple default: one node polls for every consumer. To spread
  consumers across nodes, give each consumer its own election (its own name); each holds one connection.
- **Give it its own connection.** The election holds one connection for as long as it runs, so open it
  directly rather than from your pool. It does not work through PgBouncer in transaction mode.
- **How fast it reacts.** Every `checkInterval` (5 seconds by default) a follower tries to take the lock and
  the leader checks its connection. A leader whose checks fail or stall for two intervals stops polling.
  The election also sets TCP keepalives on its session, so if the leader's host vanishes Postgres notices
  within a few intervals and releases the lock.
- **Brief overlap.** If Postgres ends the leader's session first (a failover, `pg_terminate_backend`), another
  node can take over before the old leader notices. Pollers check `isLeader()` only at the start of each
  poll, so two nodes may poll for up to about two `checkInterval`s plus one poll batch. That can queue the same
  work twice. Deterministic reaction ids absorb it while the work is still pending; work queued again after it
  finished runs again, and for an ordered event policy possibly after the aggregate's later work. An idempotent
  `handle` covers unordered work; ordered work that writes state should also guard on the event's sequence (see
  [Ordered event policies](#ordered-event-policies)).
- **Shutdown.** Stop the contracts, then the `Scheduler`, then the reactor and process managers, as described
  above, and call `election.stop()` last. It releases the lock, so another node takes over straight away.

**Know what happens when things fail:**

| Situation | Behaviour |
|---|---|
| `handle` throws or times out | `onFailure` decides: `Retry(delay)` or `GiveUp` (by default it retries with capped backoff, forever). With ordering, only that aggregate's later work in that event policy waits |
| An event policy's `on(...)` block throws, or its event can't be deserialized | The event is [parked](#when-a-mapping-fails) for that event policy and retried with capped backoff, forever, logging each failure; other event policies and the reactor carry on |
| `onFailure` or `onCompletion` throws | The work is retried after a backoff, so `handle` may run again |
| Stored work can't be read (e.g. a trigger class was renamed) | Unordered work is retried with backoff from 10 seconds up to 1 hour; ordered work about every `timeout` plus 30 seconds, counting an attempt each time |
| A node crashes mid-work | db-scheduler notices the missing heartbeat and runs it again. Ordered work runs again once its lease has expired, and the crash counts as an attempt |
| The scheduler loses a task | For ordered work and parked mappings, the [repair sweep](#ordered-event-policies) schedules it again within about 40 minutes, as long as the reactor (or process manager) is reading normally |
| The database or scheduler is down while the reactor queues work | The reactor stops the batch and resumes from its last saved position on the next poll |
| A contract can't read or map an event | The contract stops at that event and retries it every poll, for everyone listening to it |
| An event policy fed by a contract can't queue its work (the scheduler is down, or `DbSchedulerTaskScheduler` isn't bound yet) | The contract's reader stops at that event and retries it every poll, for everyone listening to the contract |
| A command loses a concurrent update | `handle` reads and decides again, up to `maxConflictRetries` times (5 by default). `OptimisticConcurrencyException` only surfaces when those run out: reduce contention on that aggregate or raise `maxConflictRetries`. Inside an outer `jdbc.transaction { }` there are no retries: retry the whole transaction (see [Several aggregates in one transaction](#several-aggregates-in-one-transaction)) |

**Tune throughput.** The reactor, contracts and process managers poll every 500ms (`pollInterval`) and read up
to 100 events per poll (`batchSize`). `Scheduler.threads(n)` caps how much work runs at once.

## Known limitations

These are known gaps in the current release. None of them loses events; most need an unusual setup or a
failure in a specific spot to show up.

**Commands**

- **Renaming a rejection class breaks reading back old rejections.** Recorded rejections are plain JSON,
  without the versioned migrations events have. If a duplicate of a command rejected under the old name
  arrives, `handle` throws `RejectionDeserializationException`. Keep old names readable with `@SerialName`.
  Duplicates normally arrive within minutes of the original, so this rarely matters.

**Leader election**

- **`stop()` must not be cancelled.** If the coroutine calling `election.stop()` is cancelled part-way, the
  lock may stay held, with its connection open, until the process exits, and no other node can lead.
  Call it from a shutdown path that isn't cancelled, or use the blocking `election.close()`.
- **Don't call `start()` and `stop()` at the same time from different threads.** Doing so can leave two
  background loops sharing one connection. Starting again after `stop()` has returned is fine.
- **A JVM `Error` stops the election silently.** An `Error` such as `OutOfMemoryError` thrown inside the
  election's background loop ends the loop without a log line. If it happened while leading, the node keeps
  the lock (and reports itself as leader until the lease runs out) until the process exits. Restart the
  process.
- **Some transitions aren't logged.** When a hung check lets the lease run out, nothing is logged until the
  check finally fails. If a check takes the lock while `stop()` is running, the "Stepped down" line is
  skipped.

**Event policies**

- **Replays repeat finished work.** A scheduler recognises a reaction id only while its work is pending. Moving the
  reactor's position back, or a crash between queueing work and saving the position after the work has already
  run, runs it again. `handle` must be idempotent. In an ordered event policy the re-run can come after the
  aggregate's later work, so ordered work that writes state should also guard on the event's sequence (see
  [Ordered event policies](#ordered-event-policies)).
- **An event policy's blocks must be deterministic.** Reaction ids number an event's triggers by their position in the
  block's output, so a re-read event is only recognised if the block returns the same triggers, in the same
  order.
- **A new event policy doesn't see history.** It shares the reactor's position, so it sees events from when it is
  first deployed. Backfilling one event policy is not supported.
- **A crash during unordered work doesn't count as an attempt.** Unordered work counts failures only, so work that
  crashes its node every time is delivered again and again, with no attempt count for `onFailure` to give up on.
  Ordered work counts attempts when they start, so a crash counts there.
- **`onFailure` and `onCompletion` run outside the event policy's timeout.** Keep them short. For ordered work they
  run inside the lease (the timeout plus 30 seconds); one that runs past it lets a duplicate delivery start the
  same work again.
- **Removing an event policy abandons its work.** Its db-scheduler task is no longer registered, so its rows in
  `scheduled_tasks` never run. db-scheduler logs a warning when it finds them due, and once they have been due
  for longer than the `Scheduler`'s `deleteUnresolvedAfter` (14 days by default in db-scheduler 16.12.0) it
  deletes them. Its rows in `ddd_reaction_row` (ordered work and parked mappings) stay there: skip its parked
  mappings and blocked work with `ReactionOperations`, and delete any other rows yourself
  (`DELETE FROM ddd_reaction_row WHERE queue_name = '<policy>'`). Let its work finish before removing it.
- **Switching an ordered event policy to unordered can drop a delay.** If it has a parked mapping in a line when it
  is switched, and the fixed mapping then produces a delayed trigger, that trigger runs without its delay.
- **Aggregate types containing `/` can share a line.** A line's key is `"<aggregate type>/<aggregate id>"`, so
  type `a/b` with id `c` and type `a` with id `b/c` share one line. Their work then waits unnecessarily; nothing
  runs out of order.
- **Unreadable ordered work holds back its line unlisted.** Ordered work whose trigger can't be decoded is retried
  about every `timeout` plus 30 seconds until a fix is deployed (it keeps its lease when decoding fails), and each
  try counts an attempt. Meanwhile it holds back its aggregate's later work, and `blockedReactions` doesn't list it,
  because it hasn't given up.
- **Prompt hand-over needs immediate execution.** When ordered work finishes, the aggregate's next work is
  scheduled to run now. It only starts straight away if the `Scheduler` uses `enableImmediateExecution()`;
  otherwise it starts on db-scheduler's next poll.
- **Replaying an aggregate that was once written out of order is slow.** For such an aggregate, every event
  pays a full check whose cost grows with the aggregate's history. This only applies to aggregates written by
  an outer transaction that changed several aggregates in a racing order, and only matters for very long
  histories.

**Process managers**

- **A process manager can't be started again after `stop()`.** `start()` then does nothing. Build a new one (an
  `EventReactor` can be started again).

## Upgrading from 0.3

0.4.0 moves per-aggregate ordering out of the queue and into kotmod's own table, `ddd_reaction_row`, and runs all
reaction work on a small [`TaskScheduler`](#using-another-scheduler) interface. There is no compatibility layer. To
upgrade:

1. **Drain in-flight work first.** Work queued by 0.3 is stored in a different shape and wouldn't run. Stop
   writing commands and let pending reactions and process manager work finish (or cancel what you no longer
   need), then deploy 0.4.0.
2. **Create `ddd_reaction_row`.** Copy its statements from `DddSchema.ddl`:

   ```sql
   CREATE TABLE ddd_reaction_row (
       queue_name    TEXT        NOT NULL,
       reaction_id   TEXT        NOT NULL,
       kind          TEXT        NOT NULL,
       line_key      TEXT,
       line_sequence BIGINT,
       line_ordinal  INT,
       item          TEXT        NOT NULL,
       attempts      INT         NOT NULL DEFAULT 0,
       blocked       BOOLEAN     NOT NULL DEFAULT FALSE,
       lease_until   TIMESTAMPTZ,
       updated_at    TIMESTAMPTZ NOT NULL,
       PRIMARY KEY (queue_name, reaction_id)
   );
   CREATE UNIQUE INDEX ddd_reaction_row_line ON ddd_reaction_row (queue_name, line_key, line_sequence, line_ordinal);
   ```

   The `scheduled_tasks_ordered_idx` index that 0.3 recommended is no longer used; you can drop it.
3. **`DbSchedulerQueues(jdbc)` becomes `DbSchedulerTaskScheduler()`**, in `io.kotmod.scheduling.dbscheduler`.
   `tasks` and `bind` work as before. `orderedRecheckDelay` is gone: nothing waits and checks again.
4. **`EventReactor` and `ProcessManager` take the scheduler:** `EventReactor(jdbc, scheduler, isLeader = …)` and
   `ProcessManager(…, scheduler = …)` (it was `queues = …`).
5. **The operator tools move to `ReactionOperations(jdbc, scheduler)`**, in `io.kotmod.reaction`, and work on every
   scheduler: `blockedReactions(policy)`, `retryBlocked(policy, id)`, `skipBlocked(policy, id)`,
   `parkedMappings(policy)` and `skipParked(policy, eventId)`, without the `SchedulerClient` argument. The changes
   are now `suspend` functions.
6. **A custom queue implements `TaskScheduler`.** `ReactionQueues`, `ReactionChannel`, `EventReactionTriggerSink`,
   `EventReactionTriggerSource`, `EventReactionTriggerSerializer`, `EventReactionTrigger`, `DispatchOrdering` and
   `ReactionOutcome` are removed. See [Using another scheduler](#using-another-scheduler).
7. **The deprecated `Reactions<T>` and `ReactionsDsl` aliases are removed.** Extend `EventPolicy<T>` (see
   [Upgrading from 0.3.0](#upgrading-from-030)).

Behaviour changes: ordered attempts are counted when they start, so a crash counts as an attempt; a recovered
parked mapping's triggers now run before its aggregate's later work on every scheduler; and an ordered event policy
is no longer limited to 9,999 triggers per event.

## Upgrading from 0.3.0

0.3.1 renames the class you extend for follow-up work from `Reactions<T>` to `EventPolicy<T>`; the docs now call it
an event policy.

1. **Extend `EventPolicy<T>`.** In 0.3.1, `Reactions<T>` still compiled, as a deprecated alias with a quick fix;
   0.4.0 removes it. The same goes for the `ReactionsDsl` marker, now `EventPolicyDsl`.
2. **Renamed parameters.** Parameters named `useCase` are now `policy`, in the same position:
   `EventReactor.register(policy)` and `DbSchedulerQueues`' `parkedMappings`, `skipParked`, `blockedReactions`,
   `retryBlocked` and `skipBlocked`. Only calls that pass them by name (`useCase = …`) need changing, to `policy = …`.
3. **Nothing stored changes.** Queue and task names, reaction ids (`<policy>/<eventId>/<n>` and
   `<policy>/<eventId>/mapping`) and queued data are as in 0.3.0, so work queued by 0.3.0 runs after the upgrade
   with no draining. Log messages now say "event policy", and the internal runtime's logger is now
   `io.kotmod.reaction.PolicyRuntime` (it was `io.kotmod.reaction.UseCaseRuntime`): update any log filters or
   levels keyed on the old name.

## Upgrading from 0.2.0

0.3 replaces event reactions built from an executor, an outbox and contract subscriptions with
[event policies](#event-policies) (classes extending `EventPolicy<T>`) run by an `EventReactor`. These steps go
straight to 0.3.1; 0.3.0 named the class `Reactions<T>` (see [Upgrading from 0.3.0](#upgrading-from-030)). Code
blocks below marked as before/after are sketches, not compiled; the [quickstart](#4-react-to-events) and the guides
show compiled code. Then follow [Upgrading from 0.3](#upgrading-from-03) to reach 0.4.0. To upgrade:

1. **Drain in-flight work first.** Queue names change: each event policy's queue is named after the policy, and
   process manager channels become `<process type>-<channel>`. Work queued under 0.2.0's task names would never
   run. Before deploying 0.3.1, stop writing commands and let pending reactions and process manager work finish
   (or cancel what you no longer need). Any rows left under the old task names are never run, and can be deleted
   from `scheduled_tasks`.
2. **`AggregateKind` gains the event type and its serialization.** It is now
   `AggregateKind<C, E, R>` (command, event, rejection), with an `eventSerialization` parameter between the
   command and rejection serializers; use the same serialization you gave your `AggregateManager`.

   <!-- not-compiled -->
   ```kotlin
   // 0.2.0
   object Orders : AggregateKind<OrderCommand, OrderRejection>(
       AggregateType("Order"), OrderCommand.serializer(), OrderRejection.serializer(),
   )

   // 0.3.1
   object Orders : AggregateKind<OrderCommand, OrderEvent, OrderRejection>(
       type = AggregateType("Order"),
       commandSerializer = OrderCommand.serializer(),
       eventSerialization = orderEventSerialization,
       rejectionSerializer = OrderRejection.serializer(),
   )
   ```

3. **An executor, an outbox and `eventToReactions` become an event policy and the reactor.** Move `eventToReactions`
   into the event policy's `on(kind)` block (no more filtering by aggregate type, deserializing, or building reaction
   ids: call `trigger(t)`), `execute` into `handle`, the retry handlers into `onFailure` (returning `Retry(delay)`
   or `GiveUp`; a timeout arrives as a `ReactionTimeoutException`) and `onCompletion` into `onCompletion`.
   Register every event policy on one `EventReactor` per context. Its `name` (default `reactor`) names its saved
   position, and `isLeader` replaces the outbox's `isLeader`.

   <!-- not-compiled -->
   ```kotlin
   // 0.2.0
   val executor = EventReactionExecutor(sink, source, createExecutionContext, execute, failureRetryHandler, timeoutRetryHandler, onCompletion)
   val eventToReactions = { event: PersistedEvent ->
       if (event.metadata.aggregateType == Orders.type) {
           listOf(EventReaction(EventReactionId("confirm-${event.metadata.eventId.value}"), SendOrderConfirmation(event.metadata.aggregateId.value)))
       } else {
           emptyList()
       }
   }
   val outbox = AggregateEventOutbox(backend, executor, eventToReactions, getPosition, savePosition, isLeader)

   // 0.3.1
   class OrderNotifications : EventPolicy<SendOrderConfirmation>("order-notifications", SendOrderConfirmation.serializer()) {
       init {
           on(Orders) { event, metadata ->
               if (event is OrderPlaced) trigger(SendOrderConfirmation(metadata.aggregateId.value))
           }
       }

       override suspend fun handle(trigger: SendOrderConfirmation, context: ReactionContext) { /* … */ }
   }

   val reactor = EventReactor(jdbc, queues, isLeader = { election.isLeader() })
   reactor.register(OrderNotifications())
   reactor.start()
   ```

   `execute` could return `EventReactionCancelled` to finish without doing anything. An event policy's `handle` does
   that by returning normally; `onCompletion` then sees `ReactionResult.Completed`, not a cancellation.

4. **Triggers are plain data.** In event policies, drop `: EventReactionTrigger`, the `timeout` field (override the policy's
   `timeout`, 60 seconds by default) and your `EventReactionTriggerSerializer` object (pass
   `triggers = X.serializer()`). A custom queue still uses `EventReactionTrigger` and
   `EventReactionTriggerSerializer`, as part of its sink and source interfaces. A trigger's `notBefore` moves to
   `trigger(t, notBefore = …)`. Ordering moves from
   the outbox's `ordering` to the policy's `ordering` property (see [Ordered event policies](#ordered-event-policies)).
5. **Contract subscriptions become `on(contract)`.** Replace `contract.subscribe(executor) { envelope -> … }` with
   an event policy whose `init` block calls `on(contract) { event, metadata -> … }`. The event is typed, and `metadata`
   replaces the envelope's. The contract must read the same database as the reactor and must be started, and
   the policy must be registered before the contract starts. See
   [Consuming another context's events](#consuming-another-contexts-events).

   <!-- not-compiled -->
   ```kotlin
   // 0.2.0
   contract.subscribe(executor) { envelope ->
       listOf(EventReaction(EventReactionId("status-${envelope.metadata.eventId.value}"), UpdateStatus(envelope.event.orderId)))
   }

   // 0.3.1
   class StatusUpdates : EventPolicy<UpdateStatus>("status-updates", UpdateStatus.serializer()) {
       init {
           on(contract) { event, metadata -> trigger(UpdateStatus(event.orderId)) }
       }
       override suspend fun handle(trigger: UpdateStatus, context: ReactionContext) { /* … */ }
   }
   reactor.register(StatusUpdates())   // before contract.start()
   ```

6. **db-scheduler.** Replace every `DbSchedulerEventReactions` and `DbSchedulerProcessManagerQueues` with one
   `DbSchedulerQueues(jdbc)`. Read `queues.tasks` only after registering every event policy and building every process
   manager, pass it to your `Scheduler`, and call `queues.bind(scheduler)` before starting the reactor or process
   managers. The operator helpers are keyed by queue name:
   `queues.blockedReactions(scheduler, "order-status-projection")`, `queues.retryBlocked(scheduler, name, id)` and
   `queues.skipBlocked(scheduler, name, id)`.

   <!-- not-compiled -->
   ```kotlin
   // 0.3.1
   val queues = DbSchedulerQueues(jdbc)
   val reactor = EventReactor(jdbc, queues, isLeader = { election.isLeader() })
   reactor.register(OrderNotifications())
   val scheduler = Scheduler.create(dataSource, *queues.tasks.toTypedArray()).enableImmediateExecution().build()
   queues.bind(scheduler)
   reactor.start()
   scheduler.start()
   ```

7. **Process managers** take a `ReactionQueues` (`DbSchedulerQueues`). `ProcessManagerQueues` and `ProcessChannel`
   are now `ReactionQueues` and `ReactionChannel`, in `io.kotmod.event.reaction`.
8. **Your own queue.** Implement `ReactionQueues` (one `channel(name, triggerSerializer, ordered)` per queue)
   instead of handing a sink and source to an executor. The sink and source interfaces are unchanged.
9. **Positions.** The reactor saves its position under its `name` (`reactor` by default); your 0.2.0 outbox
   consumer names are no longer read. A new reactor starts at the head of the event log when it first starts, so
   events written before the upgrade that were not yet reacted to are not replayed (another reason to drain
   first). If you can't stop writing commands to drain, stop just the 0.2.0 outboxes instead and let the work
   they already queued finish. Then, before the reactor's first start, give it the outboxes' position, so it
   reacts to every event written since:

   ```sql
   -- Fails if the reactor already has a position: do this before its first start.
   INSERT INTO ddd_consumer_offset (consumer_name, last_transaction_id, last_offset, updated_at)
   SELECT 'reactor', last_transaction_id, last_offset, now()
   FROM ddd_consumer_offset
   WHERE consumer_name IN ('order-outbox', 'payment-outbox')  -- your 0.2.0 outbox consumer names
   ORDER BY last_transaction_id, last_offset
   LIMIT 1;
   ```

   With several outboxes this takes the one furthest behind, so event policies that came from the others may see some
   events again (with new ids, so `handle` must be idempotent). Once the reactor is running, you can delete the
   old rows from `ddd_consumer_offset`.
10. **Deduplication only while queued.** A reaction's id is deduplicated only while it is still in the queue; once
    it has completed it is gone, so a redelivery after that can run it again. Use `context.reactionId` as the
    idempotency key of external calls.
11. **Removed from the public API:** `EventReactionExecutor`, `AggregateEventOutbox`, `EventReaction`,
    `PublicEventContract.subscribe`, `DbSchedulerEventReactions`, `DbSchedulerProcessManagerQueues`,
    `EventReactionExecutionResult`, `EventReactionCompletionResult`, `RetrySignal` and `BackoffStrategy`. Use an event
    policy's `onFailure`, `onCompletion` and `backoff(attempt)` instead.

## Upgrading from 0.1.0

0.2.0 replaces `create` and `execute` with `handle`, and commands become data. To upgrade:

1. Add the rejection columns to the command history:

   ```sql
   ALTER TABLE ddd_command_history ADD COLUMN rejection_type    VARCHAR(255);
   ALTER TABLE ddd_command_history ADD COLUMN rejection_payload TEXT;
   ```

   Existing rows read as accepted commands.
2. For each aggregate, define a sealed command type and a sealed rejection type. Make your state type
   implement `AggregateState` and decide each command in `handle`, returning `accept(...)` or `reject(...)`.
   Then add an `InitialState` object for commands on an aggregate that doesn't exist yet, as in
   [the quickstart](#2-define-state-events-commands-and-rejections).
3. Declare an `AggregateKind` for the aggregate, as `Orders` in [the quickstart](#3-wire-up-persistence), and
   build `AggregateManager` from it and the `InitialState` object. Then replace `create { }` and
   `execute<T> { }` calls with `handle(id, command)`.
   `AggregateManager`'s type arguments are now `<S, C, E, R>` (state, command, event, rejection), e.g.
   `AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>` where 0.1.0 had
   `AggregateManager<Order, OrderEvent>`.
4. Replace `catch (e: UnexpectedAggregateStateException)` with a rejection decided by the state, and drop any
   retry loop around `OptimisticConcurrencyException`: `handle` retries itself.
5. If you implemented `DomainPersistenceBackend` yourself, replace `wasCommandHandled` with
   `findHandledCommand`, add `recordCommandRejected`, and throw `CommandAlreadyRecordedException` when a
   command id is recorded twice.
6. If you implemented your own queue, add the `notBefore` parameter to your sink's `publish` and carry it
   to delivery, pass it to `block` as the fifth argument, and handle `ReactionOutcome.Wait` by delivering
   again after the delay without counting a retry.
7. A consumer with no saved position (an outbox, public contract or process manager using
   `PostgresOffsetManager`) now starts at the head of the event log, not the beginning. Where you relied on
   replaying history, pass `startFrom = StartFrom.Beginning` to `getPosition`.

## Status and contributing

kotmod is pre-1.0: the API may still change between releases. Issues and pull requests are welcome.

The repository is a multi-module Gradle build: `kotmod`, `kotmod-db-scheduler`, `kotmod-sqldelight` and
`examples` (the compiled code behind this README).

- `./gradlew test` runs the unit tests.
- `./gradlew integrationTest` runs the integration tests against Postgres in Docker (via Testcontainers),
  including the quickstart above.

kotmod is licensed under the [Apache License 2.0](LICENSE).

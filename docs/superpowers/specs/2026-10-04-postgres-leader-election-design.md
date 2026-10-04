# Postgres leader election

**Date:** 2026-10-04
**Status:** Approved design, pending implementation plan

## Goal

Ship a ready-made leader election so `AggregateEventOutbox` and `PublicEventContract` can run safely on several
nodes without users writing their own. It plugs into the existing `isLeader: () -> Boolean` parameter and needs
nothing but Postgres.

Success: with two nodes, only one polls; when it dies, another takes over within seconds.

## Decisions

- **Session advisory lock on a dedicated, long-lived connection.** Direct connections are assumed (or a
  session-mode pooler); PgBouncer in transaction mode is not supported.
- **One election per name.** One election per app is the documented default; one per consumer spreads the load.
  Both are just usage of the same class.
- **No fencing.** The brief window in which two nodes can both poll is documented, not prevented: kotmod's
  at-least-once delivery and deterministic reaction ids already absorb it.
- **Connection factory, not a pool.** The election takes `() -> Connection` and owns the connection it gets.
  It doesn't need a pool: one connection held for good would permanently use a pool slot and trip leak detection.

## API

In `io.kotmod.postgres` (core module):

```kotlin
class PostgresLeaderElection(
    connect: () -> Connection,
    val name: String,
    checkInterval: Duration = 5.seconds,
) : AutoCloseable {
    fun isLeader(): Boolean
    fun start()
    suspend fun stop()
    override fun close()
}
```

An internal constructor parameter supplies a monotonic clock (`() -> Long` nanoseconds, default
`System::nanoTime`) for tests.

Usage:

```kotlin
val election = PostgresLeaderElection({ DriverManager.getConnection(url, user, password) }, "order-service")
election.start()
AggregateEventOutbox(..., isLeader = election::isLeader)
```

- **Lock key:** `hashtextextended('kotmod-leader:' || name, 0)`, computed by Postgres. The prefix keeps it clear
  of the app's own advisory locks. Elections with the same name compete for one lock; different names are
  independent.
- **`isLeader()`:** reads state kept by the background loop and never touches the database. It is true only while
  leading **and** the last successful check is no older than `2 × checkInterval`, measured on the monotonic clock.
  It is false before `start()` and after `stop()`.
- **`start()`:** launches the loop on the election's own coroutine scope (`Dispatchers.IO`, like
  `DomainEventPoller`); the first attempt runs immediately, then every `checkInterval`. Calling it twice throws
  `IllegalStateException`.
- **`stop()`:**
  1. Marks the election as not leader.
  2. Cancels the loop.
  3. Runs `pg_advisory_unlock(key)`.
  4. Closes the connection. Failures here are logged and swallowed.
- **`close()`:** a blocking `stop()`, for shutdown hooks and `use {}`.
- **No callbacks:** there is no `onElected` or `onRevoked`; the pollers already check `isLeader()` on every tick.

## The loop

| State | Action | On failure |
|---|---|---|
| No connection | Open one with `connect()` | Stay a follower; try again next tick |
| Connected, not leader | `SELECT pg_try_advisory_lock(key)`; if true, become leader and record the check time | Drop the connection; stay a follower |
| Leader | `SELECT 1` on the lock connection; on success, record the check time | Step down at once; drop the connection |

- **Why `SELECT 1` is enough:** a session lock is released only when its connection ends or when this class
  unlocks it, so a connection that still answers still holds the lock.
- **Query timeout:** every statement runs with a query timeout of `checkInterval`, rounded up to whole seconds and
  at least 1.
- **Dropping a connection:** uses `Connection.abort(executor)`, not `close()`, which can block on a dead socket.
  Postgres releases the lock once it notices the session has gone.
- **Lease:** if a check hangs (for example, the network stalls without the socket failing), the age limit still
  makes `isLeader()` false after `2 × checkInterval`. The lock connection also gets a JDBC network timeout of
  `2 × checkInterval` (so hung checks end) and session TCP keepalives of `checkInterval` (so Postgres notices a
  vanished leader within a few intervals). `stop()` waits for the loop at most `2 × checkInterval`, then aborts
  the connection.
- **Errors:** no exception escapes the loop.
- **Logging:**
  - INFO on becoming leader and on stepping down in `stop()`;
  - WARN on losing leadership and on the first failure in a run of connect/check failures;
  - DEBUG for repeats in that run;
  - INFO when a connection succeeds again after failures.

## Overlap window

Postgres can end the leader's session first: failover, `pg_terminate_backend`, or a network cut it notices before
the client does. Another node can then take the lock at once, while the old leader only learns at its next check.
Both may poll for up to about one `checkInterval` (bounded by the lease). The effects:

- **Duplicate dispatches:** reaction ids are deterministic, so db-scheduler drops the duplicates.
- **A saved position briefly moving backwards:** events are re-read, and the duplicates are dropped the same way.
- **Ordered reactions:** both nodes produce the same ordered instance id, so ordering still holds.

Delivery is at-least-once throughout.

## Testing

Integration tests against Postgres (Testcontainers `IntegrationTest`), with `checkInterval` around 200ms:

- A single election becomes leader shortly after `start()`; `isLeader()` is false before `start()`.
- Two elections with the same name: exactly one is leader, and it stays so across many checks.
- Two elections with different names: both are leaders.
- Clean handover: `stop()` leaves no advisory lock in `pg_locks`, and the other election becomes leader within
  one interval; `isLeader()` is false after `stop()`.
- Server-side kill: `pg_terminate_backend` on the leader's backend (found via `pg_locks`). The old leader steps
  down within one interval, the other takes over, and the old one rejoins as a follower.
- Connect failures: a factory that throws for its first attempts leaves the election a follower until
  connecting succeeds, then it becomes leader.
- Lease: with a fake clock and a long `checkInterval`, advancing the clock past `2 × checkInterval` makes
  `isLeader()` false while the connection is still open.
- `start()` twice throws.

## Documentation

- **README, "Running in production":** the "use a Postgres advisory lock or your platform's leader election" line
  becomes a **Leader election** section with a snippet compiled in `ReadmeExamples.kt`. It covers:
  - one election per app by default, or one per consumer to spread the load;
  - opening its connection outside the pool;
  - no support for PgBouncer in transaction mode;
  - shutdown order (stop outboxes and contracts first, then the election);
  - the overlap window.
- **Quickstart:** keeps `isLeader = { true }`, with a pointer to that section.
- **KDoc:** for the class and its members.

## Out of scope

- Fencing tokens or epochs on position saves.
- Leadership callbacks.
- Transaction-pooler support (per-poll transaction-level locks).
- A lease table.

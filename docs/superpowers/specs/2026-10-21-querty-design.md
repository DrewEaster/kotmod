# Querty: typed Kotlin from plain SQL files

**Date:** 2026-10-21
**Status:** Design, pending approval

## Intent

Querty lets a Kotlin app keep its Postgres queries in plain `.sql` files and call them through generated,
type-safe Kotlin. It is inspired by HugSQL/Yesql (SQL lives in SQL files, named queries, snippets) and
SQLDelight/kdbgen (typed generated code), with two deliberate differences:

1. **Producing SQL is separate from running it.** A generated query is just its SQL, its typed parameter
   values and a typed row mapper. Running it is the job of a small engine module. Querty ships engine
   modules itself (blocking JDBC first) but the generated code never depends on one, so a later
   non-blocking engine (R2DBC, Vert.x) can run the same generated queries.
2. **Types come from the real, migrated schema, not from DDL parsing.** The generator builds the schema in a
   throwaway Postgres and asks Postgres itself what each query's parameters and result columns are. Querty
   has no migration mechanism and no opinion on how apps migrate.

The payoff: when the schema changes in a way that breaks a query's assumptions, the app finds out at
generation time (bad query) or compile time (changed types), not in production. Queries that reference
columns or tables that don't exist are reported against the `.sql` file and line.

The idea is years old (original spec by the user); agentic coding is what makes it worth building now.

## Goals

- Plain `.sql` files with a small amount of Querty-specific syntax (in comments or otherwise; it may be
  non-SQL — the user explicitly accepted that to keep all declarations next to the query).
- Generated Kotlin per query: a function taking typed parameters, returning a query value that exposes its
  SQL and parameters for inspection, and a typed `Row` class with its mapper.
- Exact result shape per query, declared in the file (one row, maybe one row, many rows, affected-row count).
- App types in generated code: value classes (e.g. `OrderId`) for columns and parameters, declared in the
  `.sql` file.
- Honest nullability: a result field is nullable unless Querty can prove it isn't; the `.sql` file can
  override.
- Simple snippets: named, reusable SQL text pasted into queries; never visible in the generated API.
- Optional filters are written in static SQL (`(:status IS NULL OR status = :status)`); Querty adds no
  dynamic-SQL builder.
- A blocking JDBC engine that joins whatever transaction the caller's `Connection` is in.
- Postgres only.

## Non-goals (v1)

- Migrations, or any dependency on Flyway/Liquibase.
- Dynamic SQL building in the API.
- Non-blocking engines (`suspend`/`Flow`). The core runtime must be designed so they can be added as engine
  modules without regenerating differently; they are not built now.
- Databases other than Postgres.
- Snippets with their own parameters, conditionals, or nesting beyond a snippet containing `:params`.
- Batch execution (JDBC `addBatch`). Can be added later.
- Mapping SQL errors (e.g. unique violation 23505) to app exceptions — engines surface the driver's own
  exception; apps map it as they do today.
- A published Gradle plugin (see Build integration).

## Anti-goals

- Generated code or the core runtime depending on JDBC or any engine.
- Kotmod's core depending on Querty. **Kotmod's own internals stay on plain JDBC** — Querty is for apps'
  domain persistence (repositories, projections, read queries) built on Kotmod. The user considered and
  rejected porting Kotmod internals ("added baggage").
- Needing Docker for a normal compile/build.
- Silently claiming a column is non-null when it can be null.

## Where it lives

A standalone library — no dependency on Kotmod — built inside the Kotmod repo for now so it can move out
later. Proof of concept is a separate example module that uses Querty with Kotmod.

| Module (directory) | Contents | Depends on |
|---|---|---|
| `querty/` | Core runtime: query types, parameter values, the engine-neutral row-reading interface. Tiny, no third-party dependencies beyond the Kotlin stdlib. | — |
| `querty-jdbc/` | Blocking JDBC engine: `execute(connection)` for each query shape. | `querty`, JDK `java.sql` only |
| `querty-codegen/` | The generator: `.sql` parsing, schema setup in Testcontainers Postgres, describing queries, Kotlin emission, the check mode. Runnable as a `main`. | Testcontainers, pgjdbc, a Kotlin source emitter (builder's choice, e.g. KotlinPoet) |
| `kotmod-querty-example/` (unpublished) | Proof-of-concept app: Kotmod domain persisted with Querty. | `kotmod`, `kotmod-db-scheduler` as needed, `querty`, `querty-jdbc` |

*My call:* the user asked for "one module for Querty and one example module"; I split Querty into
runtime / JDBC engine / codegen because the separation of generation from execution is the core
differentiator and the runtime must stay dependency-free. Publishing to Maven Central is out of scope for
now (the modules use `kotmod.library`, not `kotmod.published`); Kotlin package root `io.querty` (*my call*,
easy to rename).

## Writing queries

Queries live under `src/main/sql/` in the consuming module; the directory path under it becomes the Kotlin
package. Each file holds one or more named queries and snippets, separated by their header comments.

Proposed syntax (*my call* on the exact spelling — HugSQL-flavoured, all on `-- :` header lines, with one
non-SQL inline form for snippets):

```sql
-- :snippet order-columns
o.id, o.customer_id, o.status, o.item, o.reason, o.placed_at

-- :name find-order :optional
-- :doc Load one order by id
-- :type id OrderId
-- :type customer_id CustomerId
-- :type status OrderStatus
SELECT {{order-columns}}
FROM orders o
WHERE o.id = :id

-- :name customer-orders :many
-- :type customer_id CustomerId
-- :not-null customer_name
-- :nullable status
SELECT {{order-columns}}, c.name AS customer_name
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE o.customer_id = :customer_id
  AND (:status::text IS NULL OR o.status = :status)
ORDER BY o.placed_at DESC
LIMIT :limit OFFSET :offset

-- :name save-order :affected
-- :type id OrderId
-- :nullable reason
INSERT INTO orders (id, customer_id, status, item, reason, placed_at)
VALUES (:id, :customer_id, :status, :item, :reason, :placed_at)
ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, reason = EXCLUDED.reason
```

- **`-- :name <kebab-name> <shape>`** starts a query. Shapes:
  - `:one` — exactly one row; the engine throws if zero or more than one.
  - `:optional` — zero or one row (`Row?`); throws if more than one.
  - `:many` — `List<Row>`.
  - `:affected` — no rows; returns the affected-row count (`Long`).
  - `INSERT/UPDATE/DELETE … RETURNING` uses `:one`/`:optional`/`:many` like a select.
- **`-- :doc`** becomes KDoc.
- **`-- :type <name> <KotlinType>`** maps a result column and/or a parameter with that name to an app type.
  Applies to both sides, since ids flow in and out under the same name. The type is written as it should
  appear in the generated file; `-- :import com.example.orders.OrderId` lines (file-level or per query,
  *my call*) supply imports.
- **`-- :not-null <column>`** overrides the nullable default for a result column.
- **`-- :nullable <param>`** makes a parameter nullable (parameters are non-null by default). Needed for the
  optional-filter pattern and for writing null into a nullable column. Not inferred (*my call*): forgetting
  it shows up as a compile error at the call site that passes `null`, which is safe.
- **`:param`** — named parameters (`[a-z_][a-z0-9_]*`). A name may appear several times; it is one Kotlin
  parameter. The parser must not treat Postgres casts (`::text`), string literals, quoted identifiers,
  comments, or dollar-quoted strings as parameters.
- **`{{snippet-name}}`** pastes a snippet's text. Snippets are visible across all `.sql` files in the module.
  A snippet may contain `:params`, which become parameters of each query that includes it. No nesting
  required (*my call*: allow it if trivial, reject cycles).

## Generated Kotlin

One Kotlin file per `.sql` file; one `object` per query, named from the kebab name in PascalCase
(`find-order` → `FindOrder`), with a nested `Row` data class and `operator fun invoke(...)`. Result and
parameter names are snake_case → camelCase.

```kotlin
object FindOrder {
    data class Row(
        val id: OrderId,
        val customerId: CustomerId,
        val status: OrderStatus,
        val item: String,
        val reason: String?,
        val placedAt: Instant,
    )

    operator fun invoke(id: OrderId): OptionalQuery<Row> = …
}
```

Usage with the JDBC engine:

```kotlin
val order: FindOrder.Row? = FindOrder(OrderId("o-1")).execute(connection)
val orders = CustomerOrders(customerId, status = null, limit = 20, offset = 0).execute(connection)
val q = FindOrder(OrderId("o-1"))
q.sql      // inspectable
q.params   // inspectable
```

Mapping a `Row` to an app type is ordinary Kotlin (`?.let`, `.map`) — no mapper lambda on `execute`
(*my call*; the original spec sketched `execute(tx) { row -> … }`, which adds nothing once the result shape
is exact).

Generated code is committed (location *my call*: `src/generated/querty/kotlin`, added as a main source dir).

## Core runtime (`querty`)

Engine-neutral types the generated code depends on. The shape is left to the builder within these rules:

- Four query types matching the shapes (`OneQuery<R>`, `OptionalQuery<R>`, `ManyQuery<R>`,
  `AffectedQuery`), or one type plus a shape tag — builder's choice, as long as each engine can offer exactly
  `R`, `R?`, `List<R>`, `Long` at the call site.
- **SQL with placeholders an engine can render.** pgjdbc needs `?` (repeated names expanded into repeated
  positions); R2DBC/Vert.x Postgres clients want `$1…$n` (a repeated name reuses its number). The query holds
  the SQL in a form from which both can be produced; `sql` for inspection uses `$n`.
- **Typed parameter values**: each value carries its Postgres type (as described by the server), so engines
  can bind nulls and ambiguous values correctly.
- **Row reading through a small interface** (read column *i* as String/Int/Long/Boolean/Instant/…, nullable).
  Each engine adapts its native row to it. Generated mappers read by column index.

## Type mapping

Base mapping from Postgres types (as reported by describe) to Kotlin (*my call*):

| Postgres | Kotlin |
|---|---|
| `text`, `varchar`, `char`, `name` | `String` |
| `int2`, `int4` | `Int` |
| `int8` | `Long` |
| `bool` | `Boolean` |
| `float4` / `float8` | `Float` / `Double` |
| `numeric` | `java.math.BigDecimal` |
| `timestamptz` | `kotlin.time.Instant` |
| `timestamp` | `java.time.LocalDateTime` |
| `date` | `java.time.LocalDate` |
| `uuid` | `java.util.UUID` |
| `bytea` | `ByteArray` |
| `json`, `jsonb` | `String` |
| one-dimensional arrays of the above | `List<T?>` in results, `List<T>` in parameters |

Array result elements are nullable because Postgres can't say otherwise; there is no element-level
override in v1 (*my call*). Any other type is a generation error naming the file, query and column, telling the user to cast it in SQL
(e.g. `xid8::text`). `LIMIT`/`OFFSET` parameters are `Long` because Postgres describes them as `int8`
(*my call*; the original spec wanted `Int`).

App types via `-- :type`:

- **Value classes**: wrapped with the type's constructor and unwrapped with `.value`. The declared type must
  have a one-argument constructor taking the base Kotlin type and a `value` property; violations are compile
  errors in the generated code, which is acceptable.
- **Enum classes**: for `text`/`varchar` columns, by `name` (`valueOf` / `.name`). The generator needs to know
  which kind it is: `-- :type status OrderStatus enum` vs. a plain value class (*my call* on spelling). The
  generator does not load app classes (they may not compile yet).
- No general converter mechanism in v1.

## How generation works

Run on demand (after changing migrations or `.sql` files), never as part of a normal build.

1. Parse every `.sql` file under the source dir: queries, snippets, header directives. Expand snippets.
   Rewrite `:params` to `$n`, keeping a source map from rewritten SQL positions back to file lines.
2. Start Postgres in Testcontainers (image configurable; default `postgres:17`, matching Kotmod's tests).
3. Apply the schema: an ordered list of SQL script files (a Flyway-style folder sorted by version, or a
   plain list). Querty doesn't depend on Flyway or understand its placeholders/callbacks.
4. For each query, prepare it and ask Postgres to describe it (parse/describe without executing — pgjdbc's
   `getParameterMetaData()` / `getMetaData()` do this). This gives every parameter's type and every result
   column's type, and for plain column references the source table and column. Errors (unknown column,
   type mismatch, syntax) are reported with the original file and line, all errors in one run, and nothing
   is written if any query fails.
5. **Nullability**: a result column is non-null only when it is a direct reference to a `NOT NULL` table
   column *and* that table is not on the nullable side of an outer join in this query (or the column comes
   from `RETURNING` on the target table). Otherwise it is nullable, unless `-- :not-null` says otherwise.
   Parameters are non-null unless declared with `-- :nullable` (never inferred). The builder decides how to detect outer-join sides (light parsing of the `FROM` clause is
   enough; JSqlParser is an option but must not block queries it can't parse — fall back to nullable).
6. Emit Kotlin.

### Build integration

`querty-codegen` exposes a `main` (source dir, schema scripts, output dir, mode). In this repo the example
module registers two Gradle tasks that run it via `JavaExec` (*my call*: a real Gradle plugin waits until
Querty moves out of the repo):

- `generateQuerty` — writes the generated sources.
- `checkQuerty` — generates into a temp dir and fails if it differs from the committed sources. It needs
  Docker, so it is wired into `integrationTest` (which already needs Docker), not `build`.

## JDBC engine (`querty-jdbc`)

- Extension functions taking a `java.sql.Connection`: `execute(connection)` returning `R`, `R?`, `List<R>`
  or `Long` by shape. The engine never opens, commits or closes the connection, so it joins whatever
  transaction the caller is in (in Kotmod apps, via `jdbc.withConnection { conn -> … }`).
- Binds by declared Postgres type; reads via the row interface over `ResultSet`. Converts `timestamptz` via
  `OffsetDateTime` to `kotlin.time.Instant` (the conversion Kotmod uses).
- Driver exceptions propagate unchanged.

## Proof of concept (`kotmod-querty-example`)

Rebuilds the README's domain (`examples/.../readme/Quickstart.kt`, `ReadmeExamples.kt`) on Querty, so the
hand-written JDBC versions are a direct before/after comparison:

- **Repositories**: `Repository<Order>` and `Repository<Customer>` implemented with Querty (get + upsert),
  using value-class ids, an enum status, `Instant` timestamps and nullable fields (e.g. cancellation reason).
- **Projection**: `OrderStatusProjection` writing its read table via a Querty upsert with the
  `WHERE order_status.sequence < EXCLUDED.sequence` guard.
- **Read-side queries**: a customer's orders `LEFT JOIN`ed to customer details (exercises outer-join
  nullability and `:not-null`); an order search with optional filters in static SQL plus `LIMIT`/`OFFSET`;
  a lookup by a list of ids with `= ANY(:ids)`; one shared column-list snippet.
- The example's schema is its own `.sql` migration scripts (used by the generator and the tests); Kotmod's
  `ddd_*` tables come from `DddSchema.ddl` at test time only — the generator doesn't need them.
- Integration tests run everything end to end on Testcontainers Postgres inside Kotmod transactions, and
  run `checkQuerty`.

The example may extend the README domain (e.g. a `placed_at` timestamp, customer names) to exercise these
features.

**Success** means the example's persistence reads clearly better than the hand-written JDBC it replaces,
every query in it is generated (no raw JDBC fallbacks), and a deliberately broken query or schema change
produces a clear generation or compile error.

## Left to the builder

- Exact core runtime type names and structure, within the rules above.
- SQL lexing approach for `:params`, snippets and directives.
- Kotlin emitter library.
- Generator tests: unit tests for parsing/rewriting/naming; integration tests (Testcontainers) for describe,
  nullability and error reporting.

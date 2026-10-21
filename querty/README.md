# Querty

Querty lets a Kotlin app keep its Postgres queries in plain `.sql` files and call them through
generated, type-safe Kotlin. You write the SQL; Querty works out the parameter and result types by
asking a real, migrated Postgres, and generates a function and a `Row` class for every query.

```sql
-- :name find-order :optional
-- :type id OrderId
SELECT o.id, o.item, o.reason, o.placed_at
FROM orders o
WHERE o.id = :id
```

```kotlin
val order: FindOrder.Row? = FindOrder(OrderId("o-1")).execute(connection)
order?.placedAt // kotlin.time.Instant, because placed_at is a NOT NULL timestamptz
order?.reason   // String?, because reason is nullable
```

## Contents

- [Why Querty](#why-querty)
- [Status](#status)
- [Modules](#modules)
- [Quickstart](#quickstart)
- [Writing queries](#writing-queries)
  - [Named queries and result shapes](#named-queries-and-result-shapes)
  - [Parameters](#parameters)
  - [Nullability](#nullability)
  - [Your own types](#your-own-types)
  - [Snippets](#snippets)
  - [Optional filters](#optional-filters)
  - [Directive reference](#directive-reference)
- [Type mapping](#type-mapping)
- [Running queries](#running-queries)
- [Generating code](#generating-code)
- [Using Querty with Kotmod](#using-querty-with-kotmod)
- [Known limitations](#known-limitations)

## Why Querty

- **SQL stays SQL.** Queries live in `.sql` files you can read, review and paste into `psql`. There
  is no query DSL to learn and no dynamic SQL builder.
- **Types come from the real schema.** The generator builds your schema in a throwaway Postgres and
  asks Postgres to describe each query. It doesn't parse DDL or guess. When a migration changes a
  column in a way a query depends on, you find out at generation time (a query that no longer
  works) or compile time (a type that changed), not in production.
- **Producing SQL is separate from running it.** A generated query is a value: its SQL, its typed
  parameters and a row mapper. You can inspect it, and you run it through an engine module. The
  generated code never depends on JDBC, so a non-blocking engine can run the same queries later.
- **No migration opinions.** Querty doesn't migrate anything. Point it at the SQL scripts that build
  your schema, whether a Flyway-style folder or a plain list of files, and migrate production however
  you already do.

## Status

Querty is new and lives in the Kotmod repo for now. It is not published to Maven Central yet, so
use it as a project dependency from inside this build. Only Postgres is supported, and the only
engine so far is blocking JDBC (`querty-jdbc`).

## Modules

| Module | What it is | Depends on |
|---|---|---|
| `querty` | The runtime: query values (`OneQuery`, `OptionalQuery`, `ManyQuery`, `AffectedQuery`), parameters, SQL templates and the engine-neutral `RowReader`. Generated code depends only on this. | Kotlin stdlib |
| `querty-jdbc` | The blocking JDBC engine: `execute(connection)` for each query shape. | `querty`, `java.sql` |
| `querty-codegen` | The generator. Runs on demand, starts Postgres in Docker through Testcontainers, and writes Kotlin. Never on your app's runtime classpath. | Testcontainers, pgjdbc |

`kotmod-querty-example` is a working app that uses all three. It's the best place to see Querty
end to end.

## Quickstart

### 1. Add the dependencies and generator tasks

In your module's `build.gradle.kts`:

```kotlin
val querty by configurations.creating

dependencies {
    implementation(project(":querty"))
    implementation(project(":querty-jdbc"))
    querty(project(":querty-codegen"))
}

// Generated code is committed; compile it with the rest of main.
sourceSets.main {
    kotlin.srcDir("src/generated/querty/kotlin")
}

fun registerQuerty(name: String, mode: String) =
    tasks.register<JavaExec>(name) {
        group = "querty"
        classpath = querty
        mainClass.set("io.querty.codegen.MainKt")
        workingDir = projectDir
        args(
            "--sql-dir", "src/main/sql",
            "--schema", "src/main/resources/db/migration",
            "--out", "src/generated/querty/kotlin",
            mode,
        )
    }

registerQuerty("generateQuerty", "generate")
registerQuerty("checkQuerty", "check")
```

Both tasks need Docker. Neither runs as part of `build`, so a normal compile never needs Docker.

### 2. Point it at your schema

`--schema` takes the SQL scripts that build your schema, for example your migrations:

```sql
-- src/main/resources/db/migration/V1__orders.sql
CREATE TABLE orders (
    id        TEXT PRIMARY KEY,
    status    TEXT NOT NULL,
    item      TEXT NOT NULL,
    reason    TEXT,
    placed_at TIMESTAMPTZ NOT NULL
);
```

### 3. Write a query

The path of a `.sql` file under `--sql-dir` becomes its Kotlin package:
`src/main/sql/com/example/orders/orders.sql` generates `com/example/orders/OrdersQueries.kt` in
package `com.example.orders`.

```sql
-- src/main/sql/com/example/orders/orders.sql

-- :name find-order :optional
-- :doc Loads one order by id.
SELECT o.id, o.status, o.item, o.reason, o.placed_at
FROM orders o
WHERE o.id = :id
```

### 4. Generate

```
./gradlew generateQuerty
```

This writes `src/generated/querty/kotlin/com/example/orders/OrdersQueries.kt`:

```kotlin
// Generated by Querty from orders.sql. Do not edit.
package com.example.orders

/** Loads one order by id. */
object FindOrder {
    data class Row(
        val id: String,
        val status: String,
        val item: String,
        val reason: String?,
        val placedAt: kotlin.time.Instant,
    )

    operator fun invoke(id: String): OptionalQuery<Row> = …
}
```

Commit the generated file along with the `.sql` file.

### 5. Run it

```kotlin
import io.querty.jdbc.execute

dataSource.connection.use { connection ->
    val order: FindOrder.Row? = FindOrder("o-1").execute(connection)
}
```

## Writing queries

A `.sql` file holds any number of queries and snippets. Each one starts with a `-- :name` (or
`-- :snippet`) line. Directive lines that follow it describe the query, and the SQL runs until the
next `-- :name` or `-- :snippet`. Directives are ordinary SQL comments, so the file still runs in
`psql`. The one exception is the `{{snippet}}` reference, which isn't SQL. Other `--` comments are
just part of the SQL, and a trailing `;` is optional.

### Named queries and result shapes

```sql
-- :name <kebab-case-name> <shape>
```

The name becomes the Kotlin object's name in PascalCase (`find-order` → `FindOrder`). The shape says
what the query returns:

| Shape | Use it for | `execute` returns |
|---|---|---|
| `:one` | exactly one row | `Row`; throws `UnexpectedRowCountException` on zero or more than one |
| `:optional` | zero or one row | `Row?`; throws on more than one |
| `:many` | any number of rows | `List<Row>` |
| `:affected` | `INSERT`/`UPDATE`/`DELETE` with no `RETURNING` | `Long`, the affected-row count |

`INSERT … RETURNING`, `UPDATE … RETURNING` and `DELETE … RETURNING` return rows, so use `:one`,
`:optional` or `:many` for them. Querty checks the shape against the query: a row shape on a
statement with no result columns, or `:affected` on one that has them, is a generation error.

Result column names become `Row` properties in camelCase (`placed_at` → `placedAt`). Alias anything
that isn't a plain column. `SELECT count(*) + 1` with no alias has the label `?column?`, which is an
error.

### Parameters

Write parameters as `:name`, with a name in `snake_case`. They become Kotlin parameters in camelCase,
in the order they first appear in the SQL. A parameter used several times is one Kotlin parameter:

```sql
-- :name orders-on-day :many
SELECT id FROM orders
WHERE placed_at >= :day AND placed_at < :day + interval '1 day'
```

```kotlin
OrdersOnDay(day = Instant.parse("2026-10-21T00:00:00Z")).execute(connection)
```

Querty takes each parameter's type from Postgres, so `LIMIT :limit` is a `Long` (Postgres describes
`LIMIT` as `bigint`) and `= ANY(:ids)` on a text column is a `List<String>`. If Postgres can't work
out a type, typically for a bare `:x IS NULL`, add a cast: `:x::text IS NULL`.

Colons that aren't parameters are left alone. That covers `::casts`, string literals, quoted
identifiers, comments, dollar-quoted bodies and array slices like `arr[1:2]`.

### Nullability

**Results are nullable unless Querty can prove otherwise.** A column is non-null only when all of
these hold:

- it is a direct reference to a `NOT NULL` table column;
- that table isn't on the nullable side of an outer join.

Expressions, aggregates, subquery columns and anything Querty can't attribute are nullable. That
includes columns grouped by `ROLLUP`, `CUBE` or `GROUPING SETS`.

When you know better, say so:

```sql
-- :name customer-orders :many
-- :not-null customer_name
SELECT o.id, c.name AS customer_name, c.email AS customer_email
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE o.customer_id = :customer_id
```

Here `customerName` is a `String` and `customerEmail` is a `String?`. If a `:not-null` column turns
out to be null at runtime, the mapper throws `IllegalStateException("Column 'customer_name' was null
but is declared non-null")`.

**Parameters are non-null unless you declare them nullable** with `-- :nullable <param>`. Querty never
infers it. If you forget, you get a compile error at the call that passes `null`, which is safe.

### Your own types

Map a column or parameter to your own type with `-- :type <name> <KotlinType>`. The declaration
covers the result column and the parameter with that name, since ids usually flow both ways:

```sql
-- :name find-order :optional
-- :type id OrderId
-- :type status OrderStatus enum
SELECT o.id, o.status FROM orders o WHERE o.id = :id
```

```kotlin
@JvmInline value class OrderId(val value: String)
enum class OrderStatus { PENDING, SHIPPED, CANCELLED }

val order = FindOrder(OrderId("o-1")).execute(connection) // order?.id is an OrderId, order?.status an OrderStatus
```

Two kinds of type are supported:

- **Value classes** (and any class with a one-argument constructor taking the base type and a
  `value` property). Querty wraps with the constructor and unwraps with `.value`.
- **Enums**, marked with `enum`, on `text` or `varchar` columns. Querty reads with `valueOf` and writes
  `.name`.

For list parameters such as `= ANY(:ids)`, the type applies to each element, so `ids` becomes
`List<OrderId>`. If the type lives in another package, import it with `-- :import com.example.OrderId`,
either before the first query (for the whole file) or after a query's `:name` line (for that query).

There's no general converter mechanism. For anything else, map the `Row` in Kotlin.

### Snippets

A snippet is a named piece of SQL text that's pasted into every query that references it with
`{{name}}`. It's handy for a column list several queries share:

```sql
-- :snippet order-columns
o.id, o.customer_id, o.status, o.item, o.reason, o.placed_at

-- :name find-order :optional
SELECT {{order-columns}} FROM orders o WHERE o.id = :id

-- :name customer-orders :many
SELECT {{order-columns}} FROM orders o WHERE o.customer_id = :customer_id
```

Snippets are shared across every `.sql` file under `--sql-dir`, so their names must be unique. A
snippet may contain `:params`, which become parameters of each query that includes it, and it may
reference other snippets (cycles are an error). Snippets never appear in the generated API.

### Optional filters

Querty has no dynamic SQL. Write optional filters in static SQL instead:

```sql
-- :name search-orders :many
-- :nullable status
-- :nullable item_contains
SELECT o.id, o.item
FROM orders o
WHERE (:status::text IS NULL OR o.status = :status)
  AND (:item_contains::text IS NULL OR o.item ILIKE '%' || :item_contains || '%')
ORDER BY o.placed_at DESC, o.id
LIMIT :limit OFFSET :offset
```

```kotlin
SearchOrders(status = null, itemContains = "book", limit = 20, offset = 0).execute(connection)
```

Postgres plans these well for most queries. If a query with many optional filters becomes slow,
split it into separate queries.

### Directive reference

| Directive | Where | Meaning |
|---|---|---|
| `-- :name <name> <shape>` | starts a query | Name (lowercase kebab-case) and result shape |
| `-- :snippet <name>` | starts a snippet | A reusable piece of SQL text; takes no other directives |
| `-- :doc <text>` | after `:name` | KDoc for the generated object; repeat for more lines |
| `-- :type <name> <Type> [enum]` | after `:name` | Your type for the column and/or parameter called `<name>` |
| `-- :import <fully.qualified.Name>` | top of file, or after `:name` | An import for the generated file or query |
| `-- :not-null <column>` | after `:name` | Treat a result column as non-null |
| `-- :nullable <param>` | after `:name` | Make a parameter nullable |
| `:param` | in SQL | A named parameter |
| `{{snippet}}` | in SQL | Paste a snippet |

Directives go between the `:name` line and the first line of SQL.

## Type mapping

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
| one-dimensional arrays of the above, except `bytea` | `List<T?>` in results, `List<T>` in parameters |

Array elements in results are nullable because Postgres can't say otherwise. Any other type is a
generation error that tells you to cast it in SQL. For example, a Postgres enum column needs
`status::text`, and an `xid8` needs `tx::text`.

## Running queries

Import the `execute` extensions from `io.querty.jdbc` and pass a `java.sql.Connection`:

```kotlin
import io.querty.jdbc.execute

val one: Row = query.execute(connection)          // OneQuery
val maybe: Row? = query.execute(connection)       // OptionalQuery
val many: List<Row> = query.execute(connection)   // ManyQuery
val count: Long = statement.execute(connection)   // AffectedQuery
```

- **Transactions are yours.** The engine never opens, commits, rolls back or closes the connection.
  It runs in whatever transaction the connection is already in.
- **Driver exceptions pass through unchanged.** A unique violation is still a `PSQLException` with
  SQL state `23505`, so map it to your own exceptions as you would with plain JDBC.
- **Queries are inspectable values.** `query.sql` is the SQL with `$1…$n` placeholders, and
  `query.params` is the list of typed parameter values:

  ```kotlin
  val query = FindOrder(OrderId("o-1"))
  println(query.sql)    // SELECT o.id, o.status FROM orders o WHERE o.id = $1
  println(query.params) // [Param(name=id, value=o-1, type=PgType(name=text))]
  ```

  Another engine can render the same query with its own placeholder style. `query.template.render(...)`
  gives JDBC-style `?` placeholders (`PlaceholderStyle.POSITIONAL`, with repeated parameters
  expanded) or numbered `$n` placeholders (`PlaceholderStyle.NUMBERED`).

## Generating code

Run `generateQuerty` after you change a migration or a `.sql` file. The generator:

1. parses every `.sql` file under `--sql-dir` and expands snippets;
2. starts Postgres (`postgres:17` by default; change it with `--image`) and runs the `--schema`
   scripts in order;
3. asks Postgres to describe each query without running it, which gives parameter types, result
   types and each result column's source table and column;
4. writes one Kotlin file per `.sql` file.

The schema scripts can be directories, files or both, in any number of `--schema` arguments.
Files named `V<version>__<description>.sql` sort by version (`V2` before `V10`, `V1_1` before
`V1_2`, timestamp versions too). Other files follow in name order. Querty doesn't run Flyway, so
Flyway placeholders and callbacks aren't applied.

**Errors** are reported for every file in one run, as `file:line: message`, and nothing is written
if any query fails. Postgres's own errors point at the `.sql` line where the problem is, even inside
a snippet:

```
com/example/orders/shared.sql:5: Postgres: column o.reasons does not exist
com/example/orders/orders.sql:12: Parameter 'x' is used as int4 and text; cast one use so they agree
```

**Generated files** start with `// Generated by Querty`. The generator only ever writes or deletes
files with that header. If you delete or rename a `.sql` file, its old generated file is removed on
the next run. It never touches your own files in the output directory.

**`checkQuerty`** generates the code without writing it and fails if the committed code differs. Missing, extra and stale files are all listed. Run it where Docker is available, such as CI or
your integration tests. The example module makes `integrationTest` depend on it.

The generator can also be run directly:

```
java -cp <querty-codegen classpath> io.querty.codegen.MainKt \
  --sql-dir src/main/sql --schema src/main/resources/db/migration \
  --out src/generated/querty/kotlin [--image postgres:17] generate|check
```

It exits with 0 on success, 1 on query errors or differences, and 2 on bad arguments.

## Using Querty with Kotmod

Kotmod's own internals use plain JDBC. Querty is for your app's persistence on top of it:
repositories, projections and read-side queries. Kotmod hands you the transaction's connection
through `JdbcContext.withConnection`, and Querty runs in it:

```kotlin
class QuertyOrderRepository(
    private val jdbc: JdbcContext,
) : Repository<Order> {
    override fun get(id: AggregateId): Order? =
        jdbc.withConnection { conn -> FindOrder(OrderId(id.value)).execute(conn) }?.let { row ->
            when (row.status) {
                OrderStatus.PENDING -> PendingOrder(row.customerId, row.item, row.placedAt)
                OrderStatus.SHIPPED -> ShippedOrder(row.customerId, row.item, row.placedAt)
                OrderStatus.CANCELLED -> CancelledOrder(row.customerId, row.item, row.placedAt, checkNotNull(row.reason))
            }
        }

    override fun save(id: AggregateId, state: Order) {
        val query = when (state) { /* … SaveOrder(…) for each state … */ }
        jdbc.withConnection { conn -> query.execute(conn) }
    }
}
```

The repository's writes commit or roll back with the command's events, and with an outer
`jdbc.transaction { }`, like any other Kotmod repository. See `kotmod-querty-example` for the full
domain:

- Orders and Customers repositories;
- an `OrderStatusProjection` that writes through a guarded upsert;
- a read model with a `LEFT JOIN`, optional filters, paging and an `ANY(:ids)` lookup.

## Known limitations

- **Postgres only, blocking JDBC only.** The runtime is designed for other engines, such as R2DBC or
  Vert.x with `suspend`/`Flow`, but none exist yet.
- **Not published, and no Gradle plugin yet.** The `JavaExec` tasks above stand in for a plugin.
- **No batch execution.** Run a statement once per row, or write a multi-row `INSERT … SELECT
  unnest(…)`.
- **Outer-join detection is cautious.** If an outer join appears anywhere Querty can't attribute it
  precisely, every column is nullable. That covers CTEs, set operations, parenthesised joins and DML
  with joins. Use `:not-null` where you know better.
- **`:one` and `:optional` report "got 2"** for any result with more than one row.
- **No Postgres enum or domain types** without a cast to a supported type.
- **An app type imported with `:import`** whose simple name matches a Querty runtime class (`Param`,
  `PgType`, `SqlTemplate`, `notNull` or a `*Query` class) causes a conflicting-import compile error
  in the generated code.

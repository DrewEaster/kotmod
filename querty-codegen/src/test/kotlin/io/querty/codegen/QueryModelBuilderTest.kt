package io.querty.codegen

import io.querty.PgType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Parses [sql] (one query) and builds its model against a hand-built description. */
internal fun model(sql: String, paramTypes: List<String> = emptyList(), columns: List<Column> = emptyList(), fileImports: List<String> = emptyList()): QueryModel {
    val parsed = SqlFileParser.parse("orders.sql", sql)
    val def = parsed.queries.single()
    val rewritten = SqlRewriter.rewrite(def, parsed.snippets.associateBy { it.name })
    return QueryModelBuilder.build(def, rewritten, Description(paramTypes.map(::PgType), columns), fileImports)
}

internal fun col(label: String, type: String, table: String? = null, notNull: Boolean = false) =
    Column(label, PgType(type), table, if (table != null) label else null, notNull)

private fun errorOf(sql: String, paramTypes: List<String> = emptyList(), columns: List<Column> = emptyList()): QuertyError =
    assertFailsWith<QuertyException> { model(sql, paramTypes, columns) }.errors.single()

class QueryModelBuilderTest {
    @Test fun `builds params and columns with names, types and decls`() {
        val m = model(
            """
            -- :name find-order :optional
            -- :type id OrderId
            -- :import com.acme.OrderId
            SELECT o.id, o.placed_at FROM orders o WHERE o.id = :id AND o.customer_id = :customer_id
            """.trimIndent(),
            paramTypes = listOf("text", "int8"),
            columns = listOf(col("id", "text", "orders", notNull = true), col("placed_at", "timestamptz", "orders", notNull = true)),
            fileImports = listOf("com.acme.CustomerId"),
        )
        assertEquals("FindOrder", m.objectName)
        assertEquals(
            listOf(
                ParamModel("id", "id", PgType("text"), BaseType("String", "string"), TypeDecl("OrderId", false), nullable = false),
                ParamModel("customer_id", "customerId", PgType("int8"), BaseType("Long", "long"), null, nullable = false),
            ),
            m.params,
        )
        assertEquals("placedAt", m.columns[1].kotlinName)
        assertEquals(TypeDecl("OrderId", false), m.columns[0].decl)
        assertEquals(listOf("com.acme.CustomerId", "com.acme.OrderId"), m.imports)
    }

    @Test fun `a repeated param takes its type from every use`() {
        val m = model("-- :name q :many\nSELECT 1 AS x WHERE :a = 1 OR :b = 2 OR :a = 3", listOf("int4", "text", "int4"), listOf(col("x", "int4")))
        assertEquals(listOf("a" to PgType("int4"), "b" to PgType("text")), m.params.map { it.sqlName to it.type })
    }

    @Test fun `conflicting param types are an error`() {
        val error = errorOf("\n-- :name q :many\nSELECT 1 AS x WHERE :x = 1 OR :x = 'a'", listOf("int4", "text"), listOf(col("x", "int4")))
        assertEquals(QuertyError("orders.sql", 2, "Parameter 'x' is used as int4 and text; cast one use so they agree"), error)
    }

    @Test fun `unsupported column type is an error`() {
        val error = errorOf("-- :name q :many\nSELECT pg_current_xact_id() AS tx", columns = listOf(col("tx", "xid8")))
        assertEquals("Column 'tx' has unsupported type xid8; cast it in SQL (e.g. tx::text)", error.message)
        assertEquals(1, error.line)
    }

    @Test fun `unsupported param type is an error`() {
        val error = errorOf("-- :name q :affected\nDELETE FROM t WHERE tx = :tx", listOf("xid8"))
        assertEquals("Parameter 'tx' has unsupported type xid8; cast it in SQL (e.g. :tx::text)", error.message)
    }

    @Test fun `row shapes need columns`() {
        for (shape in listOf("one", "optional", "many")) {
            val error = errorOf("-- :name q :$shape\nDELETE FROM t")
            assertEquals("Query 'q' is :$shape but returns no columns; use :affected for a statement without results", error.message)
        }
    }

    @Test fun `affected must not return columns`() {
        val error = errorOf("-- :name q :affected\nDELETE FROM t RETURNING id", columns = listOf(col("id", "int4", "t")))
        assertEquals("Query 'q' is :affected but returns columns; use :one, :optional or :many", error.message)
    }

    @Test fun `affected has no columns in its model`() {
        assertEquals(emptyList(), model("-- :name q :affected\nDELETE FROM t").columns)
    }

    @Test fun `duplicate column label is an error`() {
        val error = errorOf("-- :name q :many\nSELECT a.id, b.id FROM a, b", columns = listOf(col("id", "int4", "a"), col("id", "int4", "b")))
        assertEquals("Column label 'id' appears twice; alias one of them", error.message)
    }

    @Test fun `type naming neither a param nor a column is an error, at its line`() {
        val error = errorOf("-- :name q :many\n-- :type nope Nope\nSELECT 1 AS x", columns = listOf(col("x", "int4")))
        assertEquals(QuertyError("orders.sql", 2, "':type nope' names neither a parameter nor a result column"), error)
    }

    @Test fun `not-null naming no column is an error, at its line`() {
        val error = errorOf("-- :name q :many\n-- :not-null nope\nSELECT 1 AS x", columns = listOf(col("x", "int4")))
        assertEquals(QuertyError("orders.sql", 2, "':not-null nope' names no result column"), error)
    }

    @Test fun `nullable naming no param is an error, at its line`() {
        val error = errorOf("-- :name q :many\n-- :nullable x\nSELECT 1 AS x", columns = listOf(col("x", "int4")))
        assertEquals(QuertyError("orders.sql", 2, "':nullable x' names no parameter"), error)
    }

    @Test fun `enum on a non-text type is an error, at its line`() {
        val error = errorOf("-- :name q :many\n-- :type n Kind enum\nSELECT 1 AS n", columns = listOf(col("n", "int4")))
        assertEquals(QuertyError("orders.sql", 2, "':type n' is an enum but n has type int4; enums need text or varchar"), error)
    }

    @Test fun `enum on text, varchar and text arrays is fine`() {
        val m = model(
            "-- :name q :many\n-- :type a Kind enum\n-- :type b Kind enum\n-- :type c Kind enum\nSELECT a, b, c FROM t",
            columns = listOf(col("a", "text"), col("b", "varchar"), col("c", "_text")),
        )
        assertEquals(3, m.columns.size)
    }

    @Test fun `all errors are reported together`() {
        val errors = assertFailsWith<QuertyException> {
            model("-- :name q :many\n-- :not-null a\n-- :nullable b\nSELECT 1 AS x", columns = listOf(col("x", "int4")))
        }.errors
        assertEquals(listOf(2, 3), errors.map { it.line })
    }

    @Test fun `a plain NOT NULL column is non-null and an expression is nullable`() {
        val m = model("-- :name q :many\nSELECT o.id, 1 AS n FROM orders o", columns = listOf(col("id", "text", "orders", notNull = true), col("n", "int4")))
        assertEquals(listOf(false, true), m.columns.map { it.nullable })
    }

    @Test fun `a LEFT JOINed NOT NULL column is nullable`() {
        val m = model(
            "-- :name q :many\nSELECT o.id, c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id",
            columns = listOf(col("id", "text", "orders", notNull = true), col("name", "text", "customers", notNull = true)),
        )
        assertEquals(listOf(false, true), m.columns.map { it.nullable })
    }

    @Test fun `not-null overrides a LEFT JOIN`() {
        val m = model(
            "-- :name q :many\n-- :not-null name\nSELECT c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id",
            columns = listOf(col("name", "text", "customers", notNull = true)),
        )
        assertFalse(m.columns.single().nullable)
    }

    @Test fun `with an unattributable outer join every column is nullable`() {
        val m = model(
            "-- :name q :many\nSELECT o.id FROM orders o LEFT JOIN (SELECT * FROM customers) c ON c.id = o.customer_id",
            columns = listOf(col("id", "text", "orders", notNull = true)),
        )
        assertTrue(m.columns.single().nullable)
    }

    @Test fun `a param is nullable only when declared`() {
        val m = model("-- :name q :affected\n-- :nullable b\nUPDATE t SET a = :a, b = :b", listOf("text", "text"))
        assertEquals(listOf(false, true), m.params.map { it.nullable })
    }
}

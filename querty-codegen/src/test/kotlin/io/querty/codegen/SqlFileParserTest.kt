package io.querty.codegen

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

private val findOrder = """
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
""".trimIndent()

private fun errorsOf(text: String) = assertFailsWith<QuertyException> { SqlFileParser.parse("q.sql", text) }.errors

class SqlFileParserTest {
    @Test fun `parses a query with all directives`() {
        val file = SqlFileParser.parse("orders.sql", findOrder)
        val q = file.queries.single()
        assertEquals("find-order", q.name)
        assertEquals(Shape.OPTIONAL, q.shape)
        assertEquals("Load one order by id", q.doc)
        assertEquals(TypeDecl("OrderStatus", isEnum = false), q.types["status"])
        assertEquals(TypeDecl("OrderId", isEnum = false), q.types["id"])
        assertTrue(q.body.text.startsWith("SELECT"))
        assertEquals("o.id, o.customer_id, o.status, o.item, o.reason, o.placed_at", file.snippets.single().body.text)
    }

    @Test fun `parses doc lines, imports, not-null and nullable`() {
        val q = SqlFileParser.parse(
            "q.sql",
            """
            -- :name x :many
            -- :doc first
            -- :doc second
            -- :import com.acme.Money
            -- :not-null total
            -- :nullable note
            SELECT 1;
            """.trimIndent(),
        ).queries.single()
        assertEquals("first\nsecond", q.doc)
        assertEquals(listOf("com.acme.Money"), q.imports)
        assertEquals(setOf("total"), q.notNull)
        assertEquals(setOf("note"), q.nullable)
        assertEquals("SELECT 1", q.body.text)
    }

    @Test fun `type with enum flag`() {
        val q = SqlFileParser.parse("q.sql", "-- :name x :one\n-- :type status OrderStatus enum\nSELECT 1").queries.single()
        assertEquals(TypeDecl("OrderStatus", isEnum = true), q.types["status"])
        assertNull(q.doc)
    }

    @Test fun `several queries and a snippet in one file, bodies split correctly`() {
        val file = SqlFileParser.parse(
            "q.sql",
            """
            -- :name a :one
            SELECT 1

            -- :snippet s
            x, y

            -- :name b :affected
            UPDATE t
            SET a = 1
            """.trimIndent(),
        )
        assertEquals(listOf("a", "b"), file.queries.map { it.name })
        assertEquals("SELECT 1", file.queries[0].body.text)
        assertEquals("UPDATE t\nSET a = 1", file.queries[1].body.text)
        assertEquals(Shape.AFFECTED, file.queries[1].shape)
        assertEquals("x, y", file.snippets.single().body.text)
    }

    @Test fun `body line numbers point at the file`() {
        val file = SqlFileParser.parse(
            "q.sql",
            "-- :name a :one\n-- :doc d\n-- plain comment\nSELECT 1\nFROM t\n\n-- :name b :one\n\nSELECT 2",
        )
        assertEquals(4, file.queries[0].body.firstLine)
        assertEquals("SELECT 1\nFROM t", file.queries[0].body.text)
        assertEquals(9, file.queries[1].body.firstLine)
    }

    @Test fun `file-level imports apply to the file`() {
        val file = SqlFileParser.parse("q.sql", "-- :import a.B\n-- :import c.D\n\n-- :name x :one\n-- :import e.F\nSELECT 1")
        assertEquals(listOf("a.B", "c.D"), file.fileImports)
        assertEquals(listOf("e.F"), file.queries.single().imports)
    }

    @Test fun `missing shape is an error with the line`() {
        assertEquals(
            listOf(QuertyError("q.sql", 1, "Query 'x' has no shape; expected one of :one, :optional, :many, :affected")),
            errorsOf("-- :name x\nSELECT 1"),
        )
    }

    @Test fun `unknown directive is an error`() {
        assertEquals(
            listOf(QuertyError("q.sql", 2, "Unknown directive ':frobnicate'")),
            errorsOf("-- :name x :one\n-- :frobnicate y\nSELECT 1"),
        )
    }

    @Test fun `duplicate query name in a file is an error`() {
        val errors = errorsOf("-- :name x :one\nSELECT 1\n-- :name x :many\nSELECT 2")
        assertEquals(listOf(QuertyError("q.sql", 3, "Duplicate query name 'x'")), errors)
    }

    @Test fun `query with empty body is an error`() {
        assertEquals(
            listOf(QuertyError("q.sql", 1, "Query 'x' has an empty body")),
            errorsOf("-- :name x :one\n-- :doc nothing here\n\n"),
        )
    }

    @Test fun `directive inside a body is an error`() {
        assertEquals(
            listOf(QuertyError("q.sql", 3, "Directive ':type' is not allowed inside a body; directives go before the SQL")),
            errorsOf("-- :name x :one\nSELECT 1\n-- :type a B"),
        )
    }

    @Test fun `snippet takes no other directives`() {
        assertEquals(
            listOf(QuertyError("q.sql", 2, "Snippet 's' takes no directives; found ':doc'")),
            errorsOf("-- :snippet s\n-- :doc nope\na, b"),
        )
    }

    @Test fun `invalid name is an error`() {
        assertEquals(
            listOf(QuertyError("q.sql", 1, "Invalid name 'Find_Order'; names are lowercase kebab-case, like 'find-order'")),
            errorsOf("-- :name Find_Order :one\nSELECT 1"),
        )
    }

    @Test fun `all errors in a file are reported`() {
        assertEquals(2, errorsOf("-- :name a\nSELECT 1\n-- :name b :one\n").size)
    }
}

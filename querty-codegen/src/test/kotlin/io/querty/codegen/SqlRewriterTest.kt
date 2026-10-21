package io.querty.codegen

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private fun query(sql: String, file: String = "q.sql", firstLine: Int = 1) =
    QueryDef("q", Shape.MANY, null, emptyMap(), emptyList(), emptySet(), emptySet(), SourceText(file, firstLine, sql))

private fun snippet(name: String, sql: String, file: String = "q.sql", firstLine: Int = 1) =
    name to SnippetDef(name, SourceText(file, firstLine, sql))

private fun rewrite(sql: String, vararg snippets: Pair<String, SnippetDef>) = SqlRewriter.rewrite(query(sql), mapOf(*snippets))

class SqlRewriterTest {
    @Test fun `named parameters become numbered and a repeat reuses its number`() {
        val r = rewrite("SELECT * FROM t WHERE a = :x AND b = :y OR c = :x")
        assertTrue(r.numberedSql().endsWith("a = \$1 AND b = \$2 OR c = \$1"), r.numberedSql())
        assertEquals(listOf("x", "y"), r.paramNames)
        assertEquals(listOf(0, 1, 0), r.refs)
        assertEquals("SELECT * FROM t WHERE a = ? AND b = ? OR c = ?", r.positionalSql())
    }

    @Test fun `casts are not parameters`() {
        val r = rewrite("SELECT :x::text")
        assertEquals("SELECT \$1::text", r.numberedSql())
        assertEquals(listOf("x"), r.paramNames)
    }

    @Test fun `colons in strings, quoted identifiers, comments, dollar quotes and slices are untouched`() {
        listOf("':x'", "E'\\':x'", "\"a:x\"", "-- :x", "/* :x /* :y */ */", "\$\$ :x \$\$", "\$f\$ :x \$f\$", "arr[1:2]").forEach { sql ->
            val r = rewrite("SELECT $sql")
            assertEquals("SELECT $sql", r.numberedSql())
            assertEquals(emptyList(), r.paramNames, sql)
        }
    }

    @Test fun `lexing resumes after strings and comments`() {
        val r = rewrite("SELECT 'it''s :a', :b -- :c\n, /* :d */ :e, \$t\$ :f \$t\$, :g")
        assertEquals(listOf("b", "e", "g"), r.paramNames)
    }

    @Test fun `snippet is expanded and its parameters join the query`() {
        val r = rewrite(
            "SELECT {{cols}} FROM t WHERE a = :a AND {{filter}}",
            snippet("cols", "t.id, :label AS label"),
            snippet("filter", "b = :b AND c = :a"),
        )
        assertEquals("SELECT t.id, \$1 AS label FROM t WHERE a = \$2 AND b = \$3 AND c = \$2", r.numberedSql())
        assertEquals(listOf("label", "a", "b"), r.paramNames)
    }

    @Test fun `snippets expand recursively`() {
        val r = rewrite("SELECT {{outer}}", snippet("outer", "1, {{inner}}"), snippet("inner", ":x"))
        assertEquals("SELECT 1, \$1", r.numberedSql())
    }

    @Test fun `unknown snippet reports the referencing line`() {
        val e = assertFailsWith<QuertyException> {
            SqlRewriter.rewrite(query("SELECT\n  {{missing}}\nFROM t", "orders.sql", firstLine = 5), emptyMap())
        }
        assertEquals("orders.sql", e.errors.single().file)
        assertEquals(6, e.errors.single().line)
        assertTrue("missing" in e.errors.single().message)
    }

    @Test fun `snippet cycle is an error`() {
        val e = assertFailsWith<QuertyException> {
            rewrite("SELECT {{a}}", snippet("a", "{{b}}"), snippet("b", "{{a}}"))
        }
        assertTrue("a -> b -> a" in e.message!!, e.message)
    }

    @Test fun `server offsets map back past placeholders`() {
        val r = rewrite("SELECT :a, :a,\nnope")
        val server = "SELECT \$1, \$2,\nnope"
        assertEquals("q.sql" to 2, r.locateServerOffset(server.indexOf("nope")))
        assertEquals("q.sql" to 1, r.locateServerOffset(server.indexOf("\$2") + 1))
    }

    @Test fun `server offsets after a question mark literal map to the right line`() {
        val r = rewrite("SELECT '?', :a\n, nope")
        val server = "SELECT '?', \$1\n, nope"
        assertEquals("SELECT '?', ?\n, nope", r.positionalSql())
        assertEquals("q.sql" to 2, r.locateServerOffset(server.indexOf("nope")))
        assertEquals("q.sql" to 1, r.locateServerOffset(server.indexOf("\n") - 1))
    }

    @Test fun `server offsets account for two-digit placeholder numbers`() {
        val r = rewrite((1..10).joinToString(", ", "SELECT ") { ":a" } + ",\nnope")
        val server = (1..10).joinToString(", ", "SELECT ") { "\$$it" } + ",\nnope"
        assertEquals("q.sql" to 2, r.locateServerOffset(server.indexOf("nope")))
    }

    @Test fun `source map points into the snippet's file and line`() {
        val snippets = mapOf(snippet("cols", "o.id,\no.status", "shared.sql", firstLine = 3))
        val r = SqlRewriter.rewrite(query("SELECT\n{{cols}} FROM orders o\nWHERE o.id = :id", "orders.sql", firstLine = 9), snippets)
        val sql = r.numberedSql()
        assertEquals("shared.sql" to 4, r.sourceMap.locate(sql.indexOf("o.status")))
        assertEquals("shared.sql" to 3, r.sourceMap.locate(sql.indexOf("o.id")))
        val after = r.sourceMap.locate(sql.indexOf(" FROM"))
        assertEquals("orders.sql", after.first)
        assertTrue(after.second >= 10, after.toString())
        assertEquals("orders.sql" to 11, r.sourceMap.locate(sql.indexOf("\$1")))
    }
}

package io.querty.codegen

import io.querty.PgType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryDescriberIntegrationTest {
    private val schema: Path =
        Files.createTempFile("querty-schema", ".sql").also {
            it.writeText(
                """
                CREATE TABLE customers (id text PRIMARY KEY, name text NOT NULL);
                CREATE TABLE orders (
                    id text PRIMARY KEY,
                    customer_id text NOT NULL,
                    status text NOT NULL,
                    reason text,
                    placed_at timestamptz NOT NULL
                );
                """.trimIndent(),
            )
        }
    private val db = SchemaDatabase("postgres:17", listOf(schema))
    private val describer = QueryDescriber(db.connection)

    @AfterAll
    fun close() {
        db.close()
        Files.deleteIfExists(schema)
    }

    @BeforeEach
    fun clean() {
        db.connection.createStatement().use { it.execute("TRUNCATE orders") }
        db.connection.commit()
    }

    private fun described(sql: String) = assertIs<DescribeResult.Described>(describer.describe(sql)).description

    @Test
    fun `select describes param and column types with base columns`() {
        val d = described("SELECT o.id, o.reason, o.placed_at FROM orders o WHERE o.customer_id = ? LIMIT ?")

        assertEquals(listOf(PgType.TEXT, PgType.INT8), d.paramTypes)
        val (id, reason, placedAt) = d.columns
        assertEquals("id", id.label)
        assertEquals("orders", id.baseTable)
        assertEquals("id", id.baseColumn)
        assertTrue(id.baseNotNull)
        assertFalse(reason.baseNotNull)
        assertEquals(PgType.TIMESTAMPTZ, placedAt.type)
    }

    @Test
    fun `expression column has no base`() {
        val n = described("SELECT count(*) AS n FROM orders").columns.single()

        assertEquals("n", n.label)
        assertNull(n.baseTable)
        assertNull(n.baseColumn)
        assertEquals(PgType.INT8, n.type)
    }

    @Test
    fun `insert returning describes the returned columns`() {
        val d = described("INSERT INTO orders (id, customer_id, status, placed_at) VALUES (?, ?, ?, ?) RETURNING id, reason")

        assertEquals(listOf(PgType.TEXT, PgType.TEXT, PgType.TEXT, PgType.TIMESTAMPTZ), d.paramTypes)
        assertEquals(listOf("id", "reason"), d.columns.map { it.label })
    }

    @Test
    fun `insert without returning has no columns`() {
        val d = described("INSERT INTO orders (id, customer_id, status, placed_at) VALUES (?, ?, ?, ?)")

        assertTrue(d.columns.isEmpty())
    }

    @Test
    fun `unknown column fails with a position`() {
        val failed = assertIs<DescribeResult.Failed>(describer.describe("SELECT o.nope FROM orders o"))

        assertTrue("nope" in failed.message)
        assertEquals(7, failed.position)
    }

    @Test
    fun `describe does not execute`() {
        db.connection.createStatement().use {
            it.execute("INSERT INTO orders VALUES ('1', 'c', 's', null, now())")
        }
        db.connection.commit()

        described("DELETE FROM orders")

        db.connection.createStatement().use {
            val rs = it.executeQuery("SELECT count(*) FROM orders")
            rs.next()
            assertEquals(1, rs.getInt(1))
        }
    }

    @Test
    fun `script error reports the script and line`(@TempDir dir: Path) {
        val script = dir.resolve("bad.sql")
        script.writeText("CREATE TABLE a (b text);\n\nCREATE TABLE x (y nosuchtype);\n")

        val ex = assertThrows<QuertyException> { SchemaDatabase("postgres:17", listOf(script)) }

        val error = ex.errors.single()
        assertEquals(script.toString(), error.file)
        assertEquals(3, error.line)
    }
}

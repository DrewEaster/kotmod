package io.querty.jdbc

import io.querty.AffectedQuery
import io.querty.ManyQuery
import io.querty.OneQuery
import io.querty.OptionalQuery
import io.querty.Param
import io.querty.PgType
import io.querty.RowReader
import io.querty.SqlTemplate
import io.querty.UnexpectedRowCountException
import io.querty.notNull
import org.junit.jupiter.api.BeforeEach
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.postgresql.PostgreSQLContainer
import java.math.BigDecimal
import java.sql.Connection
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Instant

class JdbcExecuteIntegrationTest {
    private data class Everything(
        val id: Int,
        val t: String? = null,
        val i4: Int? = null,
        val i8: Long? = null,
        val b: Boolean? = null,
        val f8: Double? = null,
        val n: BigDecimal? = null,
        val ts: Instant? = null,
        val lts: LocalDateTime? = null,
        val d: LocalDate? = null,
        val u: UUID? = null,
        val by: ByteArray? = null,
        val j: String? = null,
        val arr: List<Any?>? = null,
    )

    private val columns = "id, t, i4, i8, b, f8, n, ts, lts, d, u, by, j, arr"

    private fun read(r: RowReader) = Everything(
        id = notNull(r.int(0), "id"),
        t = r.string(1),
        i4 = r.int(2),
        i8 = r.long(3),
        b = r.boolean(4),
        f8 = r.double(5),
        n = r.bigDecimal(6),
        ts = r.instant(7),
        lts = r.localDateTime(8),
        d = r.localDate(9),
        u = r.uuid(10),
        by = r.bytes(11),
        j = r.string(12),
        arr = r.list(13, PgType.INT4),
    )

    private fun sql(text: String) = SqlTemplate(listOf(text), emptyList())

    private fun insert(values: Everything): AffectedQuery {
        val params = listOf(
            Param("id", values.id, PgType.INT4),
            Param("t", values.t, PgType.TEXT),
            Param("i4", values.i4, PgType.INT4),
            Param("i8", values.i8, PgType.INT8),
            Param("b", values.b, PgType.BOOL),
            Param("f8", values.f8, PgType.FLOAT8),
            Param("n", values.n, PgType.NUMERIC),
            Param("ts", values.ts, PgType.TIMESTAMPTZ),
            Param("lts", values.lts, PgType.TIMESTAMP),
            Param("d", values.d, PgType.DATE),
            Param("u", values.u, PgType.UUID),
            Param("by", values.by, PgType.BYTEA),
            Param("j", values.j, PgType.JSONB),
            Param("arr", values.arr, PgType("_int4")),
        )
        val parts = listOf("INSERT INTO everything($columns) VALUES (") + List(13) { ", " } + listOf(")")
        return AffectedQuery(SqlTemplate(parts, params.indices.toList()), params)
    }

    private fun selectAll() = OneQuery(sql("SELECT $columns FROM everything"), emptyList(), ::read)

    private fun assertSame(expected: Everything, actual: Everything) {
        assertEquals(expected.copy(by = null), actual.copy(by = null))
        assertContentEquals(expected.by, actual.by)
    }

    @BeforeEach
    fun truncate() {
        connection().use { it.createStatement().use { s -> s.execute("TRUNCATE everything") } }
    }

    @Test
    fun `every base type round-trips through insert and select`() {
        val values = Everything(
            id = 1,
            t = "hello",
            i4 = 42,
            i8 = 9_000_000_000L,
            b = true,
            f8 = 1.5,
            n = BigDecimal("12345.6789"),
            ts = Instant.fromEpochSeconds(1_700_000_000, 123_456_000),
            lts = LocalDateTime.of(2026, 10, 21, 12, 30, 15, 250_000_000),
            d = LocalDate.of(2026, 10, 21),
            u = UUID.randomUUID(),
            by = byteArrayOf(1, 2, 3, 0, -1),
            j = """{"a": 1}""",
            arr = listOf(1, 2, 3),
        )
        connection().use { conn ->
            assertEquals(1L, insert(values).execute(conn))
            assertSame(values, selectAll().execute(conn))
        }
    }

    @Test
    fun `nulls round-trip for every type`() {
        val values = Everything(id = 7)
        connection().use { conn ->
            insert(values).execute(conn)
            assertSame(values, selectAll().execute(conn))
        }
    }

    @Test
    fun `a repeated parameter binds once by name`() {
        val query = OneQuery(
            SqlTemplate(listOf("SELECT ", "::int4 + ", "::int4"), listOf(0, 0)),
            listOf(Param("x", 2, PgType.INT4)),
        ) { notNull(it.int(0), "sum") }
        connection().use { assertEquals(4, query.execute(it)) }
    }

    @Test
    fun `one throws when there are no rows`() {
        connection().use { conn ->
            val e = assertFailsWith<UnexpectedRowCountException> { selectAll().execute(conn) }
            assertEquals("exactly one", e.expected)
            assertEquals(0, e.actual)
        }
    }

    @Test
    fun `optional returns null for no rows and throws for two`() {
        val query = OptionalQuery(sql("SELECT id FROM everything"), emptyList()) { notNull(it.int(0), "id") }
        connection().use { conn ->
            assertNull(query.execute(conn))
            insert(Everything(id = 1)).execute(conn)
            assertEquals(1, query.execute(conn))
            insert(Everything(id = 2)).execute(conn)
            val e = assertFailsWith<UnexpectedRowCountException> { query.execute(conn) }
            assertEquals("at most one", e.expected)
            assertEquals(2, e.actual)
        }
    }

    @Test
    fun `many returns rows in order`() {
        val query = ManyQuery(sql("SELECT id FROM everything ORDER BY id"), emptyList()) { notNull(it.int(0), "id") }
        connection().use { conn ->
            listOf(3, 1, 2).forEach { insert(Everything(id = it)).execute(conn) }
            assertEquals(listOf(1, 2, 3), query.execute(conn))
        }
    }

    @Test
    fun `affected returns the row count`() {
        connection().use { conn ->
            listOf(1, 2, 3).forEach { insert(Everything(id = it, t = "a")).execute(conn) }
            val update = AffectedQuery(
                SqlTemplate(listOf("UPDATE everything SET t = 'b' WHERE id <= ", ""), listOf(0)),
                listOf(Param("max", 2, PgType.INT4)),
            )
            assertEquals(2L, update.execute(conn))
        }
    }

    @Test
    fun `array parameter works with ANY`() {
        val query = ManyQuery(
            SqlTemplate(listOf("SELECT id FROM everything WHERE id = ANY(", ") ORDER BY id"), listOf(0)),
            listOf(Param("ids", listOf(1, 3), PgType("_int4"))),
        ) { notNull(it.int(0), "id") }
        connection().use { conn ->
            listOf(1, 2, 3).forEach { insert(Everything(id = it)).execute(conn) }
            assertEquals(listOf(1, 3), query.execute(conn))
        }
    }

    @Test
    fun `timestamptz arrays round-trip as instants`() {
        val instants = listOf(Instant.fromEpochSeconds(1_700_000_000, 5_000), Instant.fromEpochSeconds(1_700_000_100))
        val query = OneQuery(
            SqlTemplate(listOf("SELECT ", "::timestamptz[]"), listOf(0)),
            listOf(Param("ts", instants, PgType("_timestamptz"))),
        ) { it.list(0, PgType.TIMESTAMPTZ) }
        connection().use { assertEquals(instants, query.execute(it)) }
    }

    @Test
    fun `literal question-mark operator survives`() {
        val query = OneQuery(sql("""SELECT '{"a":1}'::jsonb ? 'a'"""), emptyList()) { notNull(it.boolean(0), "r") }
        connection().use { assertTrue(query.execute(it)) }
    }

    @Test
    fun `joins the caller's transaction`() {
        connection().use { conn ->
            conn.autoCommit = false
            insert(Everything(id = 1)).execute(conn)
            conn.rollback()
            assertFalse(conn.isClosed)
            assertFalse(conn.autoCommit)
            val count = OneQuery(sql("SELECT count(*) FROM everything"), emptyList()) { notNull(it.long(0), "c") }
            assertEquals(0L, count.execute(conn))
        }
    }

    companion object {
        private val container = PostgreSQLContainer("postgres:17").apply { start() }
        private val dataSource = PGSimpleDataSource().apply {
            setURL(container.jdbcUrl)
            user = container.username
            password = container.password
        }

        init {
            connection().use {
                it.createStatement().use { s ->
                    s.execute(
                        "CREATE TABLE everything(id int4 primary key, t text, i4 int4, i8 int8, b bool, f8 float8, " +
                            "n numeric, ts timestamptz, lts timestamp, d date, u uuid, by bytea, j jsonb, arr int4[])",
                    )
                }
            }
        }

        private fun connection(): Connection = dataSource.connection
    }
}

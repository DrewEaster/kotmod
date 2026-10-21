package io.querty

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

private val a = Param("a", 1, PgType.INT4)
private val b = Param("b", "x", PgType.TEXT)
private val t = SqlTemplate(listOf("SELECT * FROM t WHERE x = ", " AND y = ", " OR x = ", ""), listOf(0, 1, 0))

class SqlTemplateTest {
    @Test fun `numbered rendering reuses a repeated parameter's number`() =
        assertEquals("SELECT * FROM t WHERE x = \$1 AND y = \$2 OR x = \$1", t.render(PlaceholderStyle.NUMBERED))

    @Test fun `positional rendering repeats the parameter in bind order`() {
        assertEquals("SELECT * FROM t WHERE x = ? AND y = ? OR x = ?", t.render(PlaceholderStyle.POSITIONAL))
        assertEquals(listOf(a, b, a), t.bindOrder(PlaceholderStyle.POSITIONAL, listOf(a, b)))
    }

    @Test fun `positional rendering escapes literal question marks`() =
        assertEquals(
            "SELECT '{}'::jsonb ?? 'a' AND ?",
            SqlTemplate(listOf("SELECT '{}'::jsonb ? 'a' AND ", ""), listOf(0)).render(PlaceholderStyle.POSITIONAL),
        )

    @Test fun `mismatched parts and refs are rejected`() {
        assertFailsWith<IllegalArgumentException> { SqlTemplate(listOf("a"), listOf(0)) }
    }

    @Test fun `array type knows its element`() {
        assertTrue(PgType("_int4").isArray)
        assertEquals(PgType.INT4, PgType("_int4").elementType)
    }

    @Test fun `notNull names the column`() {
        assertEquals(
            "Column 'c' was null but is declared non-null",
            assertFailsWith<IllegalStateException> { notNull(null as String?, "c") }.message,
        )
    }
}

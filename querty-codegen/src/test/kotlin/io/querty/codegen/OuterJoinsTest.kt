package io.querty.codegen

import kotlin.test.Test
import kotlin.test.assertEquals

private fun known(vararg tables: String) = NullableTables.Known(tables.toSet())

class OuterJoinsTest {
    @Test fun `the customer-orders query makes customers nullable`() {
        val sql = """
            SELECT o.id, o.customer_id, o.status, o.item, o.reason, o.placed_at, c.name AS customer_name
            FROM orders o
            LEFT JOIN customers c ON c.id = o.customer_id
            WHERE o.customer_id = ${'$'}1
              AND (${'$'}2::text IS NULL OR o.status = ${'$'}2)
            ORDER BY o.placed_at DESC
            LIMIT ${'$'}3 OFFSET ${'$'}4
        """.trimIndent()
        assertEquals(known("customers"), OuterJoins.nullableTables(sql))
    }

    @Test fun `an inner join makes nothing nullable`() {
        assertEquals(known(), OuterJoins.nullableTables("SELECT * FROM a JOIN b ON a.id = b.id"))
    }

    @Test fun `a right join makes the tables before it nullable`() {
        assertEquals(known("a"), OuterJoins.nullableTables("SELECT * FROM a RIGHT JOIN b ON a.id = b.id"))
        assertEquals(
            known("a", "b"),
            OuterJoins.nullableTables("SELECT * FROM a JOIN b ON a.id = b.id RIGHT JOIN c ON c.id = b.id"),
        )
    }

    @Test fun `a full outer join makes both sides nullable`() {
        assertEquals(known("a", "b"), OuterJoins.nullableTables("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id"))
    }

    @Test fun `left outer join counts as left and keywords are case-insensitive`() {
        assertEquals(known("b"), OuterJoins.nullableTables("select * from a left outer join b on a.id = b.id"))
    }

    @Test fun `a subquery or parenthesised join beside an outer join is unknown`() {
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("SELECT * FROM a LEFT JOIN (SELECT id FROM b) x ON x.id = a.id"),
        )
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("SELECT * FROM a LEFT JOIN (b JOIN c ON c.id = b.id) ON b.id = a.id"),
        )
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("SELECT * FROM a LEFT JOIN LATERAL f(a.id) x ON true"),
        )
    }

    @Test fun `a subquery without an outer join is not unknown`() {
        assertEquals(known(), OuterJoins.nullableTables("SELECT * FROM (SELECT 1) x"))
    }

    @Test fun `parentheses in an ON condition are fine`() {
        assertEquals(
            known("b"),
            OuterJoins.nullableTables("SELECT * FROM a LEFT JOIN b ON (a.id = b.id AND lower(b.k) = 'x') WHERE a.x = 1"),
        )
    }

    @Test fun `schema-qualified and quoted names use the table part`() {
        assertEquals(
            known("customers"),
            OuterJoins.nullableTables("SELECT * FROM orders o LEFT JOIN public.customers c ON c.id = o.c"),
        )
        assertEquals(
            known("customers"),
            OuterJoins.nullableTables("""SELECT * FROM orders o LEFT JOIN "public"."Customers" c ON true"""),
        )
    }

    @Test fun `join keywords in strings and comments are ignored`() {
        assertEquals(known(), OuterJoins.nullableTables("SELECT 'LEFT JOIN x' FROM a"))
        assertEquals(known(), OuterJoins.nullableTables("SELECT * FROM a -- LEFT JOIN b\nWHERE x = 'LEFT JOIN y'"))
    }

    @Test fun `a comma separated table list without outer join is empty`() {
        assertEquals(known(), OuterJoins.nullableTables("SELECT * FROM a, b WHERE a.id = b.id"))
    }

    @Test fun `a left join after a comma list`() {
        assertEquals(known("c"), OuterJoins.nullableTables("SELECT * FROM a, b LEFT JOIN c ON c.id = b.id"))
    }

    @Test fun `DML returning with no joins is empty`() {
        assertEquals(known(), OuterJoins.nullableTables("INSERT INTO a (x) VALUES (${'$'}1) RETURNING id"))
        assertEquals(known(), OuterJoins.nullableTables("DELETE FROM a WHERE id = ${'$'}1 RETURNING id"))
    }

    @Test fun `outer joins nested in the FROM are unknown`() {
        for (sql in listOf(
            "SELECT * FROM (a LEFT JOIN b ON a.id = b.id) JOIN c ON c.id = a.id",
            "SELECT * FROM a JOIN (b LEFT JOIN c ON c.id = b.id) ON b.id = a.id",
            "SELECT * FROM (SELECT * FROM x LEFT JOIN y ON y.id = x.id) s",
        )) {
            assertEquals(NullableTables.Unknown, OuterJoins.nullableTables(sql), sql)
        }
    }

    @Test fun `CTEs with outer joins are unknown`() {
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("WITH x AS (SELECT a.v FROM a LEFT JOIN b ON b.id = a.id) SELECT x.v FROM x"),
        )
    }

    @Test fun `set operations with outer joins are unknown`() {
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("(SELECT a.v FROM a LEFT JOIN b ON b.id = a.id) UNION (SELECT 1)"),
        )
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("SELECT v FROM a UNION ALL SELECT v FROM c LEFT JOIN d ON d.id = c.id"),
        )
    }

    @Test fun `quoted identifiers never match keywords`() {
        assertEquals(
            known("group"),
            OuterJoins.nullableTables("""SELECT * FROM "order" o LEFT JOIN "group" g ON g.id = o.g"""),
        )
    }

    @Test fun `IS DISTINCT FROM beside an outer join is unknown`() {
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("SELECT a IS DISTINCT FROM b FROM t RIGHT JOIN u ON true"),
        )
    }

    @Test fun `DML with an outer join is unknown`() {
        assertEquals(
            NullableTables.Unknown,
            OuterJoins.nullableTables("DELETE FROM a USING b RIGHT JOIN c ON c.id = b.id WHERE a.id = c.id"),
        )
    }

    @Test fun `left as a function is not a join`() {
        assertEquals(known(), OuterJoins.nullableTables("SELECT left(b.k, 1) FROM a JOIN b ON left(a.k, 1) = b.k JOIN c ON true"))
    }

    @Test fun `FROM ONLY names the table`() {
        assertEquals(known("b"), OuterJoins.nullableTables("SELECT * FROM ONLY a LEFT JOIN ONLY b ON b.id = a.id"))
    }
}

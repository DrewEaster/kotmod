package io.kotmod.jdbc

import io.kotmod.postgres.support.IntegrationTest
import org.junit.jupiter.api.BeforeEach
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Behaviour every [JdbcContext] implementation must have. Subclass it and implement [createJdbcContext]. */
abstract class JdbcContextContract : IntegrationTest() {
    protected abstract fun createJdbcContext(dataSource: DataSource): JdbcContext

    protected lateinit var context: JdbcContext

    @BeforeEach
    fun setUpContract() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE IF NOT EXISTS jdbc_probe (id TEXT PRIMARY KEY)")
                stmt.execute("TRUNCATE jdbc_probe")
            }
        }
        context = createJdbcContext(dataSource)
    }

    protected fun insertProbe(id: String) {
        context.withConnection { conn ->
            conn.prepareStatement("INSERT INTO jdbc_probe (id) VALUES (?)").use { ps ->
                ps.setString(1, id)
                ps.executeUpdate()
            }
        }
    }

    protected fun committedProbes(): List<String> =
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id FROM jdbc_probe ORDER BY id").use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1)) }
                }
            }
        }

    @Test
    fun `inTransaction commits when the block completes`() {
        context.inTransaction { insertProbe("a") }
        assertEquals(listOf("a"), committedProbes())
    }

    @Test
    fun `inTransaction rolls back and rethrows when the block throws`() {
        val failure = assertFailsWith<IllegalStateException> { context.inTransaction { insertProbe("a"); error("boom") } }
        assertEquals("boom", failure.message)
        assertEquals(emptyList(), committedProbes())
    }

    @Test
    fun `nested inTransaction joins the outer transaction`() {
        assertFailsWith<IllegalStateException> {
            context.inTransaction {
                context.inTransaction { insertProbe("a") }
                error("outer fails")
            }
        }
        assertEquals(emptyList(), committedProbes())
    }

    @Test
    fun `withConnection inside a transaction uses the transaction's connection`() {
        context.inTransaction {
            val first = context.withConnection { it }
            context.withConnection { conn ->
                assertSame(first, conn)
                assertFalse(conn.autoCommit)
            }
        }
    }

    @Test
    fun `withConnection outside a transaction is auto-commit`() {
        context.withConnection { assertTrue(it.autoCommit) }
        insertProbe("a")
        assertEquals(listOf("a"), committedProbes())
    }

    @Test
    fun `isInTransaction reflects the current thread`() {
        assertFalse(context.isInTransaction())
        context.inTransaction { assertTrue(context.isInTransaction()) }
        assertFalse(context.isInTransaction())
    }
}

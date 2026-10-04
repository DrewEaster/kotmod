package io.kotmod.jdbc

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame

class DataSourceJdbcContextTest {
    private val connection: Connection = mockk(relaxed = true)
    private val dataSource: DataSource = mockk { every { getConnection() } returns this@DataSourceJdbcContextTest.connection }
    private val jdbc = DataSourceJdbcContext(dataSource)

    @Test
    fun `successful transaction commits, restores auto-commit and closes the connection`() {
        every { connection.autoCommit } returns true

        assertEquals(42, jdbc.inTransaction { 42 })

        verifyOrder {
            connection.autoCommit = false
            connection.commit()
            connection.autoCommit = true
            connection.close()
        }
        verify(exactly = 0) { connection.rollback() }
    }

    @Test
    fun `failing transaction rolls back, rethrows, restores auto-commit and closes the connection`() {
        every { connection.autoCommit } returns true

        val failure = assertFailsWith<IllegalStateException> { jdbc.inTransaction { error("boom") } }

        assertEquals("boom", failure.message)
        verifyOrder {
            connection.rollback()
            connection.autoCommit = true
            connection.close()
        }
        verify(exactly = 0) { connection.commit() }
    }

    @Test
    fun `a rollback failure is attached to the original exception and the connection is still closed`() {
        every { connection.autoCommit } returns true
        every { connection.rollback() } throws SQLException("rollback failed")

        val failure = assertFailsWith<IllegalStateException> { jdbc.inTransaction { error("boom") } }

        assertEquals("rollback failed", failure.suppressed.single().message)
        verify { connection.close() }
    }

    @Test
    fun `nested inTransaction and withConnection reuse the transaction's connection`() {
        every { connection.autoCommit } returns true

        jdbc.inTransaction {
            jdbc.inTransaction { jdbc.withConnection { assertSame(connection, it) } }
        }

        verify(exactly = 1) { dataSource.connection }
        verify(exactly = 1) { connection.commit() }
    }

    @Test
    fun `withConnection outside a transaction borrows and closes a connection`() {
        jdbc.withConnection { assertSame(connection, it) }

        verify(exactly = 1) { connection.close() }
        verify(exactly = 0) { connection.commit() }
    }
}

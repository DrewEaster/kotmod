package io.kotmod.sqldelight

import app.cash.sqldelight.TransacterImpl
import app.cash.sqldelight.driver.jdbc.JdbcDriver
import app.cash.sqldelight.driver.jdbc.asJdbcDriver
import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.jdbc.JdbcContext
import io.kotmod.jdbc.JdbcContextContract
import io.kotmod.jdbc.transaction
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.PlaceOrder
import kotlinx.coroutines.runBlocking
import javax.sql.DataSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SqlDelightJdbcContextIntegrationTest : JdbcContextContract() {
    private lateinit var driver: JdbcDriver

    override fun createJdbcContext(dataSource: DataSource): JdbcContext {
        driver = dataSource.asJdbcDriver()
        return SqlDelightJdbcContext(driver)
    }

    private fun eventCount(): Int =
        dataSource.connection.use { conn ->
            conn.createStatement().use { it.executeQuery("SELECT COUNT(*) FROM ddd_domain_event").use { rs -> rs.next(); rs.getInt(1) } }
        }

    private fun statelessOrders(jdbc: JdbcContext) =
        AggregateManager(
            kind = io.kotmod.support.testOrderKind(),
            repository =
                object : io.kotmod.Repository<io.kotmod.support.Order> {
                    override fun get(id: AggregateId) = null

                    override fun save(
                        id: AggregateId,
                        state: io.kotmod.support.Order,
                    ) = Unit
                },
            backend = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
            initial = io.kotmod.support.NoOrder,
        )

    @Test
    fun `kotmod commands join the app's own SQLDelight transaction`() {
        val database = object : TransacterImpl(driver) {}
        val orders = statelessOrders(context)

        val failure =
            assertFailsWith<IllegalStateException> {
                database.transaction {
                    driver.execute(null, "INSERT INTO jdbc_probe (id) VALUES ('app')", 0)
                    runBlocking {
                        context.transaction {
                            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                        }
                    }
                    error("app fails after kotmod ran")
                }
            }

        assertEquals("app fails after kotmod ran", failure.message)
        assertEquals(emptyList(), committedProbes())
        assertEquals(0, eventCount())
    }

    @Test
    fun `SQLDelight work inside a kotmod transaction joins it`() =
        runBlocking {
            val orders = statelessOrders(context)

            context.transaction {
                driver.execute(null, "INSERT INTO jdbc_probe (id) VALUES ('app')", 0)
                orders.handle(AggregateId("o-1"), PlaceOrder("book"))
            }

            assertEquals(listOf("app"), committedProbes())
            assertEquals(1, eventCount())
        }

    @Test
    fun `two adapters over the same driver share one transaction`() =
        runBlocking {
            val sameDriverContext = SqlDelightJdbcContext(driver)
            val orders = statelessOrders(sameDriverContext)

            val failure =
                assertFailsWith<IllegalStateException> {
                    context.transaction {
                        orders.handle(AggregateId("o-1"), PlaceOrder("book"))
                        error("roll back both")
                    }
                }

            // The block's own failure, not the wrong-context check: both adapters share the transaction.
            assertEquals("roll back both", failure.message)
            assertEquals(0, eventCount())
        }
}

package com.dreweaster.ddd.postgres.support

import app.cash.sqldelight.driver.jdbc.JdbcDriver
import app.cash.sqldelight.driver.jdbc.asJdbcDriver
import com.dreweaster.ddd.postgres.DddSchema
import org.junit.jupiter.api.BeforeEach
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.postgresql.PostgreSQLContainer
import javax.sql.DataSource

/**
 * Base class for tests that need a real Postgres. A single container is started lazily
 * on first use and shared by every test class in the run (Testcontainers' Ryuk sidecar
 * removes it when the JVM exits). [DddSchema.ddl] and db-scheduler's `scheduled_tasks` DDL are
 * applied once, and every table is truncated before each test.
 */
abstract class IntegrationTest {
    protected val dataSource: DataSource get() = SharedPostgres.dataSource
    protected val driver: JdbcDriver get() = SharedPostgres.driver

    @BeforeEach
    fun truncateDddTables() {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    "TRUNCATE ddd_aggregate_root, ddd_domain_event, ddd_command_history, ddd_consumer_offset, " +
                        "scheduled_tasks RESTART IDENTITY",
                )
            }
        }
    }
}

private object SharedPostgres {
    private val container = PostgreSQLContainer("postgres:17").apply { start() }

    val dataSource: DataSource =
        PGSimpleDataSource().apply {
            setURL(container.jdbcUrl)
            user = container.username
            password = container.password
        }

    val driver: JdbcDriver = dataSource.asJdbcDriver()

    init {
        val dbSchedulerDdl =
            checkNotNull(SharedPostgres::class.java.getResource("/db-scheduler/postgresql_tables.sql")) {
                "db-scheduler DDL resource missing"
            }.readText()
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(DddSchema.ddl)
                stmt.execute(dbSchedulerDdl)
            }
        }
    }
}

package io.kotmod.jdbc

import javax.sql.DataSource

class DataSourceJdbcContextIntegrationTest : JdbcContextContract() {
    override fun createJdbcContext(dataSource: DataSource): JdbcContext = DataSourceJdbcContext(dataSource)
}

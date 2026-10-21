package io.querty.jdbc

import io.querty.AffectedQuery
import io.querty.ManyQuery
import io.querty.OneQuery
import io.querty.OptionalQuery
import io.querty.PlaceholderStyle
import io.querty.RowReader
import io.querty.Statement
import io.querty.UnexpectedRowCountException
import java.sql.Connection
import java.sql.PreparedStatement

/*
 * The engine runs on the caller's connection and never commits, rolls back, closes it or changes
 * its auto-commit mode, so statements join whatever transaction the caller has open.
 */

/** Runs the query and returns its single row; throws [UnexpectedRowCountException] for any other count. */
fun <R> OneQuery<R>.execute(connection: Connection): R =
    readRows(connection, mapper, limit = 2).let { rows ->
        if (rows.size != 1) throw UnexpectedRowCountException("exactly one", rows.size, sql)
        rows.single()
    }

/** Runs the query and returns its row, or null when there is none; throws if there are two or more. */
fun <R> OptionalQuery<R>.execute(connection: Connection): R? =
    readRows(connection, mapper, limit = 2).let { rows ->
        if (rows.size > 1) throw UnexpectedRowCountException("at most one", rows.size, sql)
        rows.firstOrNull()
    }

/** Runs the query and returns all its rows in order. */
fun <R> ManyQuery<R>.execute(connection: Connection): List<R> =
    readRows(connection, mapper, limit = Int.MAX_VALUE)

/** Runs the statement and returns the number of rows it affected. */
fun AffectedQuery.execute(connection: Connection): Long =
    prepare(connection).use { it.executeLargeUpdate() }

/** Reads at most [limit] rows, so a query that must have one row stops after the second. */
private fun <R> Statement.readRows(connection: Connection, mapper: (RowReader) -> R, limit: Int): List<R> =
    prepare(connection).use { ps ->
        ps.executeQuery().use { rs ->
            val reader = JdbcRowReader(rs)
            buildList {
                while (size < limit && rs.next()) add(mapper(reader))
            }
        }
    }

private fun Statement.prepare(connection: Connection): PreparedStatement =
    connection.prepareStatement(template.render(PlaceholderStyle.POSITIONAL)).also { ps ->
        try {
            ps.bind(template.bindOrder(PlaceholderStyle.POSITIONAL, params))
        } catch (e: Throwable) {
            ps.close()
            throw e
        }
    }

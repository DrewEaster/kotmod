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
    readAtMostOne(connection, mapper, "exactly one").let { rows ->
        if (rows.isEmpty()) throw UnexpectedRowCountException("exactly one", 0, sql)
        rows.single()
    }

/** Runs the query and returns its row, or null when there is none; throws if there are two or more. */
fun <R> OptionalQuery<R>.execute(connection: Connection): R? =
    readAtMostOne(connection, mapper, "at most one").firstOrNull()

/** Runs the query and returns all its rows in order. */
fun <R> ManyQuery<R>.execute(connection: Connection): List<R> =
    prepare(connection).use { ps ->
        ps.executeQuery().use { rs ->
            val reader = JdbcRowReader(rs)
            buildList { while (rs.next()) add(mapper(reader)) }
        }
    }

/** Runs the statement and returns the number of rows it affected. */
fun AffectedQuery.execute(connection: Connection): Long =
    prepare(connection).use { it.executeLargeUpdate() }

/**
 * Maps the first row, if any, and throws if there is a second. The second row is never mapped, so a mapper that
 * fails on it cannot hide the wrong count.
 */
private fun <R> Statement.readAtMostOne(connection: Connection, mapper: (RowReader) -> R, expected: String): List<R> =
    prepare(connection).use { ps ->
        ps.executeQuery().use { rs ->
            if (!rs.next()) {
                emptyList()
            } else {
                val row = mapper(JdbcRowReader(rs))
                if (rs.next()) throw UnexpectedRowCountException(expected, 2, sql)
                listOf(row)
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

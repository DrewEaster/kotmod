package io.querty.codegen

import io.querty.PgType
import org.postgresql.PGResultSetMetaData
import org.postgresql.util.PSQLException
import java.sql.Connection
import java.sql.ResultSetMetaData

/** One result column of a described query. [baseTable] is lower-cased; base fields are null for expressions. */
data class Column(
    val label: String,
    val type: PgType,
    val baseTable: String?,
    val baseColumn: String?,
    val baseNotNull: Boolean,
)

/** What Postgres says about a query: positional parameter types and result columns (empty without rows). */
data class Description(val paramTypes: List<PgType>, val columns: List<Column>)

sealed interface DescribeResult {
    data class Described(val description: Description) : DescribeResult

    /** [position] is the server's error position minus 1: a 0-based offset into the SQL pgjdbc sent. */
    data class Failed(val message: String, val position: Int?) : DescribeResult
}

/** Asks Postgres to describe queries (prepare only, never executed) on a connection to the schema database. */
class QueryDescriber(private val connection: Connection) {
    fun describe(positionalSql: String): DescribeResult =
        try {
            connection.prepareStatement(positionalSql).use { statement ->
                val params = statement.parameterMetaData
                val paramTypes = (1..params.parameterCount).map { PgType(params.getParameterTypeName(it)) }
                val columns = statement.metaData?.let(::columnsOf).orEmpty()
                DescribeResult.Described(Description(paramTypes, columns))
            }
        } catch (e: PSQLException) {
            val server = e.serverErrorMessage
            DescribeResult.Failed(server?.message ?: e.message.orEmpty(), server?.position?.takeIf { it > 0 }?.minus(1))
        } finally {
            connection.rollback()
        }

    private fun columnsOf(meta: ResultSetMetaData): List<Column> {
        val pgMeta = meta.unwrap(PGResultSetMetaData::class.java)
        return (1..meta.columnCount).map { i ->
            val table = pgMeta.getBaseTableName(i).takeUnless { it.isNullOrBlank() }
            val column = pgMeta.getBaseColumnName(i).takeUnless { it.isNullOrBlank() }
            Column(
                label = meta.getColumnLabel(i),
                type = PgType(meta.getColumnTypeName(i)),
                baseTable = table?.lowercase(),
                baseColumn = column,
                baseNotNull = table != null && meta.isNullable(i) == ResultSetMetaData.columnNoNulls,
            )
        }
    }
}

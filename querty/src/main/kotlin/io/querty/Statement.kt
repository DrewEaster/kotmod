package io.querty

/** A SQL statement ready to run: its template, bound params, and how results are shaped. */
sealed class Statement(val template: SqlTemplate, val params: List<Param>) {
    val sql: String get() = template.render(PlaceholderStyle.NUMBERED)

    override fun toString(): String = sql
}

/** A query that must return exactly one row. */
class OneQuery<R>(template: SqlTemplate, params: List<Param>, val mapper: (RowReader) -> R) :
    Statement(template, params)

/** A query that returns zero or one row. */
class OptionalQuery<R>(template: SqlTemplate, params: List<Param>, val mapper: (RowReader) -> R) :
    Statement(template, params)

/** A query that returns any number of rows. */
class ManyQuery<R>(template: SqlTemplate, params: List<Param>, val mapper: (RowReader) -> R) :
    Statement(template, params)

/** A statement that returns the number of rows it affected. */
class AffectedQuery(template: SqlTemplate, params: List<Param>) : Statement(template, params)

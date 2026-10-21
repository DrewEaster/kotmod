package io.querty.jdbc

import io.querty.Param
import io.querty.PgType
import java.sql.PreparedStatement
import java.sql.Types
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.time.Instant
import kotlin.time.toJavaInstant

/** Binds [params], in order, to the `?` placeholders of this statement. */
internal fun PreparedStatement.bind(params: List<Param>) {
    params.forEachIndexed { k, param -> bindOne(k + 1, param) }
}

private fun PreparedStatement.bindOne(index: Int, param: Param) {
    val type = param.type
    val value = param.value
    if (value == null) {
        if (type.isArray || type == PgType.JSON || type == PgType.JSONB || type == PgType.UUID) {
            setNull(index, Types.OTHER)
        } else {
            setObject(index, null)
        }
        return
    }
    when {
        type.isArray -> {
            val elementType = type.elementType
            val elements = (value as List<*>).map { toJdbcValue(elementType, it) }
            val array = connection.createArrayOf(elementType.name, elements.toTypedArray())
            setArray(index, array)
        }
        type == PgType.JSON || type == PgType.JSONB -> setObject(index, value, Types.OTHER)
        else -> setObject(index, toJdbcValue(type, value))
    }
}

private fun toJdbcValue(type: PgType, value: Any?): Any? =
    if (type == PgType.TIMESTAMPTZ && value is Instant) {
        OffsetDateTime.ofInstant(value.toJavaInstant(), ZoneOffset.UTC)
    } else {
        value
    }

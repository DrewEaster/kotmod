package io.querty.jdbc

import io.querty.PgType
import io.querty.RowReader
import java.math.BigDecimal
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.time.Instant
import kotlin.time.toKotlinInstant

/** Reads the current row of [rs]. Typed `getObject` calls return null for SQL NULL, so no `wasNull()` is needed. */
internal class JdbcRowReader(private val rs: ResultSet) : RowReader {
    override fun string(i: Int): String? = rs.getString(i + 1)
    override fun int(i: Int): Int? = rs.getObject(i + 1, Int::class.javaObjectType)
    override fun long(i: Int): Long? = rs.getObject(i + 1, Long::class.javaObjectType)
    override fun boolean(i: Int): Boolean? = rs.getObject(i + 1, Boolean::class.javaObjectType)
    override fun float(i: Int): Float? = rs.getObject(i + 1, Float::class.javaObjectType)
    override fun double(i: Int): Double? = rs.getObject(i + 1, Double::class.javaObjectType)
    override fun bigDecimal(i: Int): BigDecimal? = rs.getBigDecimal(i + 1)
    override fun instant(i: Int): Instant? = rs.getObject(i + 1, OffsetDateTime::class.java)?.toInstant()?.toKotlinInstant()
    override fun localDateTime(i: Int): LocalDateTime? = rs.getObject(i + 1, LocalDateTime::class.java)
    override fun localDate(i: Int): LocalDate? = rs.getObject(i + 1, LocalDate::class.java)
    override fun uuid(i: Int): UUID? = rs.getObject(i + 1, UUID::class.java)
    override fun bytes(i: Int): ByteArray? = rs.getBytes(i + 1)

    override fun list(i: Int, elementType: PgType): List<Any?>? {
        val array = rs.getArray(i + 1) ?: return null
        try {
            val elements = array.array as Array<*>
            return elements.map { if (elementType == PgType.TIMESTAMPTZ) toInstant(it) else it }
        } finally {
            array.free()
        }
    }

    private fun toInstant(element: Any?): Instant? = when (element) {
        null -> null
        is Timestamp -> element.toInstant().toKotlinInstant()
        is OffsetDateTime -> element.toInstant().toKotlinInstant()
        else -> error("Unexpected timestamptz array element of type ${element::class.java.name}")
    }
}

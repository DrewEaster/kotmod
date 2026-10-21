package io.querty

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID
import kotlin.time.Instant

/** Reads the current row by 0-based column index. Every method returns null for SQL NULL. */
interface RowReader {
    fun string(i: Int): String?
    fun int(i: Int): Int?
    fun long(i: Int): Long?
    fun boolean(i: Int): Boolean?
    fun float(i: Int): Float?
    fun double(i: Int): Double?
    fun bigDecimal(i: Int): BigDecimal?
    fun instant(i: Int): Instant?
    fun localDateTime(i: Int): LocalDateTime?
    fun localDate(i: Int): LocalDate?
    fun uuid(i: Int): UUID?
    fun bytes(i: Int): ByteArray?
    fun list(i: Int, elementType: PgType): List<Any?>?
}

/** Unwraps a column the query declares non-null, failing loudly if the database disagrees. */
fun <T : Any> notNull(value: T?, column: String): T =
    checkNotNull(value) { "Column '$column' was null but is declared non-null" }

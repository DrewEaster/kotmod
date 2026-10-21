package io.querty.codegen

import io.querty.PgType

/** The Kotlin type for a Postgres type and the `RowReader` method that reads it. */
data class BaseType(val kotlinType: String, val readCall: String)

object TypeMapping {
    private val scalars = mapOf(
        "text" to BaseType("String", "string"),
        "varchar" to BaseType("String", "string"),
        "bpchar" to BaseType("String", "string"),
        "name" to BaseType("String", "string"),
        "int2" to BaseType("Int", "int"),
        "int4" to BaseType("Int", "int"),
        "int8" to BaseType("Long", "long"),
        "bool" to BaseType("Boolean", "boolean"),
        "float4" to BaseType("Float", "float"),
        "float8" to BaseType("Double", "double"),
        "numeric" to BaseType("java.math.BigDecimal", "bigDecimal"),
        "timestamptz" to BaseType("kotlin.time.Instant", "instant"),
        "timestamp" to BaseType("java.time.LocalDateTime", "localDateTime"),
        "date" to BaseType("java.time.LocalDate", "localDate"),
        "uuid" to BaseType("java.util.UUID", "uuid"),
        "bytea" to BaseType("ByteArray", "bytes"),
        "json" to BaseType("String", "string"),
        "jsonb" to BaseType("String", "string"),
    )

    /** The mapping for [type]; for an array type, its element's mapping with `readCall = "list"`. Null if unsupported. */
    fun base(type: PgType): BaseType? {
        if (type.isArray) return scalars[type.elementType.name]?.copy(readCall = "list")
        return scalars[type.name]
    }
}

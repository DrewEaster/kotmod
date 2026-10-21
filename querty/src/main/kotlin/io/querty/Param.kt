package io.querty

/** One bound statement parameter. For array types, [value] is a `List<*>` of base JVM values. */
data class Param(val name: String, val value: Any?, val type: PgType)

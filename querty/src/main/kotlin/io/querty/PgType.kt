package io.querty

/**
 * A Postgres type, named by its `pg_type.typname`: `"text"`, `"int4"`, `"timestamptz"`.
 * Array types carry a leading underscore, e.g. `"_int4"`.
 */
@JvmInline
value class PgType(val name: String) {
    val isArray: Boolean get() = name.startsWith("_")

    /** The element type of an array type. */
    val elementType: PgType get() = PgType(name.removePrefix("_"))

    companion object {
        val TEXT = PgType("text")
        val VARCHAR = PgType("varchar")
        val BPCHAR = PgType("bpchar")
        val NAME = PgType("name")
        val INT2 = PgType("int2")
        val INT4 = PgType("int4")
        val INT8 = PgType("int8")
        val BOOL = PgType("bool")
        val FLOAT4 = PgType("float4")
        val FLOAT8 = PgType("float8")
        val NUMERIC = PgType("numeric")
        val TIMESTAMPTZ = PgType("timestamptz")
        val TIMESTAMP = PgType("timestamp")
        val DATE = PgType("date")
        val UUID = PgType("uuid")
        val BYTEA = PgType("bytea")
        val JSON = PgType("json")
        val JSONB = PgType("jsonb")
    }
}

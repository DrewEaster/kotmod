package io.querty.codegen

import io.querty.PgType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class TypeMappingTest {
    @Test fun `maps every supported scalar type`() {
        val expected = mapOf(
            "text" to ("String" to "string"),
            "varchar" to ("String" to "string"),
            "bpchar" to ("String" to "string"),
            "name" to ("String" to "string"),
            "int2" to ("Int" to "int"),
            "int4" to ("Int" to "int"),
            "int8" to ("Long" to "long"),
            "bool" to ("Boolean" to "boolean"),
            "float4" to ("Float" to "float"),
            "float8" to ("Double" to "double"),
            "numeric" to ("java.math.BigDecimal" to "bigDecimal"),
            "timestamptz" to ("kotlin.time.Instant" to "instant"),
            "timestamp" to ("java.time.LocalDateTime" to "localDateTime"),
            "date" to ("java.time.LocalDate" to "localDate"),
            "uuid" to ("java.util.UUID" to "uuid"),
            "bytea" to ("ByteArray" to "bytes"),
            "json" to ("String" to "string"),
            "jsonb" to ("String" to "string"),
        )
        for ((pg, mapping) in expected) {
            assertEquals(BaseType(mapping.first, mapping.second), TypeMapping.base(PgType(pg)), pg)
        }
    }

    @Test fun `an array maps to its element type read as a list`() {
        assertEquals(BaseType("Int", "list"), TypeMapping.base(PgType("_int4")))
        assertEquals(BaseType("String", "list"), TypeMapping.base(PgType("_text")))
    }

    @Test fun `unsupported types map to null`() {
        assertNull(TypeMapping.base(PgType("xid8")))
        assertNull(TypeMapping.base(PgType("_xid8")))
        assertNull(TypeMapping.base(PgType("_bytea")))
    }
}

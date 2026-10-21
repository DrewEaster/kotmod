package io.querty.codegen

import kotlin.test.Test
import kotlin.test.assertEquals

class NamingTest {
    @Test fun `object names are PascalCase from kebab-case`() {
        assertEquals("FindOrder", Naming.objectName("find-order"))
    }

    @Test fun `property names are camelCase from snake_case`() {
        assertEquals("customerId", Naming.propertyName("customer_id"))
        assertEquals("sslValidFrom", Naming.propertyName("ssl_valid_from"))
        assertEquals("status", Naming.propertyName("status"))
    }

    @Test fun `hard keywords are backticked`() {
        assertEquals("`in`", Naming.propertyName("in"))
        assertEquals("`class`", Naming.propertyName("class"))
    }

    @Test fun `file names come from the file name only`() {
        assertEquals("OrdersQueries.kt", Naming.fileName("orders.sql"))
        assertEquals("OrderStatusQueries.kt", Naming.fileName("order_status.sql"))
        assertEquals("OrderStatusQueries.kt", Naming.fileName("shop/order_status.sql"))
    }
}

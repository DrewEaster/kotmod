package io.kotmod.querty.example

import io.kotmod.jdbc.JdbcContext
import io.querty.jdbc.execute

/** The read side: order lists for screens, straight from the generated queries. */
class OrderReadModel(
    private val jdbc: JdbcContext,
) {
    fun customerOrders(customerId: CustomerId): List<CustomerOrders.Row> =
        jdbc.withConnection { conn -> CustomerOrders(customerId).execute(conn) }

    fun search(
        status: OrderStatus?,
        itemContains: String?,
        limit: Long,
        offset: Long,
    ): List<SearchOrders.Row> = jdbc.withConnection { conn -> SearchOrders(status, itemContains, limit, offset).execute(conn) }

    fun byIds(ids: List<OrderId>): List<OrdersByIds.Row> = jdbc.withConnection { conn -> OrdersByIds(ids).execute(conn) }
}

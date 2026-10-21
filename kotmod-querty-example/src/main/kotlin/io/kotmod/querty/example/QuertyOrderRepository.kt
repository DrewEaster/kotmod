package io.kotmod.querty.example

import io.kotmod.AggregateId
import io.kotmod.Repository
import io.kotmod.jdbc.JdbcContext
import io.querty.jdbc.execute

class QuertyOrderRepository(
    private val jdbc: JdbcContext,
) : Repository<Order> {
    override fun get(id: AggregateId): Order? =
        jdbc.withConnection { conn -> FindOrder(OrderId(id.value)).execute(conn) }?.let { row ->
            when (row.status) {
                OrderStatus.PENDING -> PendingOrder(row.customerId, row.item, row.placedAt)
                OrderStatus.SHIPPED -> ShippedOrder(row.customerId, row.item, row.placedAt)
                OrderStatus.CANCELLED -> CancelledOrder(row.customerId, row.item, row.placedAt, checkNotNull(row.reason))
            }
        }

    override fun save(
        id: AggregateId,
        state: Order,
    ) {
        val orderId = OrderId(id.value)
        val query =
            when (state) {
                is PendingOrder -> SaveOrder(orderId, state.customerId, OrderStatus.PENDING, state.item, null, state.placedAt)
                is ShippedOrder -> SaveOrder(orderId, state.customerId, OrderStatus.SHIPPED, state.item, null, state.placedAt)
                is CancelledOrder ->
                    SaveOrder(orderId, state.customerId, OrderStatus.CANCELLED, state.item, state.reason, state.placedAt)
            }
        jdbc.withConnection { conn -> query.execute(conn) }
    }
}

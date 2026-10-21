package io.kotmod.querty.example

import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.jdbc.JdbcContext
import io.kotmod.reaction.EventPolicy
import io.kotmod.reaction.ReactionContext
import io.querty.jdbc.execute
import kotlinx.serialization.Serializable

@Serializable
sealed interface OrderStatusChange

@Serializable
data class StatusChanged(
    val orderId: String,
    val status: String,
    val sequence: Long,
) : OrderStatusChange

class OrderStatusProjection(
    private val jdbc: JdbcContext,
) : EventPolicy<OrderStatusChange>(
        name = "order-status-projection",
        triggers = OrderStatusChange.serializer(),
    ) {
    init {
        on(Orders) { event, metadata ->
            val status =
                when (event) {
                    is OrderPlaced -> "placed"
                    is OrderShipped -> "shipped"
                    is OrderCancelled -> "cancelled"
                }
            trigger(StatusChanged(metadata.aggregateId.value, status, metadata.sequence))
        }
    }

    override val ordering = ReactionOrdering.PerAggregate(onGiveUp = OnGiveUp.BlockAggregate)

    override suspend fun handle(
        trigger: OrderStatusChange,
        context: ReactionContext,
    ) = when (trigger) {
        is StatusChanged -> saveStatus(trigger)
    }

    // Writes only if the stored status came from an earlier event, so a stale re-run can't overwrite a newer one.
    private fun saveStatus(change: StatusChanged) {
        jdbc.withConnection { conn -> SaveOrderStatus(change.orderId, change.status, change.sequence).execute(conn) }
    }
}

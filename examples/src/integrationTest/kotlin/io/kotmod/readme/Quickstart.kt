package io.kotmod.readme

// Keep in sync with README.md (Quickstart, steps 2, 3 and 5).

import io.kotmod.AggregateId
import io.kotmod.CommandHandlers
import io.kotmod.DomainEvent
import io.kotmod.Outcome
import io.kotmod.Repository
import io.kotmod.accept
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.jdbc.JdbcContext
import io.kotmod.reject
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.time.Duration

sealed interface Order

data class PendingOrder(
    val item: String,
) : Order

data class ShippedOrder(
    val item: String,
) : Order

data class CancelledOrder(
    val item: String,
    val reason: String,
) : Order

@Serializable
sealed interface OrderEvent : DomainEvent

@Serializable
data class OrderPlaced(
    val item: String,
) : OrderEvent

@Serializable
data class OrderShipped(
    val item: String,
) : OrderEvent

@Serializable
data class OrderCancelled(
    val item: String,
    val reason: String,
) : OrderEvent

@Serializable
sealed interface OrderCommand

@Serializable
data class PlaceOrder(
    val item: String,
) : OrderCommand

@Serializable
data object ShipOrder : OrderCommand

@Serializable
data class CancelOrder(
    val reason: String,
) : OrderCommand

@Serializable
sealed interface OrderRejection

@Serializable
data object OrderAlreadyPlaced : OrderRejection

@Serializable
data object OrderNotFound : OrderRejection

@Serializable
data object OrderAlreadyShipped : OrderRejection

@Serializable
data object OrderAlreadyCancelled : OrderRejection

@Serializable
data object CancellationReasonMissing : OrderRejection

typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

fun placeOrder(item: String): OrderOutcome = accept(PendingOrder(item), OrderPlaced(item))

fun PendingOrder.ship(): OrderOutcome = accept(ShippedOrder(item), OrderShipped(item))

fun PendingOrder.cancel(reason: String): OrderOutcome =
    if (reason.isBlank()) {
        reject(CancellationReasonMissing)
    } else {
        accept(CancelledOrder(item, reason), OrderCancelled(item, reason))
    }

object OrderCommands : CommandHandlers<Order, OrderCommand, OrderEvent, OrderRejection>(
    rejectionSerializer = OrderRejection.serializer(),
) {
    override fun OrderCommand.handler() =
        when (this) {
            is PlaceOrder -> creates(otherwise = { OrderAlreadyPlaced }) { placeOrder(item) }
            ShipOrder -> on<PendingOrder>(otherwise = ::notPending) { it.ship() }
            is CancelOrder -> on<PendingOrder>(otherwise = ::notPending) { it.cancel(reason) }
        }

    private fun notPending(order: Order?): OrderRejection =
        when (order) {
            is ShippedOrder -> OrderAlreadyShipped
            is CancelledOrder -> OrderAlreadyCancelled
            else -> OrderNotFound
        }
}

class OrderRepository(
    private val jdbc: JdbcContext,
) : Repository<Order> {
    override fun get(id: AggregateId): Order? =
        jdbc.withConnection { conn ->
            conn.prepareStatement("SELECT status, item, reason FROM orders WHERE id = ?").use { ps ->
                ps.setString(1, id.value)
                ps.executeQuery().use { rs ->
                    if (!rs.next()) {
                        null
                    } else {
                        val item = rs.getString("item")
                        when (rs.getString("status")) {
                            "PENDING" -> PendingOrder(item)
                            "SHIPPED" -> ShippedOrder(item)
                            else -> CancelledOrder(item, rs.getString("reason"))
                        }
                    }
                }
            }
        }

    override fun save(
        id: AggregateId,
        state: Order,
    ) {
        val (status, item, reason) =
            when (state) {
                is PendingOrder -> Triple("PENDING", state.item, null)
                is ShippedOrder -> Triple("SHIPPED", state.item, null)
                is CancelledOrder -> Triple("CANCELLED", state.item, state.reason)
            }
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO orders (id, status, item, reason) VALUES (?, ?, ?, ?) " +
                        "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, item = EXCLUDED.item, reason = EXCLUDED.reason",
                ).use { ps ->
                    ps.setString(1, id.value)
                    ps.setString(2, status)
                    ps.setString(3, item)
                    ps.setString(4, reason)
                    ps.executeUpdate()
                }
        }
    }
}

@Serializable
sealed interface OrderNotification : EventReactionTrigger

@Serializable
data class SendOrderConfirmation(
    val orderId: String,
    override val timeout: Duration? = null,
) : OrderNotification

object OrderNotificationSerializer : EventReactionTriggerSerializer<OrderNotification> {
    override suspend fun serialize(trigger: OrderNotification): String = Json.encodeToString(OrderNotification.serializer(), trigger)

    override suspend fun deserialize(serializedTrigger: String): OrderNotification =
        Json.decodeFromString(OrderNotification.serializer(), serializedTrigger)
}

fun sendConfirmation(orderId: String) {
    println("Sending confirmation for order $orderId")
}

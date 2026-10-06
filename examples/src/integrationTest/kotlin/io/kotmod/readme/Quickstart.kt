package io.kotmod.readme

// Keep in sync with README.md (Quickstart, steps 2, 3 and 4).

import io.kotmod.AggregateId
import io.kotmod.AggregateKind
import io.kotmod.AggregateState
import io.kotmod.AggregateType
import io.kotmod.DomainEvent
import io.kotmod.InitialState
import io.kotmod.Outcome
import io.kotmod.Repository
import io.kotmod.accept
import io.kotmod.jdbc.JdbcContext
import io.kotmod.reaction.FailureDecision
import io.kotmod.reaction.GiveUp
import io.kotmod.reaction.ReactionContext
import io.kotmod.reaction.ReactionResult
import io.kotmod.reaction.Reactions
import io.kotmod.reaction.Retry
import io.kotmod.reject
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable

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

object Orders : AggregateKind<OrderCommand, OrderEvent, OrderRejection>(
    type = AggregateType("Order"),
    commandSerializer = OrderCommand.serializer(),
    eventSerialization =
        jsonDataSerializationContext<OrderEvent> {
            +OrderPlaced.serializer().toEventSerializer()
            +OrderShipped.serializer().toEventSerializer()
            +OrderCancelled.serializer().toEventSerializer()
        },
    rejectionSerializer = OrderRejection.serializer(),
)

typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

sealed interface Order : AggregateState<Order, OrderCommand, OrderEvent, OrderRejection>

object NoOrder : InitialState<Order, OrderCommand, OrderEvent, OrderRejection> {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> accept(PendingOrder(command.item), OrderPlaced(command.item))
            ShipOrder, is CancelOrder -> reject(OrderNotFound)
        }
}

data class PendingOrder(
    val item: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder -> accept(ShippedOrder(item), OrderShipped(item))
            is CancelOrder -> cancel(command.reason)
        }

    private fun cancel(reason: String): OrderOutcome =
        if (reason.isBlank()) {
            reject(CancellationReasonMissing)
        } else {
            accept(CancelledOrder(item, reason), OrderCancelled(item, reason))
        }
}

data class ShippedOrder(
    val item: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyShipped)
        }
}

data class CancelledOrder(
    val item: String,
    val reason: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyCancelled)
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
sealed interface OrderNotification

@Serializable
data class SendOrderConfirmation(
    val orderId: String,
) : OrderNotification

@Serializable
data class SendReviewReminder(
    val orderId: String,
) : OrderNotification

class OrderNotifications(
    private val confirm: (orderId: String) -> Unit,
) : Reactions<OrderNotification>(
        name = "order-notifications",
        triggers = OrderNotification.serializer(),
    ) {
    init {
        on(Orders) { event, metadata ->
            when (event) {
                is OrderPlaced -> trigger(SendOrderConfirmation(metadata.aggregateId.value))
                is OrderShipped, is OrderCancelled -> Unit
            }
        }
    }

    override suspend fun handle(
        trigger: OrderNotification,
        context: ReactionContext,
    ) = when (trigger) {
        is SendOrderConfirmation -> confirm(trigger.orderId)
        is SendReviewReminder -> println("Asking for a review of order ${trigger.orderId}")
    }

    override fun onFailure(
        trigger: OrderNotification,
        attempt: Int,
        error: Throwable,
    ): FailureDecision = if (attempt < 5) Retry(backoff(attempt)) else GiveUp

    override suspend fun onCompletion(
        trigger: OrderNotification,
        result: ReactionResult,
    ) {
        println("$trigger finished: $result")
    }
}

fun sendConfirmation(orderId: String) {
    println("Sending confirmation for order $orderId")
}

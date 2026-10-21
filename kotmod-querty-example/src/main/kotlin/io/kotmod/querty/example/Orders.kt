package io.kotmod.querty.example

import io.kotmod.AggregateKind
import io.kotmod.AggregateState
import io.kotmod.AggregateType
import io.kotmod.DomainEvent
import io.kotmod.InitialState
import io.kotmod.Outcome
import io.kotmod.accept
import io.kotmod.reject
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlin.time.Instant

@Serializable
sealed interface OrderEvent : DomainEvent

@Serializable
data class OrderPlaced(
    val customerId: String,
    val item: String,
    val placedAt: Instant,
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
    val customerId: String,
    val item: String,
    val placedAt: Instant,
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
            is PlaceOrder ->
                accept(
                    PendingOrder(CustomerId(command.customerId), command.item, command.placedAt),
                    OrderPlaced(command.customerId, command.item, command.placedAt),
                )
            ShipOrder, is CancelOrder -> reject(OrderNotFound)
        }
}

data class PendingOrder(
    val customerId: CustomerId,
    val item: String,
    val placedAt: Instant,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder -> accept(ShippedOrder(customerId, item, placedAt), OrderShipped(item))
            is CancelOrder -> cancel(command.reason)
        }

    private fun cancel(reason: String): OrderOutcome =
        if (reason.isBlank()) {
            reject(CancellationReasonMissing)
        } else {
            accept(CancelledOrder(customerId, item, placedAt, reason), OrderCancelled(item, reason))
        }
}

data class ShippedOrder(
    val customerId: CustomerId,
    val item: String,
    val placedAt: Instant,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyShipped)
        }
}

data class CancelledOrder(
    val customerId: CustomerId,
    val item: String,
    val placedAt: Instant,
    val reason: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyPlaced)
            ShipOrder, is CancelOrder -> reject(OrderAlreadyCancelled)
        }
}

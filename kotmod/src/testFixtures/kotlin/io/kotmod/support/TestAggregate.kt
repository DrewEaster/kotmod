package io.kotmod.support

import io.kotmod.CommandHandlers
import io.kotmod.DomainEvent
import io.kotmod.Outcome
import io.kotmod.accept
import kotlinx.serialization.Serializable

sealed interface Order

data class PendingOrder(
    val name: String,
) : Order

data class ShippedOrder(
    val name: String,
) : Order

data class CancelledOrder(
    val name: String,
    val reason: String,
) : Order

sealed interface OrderCommand

data class PlaceOrder(
    val name: String,
) : OrderCommand

data object ShipOrder : OrderCommand

data class CancelOrder(
    val reason: String,
) : OrderCommand

/** Test-only: decides with [block], for scenarios such as a side effect between read and write. */
class DecideWith(
    val block: suspend (Order?) -> Outcome<Order, OrderEvent, OrderRejection>,
) : OrderCommand

@Serializable
sealed interface OrderRejection

@Serializable
data object OrderAlreadyExists : OrderRejection

@Serializable
data object OrderNotFound : OrderRejection

@Serializable
data class OrderNotPending(
    val actual: String,
) : OrderRejection

typealias OrderOutcome = Outcome<Order, OrderEvent, OrderRejection>

fun placeOrder(name: String): OrderOutcome = accept(PendingOrder(name), OrderPlaced(name))

fun PendingOrder.ship(): OrderOutcome = accept(ShippedOrder(name), OrderShipped(name))

fun PendingOrder.cancel(reason: String): OrderOutcome = accept(CancelledOrder(name, reason), OrderCancelled(name, reason))

object OrderCommands : CommandHandlers<Order, OrderCommand, OrderEvent, OrderRejection>(
    rejectionSerializer = OrderRejection.serializer(),
) {
    override fun OrderCommand.handler() =
        when (this) {
            is PlaceOrder -> creates(otherwise = { OrderAlreadyExists }) { placeOrder(name) }
            ShipOrder -> on<PendingOrder>(otherwise = ::notPending) { it.ship() }
            is CancelOrder -> on<PendingOrder>(otherwise = ::notPending) { it.cancel(reason) }
            is DecideWith -> any(block)
        }

    private fun notPending(order: Order?): OrderRejection = if (order == null) OrderNotFound else OrderNotPending(order::class.simpleName!!)
}

sealed interface OrderEvent : DomainEvent

@Serializable
data class OrderPlaced(
    val name: String,
) : OrderEvent

@Serializable
data class OrderShipped(
    val name: String,
) : OrderEvent

@Serializable
data class OrderCancelled(
    val name: String,
    val reason: String,
) : OrderEvent

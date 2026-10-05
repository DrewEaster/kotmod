package io.kotmod.support

import io.kotmod.AggregateKind
import io.kotmod.AggregateState
import io.kotmod.AggregateType
import io.kotmod.DomainEvent
import io.kotmod.InitialState
import io.kotmod.Outcome
import io.kotmod.accept
import io.kotmod.reject
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

sealed interface Order : AggregateState<Order, OrderCommand, OrderEvent, OrderRejection>

data class PendingOrder(
    val name: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyExists)
            ShipOrder -> ship()
            is CancelOrder -> cancel(command.reason)
            is DecideWith -> command.block(this)
        }
}

data class ShippedOrder(
    val name: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyExists)
            ShipOrder, is CancelOrder -> reject(OrderNotPending("ShippedOrder"))
            is DecideWith -> command.block(this)
        }
}

data class CancelledOrder(
    val name: String,
    val reason: String,
) : Order {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> reject(OrderAlreadyExists)
            ShipOrder, is CancelOrder -> reject(OrderNotPending("CancelledOrder"))
            is DecideWith -> command.block(this)
        }
}

/** The order before it exists: only placing it is accepted. */
object NoOrder : InitialState<Order, OrderCommand, OrderEvent, OrderRejection> {
    override suspend fun handle(command: OrderCommand): OrderOutcome =
        when (command) {
            is PlaceOrder -> placeOrder(command.name)
            ShipOrder, is CancelOrder -> reject(OrderNotFound)
            is DecideWith -> command.block(null)
        }
}

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

/** Test commands are never serialized (DecideWith holds a lambda), so the test kind's command serializer refuses. */
object TestOrderCommandSerializer : KSerializer<OrderCommand> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("io.kotmod.support.TestOrderCommand", PrimitiveKind.STRING)

    override fun serialize(
        encoder: Encoder,
        value: OrderCommand,
    ): Unit = error("test commands are not serialized")

    override fun deserialize(decoder: Decoder): OrderCommand = error("test commands are not serialized")
}

fun testOrderKind(type: String = "Order"): AggregateKind<OrderCommand, OrderRejection> =
    AggregateKind(AggregateType(type), TestOrderCommandSerializer, OrderRejection.serializer())

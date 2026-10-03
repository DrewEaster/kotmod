package com.dreweaster.ddd.support

import com.dreweaster.ddd.DomainEvent
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

fun PendingOrder.ship(): Pair<Order, List<OrderEvent>> = ShippedOrder(name) to listOf(OrderShipped(name))

fun PendingOrder.cancel(reason: String): Pair<Order, List<OrderEvent>> =
    CancelledOrder(name, reason) to listOf(OrderCancelled(name, reason))

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

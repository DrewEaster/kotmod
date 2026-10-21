package io.kotmod.querty.example

@JvmInline
value class OrderId(
    val value: String,
)

@JvmInline
value class CustomerId(
    val value: String,
)

enum class OrderStatus { PENDING, SHIPPED, CANCELLED }

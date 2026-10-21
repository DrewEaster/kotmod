package shop

@JvmInline
value class OrderId(val value: String)

@JvmInline
value class CustomerId(val value: String)

enum class OrderStatus { PLACED, SHIPPED, CANCELLED }

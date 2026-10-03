package com.dreweaster.ddd

data class SerializedEvent(
    val type: String,
    val version: Int,
    val payload: String,
)

interface DataSerializationContext<E : DomainEvent> {
    fun serialize(event: E): SerializedEvent

    fun deserialize(serialized: SerializedEvent): E
}

package com.dreweaster.ddd

/**
 * The stored form of a [DomainEvent].
 *
 * @property type the event's type name, used to pick a deserializer.
 * @property version the schema version of [payload] for that type.
 * @property payload the serialized event body.
 */
data class SerializedEvent(
    val type: String,
    val version: Int,
    val payload: String,
)

/**
 * Converts domain events to and from their stored [SerializedEvent] form, including reading older
 * versions of an event. See [com.dreweaster.ddd.serialization.jsonDataSerializationContext] for the
 * JSON implementation with schema migrations.
 */
interface DataSerializationContext<E : DomainEvent> {
    /** Serializes [event] at its current schema version. */
    fun serialize(event: E): SerializedEvent

    /** Deserializes [serialized], migrating it to the current event class if it was stored at an older version. */
    fun deserialize(serialized: SerializedEvent): E
}

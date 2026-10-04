package io.kotmod

/** Thrown when no serializer is registered for an event's class [type]. */
class EventSerializationFailedException(
    val type: String,
) : DddException("No serializer found for type '$type'")

/** Thrown when no deserializer is registered for a stored event's [type] at [version]. */
class EventDeserializationFailedException(
    val type: String,
    val version: Int,
) : DddException("No deserializer found for type '$type' at version $version")

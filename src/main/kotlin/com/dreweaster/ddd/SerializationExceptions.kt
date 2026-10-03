package com.dreweaster.ddd

class EventSerializationFailedException(
    val type: String,
) : DddException("No serializer found for type '$type'")

class EventDeserializationFailedException(
    val type: String,
    val version: Int,
) : DddException("No deserializer found for type '$type' at version $version")

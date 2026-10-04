package io.kotmod.postgres.support

import io.kotmod.DataSerializationContext
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import io.kotmod.support.OrderCancelled
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped

/**
 * In-tree illustration of [jsonDataSerializationContext]: registers all
 * three [OrderEvent] variants with their kotlinx-generated serializers
 * and no migrations.
 */
fun orderEventSerialization(): DataSerializationContext<OrderEvent> =
    jsonDataSerializationContext {
        +OrderPlaced.serializer().toEventSerializer()
        +OrderShipped.serializer().toEventSerializer()
        +OrderCancelled.serializer().toEventSerializer()
    }

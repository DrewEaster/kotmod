package com.dreweaster.ddd.postgres.support

import com.dreweaster.ddd.DataSerializationContext
import com.dreweaster.ddd.serialization.jsonDataSerializationContext
import com.dreweaster.ddd.serialization.toEventSerializer
import com.dreweaster.ddd.support.OrderCancelled
import com.dreweaster.ddd.support.OrderEvent
import com.dreweaster.ddd.support.OrderPlaced
import com.dreweaster.ddd.support.OrderShipped

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

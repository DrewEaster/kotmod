package com.dreweaster.ddd.serialization

import com.dreweaster.ddd.DomainEvent
import kotlinx.serialization.KSerializer

interface DataSerializer<T : Any> {
    val serializer: KSerializer<T>

    fun configure(factory: DataSerializerBuilderFactory<T>)
}

inline fun <reified T : DomainEvent> KSerializer<T>.toEventSerializer(
    initialClassName: String =
        T::class.qualifiedName
            ?: throw IllegalArgumentException("Local/anonymous class cannot be an event type"),
    noinline configure: DataSerializerBuilder<T>.() -> Unit = {},
): DataSerializer<T> {
    val kSerializer = this
    return object : DataSerializer<T> {
        override val serializer = kSerializer

        override fun configure(factory: DataSerializerBuilderFactory<T>) {
            factory.create(initialClassName).apply(configure)
        }
    }
}

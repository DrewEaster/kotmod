package io.kotmod.serialization

import io.kotmod.DomainEvent
import kotlinx.serialization.KSerializer

/**
 * Everything needed to store one event type: its kotlinx [serializer] for the current class, and the
 * history of format changes and renames applied through [configure].
 */
interface DataSerializer<T : Any> {
    /** The kotlinx serializer for the event's current class. */
    val serializer: KSerializer<T>

    /** Describes the event's name and migration history by starting a builder from [factory]. */
    fun configure(factory: DataSerializerBuilderFactory<T>)
}

/**
 * Turns a kotlinx serializer into a [DataSerializer] for event type [T].
 *
 * @param initialClassName the name the event was first stored under; defaults to [T]'s qualified name.
 *   Pass the original name if the class has since been renamed, and record the rename in [configure].
 * @param configure the event's migration history, e.g. `migrateFormat { ... }`.
 */
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

package io.kotmod.serialization

import kotlinx.serialization.json.JsonObject

/**
 * Describes how an event type's stored form has changed over time. Each call adds one schema version,
 * in the order the changes were made.
 */
interface DataSerializerBuilder<T : Any> {

    /** Adds a version whose JSON differs from the previous one; [migration] converts the previous version's JSON to this one. */
    fun migrateFormat(migration: (JsonObject) -> JsonObject): DataSerializerBuilder<T>

    /** Adds a version recording that the event class was renamed (or moved) to [className]. */
    fun migrateClassName(className: String): DataSerializerBuilder<T>
}

/** Starts a [DataSerializerBuilder] for an event type first stored under [create]'s `initialClassName`. */
interface DataSerializerBuilderFactory<T : Any> {
    /** Starts the history of an event type first stored under [initialClassName], at version 1. */
    fun create(initialClassName: String): DataSerializerBuilder<T>
}

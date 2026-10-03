package com.dreweaster.ddd.serialization

import kotlinx.serialization.json.JsonObject

interface DataSerializerBuilder<T : Any> {

    fun migrateFormat(migration: (JsonObject) -> JsonObject): DataSerializerBuilder<T>

    fun migrateClassName(className: String): DataSerializerBuilder<T>
}

interface DataSerializerBuilderFactory<T : Any> {
    fun create(initialClassName: String): DataSerializerBuilder<T>
}

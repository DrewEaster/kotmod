package com.dreweaster.ddd.serialization

import com.dreweaster.ddd.DataSerializationContext
import com.dreweaster.ddd.DomainEvent
import com.dreweaster.ddd.EventDeserializationFailedException
import com.dreweaster.ddd.EventSerializationFailedException
import com.dreweaster.ddd.SerializedEvent
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

class JsonDataSerializationContext<E : DomainEvent> internal constructor(
    serializers: List<DataSerializer<out E>>,
    private val json: Json,
) : DataSerializationContext<E> {
    private val deserializers: Map<Pair<String, Int>, (String) -> E>
    private val serializersByClassName: Map<String, (E) -> Pair<String, Int>>
    private val history: Map<String, List<VersionMetadata>>

    init {
        val configs = serializers.map { ds -> configureOne(ds) }
        this.deserializers =
            configs.fold<MappingConfiguration, Map<Pair<String, Int>, (String) -> E>>(emptyMap()) { acc, c ->
                acc + c.buildDeserializers()
            }
        this.serializersByClassName =
            configs.fold<MappingConfiguration, Map<String, (E) -> Pair<String, Int>>>(emptyMap()) { acc, c ->
                acc + c.buildSerializer()
            }
        this.history =
            configs.fold<MappingConfiguration, Map<String, List<VersionMetadata>>>(emptyMap()) { acc, c ->
                acc + (c.currentClassName!! to c.history)
            }
    }

    @Suppress("UNCHECKED_CAST")
    private fun configureOne(ds: DataSerializer<*>): MappingConfiguration {
        val config = MappingConfiguration(ds.serializer as KSerializer<Any>)
        (ds as DataSerializer<Any>).configure(config)
        return config
    }

    override fun serialize(event: E): SerializedEvent {
        val className =
            event::class.qualifiedName
                ?: throw EventSerializationFailedException("<local/anonymous>")
        val entry =
            serializersByClassName[className]
                ?: throw EventSerializationFailedException(className)
        val (payload, version) = entry(event)
        return SerializedEvent(type = className, version = version, payload = payload)
    }

    override fun deserialize(serialized: SerializedEvent): E {
        val entry =
            deserializers[serialized.type to serialized.version]
                ?: throw EventDeserializationFailedException(serialized.type, serialized.version)
        return entry(serialized.payload)
    }

    /** Returns the full `(className, version)` history chain for the current event type — oldest first, newest last. */
    fun historyFor(currentEventType: String): List<VersionMetadata> = history[currentEventType] ?: emptyList()

    private inner class MappingConfiguration(
        private val kSerializer: KSerializer<Any>,
    ) : DataSerializerBuilderFactory<Any>,
        DataSerializerBuilder<Any> {
        var currentClassName: String? = null
        private var currentVersion: Int = 0
        private var migrations: List<Migration> = emptyList()
        var history: List<VersionMetadata> = emptyList()

        override fun create(initialClassName: String): DataSerializerBuilder<Any> {
            currentClassName = initialClassName
            currentVersion = 1
            history = history + VersionMetadata(initialClassName, 1)
            return this
        }

        override fun migrateFormat(migration: (JsonObject) -> JsonObject): DataSerializerBuilder<Any> {
            val className = currentClassName ?: error("create(initialClassName) must be called before migrateFormat")
            migrations = migrations + FormatMigration(className, currentVersion, currentVersion + 1, migration)
            currentVersion += 1
            history = history + VersionMetadata(className, currentVersion)
            return this
        }

        override fun migrateClassName(className: String): DataSerializerBuilder<Any> {
            val fromName = currentClassName ?: error("create(initialClassName) must be called before migrateClassName")
            migrations = migrations + ClassNameMigration(fromName, className, currentVersion, currentVersion + 1)
            currentClassName = className
            currentVersion += 1
            history = history + VersionMetadata(className, currentVersion)
            return this
        }

        fun buildSerializer(): Map<String, (E) -> Pair<String, Int>> {
            val className = currentClassName!!
            val versionAtRegistration = currentVersion
            return mapOf(
                className to { event: E ->
                    json.encodeToString(kSerializer, event) to versionAtRegistration
                },
            )
        }

        fun buildDeserializers(): Map<Pair<String, Int>, (String) -> E> {
            val result = mutableMapOf<Pair<String, Int>, (String) -> E>()

            // Current schema — decode directly, no migration.
            result[currentClassName!! to currentVersion] = { payload ->
                @Suppress("UNCHECKED_CAST")
                json.decodeFromString(kSerializer, payload) as E
            }

            // Historical pairs — compose migrations from that point forward.
            if (migrations.isNotEmpty()) {
                val migrationFns = migrations.map { it.migrationFunction }
                for (i in migrations.indices) {
                    val migration = migrations[i]
                    // Compose migrations[i..lastIndex] left-to-right.
                    val chain =
                        migrationFns
                            .drop(i)
                            .reduce { composed, next -> { obj -> next(composed(obj)) } }

                    result[migration.fromClassName to migration.fromVersion] = { payload ->
                        val root = json.decodeFromString(JsonElement.serializer(), payload).jsonObject
                        val migrated = chain(root)
                        @Suppress("UNCHECKED_CAST")
                        json.decodeFromJsonElement(kSerializer, migrated) as E
                    }
                }
            }

            return result
        }
    }

    private sealed interface Migration {
        val fromClassName: String
        val toClassName: String
        val fromVersion: Int
        val toVersion: Int
        val migrationFunction: (JsonObject) -> JsonObject
    }

    private data class FormatMigration(
        private val className: String,
        override val fromVersion: Int,
        override val toVersion: Int,
        override val migrationFunction: (JsonObject) -> JsonObject,
    ) : Migration {
        override val fromClassName = className
        override val toClassName = className
    }

    private data class ClassNameMigration(
        override val fromClassName: String,
        override val toClassName: String,
        override val fromVersion: Int,
        override val toVersion: Int,
        override val migrationFunction: (JsonObject) -> JsonObject = { it },
    ) : Migration
}

class JsonDataSerializationContextBuilder<E : DomainEvent> {
    internal val serializers = mutableListOf<DataSerializer<out E>>()

    operator fun DataSerializer<out E>.unaryPlus() {
        serializers.add(this)
    }
}

fun <E : DomainEvent> jsonDataSerializationContext(
    json: Json = Json,
    init: JsonDataSerializationContextBuilder<E>.() -> Unit,
): JsonDataSerializationContext<E> {
    val builder = JsonDataSerializationContextBuilder<E>()
    builder.init()
    return JsonDataSerializationContext(builder.serializers, json)
}

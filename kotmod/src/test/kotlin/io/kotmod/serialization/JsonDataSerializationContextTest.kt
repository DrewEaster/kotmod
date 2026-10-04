package io.kotmod.serialization

import io.kotmod.DomainEvent
import io.kotmod.EventDeserializationFailedException
import io.kotmod.EventSerializationFailedException
import io.kotmod.SerializedEvent
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class JsonDataSerializationContextTest {
    sealed interface TestEvent : DomainEvent {
        @Serializable
        data class Opened(
            val id: String,
        ) : TestEvent

        @Serializable
        data class Closed(
            val id: String,
            val reason: String,
        ) : TestEvent

        @Serializable
        data class Archived(
            val id: String,
        ) : TestEvent
    }

    private val openedType = TestEvent.Opened::class.qualifiedName!!
    private val closedType = TestEvent.Closed::class.qualifiedName!!
    private val archivedType = TestEvent.Archived::class.qualifiedName!!

    @Test
    fun `round-trip - simple`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Opened.serializer().toEventSerializer()
            }
        val event = TestEvent.Opened("x")

        val serialized = ctx.serialize(event)

        assertEquals(openedType, serialized.type)
        assertEquals(1, serialized.version)
        assertEquals("""{"id":"x"}""", serialized.payload)
        assertEquals(event, ctx.deserialize(serialized))
    }

    @Test
    fun `round-trip - multiple sealed variants in one context`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Opened.serializer().toEventSerializer()
                +TestEvent.Closed.serializer().toEventSerializer()
            }
        val opened = TestEvent.Opened("x")
        val closed = TestEvent.Closed("y", "expired")

        assertEquals(opened, ctx.deserialize(ctx.serialize(opened)))
        assertEquals(closed, ctx.deserialize(ctx.serialize(closed)))
    }

    @Test
    fun `serialize of unregistered class throws EventSerializationFailedException`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Opened.serializer().toEventSerializer()
            }
        val archived = TestEvent.Archived("x")

        val thrown =
            assertFailsWith<EventSerializationFailedException> {
                ctx.serialize(archived)
            }
        assertEquals(archivedType, thrown.type)
    }

    @Test
    fun `deserialize with unknown type throws EventDeserializationFailedException`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Opened.serializer().toEventSerializer()
            }

        val thrown =
            assertFailsWith<EventDeserializationFailedException> {
                ctx.deserialize(SerializedEvent("unknown.Type", 1, "{}"))
            }
        assertEquals("unknown.Type", thrown.type)
        assertEquals(1, thrown.version)
    }

    @Test
    fun `deserialize with known type but unknown version throws EventDeserializationFailedException`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Opened.serializer().toEventSerializer()
            }

        val thrown =
            assertFailsWith<EventDeserializationFailedException> {
                ctx.deserialize(SerializedEvent(openedType, 99, """{"id":"x"}"""))
            }
        assertEquals(openedType, thrown.type)
        assertEquals(99, thrown.version)
    }

    @Test
    fun `migrateFormat renames a field between v1 and v2`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Closed.serializer().toEventSerializer {
                    migrateFormat { obj ->
                        // v1 → v2: rename "why" to "reason"
                        JsonObject(obj.mapKeys { (k, _) -> if (k == "why") "reason" else k })
                    }
                }
            }

        val v1Payload = """{"id":"x","why":"stale"}"""
        val result = ctx.deserialize(SerializedEvent(closedType, 1, v1Payload))

        assertEquals(TestEvent.Closed("x", "stale"), result)
    }

    @Test
    fun `migrateClassName bridges an historical rename`() {
        val oldName = "old.pkg.Retired"
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Archived.serializer().toEventSerializer(initialClassName = oldName) {
                    migrateClassName(TestEvent.Archived::class.qualifiedName!!)
                }
            }

        // Historical event written under the old name and v1.
        val historical = ctx.deserialize(SerializedEvent(oldName, 1, """{"id":"x"}"""))
        assertEquals(TestEvent.Archived("x"), historical)

        // New writes go out under the current name and v2.
        val serialized = ctx.serialize(TestEvent.Archived("x"))
        assertEquals(archivedType, serialized.type)
        assertEquals(2, serialized.version)
    }

    @Test
    fun `chained migrateFormat migrations run in declaration order`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Closed.serializer().toEventSerializer {
                    // v1 → v2: rename "why" -> "reason_code"
                    migrateFormat { obj ->
                        JsonObject(obj.mapKeys { (k, _) -> if (k == "why") "reason_code" else k })
                    }
                    // v2 → v3: rename "reason_code" -> "reason"
                    migrateFormat { obj ->
                        JsonObject(obj.mapKeys { (k, _) -> if (k == "reason_code") "reason" else k })
                    }
                }
            }

        val v1Payload = """{"id":"x","why":"stale"}"""
        val result = ctx.deserialize(SerializedEvent(closedType, 1, v1Payload))

        assertEquals(TestEvent.Closed("x", "stale"), result)
    }

    @Test
    fun `mixed chain - migrateClassName then migrateFormat`() {
        val oldName = "old.pkg.Retired"
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Archived.serializer().toEventSerializer(initialClassName = oldName) {
                    migrateClassName(TestEvent.Archived::class.qualifiedName!!)
                    // v2 → v3: rename "identifier" -> "id"
                    migrateFormat { obj ->
                        JsonObject(obj.mapKeys { (k, _) -> if (k == "identifier") "id" else k })
                    }
                }
            }

        val v1Payload = """{"identifier":"x"}"""
        val result = ctx.deserialize(SerializedEvent(oldName, 1, v1Payload))

        assertEquals(TestEvent.Archived("x"), result)
    }

    @Test
    fun `historyFor returns the full chain oldest-first`() {
        val oldName = "old.pkg.Retired"
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Archived.serializer().toEventSerializer(initialClassName = oldName) {
                    migrateClassName(TestEvent.Archived::class.qualifiedName!!)
                    migrateFormat { it }
                }
            }

        val history = ctx.historyFor(archivedType)

        assertEquals(
            listOf(
                VersionMetadata(oldName, 1),
                VersionMetadata(archivedType, 2),
                VersionMetadata(archivedType, 3),
            ),
            history,
        )
    }

    @Test
    fun `DSL smoke - three registered event types round-trip`() {
        val ctx =
            jsonDataSerializationContext<TestEvent> {
                +TestEvent.Opened.serializer().toEventSerializer()
                +TestEvent.Closed.serializer().toEventSerializer()
                +TestEvent.Archived.serializer().toEventSerializer()
            }
        val opened = TestEvent.Opened("1")
        val closed = TestEvent.Closed("2", "expired")
        val archived = TestEvent.Archived("3")

        assertEquals(opened, ctx.deserialize(ctx.serialize(opened)))
        assertEquals(closed, ctx.deserialize(ctx.serialize(closed)))
        assertEquals(archived, ctx.deserialize(ctx.serialize(archived)))
    }
}

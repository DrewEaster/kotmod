package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DomainEvent
import io.kotmod.HandledCommand
import io.kotmod.support.ClosedWindow
import io.kotmod.support.Elapsed
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.Window
import io.kotmod.support.WindowClosed
import io.kotmod.support.WindowInput
import io.kotmod.support.windowEventSerialization
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class ProcessInstancesTest {
    private val type = AggregateType("Window")
    private val id = AggregateId("window-o-1")
    private val backend = StubPersistenceBackend<DomainEvent>()
    private val repository = StubRepository<Window>()

    private fun instances(targetTypes: Set<AggregateType> = setOf(AggregateType("Order"))) =
        ProcessInstances(type, repository, backend, ProcessEventSerialization(windowEventSerialization()), NoWindow, WindowInput.serializer(), targetTypes)

    @Test
    fun `the first input starts the process and records its scheduled input`() =
        runBlocking {
            instances().deliver(id, Opened("o-1", closeAtEpochSeconds = 100), "in-e-1")

            assertEquals(OpenWindow("o-1"), repository.store[id])
            assertEquals(
                listOf(InputScheduled(input = """{"type":"io.kotmod.support.Elapsed"}""", at = "1970-01-01T00:01:40Z")),
                backend.events.map { it.event },
            )
        }

    @Test
    fun `an ignored input for a process that doesn't exist creates nothing but is recorded`() =
        runBlocking<Unit> {
            instances().deliver(id, Elapsed, "in-e-1")

            assertNull(repository.store[id])
            assertNull(backend.metas[StubPersistenceBackend.Key(type, id)])
            assertIs<HandledCommand.Rejected>(backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("in-e-1"))])
        }

    @Test
    fun `a redelivered input is not applied twice`() =
        runBlocking {
            instances().deliver(id, Opened("o-1", 100), "in-e-1")
            instances().deliver(id, Opened("o-1", 100), "in-e-1")

            assertEquals(1, backend.events.size)
        }

    @Test
    fun `a transition records the app's events and the requested command`() =
        runBlocking {
            instances().deliver(id, Opened("o-1", 100), "in-e-1")
            instances().deliver(id, Elapsed, "sched-e-2")

            assertEquals(ClosedWindow("o-1"), repository.store[id])
            assertEquals(
                listOf(WindowClosed("o-1"), CommandRequested(targetType = "Order", targetId = "o-1", command = "\"ship\"")),
                backend.events.drop(1).map { it.event },
            )
        }

    @Test
    fun `a command for an unregistered aggregate type fails before anything is recorded`() =
        runBlocking {
            instances(targetTypes = emptySet()).deliver(id, Opened("o-1", 100), "in-e-1")

            assertFailsWith<IllegalArgumentException> { instances(targetTypes = emptySet()).deliver(id, Elapsed, "sched-e-2") }
            assertEquals(OpenWindow("o-1"), repository.store[id])
            assertNull(backend.commands[StubPersistenceBackend.CommandKey(type, id, CommandId("sched-e-2"))])
        }

    @Test
    fun `envelopes round-trip and the app's events go through its own serialization`() {
        val serialization = ProcessEventSerialization(windowEventSerialization())
        val requested = CommandRequested("Order", "o-1", "\"ship\"")
        val scheduled = InputScheduled("{}", "2026-10-05T10:00:00Z")

        assertEquals(ProcessEventSerialization.COMMAND_REQUESTED, serialization.serialize(requested).type)
        assertEquals(requested, serialization.deserialize(serialization.serialize(requested)))
        assertEquals(scheduled, serialization.deserialize(serialization.serialize(scheduled)))
        assertEquals(WindowClosed("o-1"), serialization.deserialize(serialization.serialize(WindowClosed("o-1"))))
        assertEquals(windowEventSerialization().serialize(WindowClosed("o-1")), serialization.serialize(WindowClosed("o-1")))
    }
}

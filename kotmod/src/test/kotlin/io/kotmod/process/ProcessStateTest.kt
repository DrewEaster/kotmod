package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.RequestedCommand
import io.kotmod.support.CancelOrder
import io.kotmod.support.ClosedWindow
import io.kotmod.support.DecideWith
import io.kotmod.support.Elapsed
import io.kotmod.support.NoWindow
import io.kotmod.support.OpenWindow
import io.kotmod.support.Opened
import io.kotmod.support.OrderCommand
import io.kotmod.support.OrderNotFound
import io.kotmod.support.PlaceOrder
import io.kotmod.support.Refunded
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShipOrder
import io.kotmod.support.TestOrderCommandSerializer
import io.kotmod.support.Window
import io.kotmod.support.WindowClosed
import io.kotmod.support.WindowEvent
import io.kotmod.support.WindowInput
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.time.Instant

class ProcessStateTest {
    @Test
    fun `the initial state starts the process and schedules its timeout`() =
        runBlocking {
            assertEquals<ProcessOutcome<*, *, *>>(
                ProcessOutcome.Transition(OpenWindow("o-1"), emptyList(), emptyList(), listOf(ScheduledInput(Elapsed, Instant.fromEpochSeconds(100)))),
                NoWindow.handle(Opened("o-1", closeAtEpochSeconds = 100)),
            )
        }

    @Test
    fun `the initial state ignores inputs that don't start the process`() =
        runBlocking {
            assertEquals(ProcessOutcome.Ignore, NoWindow.handle(Elapsed))
        }

    @Test
    fun `a state records a fact and requests a command`() =
        runBlocking {
            assertEquals(
                ProcessOutcome.Transition(
                    ClosedWindow("o-1"),
                    listOf(WindowClosed("o-1")),
                    listOf(RequestedCommand(testOrders, AggregateId("o-1"), ShipOrder)),
                    emptyList(),
                ),
                OpenWindow("o-1").handle(Elapsed),
            )
        }

    @Test
    fun `a closed window records why a command was refused`() =
        runBlocking {
            assertEquals<ProcessOutcome<*, *, *>>(
                ProcessOutcome.Transition<Window, WindowEvent, WindowInput>(ClosedWindow("o-1", blocked = OrderNotFound), emptyList(), emptyList(), emptyList()),
                ClosedWindow("o-1").handle(ReleaseBlocked(OrderNotFound)),
            )
            assertEquals(ProcessOutcome.Ignore, ClosedWindow("o-1").handle(Refunded))
        }

    @Test
    fun `a kind builds a requested command for one of its aggregates`() {
        assertEquals(RequestedCommand(testOrders, AggregateId("o-1"), ShipOrder), testOrders.command(AggregateId("o-1"), ShipOrder))
        assertEquals("\"ship\"", testOrders.command(AggregateId("o-1"), ShipOrder).encodeCommand())
    }

    @Test
    fun `test commands round-trip, except the lambda-holding DecideWith`() {
        for (command in listOf<OrderCommand>(PlaceOrder("book"), ShipOrder, CancelOrder("changed mind"))) {
            assertEquals(command, Json.decodeFromString(TestOrderCommandSerializer, Json.encodeToString(TestOrderCommandSerializer, command)))
        }
        assertFails { Json.encodeToString(TestOrderCommandSerializer, DecideWith { error("never") }) }
    }
}

package io.kotmod.support

import io.kotmod.AggregateId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainEvent
import io.kotmod.process.ProcessInitialState
import io.kotmod.process.ProcessOutcome
import io.kotmod.process.ProcessState
import io.kotmod.process.ignore
import io.kotmod.process.schedule
import io.kotmod.process.transition
import io.kotmod.serialization.jsonDataSerializationContext
import io.kotmod.serialization.toEventSerializer
import kotlinx.serialization.Serializable
import kotlin.time.Instant

// A test process: a window that opens for an order and, when it elapses, asks to ship the order.

@Serializable
sealed interface WindowInput

@Serializable
data class Opened(
    val orderId: String,
    val closeAtEpochSeconds: Long,
) : WindowInput

@Serializable
data object Elapsed : WindowInput

@Serializable
data object Refunded : WindowInput

@Serializable
data class ReleaseBlocked(
    val rejection: OrderRejection,
) : WindowInput

@Serializable
sealed interface WindowEvent : DomainEvent

@Serializable
data class WindowClosed(
    val orderId: String,
) : WindowEvent

typealias WindowOutcome = ProcessOutcome<Window, WindowEvent, WindowInput>

sealed interface Window : ProcessState<Window, WindowInput, WindowEvent>

object NoWindow : ProcessInitialState<Window, WindowInput, WindowEvent> {
    override suspend fun handle(input: WindowInput): WindowOutcome =
        when (input) {
            is Opened -> transition(OpenWindow(input.orderId), schedule = listOf(schedule(Elapsed, Instant.fromEpochSeconds(input.closeAtEpochSeconds))))
            Elapsed, Refunded, is ReleaseBlocked -> ignore()
        }
}

data class OpenWindow(
    val orderId: String,
) : Window {
    override suspend fun handle(input: WindowInput): WindowOutcome =
        when (input) {
            Elapsed ->
                transition(
                    ClosedWindow(orderId),
                    events = listOf(WindowClosed(orderId)),
                    commands = listOf(testOrders.command(AggregateId(orderId), ShipOrder)),
                )
            Refunded -> transition(ClosedWindow(orderId))
            is Opened, is ReleaseBlocked -> ignore()
        }
}

data class ClosedWindow(
    val orderId: String,
    val blocked: OrderRejection? = null,
) : Window {
    override suspend fun handle(input: WindowInput): WindowOutcome =
        when (input) {
            is ReleaseBlocked -> transition(ClosedWindow(orderId, input.rejection))
            is Opened, Elapsed, Refunded -> ignore()
        }
}

fun windowEventSerialization(): DataSerializationContext<WindowEvent> = jsonDataSerializationContext { +WindowClosed.serializer().toEventSerializer() }

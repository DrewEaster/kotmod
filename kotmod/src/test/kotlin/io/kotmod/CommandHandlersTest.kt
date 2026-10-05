package io.kotmod

import kotlinx.coroutines.delay
import kotlinx.serialization.Serializable
import kotlin.test.Test
import kotlin.test.assertEquals

class CommandHandlersTest {
    private sealed interface Payout

    private data class Held(val amount: Long) : Payout

    private data class Released(val reference: String) : Payout

    private sealed interface PayoutEvent : DomainEvent

    private data class HoldPlaced(val amount: Long) : PayoutEvent

    private data class FundsReleased(val reference: String) : PayoutEvent

    private sealed interface PayoutCommand

    private data class Hold(val amount: Long) : PayoutCommand

    private data class Release(val reference: String) : PayoutCommand

    private data object Inspect : PayoutCommand

    @Serializable
    sealed interface PayoutRejection

    @Serializable
    data class AmountOverLimit(val limit: Long) : PayoutRejection

    @Serializable
    data object PayoutNotFound : PayoutRejection

    @Serializable
    data class PayoutNotHeld(val actual: String) : PayoutRejection

    @Serializable
    data object PayoutAlreadyExists : PayoutRejection

    private object PayoutCommands : CommandHandlers<Payout, PayoutCommand, PayoutEvent, PayoutRejection>(
        rejectionSerializer = PayoutRejection.serializer(),
    ) {
        override fun PayoutCommand.handler() =
            when (this) {
                is Hold -> creates(otherwise = { PayoutAlreadyExists }) {
                    if (amount > 100) reject(AmountOverLimit(100)) else accept(Held(amount), HoldPlaced(amount))
                }
                is Release ->
                    on<Held>(otherwise = { state -> if (state == null) PayoutNotFound else PayoutNotHeld(state::class.simpleName!!) }) {
                        accept(Released(reference), FundsReleased(reference))
                    }
                Inspect ->
                    any { state ->
                        delay(1) // blocks may suspend
                        accept(state ?: Held(0))
                    }
            }
    }

    private fun decide(
        command: PayoutCommand,
        state: Payout?,
    ) = kotlinx.coroutines.runBlocking { PayoutCommands.decide(command, state) }

    @Test
    fun `on runs the block when the state has the required type`() {
        assertEquals(Outcome.Accept(Released("r-1"), listOf(FundsReleased("r-1"))), decide(Release("r-1"), Held(5)))
    }

    @Test
    fun `on rejects with otherwise when the state has another type`() {
        assertEquals(Outcome.Reject(PayoutNotHeld("Released")), decide(Release("r-2"), Released("r-1")))
    }

    @Test
    fun `on passes null to otherwise when the aggregate does not exist`() {
        assertEquals(Outcome.Reject(PayoutNotFound), decide(Release("r-1"), null))
    }

    @Test
    fun `creates runs the block only when the aggregate does not exist`() {
        assertEquals(Outcome.Accept(Held(5), listOf(HoldPlaced(5))), decide(Hold(5), null))
        assertEquals(Outcome.Reject(PayoutAlreadyExists), decide(Hold(5), Held(1)))
    }

    @Test
    fun `a pure function can reject for a business reason`() {
        assertEquals(Outcome.Reject(AmountOverLimit(100)), decide(Hold(500), null))
    }

    @Test
    fun `any sees the full state, including null`() {
        assertEquals(Outcome.Accept(Held(0), emptyList<PayoutEvent>()), decide(Inspect, null))
        assertEquals(Outcome.Accept(Released("r"), emptyList<PayoutEvent>()), decide(Inspect, Released("r")))
    }

    @Test
    fun `accept keeps events in order`() {
        val outcome: Outcome<Payout, PayoutEvent, PayoutRejection> = accept(Held(1), HoldPlaced(1), HoldPlaced(2))
        assertEquals(listOf(HoldPlaced(1), HoldPlaced(2)), (outcome as Outcome.Accept).events)
    }
}

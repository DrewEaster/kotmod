package io.kotmod

import io.kotmod.support.CancelOrder
import io.kotmod.support.DecideWith
import io.kotmod.support.NoOrder
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.OrderNotPending
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped
import io.kotmod.support.PendingOrder
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ShipOrder
import io.kotmod.support.ShippedOrder
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class AggregateStateTest {
    @Test
    fun `a state accepts a command it allows with a new state and events`() =
        runTest {
            assertEquals(Outcome.Accept(ShippedOrder("book"), listOf(OrderShipped("book"))), PendingOrder("book").handle(ShipOrder))
        }

    @Test
    fun `a state rejects a command it does not allow`() =
        runTest {
            assertEquals(Outcome.Reject(OrderNotPending("ShippedOrder")), ShippedOrder("book").handle(CancelOrder("too late")))
        }

    @Test
    fun `the initial state accepts creating the aggregate`() =
        runTest {
            assertEquals(Outcome.Accept(PendingOrder("book"), listOf(OrderPlaced("book"))), NoOrder.handle(PlaceOrder("book")))
        }

    @Test
    fun `the initial state rejects commands for an aggregate that does not exist`() =
        runTest {
            assertEquals(Outcome.Reject(OrderNotFound), NoOrder.handle(ShipOrder))
        }

    @Test
    fun `a state may suspend while deciding`() =
        runTest {
            val outcome =
                PendingOrder("book").handle(
                    DecideWith { state ->
                        delay(1)
                        accept(state!!)
                    },
                )

            assertEquals(Outcome.Accept(PendingOrder("book"), emptyList<OrderEvent>()), outcome)
        }
}

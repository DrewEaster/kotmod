package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.support.NoOrder
import io.kotmod.support.Order
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderNotFound
import io.kotmod.support.PlaceOrder
import io.kotmod.support.ReleaseBlocked
import io.kotmod.support.ShippedOrder
import io.kotmod.support.StubPersistenceBackend
import io.kotmod.support.StubRepository
import io.kotmod.support.WindowInput
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.time.Instant

class ProcessRuntimeTest {
    private val orderRepository = StubRepository<Order>()
    private val orders = AggregateManager(testOrders, orderRepository, StubPersistenceBackend<OrderEvent>(), NoOrder)
    private val payoutTarget: ProcessTarget<WindowInput> = target(orders) { _, rejection -> ReleaseBlocked(rejection) }

    @Test
    fun `a target runs an accepted command and returns no feedback`() =
        runBlocking {
            orders.handle(AggregateId("o-1"), PlaceOrder("o-1"))

            val feedback = payoutTarget.send(AggregateId("o-1"), "\"ship\"", CommandId("Window-e-9"), CorrelationId("Window/window-o-1"))

            assertNull(feedback)
            assertEquals(ShippedOrder("o-1"), orderRepository.store[AggregateId("o-1")])
            assertEquals(testOrders.type, payoutTarget.type)
        }

    @Test
    fun `a target maps a typed rejection to an input`() =
        runBlocking {
            assertEquals(
                ReleaseBlocked(OrderNotFound),
                payoutTarget.send(AggregateId("o-1"), "\"ship\"", CommandId("Window-e-9"), CorrelationId("Window/window-o-1")),
            )
        }

    @Test
    fun `triggers round-trip through their JSON serializers`() =
        runBlocking {
            val inputs = JsonTriggerSerializer(InputTrigger.serializer())
            val commands = JsonTriggerSerializer(CommandTrigger.serializer())
            val input = InputTrigger("window-o-1", "{}", "in-e-1")
            val command = CommandTrigger("window-o-1", "Order", "o-1", "\"ship\"", "Window-e-2")

            assertEquals(input, inputs.deserialize(inputs.serialize(input)))
            assertEquals(command, commands.deserialize(commands.serialize(command)))
        }

    @Test
    fun `a process executor completes a run, retries a failure and waits for notBefore`() =
        runBlocking {
            val queues = ManualQueues()
            val now = Instant.parse("2026-10-05T10:00:00Z")
            var fail = true
            val runs = mutableListOf<String>()
            val executor =
                processExecutor(queues.channel("internal", JsonTriggerSerializer(InputTrigger.serializer()), ordered = false), clock = { now }) {
                    runs += it.inputId
                    if (fail) error("boom")
                }
            executor.start()

            executor.dispatch(EventReactionId("a"), InputTrigger("p", "{}", "a"))
            assertIs<ReactionOutcome.Retry>(queues.deliver("internal").single())
            fail = false
            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = false)), queues.deliver("internal"))

            executor.dispatch(EventReactionId("later"), InputTrigger("p", "{}", "later"), notBefore = Instant.parse("2026-10-05T10:05:00Z"))
            assertIs<ReactionOutcome.Wait>(queues.deliver("internal").single())
            assertEquals(listOf("a", "a"), runs)
        }
}

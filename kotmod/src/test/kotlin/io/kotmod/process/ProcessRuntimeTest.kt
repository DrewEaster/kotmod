package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.CommandId
import io.kotmod.CorrelationId
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.InMemoryReactionRows
import io.kotmod.event.reaction.OrderedItem
import io.kotmod.event.reaction.Produced
import io.kotmod.event.reaction.ReactionQueue
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.event.reaction.TaskPayload
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskOutcome
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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class ProcessRuntimeTest {
    private val orderRepository = StubRepository<Order>()
    private val orders = AggregateManager(testOrders, orderRepository, StubPersistenceBackend<OrderEvent>(), NoOrder)
    private val payoutTarget: ProcessTarget<WindowInput> = target(orders) { _, rejection -> ReleaseBlocked(rejection) }

    private val now = Instant.parse("2026-10-05T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val rows = InMemoryReactionRows { now }

    private fun queue(name: String = "Window-internal"): ReactionQueue<InputTrigger> =
        processQueue(name, InputTrigger.serializer(), scheduler, rows) { now }

    private fun input(id: String) = InputTrigger("p", "{}", id)

    private fun TaskOutcome?.delay(): Duration? =
        when (this) {
            is TaskOutcome.RunAgain -> at - now
            TaskOutcome.Done -> null
            null -> error("Nothing was delivered")
        }

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
    fun `items keep the JSON form already queued by earlier versions`() {
        val input = InputTrigger("window-o-1", "{}", "in-e-1")
        val command = CommandTrigger("window-o-1", "Order", "o-1", "\"ship\"", "Window-e-2")
        val inputJson = """{"processId":"window-o-1","input":"{}","inputId":"in-e-1"}"""
        val commandJson = """{"processId":"window-o-1","targetType":"Order","targetId":"o-1","command":"\"ship\"","commandId":"Window-e-2"}"""

        assertEquals(inputJson, Json.encodeToString(InputTrigger.serializer(), input))
        assertEquals(commandJson, Json.encodeToString(CommandTrigger.serializer(), command))
        assertEquals(input, Json.decodeFromString(InputTrigger.serializer(), inputJson))
        assertEquals(command, Json.decodeFromString(CommandTrigger.serializer(), commandJson))
    }

    @Test
    fun `a process queue runs on the scheduler's queue of its name and completes a run`() =
        runBlocking {
            val runs = mutableListOf<String>()
            val queue = queue("Window-commands").also { q -> q.startProcess { runs += it.inputId } }

            queue.publish(Produced(EventReactionId("a"), input("a")))

            assertEquals(listOf<TaskOutcome>(TaskOutcome.Done), scheduler.queue("Window-commands").deliverAll())
            assertEquals(listOf("a"), runs)
        }

    @Test
    fun `a failing input is retried forever with capped backoff, attempts counted`() =
        runBlocking {
            var seen = 0
            val queue = queue("u")
            queue.startProcess {
                seen++
                error("down")
            }
            queue.publish(Produced(EventReactionId("a"), input("a")))

            val delays = (1..12).map { scheduler.queue("u").deliverNext().delay() }

            val backoff = BackoffStrategy()
            assertEquals((0 until 12).map { backoff.calculateBackoff(it) }, delays)
            assertEquals(600.seconds, delays.last())
            assertEquals(12, seen)
            val task = ReactionTasks.decode(scheduler.queue("u").pending.single().payload)
            assertEquals(12, assertIs<TaskPayload.Unordered>(task).attempt)
        }

    @Test
    fun `an ordered item is leased for the process timeout plus the margin, and its attempt grows on each retry`() =
        runBlocking<Unit> {
            val leases = mutableListOf<Instant?>()
            val ordered = queue("o")
            ordered.startProcess {
                leases += rows.rows("o").single().leaseUntil
                error("down")
            }
            ordered.publishOrdered(listOf(OrderedItem(EventReactionId("a"), "Order/o-1", 1, 0, input("a"))))

            val delays = (1..3).map { scheduler.queue("o").deliverNext().delay() }

            assertEquals(listOf(1.seconds, 2.seconds, 4.seconds), delays)
            assertEquals(List<Instant?>(3) { now + 90.seconds }, leases)
            assertEquals(3, rows.rows("o").single().attempts)
        }

    @Test
    fun `a scheduled input waits for its time without running`() =
        runBlocking {
            var ran = false
            val queue = queue().also { q -> q.startProcess { ran = true } }
            val at = Instant.parse("2026-10-05T10:05:00Z")

            queue.publish(Produced(EventReactionId("later"), input("later"), notBefore = at))

            assertEquals(at - now, scheduler.queue("Window-internal").deliverNext().delay())
            assertTrue(!ran)
        }

    @Test
    fun `an input that runs past 60 seconds is retried`() =
        runTest {
            var runs = 0
            val queue = queue().also { q ->
                q.startProcess {
                    runs++
                    delay(61.seconds)
                }
            }
            queue.publish(Produced(EventReactionId("slow"), input("slow")))

            val outcome = scheduler.queue("Window-internal").deliverNext()

            assertEquals(1.seconds, outcome.delay())
            assertEquals(1, runs)
        }

    @Test
    fun `a CancellationException thrown by the work itself is retried like any failure`() =
        runBlocking {
            val queue = queue().also { q -> q.startProcess { throw CancellationException("app code") } }
            queue.publish(Produced(EventReactionId("a"), input("a")))

            assertEquals(1.seconds, scheduler.queue("Window-internal").deliverNext().delay())
        }
}

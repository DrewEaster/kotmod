package io.kotmod.reaction

import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.event.reaction.InMemoryReactionRows
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.process.ProcessEventSerialization
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskQueue
import io.kotmod.scheduling.TaskScheduler
import io.kotmod.support.OrderPlaced
import io.kotmod.support.persistedEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

class ContractSourceTest {
    private val log = InMemoryLog()
    private val now = Instant.parse("2026-10-06T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val rows = InMemoryReactionRows { now }
    private val queue = scheduler.queue("fraud-checks")
    private var position = EventLogPosition.START
    private val payments = paymentContract(log)

    private fun reactor(vararg policies: EventPolicy<Notice>) =
        EventReactor(
            scheduler = scheduler,
            rows = rows,
            polling = log,
            readEvent = log::readEvent,
            getPosition = { position },
            savePosition = { position = it },
            isLeader = { true },
            name = "reactor",
            pollInterval = 50.milliseconds,
            batchSize = 100,
            clock = { now },
        ).also { reactor ->
            policies.forEach { reactor.register(it) }
            reactor.startPoliciesForTest()
        }

    /** An ordered event policy over orders (a Confirm per placed order) and payments (a Flag per declined payment). */
    private fun fraudChecks(mapping: TriggerScope<Notice>.(PaymentDeclined, EventMetadata) -> Unit = { event, _ -> trigger(Flag(event.customerId)) }) =
        RecordingPolicy(name = "fraud-checks", ordering = ReactionOrdering.PerAggregate()).apply { listenTo(payments, mapping) }

    @Test
    fun `an event policy with a local and a contract source gets typed events from both, and their ordering keys never mix`() =
        runBlocking {
            val fraud = fraudChecks()
            val reactor = reactor(fraud)
            log.add(orderEvent(OrderPlaced("book"), eventId = "e-1", orderId = "o-1"))
            log.add(paymentEvent("c-1", eventId = "p-1"))

            reactor.tickForTest()
            payments.tickForTest()

            val lines = rows.rows("fraud-checks")
            assertEquals(listOf("fraud-checks/e-1/0", "fraud-checks/p-1/0"), lines.map { it.reactionId })
            assertEquals(listOf("Order/o-1", "Payment/c-1"), lines.map { it.key })
            queue.deliverAll()
            assertEquals(listOf<Notice>(Confirm("o-1"), Flag("c-1")), fraud.handled.map { it.first })
        }

    @Test
    fun `a public event reaches an event policy that only listens to the contract, through the contract's own reader`() =
        runBlocking {
            val chargebacks = RecordingPolicy(name = "chargebacks", kind = null).apply { listenTo(payments) { event, _ -> trigger(Flag(event.customerId)) } }
            val reactor = reactor(chargebacks)
            log.add(paymentEvent("c-1", eventId = "p-1"))

            reactor.tickForTest()
            assertTrue(scheduler.queue("chargebacks").pending.isEmpty())
            payments.tickForTest()

            assertEquals(listOf("chargebacks/p-1/0"), scheduler.queue("chargebacks").names())
        }

    @Test
    fun `an event the contract keeps private triggers nothing`() =
        runBlocking {
            reactor(fraudChecks())
            log.add(paymentEvent("c-1", declined = false))

            payments.tickForTest()

            assertTrue(queue.pending.isEmpty())
            assertTrue(rows.rows("fraud-checks").isEmpty())
        }

    @Test
    fun `an event policy's contract block that throws is parked, the contract's reader moves on, and the fix runs it through the contract`() =
        runBlocking {
            var brokenFor = 1
            val fraud =
                fraudChecks { event, _ ->
                    if (brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Flag(event.customerId))
                }
            reactor(fraud)
            log.add(paymentEvent("c-1", eventId = "p-1"))
            log.add(paymentEvent("c-2", eventId = "p-2"))

            payments.tickForTest()

            assertEquals(listOf("fraud-checks/p-1/mapping", "fraud-checks/p-2/0"), rows.rows("fraud-checks").map { it.reactionId })
            queue.deliverNext() // the fixed mapping, re-run through the contract
            // The re-run queues what the inline path would have: same id, in the same place in the same line.
            val rerun = rows.rows("fraud-checks").first()
            assertEquals(listOf("fraud-checks/p-1/0", "Payment/c-1", 1L, 0), listOf(rerun.reactionId, rerun.key, rerun.sequence, rerun.ordinal))
            assertEquals(Flag("c-1"), rerun.policyItem.notice())
            queue.deliverAll()
            assertEquals(listOf<Notice>(Flag("c-2"), Flag("c-1")), fraud.handled.map { it.first })
        }

    @Test
    fun `registering an event policy after its contract started is refused`() =
        runBlocking<Unit> {
            payments.start()
            try {
                assertFailsWith<IllegalStateException> { reactor(fraudChecks()) }
            } finally {
                payments.stop()
            }
        }

    @Test
    fun `a refused registration changes nothing, so an earlier contract gets no listener and no queue is created`() =
        runBlocking<Unit> {
            val other = paymentContract(log, setOf(io.kotmod.AggregateType("Refund")))
            val twoContracts = RecordingPolicy(name = "two", kind = null).apply {
                listenTo(payments) { event, _ -> trigger(Flag(event.customerId)) }
                listenTo(other) { event, _ -> trigger(Flag(event.customerId)) }
            }
            other.start()
            try {
                val asked = mutableListOf<String>()
                val watching =
                    object : TaskScheduler {
                        override fun queue(name: String): TaskQueue = scheduler.queue(name).also { asked += name }
                    }
                val reactor = EventReactor(watching, rows, log, log::readEvent, { position }, { position = it }, { true }, "reactor", 50.milliseconds, 100) { now }
                assertFailsWith<IllegalStateException> { reactor.register(twoContracts) }
                log.add(paymentEvent("c-1", eventId = "p-1"))
                payments.tickForTest()
                assertTrue(scheduler.queue("two").pending.isEmpty())
                assertTrue(asked.isEmpty())
            } finally {
                other.stop()
            }
        }

    @Test
    fun `a conversion failure inside the contract stops its reader and parks nothing in the event policy`() =
        runBlocking<Unit> {
            reactor(fraudChecks())
            log.add(paymentEvent("c-1", eventId = "p-1").let { it.copy(serialized = it.serialized.copy(payload = "garbage")) })

            assertFails { payments.tickForTest() }

            assertTrue(queue.pending.isEmpty())
            assertTrue(rows.rows("fraud-checks").isEmpty())
        }

    @Test
    fun `a process manager envelope never reaches a contract block`() =
        runBlocking<Unit> {
            var called = false
            reactor(fraudChecks { _, _ -> called = true })
            log.add(persistedEvent(globalOffset = 1, aggregateType = "Payment", eventType = ProcessEventSerialization.COMMAND_REQUESTED))

            payments.tickForTest()

            assertTrue(!called && queue.pending.isEmpty() && rows.rows("fraud-checks").isEmpty())
        }
}

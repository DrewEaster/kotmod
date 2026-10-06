package io.kotmod.reaction

import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.process.ManualQueues
import io.kotmod.process.ProcessEventSerialization
import io.kotmod.support.persistedEvent
import kotlin.test.assertFails
import io.kotmod.support.OrderPlaced
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

class ContractSourceTest {
    private val log = InMemoryLog()
    private val queues = ManualQueues(enforceOrdering = true)
    private var position = EventLogPosition.START
    private val payments = paymentContract(log)

    private fun reactor(vararg useCases: Reactions<Notice>) =
        EventReactor(
            queues = queues,
            polling = log,
            readEvent = log::readEvent,
            getPosition = { position },
            savePosition = { position = it },
            isLeader = { true },
            name = "reactor",
            pollInterval = 50.milliseconds,
            batchSize = 100,
            clock = { Instant.parse("2026-10-06T10:00:00Z") },
        ).also { reactor ->
            useCases.forEach { reactor.register(it) }
            reactor.startUseCasesForTest()
        }

    /** An ordered use case over orders (a Confirm per placed order) and payments (a Flag per declined payment). */
    private fun fraudChecks(mapping: TriggerScope<Notice>.(PaymentDeclined, EventMetadata) -> Unit = { event, _ -> trigger(Flag(event.customerId)) }) =
        RecordingUseCase(name = "fraud-checks", ordering = ReactionOrdering.PerAggregate()).apply { listenTo(payments, mapping) }

    @Test
    fun `a use case with a local and a contract source gets typed events from both, and their ordering keys never mix`() =
        runBlocking {
            val fraud = fraudChecks()
            val reactor = reactor(fraud)
            log.add(orderEvent(OrderPlaced("book"), eventId = "e-1", orderId = "o-1"))
            log.add(paymentEvent("c-1", eventId = "p-1"))

            reactor.tickForTest()
            payments.tickForTest()

            val pending = queues.pending("fraud-checks")
            assertEquals(listOf("fraud-checks/e-1/0", "fraud-checks/p-1/0"), pending.map { it.id.value })
            assertEquals(listOf("Order/o-1", "Payment/c-1"), pending.map { it.ordering?.key })
            queues.deliver("fraud-checks")
            assertEquals(listOf<Notice>(Confirm("o-1"), Flag("c-1")), fraud.handled.map { it.first })
        }

    @Test
    fun `a public event reaches a use case that only listens to the contract, through the contract's own reader`() =
        runBlocking {
            val chargebacks = RecordingUseCase(name = "chargebacks", kind = null).apply { listenTo(payments) { event, _ -> trigger(Flag(event.customerId)) } }
            val reactor = reactor(chargebacks)
            log.add(paymentEvent("c-1", eventId = "p-1"))

            reactor.tickForTest()
            assertTrue(queues.pending("chargebacks").isEmpty())
            payments.tickForTest()

            assertEquals(listOf("chargebacks/p-1/0"), queues.pending("chargebacks").map { it.id.value })
        }

    @Test
    fun `an event the contract keeps private triggers nothing`() =
        runBlocking {
            reactor(fraudChecks())
            log.add(paymentEvent("c-1", declined = false))

            payments.tickForTest()

            assertTrue(queues.published.isEmpty())
        }

    @Test
    fun `a use case's contract block that throws is parked, the contract's reader moves on, and the fix runs it through the contract`() =
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

            assertEquals(listOf("fraud-checks/p-1/mapping", "fraud-checks/p-2/0"), queues.pending("fraud-checks").map { it.id.value })
            repeat(4) { queues.deliver("fraud-checks") }
            assertEquals(listOf<Notice>(Flag("c-2"), Flag("c-1")), fraud.handled.map { it.first })
            // The re-run publishes what the inline path would have: same id and ordering key.
            val rerun = queues.published.single { it.id.value == "fraud-checks/p-1/0" }
            assertEquals("Payment/c-1", rerun.ordering?.key)
            assertEquals(Flag("c-1"), rerun.notice())
        }

    @Test
    fun `registering a use case after its contract started is refused`() =
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
            val twoContracts = RecordingUseCase(name = "two", kind = null).apply {
                listenTo(payments) { event, _ -> trigger(Flag(event.customerId)) }
                listenTo(other) { event, _ -> trigger(Flag(event.customerId)) }
            }
            other.start()
            try {
                val reactor =
                    EventReactor(queues, log, log::readEvent, { position }, { position = it }, { true }, "reactor", 50.milliseconds, 100) {
                        Instant.parse("2026-10-06T10:00:00Z")
                    }
                assertFailsWith<IllegalStateException> { reactor.register(twoContracts) }
                log.add(paymentEvent("c-1", eventId = "p-1"))
                payments.tickForTest()
                assertTrue(queues.published.isEmpty())
                assertTrue(queues.channels.isEmpty())
            } finally {
                other.stop()
            }
        }

    @Test
    fun `a conversion failure inside the contract stops its reader and parks nothing in the use case`() =
        runBlocking<Unit> {
            reactor(fraudChecks())
            log.add(paymentEvent("c-1", eventId = "p-1").let { it.copy(serialized = it.serialized.copy(payload = "garbage")) })

            assertFails { payments.tickForTest() }

            assertTrue(queues.published.isEmpty())
        }

    @Test
    fun `a process manager envelope never reaches a contract block`() =
        runBlocking<Unit> {
            var called = false
            reactor(fraudChecks { _, _ -> called = true })
            log.add(persistedEvent(globalOffset = 1, aggregateType = "Payment", eventType = ProcessEventSerialization.COMMAND_REQUESTED))

            payments.tickForTest()

            assertTrue(!called && queues.published.isEmpty())
        }
}

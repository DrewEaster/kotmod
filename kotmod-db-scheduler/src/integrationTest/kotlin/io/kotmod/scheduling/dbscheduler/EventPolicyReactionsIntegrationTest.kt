package io.kotmod.scheduling.dbscheduler

import io.kotmod.EventLogPosition
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.reaction.ReactionContext
import io.kotmod.reaction.Retry
import io.kotmod.support.OrderShipped
import io.kotmod.support.testOrderKind
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds

class EventPolicyReactionsIntegrationTest : IntegrationTest() {
    @Test
    fun `several event policies each run their own reactions for one event, and one listening to another type runs nothing`() =
        runBlocking {
            val emails = OrderWork("emails")
            val audits = OrderWork("audits")
            val invoices = OrderWork("invoices", kind = testOrderKind("Invoice"))

            runningReactor(dataSource, jdbc, listOf(emails, audits, invoices)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                eventually { emails.seen.handled.size == 1 && audits.seen.handled.size == 1 }
                delay(500)
            }

            assertEquals(listOf<Work>(Confirm("o-1", 1)), emails.seen.handled.toList())
            assertEquals(listOf<Work>(Confirm("o-1", 1)), audits.seen.handled.toList())
            assertTrue(invoices.seen.contexts.isEmpty())
            assertEquals(listOf("emails/e-1/0"), emails.seen.contexts.map { it.reactionId })
        }

    @Test
    fun `replaying the log while work is pending adds no duplicate work, and handle sees one stable reaction id`() =
        runBlocking {
            val mapped = AtomicInteger()
            val emails =
                OrderWork(
                    "emails",
                    mapping = { _, m ->
                        mapped.incrementAndGet()
                        trigger(Confirm(m.aggregateId.value, m.sequence))
                    },
                    work = { _, context -> if (context.attempt == 0) error("first attempt fails") },
                    decide = { _, _, _ -> Retry(3.seconds) },
                )
            val offsets = PostgresOffsetManager(jdbc)

            runningReactor(dataSource, jdbc, listOf(emails)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                eventually { emails.seen.contexts.size == 1 && offsets.getPosition("reactor").globalOffset == 1L }
                offsets.savePosition("reactor", EventLogPosition.START) // replay, while the reaction waits for its retry
                eventually { mapped.get() >= 2 && offsets.getPosition("reactor").globalOffset == 1L } // the log really was replayed
                eventually(10.seconds) { emails.seen.handled.size == 1 }
                delay(500)
            }

            assertEquals(listOf(ReactionContext("emails/e-1/0", 0), ReactionContext("emails/e-1/0", 1)), emails.seen.contexts.toList())
        }

    @Test
    fun `an ordered and an unordered event policy on the same aggregate don't block each other`() =
        runBlocking {
            val projection = OrderWork("projection", ordering = ReactionOrdering.PerAggregate(), work = { w, _ -> if (w == Confirm("o-1", 1)) error("projection down") })
            val emails = OrderWork("emails")

            runningReactor(dataSource, jdbc, listOf(projection, emails)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                jdbc.appendOrderEvent("e-2", "o-1", 2, OrderShipped("book"))
                eventually { emails.seen.handled.size == 2 && projection.seen.failures.size >= 3 }
            }

            assertEquals(listOf<Work>(Confirm("o-1", 1), Confirm("o-1", 2)), emails.seen.handled.toList())
            assertTrue(projection.seen.handled.isEmpty(), "o-1's second event waits behind its failing first, in this event policy only")
        }

    @Test
    fun `an event policy with a local and a contract source gets typed events from both, and their ordering keys never mix`() =
        runBlocking {
            val payments = paymentContract(jdbc)
            val fraud =
                OrderWork("fraud", ordering = ReactionOrdering.PerAggregate(), work = { w, _ -> if (w is Flag) error("fraud service down for payments") })
                    .apply { listenTo(payments) { event, _ -> trigger(Flag(event.customerId)) } }

            runningReactor(dataSource, jdbc, listOf(fraud), contract = payments) { _, _ ->
                // The same aggregate id in both contexts: a key that dropped the aggregate type would queue Confirm behind the failing Flag.
                jdbc.appendPaymentEvent("p-1", "x")
                jdbc.appendOrderEvent("e-1", "x", 2)
                eventually { fraud.seen.handled.contains(Confirm("x", 2)) && fraud.seen.failures.size >= 2 }
            }

            assertEquals(listOf<Work>(Confirm("x", 2)), fraud.seen.handled.toList())
            assertEquals(setOf("fraud/e-1/0", "fraud/p-1/0"), fraud.seen.contexts.map { it.reactionId }.toSet())
        }

    @Test
    fun `a delayed trigger waits until notBefore, then runs once`() =
        runBlocking {
            val reminders = OrderWork("reminders", mapping = { _, m -> trigger(Remind(m.aggregateId.value), notBefore = Clock.System.now() + 2.seconds) })

            runningReactor(dataSource, jdbc, listOf(reminders)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                delay(1_000)
                assertTrue(reminders.seen.contexts.isEmpty(), "ran before notBefore")
                eventually(10.seconds) { reminders.seen.handled.isNotEmpty() }
                delay(500)
            }

            assertEquals(listOf<Work>(Remind("o-1")), reminders.seen.handled.toList())
        }

    @Test
    fun `a delayed trigger from a parked mapping that succeeds later waits until notBefore, then runs`() =
        runBlocking {
            val brokenFor = AtomicInteger(1)
            val mappingCalls = AtomicInteger()
            val mappingFailed = AtomicBoolean(false)
            val reminders =
                OrderWork("reminders", mapping = { _, m ->
                    mappingCalls.incrementAndGet()
                    if (brokenFor.getAndDecrement() > 0) {
                        mappingFailed.set(true)
                        error("fix not deployed yet")
                    }
                    trigger(Remind(m.aggregateId.value), notBefore = Clock.System.now() + 2.seconds)
                })

            runningReactor(dataSource, jdbc, listOf(reminders)) { scheduler, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                eventually { mappingFailed.get() } // the mapping really failed first, so it was parked
                eventually { "reminders/e-1/0" in scheduler.instanceIds("reminders") }
                assertTrue(mappingCalls.get() >= 2, "the parked mapping was retried")
                assertTrue(reminders.seen.contexts.isEmpty(), "ran before notBefore")
                eventually(10.seconds) { reminders.seen.handled.isNotEmpty() }
            }

            assertEquals(listOf<Work>(Remind("o-1")), reminders.seen.handled.toList())
        }

    @Test
    fun `an event policy added to a running context sees only events from then on`() =
        runBlocking {
            val emails = OrderWork("emails")
            runningReactor(dataSource, jdbc, listOf(emails)) { _, _ ->
                jdbc.appendOrderEvent("e-1", "o-1", 1)
                eventually { emails.seen.handled.size == 1 && PostgresOffsetManager(jdbc).getPosition("reactor").globalOffset == 1L }
            }

            val newcomer = OrderWork("newcomer")
            runningReactor(dataSource, jdbc, listOf(OrderWork("emails"), newcomer)) { _, _ ->
                jdbc.appendOrderEvent("e-2", "o-2", 1)
                eventually { newcomer.seen.handled.isNotEmpty() }
                delay(500)
            }

            assertEquals(listOf<Work>(Confirm("o-2", 1)), newcomer.seen.handled.toList())
        }

    @Test
    fun `a context consumes another context's contract end to end`() =
        runBlocking {
            val payments = paymentContract(jdbc)
            val chargebacks = OrderWork("chargebacks", kind = null).apply { listenTo(payments) { event, _ -> trigger(Flag(event.customerId)) } }

            runningReactor(dataSource, jdbc, listOf(chargebacks), contract = payments) { _, _ ->
                jdbc.appendPaymentEvent("p-1", "c-1")
                jdbc.appendPaymentEvent("p-2", "c-2", event = PaymentTaken("c-2"))
                eventually { chargebacks.seen.handled.isNotEmpty() }
                delay(500)
            }

            assertEquals(listOf<Work>(Flag("c-1")), chargebacks.seen.handled.toList())
            assertEquals(listOf("chargebacks/p-1/0"), chargebacks.seen.contexts.map { it.reactionId })
        }
}

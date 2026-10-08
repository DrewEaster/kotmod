package io.kotmod.reaction

import io.kotmod.EventLogPosition
import io.kotmod.event.reaction.InMemoryReactionRows
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionRow
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.event.reaction.RowKind
import io.kotmod.process.ProcessEventSerialization
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped
import io.kotmod.support.persistedEvent
import io.kotmod.support.testOrderKind
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CountDownLatch
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant

class EventReactorTest {
    private var now = Instant.parse("2026-10-06T10:00:00Z")
    private val log = InMemoryLog()
    private val scheduler = ManualTaskScheduler()
    private val rows = InMemoryReactionRows { now }
    private var position = EventLogPosition.START
    private val saves = mutableListOf<EventLogPosition>()
    private var leader = true
    private var positionReads = 0
    private var failSaves = 0

    private fun reactor(
        vararg policies: EventPolicy<Notice>,
        rows: ReactionRows = this.rows,
    ) = EventReactor(
        scheduler = scheduler,
        rows = rows,
        polling = log,
        readEvent = log::readEvent,
        getPosition = {
            positionReads++
            position
        },
        savePosition = {
            if (failSaves > 0) {
                failSaves--
                error("crashed before saving the position")
            }
            saves += it
            position = it
        },
        isLeader = { leader },
        name = "reactor",
        pollInterval = 50.milliseconds,
        batchSize = 100,
        sweepEvery = 10.minutes,
        sweepIdle = 30.minutes,
        clock = { now },
    ).also { reactor -> policies.forEach { reactor.register(it) } }

    private fun queue(name: String) = scheduler.queue(name)

    /** Every trigger appends `<orderId>#<sequence>`. */
    private fun everyEvent(
        name: String,
        ordering: ReactionOrdering = ReactionOrdering.Unordered,
    ) = RecordingPolicy(name = name, ordering = ordering, mapping = { _, m -> trigger(Confirm("${m.aggregateId.value}#${m.sequence}")) })

    /** Puts [notice] at the front of [key]'s line in [policy]'s rows, with no task: as if its backend lost the task. */
    private fun lostFront(
        policy: String,
        key: String,
        notice: Notice,
    ) = rows.inLine(policy, key) { insert(ReactionRow(policy, "$policy/lost", RowKind.ORDERED, key, 1, 0, storedTrigger(notice))) }

    @Test
    fun `several event policies each get their own triggers for one event in their own queues, and one listening to other types gets nothing`() =
        runBlocking {
            val reactor =
                reactor(
                    RecordingPolicy(name = "confirmations"),
                    everyEvent("audits"),
                    RecordingPolicy(name = "invoices", kind = testOrderKind("Invoice"), mapping = { _, _ -> error("must not be called") }),
                )
            log.add(orderEvent(OrderPlaced("book")))

            reactor.tickForTest()

            assertEquals(listOf("confirmations/e-1/0"), queue("confirmations").names())
            assertEquals(listOf<Notice>(Confirm("o-1#1")), queue("audits").unordered().map { policyItem(it.item).notice() })
            assertTrue(queue("invoices").pending.isEmpty())
            assertEquals(log.events.single().position, position)
        }

    @Test
    fun `when one event policy's mapping throws, the others still get their triggers, only that event policy parks the event, and the reader moves on`() =
        runBlocking {
            val reactor = reactor(RecordingPolicy(name = "confirmations"), RecordingPolicy(name = "broken", mapping = { _, _ -> error("broken mapping") }))
            log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1"))
            log.add(orderEvent(OrderPlaced("b"), eventId = "e-2", orderId = "o-2"))

            reactor.tickForTest()

            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-2/0"), queue("confirmations").names())
            assertEquals(listOf("broken/e-1/mapping", "broken/e-2/mapping"), rows.rows("broken").map { it.reactionId })
            assertTrue(rows.rows("confirmations").isEmpty())
            assertEquals(log.events.last().position, position)
        }

    @Test
    fun `a crash between queueing triggers and saving the position loses nothing and duplicates nothing`() =
        runBlocking {
            val policy = RecordingPolicy()
            val scheduled = mutableListOf<String>()
            queue("confirmations").beforeSchedule = { scheduled += it }
            val reactor = reactor(policy).also { it.startPoliciesForTest() }
            log.add(orderEvent(OrderPlaced("book")))
            failSaves = 1

            assertFailsWith<IllegalStateException> { reactor.tickForTest() }
            assertEquals(EventLogPosition.START, position)
            reactor.tickForTest()
            queue("confirmations").deliverAll()

            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-1/0"), scheduled)
            assertEquals(listOf(ReactionContext("confirmations/e-1/0", 0)), policy.handled.map { it.second })
            assertEquals(log.events.single().position, position)
        }

    @Test
    fun `replaying from an earlier position queues the same ids, which the scheduler absorbs while pending`() =
        runBlocking {
            val policy = RecordingPolicy()
            val reactor = reactor(policy).also { it.startPoliciesForTest() }
            log.add(orderEvent(OrderPlaced("book")))

            reactor.tickForTest()
            position = EventLogPosition.START
            reactor.tickForTest()
            queue("confirmations").deliverAll()

            assertEquals(listOf(ReactionContext("confirmations/e-1/0", 0)), policy.handled.map { it.second })
        }

    @Test
    fun `replaying an ordered event policy's events adds nothing to its lines`() =
        runBlocking {
            val policy = everyEvent("projection", ReactionOrdering.PerAggregate())
            val reactor = reactor(policy).also { it.startPoliciesForTest() }
            log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", sequence = 1))
            log.add(orderEvent(OrderShipped("a"), eventId = "e-2", sequence = 2))

            reactor.tickForTest()
            position = EventLogPosition.START
            reactor.tickForTest()

            assertEquals(listOf("projection/e-1/0", "projection/e-2/0"), rows.rows("projection").map { it.reactionId })
            queue("projection").deliverAll()
            assertEquals(listOf<Notice>(Confirm("o-1#1"), Confirm("o-1#2")), policy.handled.map { it.first })
        }

    @Test
    fun `an ordered and an unordered event policy on the same aggregate have their own queues and don't block each other`() =
        runBlocking {
            val projection = everyEvent("projection", ReactionOrdering.PerAggregate()).apply { failWith = { _, _ -> RuntimeException("projection down") } }
            val emails = everyEvent("emails")
            val reactor = reactor(projection, emails).also { it.startPoliciesForTest() }
            log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", sequence = 1))
            log.add(orderEvent(OrderShipped("a"), eventId = "e-2", sequence = 2))

            reactor.tickForTest()
            queue("projection").deliverNext()
            queue("emails").deliverAll()

            assertEquals(listOf("Order/o-1", "Order/o-1"), rows.rows("projection").map { it.key })
            assertTrue(rows.rows("emails").isEmpty())
            assertEquals(listOf<Notice>(Confirm("o-1#1")), projection.handled.map { it.first })
            assertEquals(listOf<Notice>(Confirm("o-1#1"), Confirm("o-1#2")), emails.handled.map { it.first })
        }

    @Test
    fun `two event policies with the same name are refused`() {
        val reactor = reactor(RecordingPolicy())

        val error = assertFailsWith<IllegalArgumentException> { reactor.register(RecordingPolicy()) }
        assertEquals("Reactor reactor already has an event policy named confirmations", error.message)
    }

    @Test
    fun `registering after start is refused`() =
        runBlocking<Unit> {
            leader = false
            val reactor = reactor()
            reactor.start()
            try {
                assertFailsWith<IllegalStateException> { reactor.register(RecordingPolicy()) }
            } finally {
                reactor.stop()
            }
        }

    @Test
    fun `an event policy can be registered with only one reactor`() {
        val policy = RecordingPolicy()
        reactor(policy)

        assertFailsWith<IllegalStateException> { reactor(policy) }
    }

    @Test
    fun `an event policy added to a running context sees only events from then on`() =
        runBlocking {
            reactor(RecordingPolicy()).run {
                log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1"))
                tickForTest()
            }
            val restarted = reactor(RecordingPolicy(), RecordingPolicy(name = "newcomer"))
            log.add(orderEvent(OrderPlaced("b"), eventId = "e-2", orderId = "o-2"))

            restarted.tickForTest()

            assertEquals(listOf("newcomer/e-2/0"), queue("newcomer").names())
        }

    @Test
    fun `the reader only reads while leader`() =
        runBlocking {
            leader = false
            val reactor = reactor(RecordingPolicy())
            log.add(orderEvent(OrderPlaced("a")))

            reactor.tickForTest()

            assertTrue(queue("confirmations").pending.isEmpty())
            assertEquals(EventLogPosition.START, position)
        }

    @Test
    fun `the position is saved after each event`() =
        runBlocking {
            val reactor = reactor(RecordingPolicy())
            log.add(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1"))
            log.add(orderEvent(OrderPlaced("b"), eventId = "e-2", orderId = "o-2"))

            reactor.tickForTest()

            assertEquals(log.events.map { it.position }, saves)
        }

    @Test
    fun `a scheduler that fails to schedule stops the batch, parking nothing and saving no position`() =
        runBlocking {
            val reactor = reactor(RecordingPolicy())
            log.add(orderEvent(OrderPlaced("a")))
            queue("confirmations").failNextSchedules = 1

            assertFailsWith<IllegalStateException> { reactor.tickForTest() }
            assertTrue(queue("confirmations").pending.isEmpty())
            assertTrue(rows.rows("confirmations").isEmpty())
            assertEquals(EventLogPosition.START, position)
            reactor.tickForTest()
            assertEquals(listOf("confirmations/e-1/0"), queue("confirmations").names())
        }

    @Test
    fun `when one event policy's queue fails, the event is read again and the other event policy's trigger is queued again under the same id`() =
        runBlocking {
            val scheduled = mutableListOf<String>()
            queue("a").beforeSchedule = { scheduled += it }
            val reactor = reactor(RecordingPolicy(name = "a"), RecordingPolicy(name = "b"))
            log.add(orderEvent(OrderPlaced("book")))
            queue("b").failNextSchedules = 1

            assertFailsWith<IllegalStateException> { reactor.tickForTest() }
            assertEquals(listOf("a/e-1/0"), scheduled)
            assertEquals(EventLogPosition.START, position)
            reactor.tickForTest()

            assertEquals(listOf("a/e-1/0", "a/e-1/0"), scheduled)
            assertEquals(listOf("a/e-1/0"), queue("a").names())
            assertEquals(listOf("b/e-1/0"), queue("b").names())
            assertEquals(log.events.single().position, position)
        }

    @Test
    fun `kotmod's internal events never reach an event policy`() =
        runBlocking {
            val reactor = reactor(RecordingPolicy(mapping = { _, _ -> error("must not be called") }))
            log.add(persistedEvent(globalOffset = 1, eventType = ProcessEventSerialization.COMMAND_REQUESTED))

            reactor.tickForTest()

            assertTrue(queue("confirmations").pending.isEmpty())
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `the first tick sweeps, then at most every sweepEvery`() =
        runBlocking {
            val policy = everyEvent("projection", ReactionOrdering.PerAggregate())
            val reactor = reactor(policy).also { it.startPoliciesForTest() }
            val lost = ReactionTasks.frontName("Order/o-1", "projection/lost")
            lostFront("projection", "Order/o-1", Confirm("lost"))
            now += 31.minutes

            leader = false
            reactor.tickForTest()
            assertTrue(queue("projection").pending.isEmpty(), "only the leader sweeps")

            leader = true
            reactor.tickForTest()
            assertEquals(listOf(lost), queue("projection").names(), "the first tick sweeps")

            queue("projection").lose(lost)
            now += 9.minutes
            reactor.tickForTest()
            assertTrue(queue("projection").pending.isEmpty(), "no sweep before sweepEvery")

            now += 1.minutes
            reactor.tickForTest()
            assertEquals(listOf(lost), queue("projection").names(), "a sweep once sweepEvery has passed")
            queue("projection").deliverAll()
            assertEquals(listOf<Notice>(Confirm("lost")), policy.handled.map { it.first })
        }

    @Test
    fun `a sweep failure doesn't stop reading`() =
        runBlocking {
            val failing =
                object : ReactionRows by rows {
                    override fun stale(
                        queue: String,
                        now: Instant,
                        before: Instant,
                    ): List<ReactionRow> = if (queue == "a") error("database down") else rows.stale(queue, now, before)
                }
            val reactor = reactor(everyEvent("a"), everyEvent("b", ReactionOrdering.PerAggregate()), rows = failing)
            lostFront("b", "Order/o-9", Confirm("lost"))
            now += 31.minutes
            log.add(orderEvent(OrderPlaced("book")))

            reactor.tickForTest()

            assertEquals(log.events.single().position, position)
            assertEquals(listOf("a/e-1/0"), queue("a").names())
            assertEquals(
                setOf(ReactionTasks.frontName("Order/o-9", "b/lost"), ReactionTasks.frontName("Order/o-1", "b/e-1/0")),
                queue("b").names().toSet(),
            )
        }

    @Test
    fun `a shutdown during a sweep cancels the tick instead of being logged as a sweep failure`() =
        runBlocking<Unit> {
            val sweeping = CountDownLatch(1)
            val release = CountDownLatch(1)
            val slow =
                object : ReactionRows by rows {
                    override fun stale(
                        queue: String,
                        now: Instant,
                        before: Instant,
                    ): List<ReactionRow> {
                        sweeping.countDown()
                        release.await()
                        return emptyList()
                    }
                }
            val reactor = reactor(everyEvent("a"), rows = slow)
            var finished = false

            val tick =
                launch(Dispatchers.Default) {
                    reactor.tickForTest()
                    finished = true
                }
            sweeping.await()
            tick.cancel()
            release.countDown()
            tick.join()

            assertFalse(finished)
        }

    @Test
    fun `start fixes a new reactor's starting position before it returns`() =
        runBlocking {
            leader = false
            val reactor = reactor(RecordingPolicy())

            reactor.start()
            try {
                assertEquals(1, positionReads)
            } finally {
                reactor.stop()
            }
        }

    @Test
    fun `a stopped reactor can be started again`() =
        runBlocking {
            leader = false
            val reactor = reactor(RecordingPolicy())

            reactor.start()
            reactor.start()
            reactor.stop()
            reactor.start()
            try {
                assertEquals(2, positionReads)
            } finally {
                reactor.stop()
            }
        }
}

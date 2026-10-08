package io.kotmod.reaction

import io.kotmod.PersistedEvent
import io.kotmod.event.reaction.InMemoryReactionRows
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.event.reaction.RowKind
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.support.OrderPlaced
import io.kotmod.support.OrderShipped
import io.kotmod.support.persistedEvent
import kotlinx.coroutines.runBlocking
import java.io.IOException
import java.util.concurrent.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class PolicyMappingTest {
    private var now = Instant.parse("2026-10-06T10:00:00Z")
    private val log = InMemoryLog()
    private val scheduler = ManualTaskScheduler()
    private val rows = InMemoryReactionRows { now }
    private val queue = scheduler.queue("confirmations")

    private fun runtime(policy: EventPolicy<Notice>) = PolicyRuntime(policy, scheduler, rows, log::readEvent) { now }.also { it.start() }

    /** Appends [event] to the log and routes it to this event policy, as the reactor does. */
    private suspend fun PolicyRuntime<Notice>.read(event: PersistedEvent) = routeLocal(log.add(event))

    private suspend fun deliverNext(): Duration? = queue.deliverNext().againAfter(now)

    private fun rowIds() = rows.rows("confirmations").map { it.reactionId }

    @Test
    fun `each trigger gets a deterministic id, and an ordered event policy puts it in its aggregate's line at the event's sequence and its position`() =
        runBlocking {
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value))
                    trigger(Confirm("${m.aggregateId.value}-again"))
                })

            runtime(policy).read(orderEvent(OrderPlaced("book"), eventId = "e-1", orderId = "o-1", sequence = 3))

            val line = rows.rows("confirmations")
            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-1/1"), line.map { it.reactionId })
            assertEquals(listOf(Triple("Order/o-1", 3L, 0), Triple("Order/o-1", 3L, 1)), line.map { Triple(it.key, it.sequence, it.ordinal) })
            assertEquals(listOf<Notice>(Confirm("o-1"), Confirm("o-1-again")), line.map { it.policyItem.notice() })
            assertEquals(listOf(ReactionTasks.frontName("Order/o-1", "confirmations/e-1/0")), queue.names())
        }

    @Test
    fun `an unordered event policy queues each trigger as a task, delayed ones with their notBefore`() =
        runBlocking {
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value))
                    trigger(Confirm("${m.aggregateId.value}-later"), notBefore = now + 1.hours)
                })

            runtime(policy).read(orderEvent(OrderPlaced("book")))

            val tasks = queue.unordered()
            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-1/1"), tasks.map { it.reactionId })
            assertEquals(listOf(null, (now + 1.hours).toString()), tasks.map { it.notBefore })
            assertEquals(listOf(now, now + 1.hours), queue.pending.map { it.at })
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `events of aggregate types the event policy doesn't listen to are skipped`() =
        runBlocking {
            val policy = RecordingPolicy(mapping = { _, _ -> error("must not be called") })

            runtime(policy).read(paymentEvent("c-1"))

            assertTrue(queue.pending.isEmpty())
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `an unordered event policy's block that throws after triggering keeps the event in the table and queues nothing else`() =
        runBlocking {
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value))
                    error("broken mapping")
                })

            runtime(policy).read(orderEvent(OrderPlaced("book")))

            val parked = rows.rows("confirmations").single()
            assertEquals("confirmations/e-1/mapping", parked.reactionId)
            assertEquals(RowKind.KEPT, parked.kind)
            assertEquals(ParkedItem("e-1", "Order", "o-1", "aggregate kind Order"), parked.policyItem)
            assertEquals(listOf("confirmations/e-1/mapping"), queue.names())
            assertTrue(queue.unordered().isEmpty())
        }

    @Test
    fun `an event that can't be deserialized is parked`() =
        runBlocking {
            runtime(RecordingPolicy()).read(persistedEvent(globalOffset = 1, eventType = "OrderRefunded"))

            assertEquals(listOf("confirmations/e-1/mapping"), rowIds())
        }

    @Test
    fun `an ordered event policy parks an event whose block produces a delayed trigger, in the event's place in its line`() =
        runBlocking {
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value), notBefore = now + 1.hours)
                })

            runtime(policy).read(orderEvent(OrderPlaced("book"), sequence = 2))

            val parked = rows.rows("confirmations").single()
            assertEquals("confirmations/e-1/mapping", parked.reactionId)
            assertEquals(Triple("Order/o-1", 2L, 0), Triple(parked.key, parked.sequence, parked.ordinal))
            assertEquals(RowKind.ORDERED, parked.kind)
            assertEquals(listOf(ReactionTasks.frontName("Order/o-1", "confirmations/e-1/mapping")), queue.names())
        }

    @Test
    fun `an unordered policy's parked mapping is kept in the table and queues its triggers as unordered work once fixed`() =
        runBlocking {
            var brokenFor = 2
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    if (brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm(m.aggregateId.value))
                })
            runtime(policy).read(orderEvent(OrderPlaced("book")))

            assertEquals(1.seconds, deliverNext())
            assertEquals(listOf(RowKind.KEPT to 1), rows.rows("confirmations").map { it.kind to it.attempts })
            assertEquals(null, deliverNext())

            assertTrue(rows.rows("confirmations").isEmpty())
            assertEquals(listOf("confirmations/e-1/0"), queue.unordered().map { it.reactionId })
            queue.deliverAll()
            assertEquals(listOf<Notice>(Confirm("o-1")), policy.handled.map { it.first })
            assertEquals(listOf("confirmations/e-1/0"), policy.handled.map { it.second.reactionId })
        }

    @Test
    fun `an ordered policy's parked mapping, once fixed, runs its triggers before the aggregate's later work`() =
        runBlocking {
            var brokenFor = 2
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = { _, m ->
                    val parkedEvent = m.aggregateId.value == "o-1" && m.sequence == 1L
                    if (parkedEvent && brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm("${m.aggregateId.value}#${m.sequence}"))
                    if (parkedEvent) trigger(Confirm("o-1#1-again"))
                })
            val runtime = runtime(policy)
            runtime.read(orderEvent(OrderPlaced("a"), eventId = "e-1", orderId = "o-1", sequence = 1))
            runtime.read(orderEvent(OrderShipped("a"), eventId = "e-2", orderId = "o-1", sequence = 2))
            runtime.read(orderEvent(OrderPlaced("b"), eventId = "e-3", orderId = "o-2", sequence = 1))

            assertEquals(listOf(1.seconds, null), listOf(deliverNext(), deliverNext()))

            assertEquals(listOf<Notice>(Confirm("o-2#1")), policy.handled.map { it.first })
            assertEquals(listOf("confirmations/e-1/mapping", "confirmations/e-2/0"), rowIds())
            // The fixed mapping's triggers take its place in line: the strictly first-in, first-out scheduler still
            // runs them before the aggregate's later work.
            assertEquals(null, deliverNext())
            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-1/1", "confirmations/e-2/0"), rowIds())
            queue.deliverAll()
            assertEquals(
                listOf<Notice>(Confirm("o-2#1"), Confirm("o-1#1"), Confirm("o-1#1-again"), Confirm("o-1#2")),
                policy.handled.map { it.first },
            )
        }

    @Test
    fun `routing an event again after its mapping succeeded queues no new work`() =
        runBlocking {
            val runtime = runtime(RecordingPolicy())
            val event = log.add(orderEvent(OrderPlaced("book")))

            runtime.routeLocal(event)
            runtime.routeLocal(event)

            assertEquals(listOf("confirmations/e-1/0"), queue.names())
        }

    @Test
    fun `routing an ordered event again queues no new work`() =
        runBlocking {
            val runtime = runtime(RecordingPolicy(ordering = ReactionOrdering.PerAggregate()))
            val event = log.add(orderEvent(OrderPlaced("book")))

            runtime.routeLocal(event)
            runtime.routeLocal(event)

            assertEquals(listOf("confirmations/e-1/0"), rowIds())
            assertEquals(1, queue.pending.size)
        }

    @Test
    fun `routing a parked event again queues no new work`() =
        runBlocking {
            val runtime = runtime(RecordingPolicy(mapping = { _, _ -> error("broken") }))
            val event = log.add(orderEvent(OrderPlaced("book")))

            runtime.routeLocal(event)
            runtime.routeLocal(event)

            assertEquals(listOf("confirmations/e-1/mapping"), rowIds())
            assertEquals(listOf("confirmations/e-1/mapping"), queue.names())
        }

    @Test
    fun `if queueing fails part-way, the failure reaches the reader and routing the event again queues every trigger exactly once`() =
        runBlocking {
            var schedules = 0
            queue.beforeSchedule = { if (++schedules == 2) throw IOException("scheduler unavailable") }
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    trigger(Confirm(m.aggregateId.value))
                    trigger(Confirm("${m.aggregateId.value}-again"))
                })
            val runtime = runtime(policy)
            val event = log.add(orderEvent(OrderPlaced("book")))

            assertFailsWith<IOException> { runtime.routeLocal(event) }
            assertEquals(listOf("confirmations/e-1/0"), queue.names())
            runtime.routeLocal(event)
            queue.deliverAll()

            assertEquals(listOf<Notice>(Confirm("o-1"), Confirm("o-1-again")), policy.handled.map { it.first })
            assertEquals(listOf("confirmations/e-1/0", "confirmations/e-1/1"), policy.handled.map { it.second.reactionId })
        }

    @Test
    fun `a block that throws a CancellationException while the reader is running is parked`() =
        runBlocking {
            val policy = RecordingPolicy(mapping = { _, _ -> throw CancellationException("the app's, not the reader's") })

            runtime(policy).read(orderEvent(OrderPlaced("book")))

            assertEquals(listOf("confirmations/e-1/mapping"), rowIds())
        }

    @Test
    fun `a parked mapping whose block throws a CancellationException keeps retrying`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> throw CancellationException("the app's") })).read(orderEvent(OrderPlaced("book")))

            assertEquals(1.seconds, deliverNext())
            assertEquals(listOf("confirmations/e-1/mapping"), rowIds())
        }

    @Test
    fun `a mapping that always throws stays parked, retrying with growing backoff, and is never dropped`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> error("always broken") })).read(orderEvent(OrderPlaced("book")))

            val outcomes = (1..5).map { deliverNext() }

            assertEquals(listOf(1, 2, 4, 8, 16).map { it.seconds }, outcomes)
            assertEquals(listOf("confirmations/e-1/mapping" to 5), rows.rows("confirmations").map { it.reactionId to it.attempts })
            assertEquals(listOf("confirmations/e-1/mapping"), queue.names())
        }

    @Test
    fun `an ordered mapping that always throws stays at the front of its line, retrying with growing backoff`() =
        runBlocking {
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = { event, m ->
                    if (event is OrderPlaced) error("always broken")
                    trigger(Confirm("${m.aggregateId.value}#${m.sequence}"))
                })
            val runtime = runtime(policy)
            runtime.read(orderEvent(OrderPlaced("a"), eventId = "e-1", sequence = 1))
            runtime.read(orderEvent(OrderShipped("a"), eventId = "e-2", sequence = 2))

            val outcomes = (1..3).map { deliverNext() }

            assertEquals(listOf(1, 2, 4).map { it.seconds }, outcomes)
            assertEquals(listOf("confirmations/e-1/mapping", "confirmations/e-2/0"), rowIds())
            assertTrue(policy.handled.isEmpty())
        }

    @Test
    fun `a parked mapping redelivered after it succeeded adds no duplicate work while its triggers are pending`() =
        runBlocking {
            var brokenFor = 1
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    if (brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm(m.aggregateId.value))
                }).apply { failWith = { _, context -> if (context.attempt == 0) RuntimeException("first try fails") else null } }
            runtime(policy).read(orderEvent(OrderPlaced("book")))

            assertEquals(null, deliverNext()) // the parked mapping succeeds and queues confirmations/e-1/0
            ReactionTasks.scheduleKept(queue, "confirmations/e-1/mapping", now) // the backend delivers it again
            queue.deliverAll()

            assertEquals(
                listOf(ReactionContext("confirmations/e-1/0", 0), ReactionContext("confirmations/e-1/0", 1)),
                policy.handled.map { it.second },
            )
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `a delayed trigger from a parked mapping that succeeds later waits until notBefore, then runs`() =
        runBlocking {
            var brokenFor = 1
            val remindAt = now + 1.hours
            val policy =
                RecordingPolicy(mapping = { _, m ->
                    if (brokenFor-- > 0) error("fix not deployed yet")
                    trigger(Confirm(m.aggregateId.value), notBefore = remindAt)
                })
            runtime(policy).read(orderEvent(OrderPlaced("book")))

            deliverNext()
            assertEquals(remindAt.toString(), queue.unordered().single().notBefore)
            assertEquals(1.hours, deliverNext())
            assertTrue(policy.handled.isEmpty())
            now = remindAt
            deliverNext()

            assertEquals(listOf<Notice>(Confirm("o-1")), policy.handled.map { it.first })
        }

    @Test
    fun `a parked mapping whose event policy no longer listens to the event's aggregate type finishes without triggers`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> error("broken") })).read(orderEvent(OrderPlaced("book")))
            runtime(RecordingPolicy(kind = null)) // the redeployed event policy no longer listens to orders

            assertEquals(null, deliverNext())
            assertTrue(queue.pending.isEmpty())
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `an ordered parked mapping whose event policy no longer listens to the event's aggregate type leaves its line`() =
        runBlocking {
            val ordered = ReactionOrdering.PerAggregate()
            runtime(RecordingPolicy(ordering = ordered, mapping = { _, _ -> error("broken") })).read(orderEvent(OrderPlaced("book")))
            runtime(RecordingPolicy(ordering = ordered, kind = null))

            assertEquals(null, deliverNext())
            assertTrue(queue.pending.isEmpty())
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `a parked mapping whose event is missing from the log keeps retrying`() =
        runBlocking {
            runtime(RecordingPolicy(mapping = { _, _ -> error("broken") })).routeLocal(orderEvent(OrderPlaced("book")))

            assertEquals(1.seconds, deliverNext())
            assertEquals(listOf("confirmations/e-1/mapping"), rowIds())
        }

    @Test
    fun `a policy switched from ordered to unordered still runs its existing line`() =
        runBlocking {
            var broken = true
            val mapping: TriggerScope<Notice>.(io.kotmod.support.OrderEvent, io.kotmod.EventMetadata) -> Unit = { _, m ->
                if (broken && m.sequence == 1L) error("fix not deployed yet")
                trigger(Confirm("${m.aggregateId.value}#${m.sequence}"))
            }
            val before = runtime(RecordingPolicy(ordering = ReactionOrdering.PerAggregate(), mapping = mapping))
            before.read(orderEvent(OrderPlaced("a"), eventId = "e-1", sequence = 1))
            before.read(orderEvent(OrderShipped("a"), eventId = "e-2", sequence = 2))
            before.read(orderEvent(OrderShipped("a"), eventId = "e-3", sequence = 3))
            before.stop()
            broken = false

            val after = RecordingPolicy(mapping = mapping)
            runtime(after)
            queue.deliverAll()

            assertEquals(listOf<Notice>(Confirm("o-1#1"), Confirm("o-1#2"), Confirm("o-1#3")), after.handled.map { it.first })
            assertTrue(rows.rows("confirmations").isEmpty())
            assertTrue(queue.pending.isEmpty())
        }
}

package io.kotmod.reaction

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.Repository
import io.kotmod.accept
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ItemResult
import io.kotmod.event.reaction.OrderedItem
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionQueue
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresReactionRows
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskOutcome
import io.kotmod.support.DecideWith
import io.kotmod.support.NoOrder
import io.kotmod.support.Order
import io.kotmod.support.OrderCommand
import io.kotmod.support.OrderEvent
import io.kotmod.support.OrderRejection
import io.kotmod.support.OrderShipped
import io.kotmod.support.PlaceOrder
import io.kotmod.support.testOrderKind
import io.kotmod.support.testOrders
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant

/** [ReactionQueue]'s ordered lines on Postgres rows, with real threads. */
class OrderedLinesIntegrationTest : IntegrationTest() {
    private val key = "Order/o-1"

    private fun e(n: Int) = OrderedItem(EventReactionId("lines/e$n/0"), key, n.toLong(), 0, "e$n")

    private fun front(n: Int) = ReactionTasks.frontName(key, "lines/e$n/0")

    private fun queue(
        tasks: ManualTaskScheduler.Queue,
        clock: () -> Instant = { Clock.System.now() },
    ) = ReactionQueue("lines", String.serializer(), tasks, PostgresReactionRows(jdbc, clock), 1.minutes, clock)

    /** Busy-waits [nanos], finer than sleeping. */
    private fun spin(nanos: Long) {
        val until = System.nanoTime() + nanos
        while (System.nanoTime() < until) Thread.onSpinWait()
    }

    @Test
    fun `concurrent publishing and finishing never strands a line`(): Unit =
        runBlocking {
            val tasks = ManualTaskScheduler().queue("lines")
            val handled = Collections.synchronizedList(mutableListOf<String>())
            val lines = queue(tasks)
            lines.start { handled += it.item; ItemResult.Completed }
            lines.publishOrdered(listOf(e(0)))

            // Guards against ReactionQueue reading a line's next front outside the lock that deletes the finished item
            // (e.g. before the locked change): a publish landing in between sees the finished item still in front, its
            // schedule is ignored, and nothing schedules the new item. That mutation is caught within the first sweep
            // of the publish delay below, as long as a delivery here takes less than its 0-8ms range.
            for (n in 0 until 200) {
                // e(n) is the line's pending front; publish e(n + 1) while e(n) finishes, on two threads at once.
                val go = CompletableDeferred<Unit>()
                // The publish starts a little later each time (0 to 8ms, four times over), so across the iterations it lands
                // on every step of the finishing delivery: its start, its handler and its finish.
                val publishing = async(Dispatchers.IO) { go.await(); spin((n % 50) * 160_000L); lines.publishOrdered(listOf(e(n + 1))) }
                val finishing = async(Dispatchers.IO) { go.await(); tasks.deliver(front(n)) }
                go.complete(Unit)
                publishing.await()
                assertEquals(TaskOutcome.Done, finishing.await())
                // A publish that saw e(n) still in front may have scheduled it again after it finished: a stale task,
                // which only schedules the real front.
                while (true) {
                    val stale = tasks.pending.firstOrNull { it.name != front(n + 1) } ?: break
                    tasks.deliver(stale.name)
                }
                assertEquals(listOf(front(n + 1)), tasks.pending.map { it.name }, "the line was stranded after e$n finished")
            }
            tasks.deliverAll()

            assertEquals((0..200).map { "e$it" }, handled)
            assertEquals(emptyList(), PostgresReactionRows(jdbc).list("lines"))
            assertEquals(emptyList(), tasks.pending)
        }

    @Test
    fun `two nodes delivering the same front at once run it once`(): Unit =
        runBlocking {
            val nodeA = ManualTaskScheduler().queue("lines")
            val nodeB = ManualTaskScheduler().queue("lines")
            val runs = AtomicInteger()
            val running = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val onA = queue(nodeA)
            val onB = queue(nodeB)
            onA.start {
                runs.incrementAndGet()
                running.complete(Unit)
                release.await()
                ItemResult.Completed
            }
            onB.start { runs.incrementAndGet(); ItemResult.Completed }
            onA.publishOrdered(listOf(e(0)))
            // The same task reaches the other node too (a duplicate delivery).
            ReactionTasks.scheduleFront(nodeB, key, "lines/e0/0", Clock.System.now())

            val first = async(Dispatchers.IO) { nodeA.deliver(front(0)) }
            running.await()
            val second = nodeB.deliver(front(0))
            release.complete(Unit)

            val lease = assertIs<TaskOutcome.RunAgain>(second)
            assertTrue(lease.at > Clock.System.now(), "the duplicate waits for the running attempt's lease")
            assertEquals(TaskOutcome.Done, first.await())
            assertEquals(1, runs.get())
            assertEquals(emptyList(), PostgresReactionRows(jdbc).list("lines"))
        }

    @Test
    fun `the sweep recovers a line whose task was lost`(): Unit =
        runBlocking {
            var now = Instant.parse("2026-10-08T10:00:00Z")
            val tasks = ManualTaskScheduler().queue("lines")
            val handled = mutableListOf<String>()
            val lines = queue(tasks) { now }
            lines.start { handled += it.item; ItemResult.Completed }
            lines.publishOrdered(listOf(e(0), e(1)))
            tasks.lose(front(0))

            now += 29.minutes
            lines.sweep(30.minutes)
            assertEquals(emptyList(), tasks.pending, "a front idle for less than the sweep's threshold is left alone")

            now += 2.minutes
            lines.sweep(30.minutes)
            assertEquals(listOf(front(0)), tasks.pending.map { it.name })
            tasks.deliverAll()
            assertEquals(listOf("e0", "e1"), handled)
            assertEquals(emptyList(), PostgresReactionRows(jdbc).list("lines"))
        }

    /**
     * Triggers two reactions per event; can't map an aggregate's second event while [broken]. The first reaction to
     * o-1's second event fails once (and is retried), so its aggregate's later work must wait for it.
     */
    private class Confirmations : EventPolicy<String>("confirmations", String.serializer()) {
        val handled: MutableList<String> = Collections.synchronizedList(mutableListOf())
        private val failedOnce = AtomicInteger()

        @Volatile
        var broken = true

        override val ordering = ReactionOrdering.PerAggregate()

        init {
            on(testOrders) { _, metadata ->
                check(!(broken && metadata.sequence == 2L)) { "can't map ${metadata.eventId.value}" }
                trigger("${metadata.aggregateId.value}#${metadata.sequence}a")
                trigger("${metadata.aggregateId.value}#${metadata.sequence}b")
            }
        }

        override suspend fun handle(
            trigger: String,
            context: ReactionContext,
        ) {
            check(trigger != "o-1#2a" || failedOnce.getAndIncrement() > 0) { "o-1#2a fails once" }
            handled += trigger
        }
    }

    private class Orders : Repository<Order> {
        private val states = ConcurrentHashMap<AggregateId, Order>()

        override fun get(id: AggregateId): Order? = states[id]

        override fun save(
            id: AggregateId,
            state: Order,
        ) {
            states[id] = state
        }
    }

    @Test
    fun `an event policy end to end on Postgres rows`(): Unit =
        runBlocking {
            val orders =
                AggregateManager<Order, OrderCommand, OrderEvent, OrderRejection>(
                    testOrderKind(),
                    Orders(),
                    PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()),
                    NoOrder,
                )
            val scheduler = ManualTaskScheduler()
            val reactor = EventReactor(jdbc, scheduler, isLeader = { true })
            val policy = Confirmations()
            reactor.register(policy)
            reactor.start()
            reactor.stop() // fixes the starting position; the test drives the reader and the scheduler itself
            reactor.startPoliciesForTest()
            // o-1 gets events 1 to 4, o-2 events 1 to 2; o-1's event 2 and o-2's event 2 can't be mapped yet.
            val twoMore = DecideWith { state -> accept(checkNotNull(state), OrderShipped("a"), OrderShipped("b")) }
            orders.handle(AggregateId("o-1"), PlaceOrder("book"))
            orders.handle(AggregateId("o-2"), PlaceOrder("pen"))
            orders.handle(AggregateId("o-1"), twoMore)
            orders.handle(AggregateId("o-1"), DecideWith { state -> accept(checkNotNull(state), OrderShipped("c")) })
            orders.handle(AggregateId("o-2"), DecideWith { state -> accept(checkNotNull(state), OrderShipped("d")) })

            reactor.tickForTest()
            val tasks = scheduler.queue("confirmations")
            // Run everything until only the two parked mappings are left, failing again.
            var deliveries = 0
            while (tasks.pending.any { !it.name.endsWith("/mapping") }) {
                check(deliveries++ < 100) { "Still delivering: ${tasks.pending}" }
                tasks.deliverNext()
            }

            val mappingFronts = listOf("Order/o-1", "Order/o-2").map { it to tasks.pending.single { task -> task.name.startsWith("line/$it/") }.name }
            assertTrue(mappingFronts.all { (_, name) -> name.endsWith("/mapping") }, "each line waits on its parked mapping: $mappingFronts")
            assertEquals(listOf("o-1#1a", "o-1#1b"), policy.handled.filter { it.startsWith("o-1") })
            assertEquals(listOf("o-2#1a", "o-2#1b"), policy.handled.filter { it.startsWith("o-2") })
            assertEquals(2, ReactionOperations(jdbc, scheduler).parkedMappings("confirmations").size)

            policy.broken = false
            tasks.deliverAll()

            assertEquals(listOf("o-1#1a", "o-1#1b", "o-1#2a", "o-1#2b", "o-1#3a", "o-1#3b", "o-1#4a", "o-1#4b"), policy.handled.filter { it.startsWith("o-1") })
            assertEquals(listOf("o-2#1a", "o-2#1b", "o-2#2a", "o-2#2b"), policy.handled.filter { it.startsWith("o-2") })
            assertEquals(emptyList(), PostgresReactionRows(jdbc).list("confirmations"))
        }
}

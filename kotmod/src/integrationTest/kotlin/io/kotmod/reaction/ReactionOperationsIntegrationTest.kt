package io.kotmod.reaction

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.RowKind
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresReactionRows
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.support.OrderPlaced
import io.kotmod.support.testOrders
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Clock

class ReactionOperationsIntegrationTest : IntegrationTest() {
    /** Ordered, blocking its aggregate when a reaction in [failing] gives up (after one attempt). */
    private class Blocking : EventPolicy<String>("blocking", String.serializer()) {
        val handled: MutableList<String> = Collections.synchronizedList(mutableListOf())
        val failing: MutableSet<String> = Collections.synchronizedSet(mutableSetOf())

        override val ordering = ReactionOrdering.PerAggregate(OnGiveUp.BlockAggregate)

        init {
            on(testOrders) { _, metadata -> trigger("${metadata.aggregateId.value}#${metadata.sequence}") }
        }

        override suspend fun handle(
            trigger: String,
            context: ReactionContext,
        ) {
            check(trigger !in failing) { "failing $trigger" }
            handled += trigger
        }

        override fun onFailure(
            trigger: String,
            attempt: Int,
            error: Throwable,
        ): FailureDecision = GiveUp
    }

    /** Can't map an aggregate's first event while [broken]; counts every mapping it tries. */
    private class Mapping(
        name: String,
        override val ordering: ReactionOrdering,
    ) : EventPolicy<String>(name, String.serializer()) {
        val handled: MutableList<String> = Collections.synchronizedList(mutableListOf())
        val mappings = AtomicInteger()

        @Volatile
        var broken = true

        /** Runs at the start of every mapping (the test can hold a mapping here). */
        @Volatile
        var onMap: () -> Unit = {}

        init {
            on(testOrders) { _, metadata ->
                onMap()
                mappings.incrementAndGet()
                check(!(broken && metadata.sequence == 1L)) { "can't map ${metadata.eventId.value}" }
                trigger("${metadata.aggregateId.value}#${metadata.sequence}")
            }
        }

        override suspend fun handle(
            trigger: String,
            context: ReactionContext,
        ) {
            handled += trigger
        }
    }

    private val scheduler = ManualTaskScheduler()
    private val rows get() = PostgresReactionRows(jdbc)

    private fun operations(on: ManualTaskScheduler = scheduler) = ReactionOperations(rows, on, { Clock.System.now() })

    private fun appendOrderPlaced(
        eventId: String,
        orderId: String,
        sequence: Long,
    ) {
        PostgresDomainPersistenceBackend(jdbc, orderEventSerialization()).appendEvents(
            listOf(
                PendingEvent(
                    EventMetadata(EventId(eventId), AggregateType("Order"), AggregateId(orderId), CommandId("cmd-$eventId"), null, Clock.System.now(), sequence),
                    OrderPlaced("book"),
                ),
            ),
        )
    }

    /** Registers [policies] with a reactor on [scheduler] and reads the events written by [write]. */
    private suspend fun react(
        vararg policies: EventPolicy<String>,
        write: () -> Unit,
    ) {
        val reactor = EventReactor(jdbc, scheduler, isLeader = { true })
        policies.forEach { reactor.register(it) }
        reactor.start()
        reactor.stop() // fixes the starting position; the test drives the reader and the scheduler itself
        reactor.startPoliciesForTest()
        write()
        reactor.tickForTest()
    }

    private fun front(
        key: String,
        reactionId: String,
    ) = "line/$key/$reactionId"

    /** o-1's first reaction blocks o-1's line, holding back its second; o-2's runs. */
    private suspend fun blockOrderOne(): Blocking {
        val policy = Blocking().apply { failing += "o-1#1" }
        react(policy) {
            appendOrderPlaced("e-1", "o-1", 1)
            appendOrderPlaced("e-2", "o-1", 2)
            appendOrderPlaced("e-3", "o-2", 1)
        }
        scheduler.queue("blocking").deliverAll()
        assertEquals(listOf("o-2#1"), policy.handled)
        return policy
    }

    @Test
    fun `blockedReactions lists each blocked reaction with its line, place and attempts`(): Unit =
        runBlocking {
            blockOrderOne()

            assertEquals(
                listOf(BlockedReaction("Order/o-1", EventReactionId("blocking/e-1/0"), 1, 1)),
                operations().blockedReactions("blocking"),
            )
            assertEquals(emptyList(), operations().blockedReactions("nothing-here"))
        }

    @Test
    fun `retryBlocked unblocks the reaction, resets its attempts and schedules it, and it runs`(): Unit =
        runBlocking {
            val policy = blockOrderOne()
            policy.failing.clear()

            operations().retryBlocked("blocking", EventReactionId("blocking/e-1/0"))

            val row = rows.list("blocking").first()
            assertEquals("blocking/e-1/0", row.reactionId)
            assertEquals(false, row.blocked)
            assertEquals(0, row.attempts)
            assertEquals(listOf(front("Order/o-1", "blocking/e-1/0")), scheduler.queue("blocking").pending.map { it.name })
            assertEquals(emptyList(), operations().blockedReactions("blocking"))

            scheduler.queue("blocking").deliverAll()
            assertEquals(listOf("o-2#1", "o-1#1", "o-1#2"), policy.handled)
            assertEquals(emptyList(), rows.list("blocking"))
        }

    @Test
    fun `skipBlocked deletes the reaction and the line's next one runs`(): Unit =
        runBlocking {
            val policy = blockOrderOne()

            operations().skipBlocked("blocking", EventReactionId("blocking/e-1/0"))

            assertEquals(listOf("blocking/e-2/0"), rows.list("blocking").map { it.reactionId })
            assertEquals(listOf(front("Order/o-1", "blocking/e-2/0")), scheduler.queue("blocking").pending.map { it.name })
            scheduler.queue("blocking").deliverAll()
            assertEquals(listOf("o-2#1", "o-1#2"), policy.handled)
            assertEquals(emptyList(), rows.list("blocking"))
        }

    @Test
    fun `retryBlocked and skipBlocked refuse a reaction that doesn't exist or isn't blocked`(): Unit =
        runBlocking {
            blockOrderOne()
            val ops = operations()

            assertFailsWith<IllegalArgumentException> { ops.retryBlocked("blocking", EventReactionId("blocking/e-9/0")) }
            assertFailsWith<IllegalArgumentException> { ops.skipBlocked("blocking", EventReactionId("blocking/e-9/0")) }
            assertFailsWith<IllegalArgumentException> { ops.retryBlocked("other", EventReactionId("blocking/e-1/0")) }
            // e-2's reaction is waiting behind the blocked one, not blocked itself.
            assertFailsWith<IllegalArgumentException> { ops.retryBlocked("blocking", EventReactionId("blocking/e-2/0")) }
            assertFailsWith<IllegalArgumentException> { ops.skipBlocked("blocking", EventReactionId("blocking/e-2/0")) }
            assertEquals(listOf("blocking/e-1/0", "blocking/e-2/0"), rows.list("blocking").map { it.reactionId })
        }

    /**
     * Parks [first] ([order]'s first event, followed by [second]) for an ordered and an unordered policy, each failing
     * once more on delivery.
     */
    private suspend fun parkMappings(
        order: String = "o-1",
        first: String = "e-1",
        second: String = "e-2",
    ): Pair<Mapping, Mapping> {
        val ordered = Mapping("ordered-mapping", ReactionOrdering.PerAggregate())
        val unordered = Mapping("unordered-mapping", ReactionOrdering.Unordered)
        react(ordered, unordered) {
            appendOrderPlaced(first, order, 1)
            appendOrderPlaced(second, order, 2)
        }
        // The parked mappings are each first in their queue; one more failed attempt each.
        assertEquals(front("Order/$order", "ordered-mapping/$first/mapping"), scheduler.queue("ordered-mapping").pending.first().name)
        scheduler.queue("ordered-mapping").deliverNext()
        assertEquals("unordered-mapping/$first/mapping", scheduler.queue("unordered-mapping").pending.first().name)
        scheduler.queue("unordered-mapping").deliverNext()
        return ordered to unordered
    }

    @Test
    fun `parkedMappings lists ordered and unordered parked mappings with their attempts`(): Unit =
        runBlocking {
            parkMappings()

            assertEquals(RowKind.ORDERED, rows.list("ordered-mapping").first().kind)
            assertEquals(
                listOf(ParkedMapping(EventId("e-1"), EventReactionId("ordered-mapping/e-1/mapping"), 1)),
                operations().parkedMappings("ordered-mapping"),
            )
            assertEquals(RowKind.KEPT, rows.list("unordered-mapping").single { it.reactionId.endsWith("/mapping") }.kind)
            assertEquals(
                listOf(ParkedMapping(EventId("e-1"), EventReactionId("unordered-mapping/e-1/mapping"), 1)),
                operations().parkedMappings("unordered-mapping"),
            )
            assertEquals(emptyList(), operations().blockedReactions("ordered-mapping"))
        }

    @Test
    fun `skipParked on an ordered policy deletes the parked mapping and lets its line continue`(): Unit =
        runBlocking {
            val (ordered, _) = parkMappings()

            operations().skipParked("ordered-mapping", EventId("e-1"))

            assertEquals(listOf("ordered-mapping/e-2/0"), rows.list("ordered-mapping").map { it.reactionId })
            assertTrue(front("Order/o-1", "ordered-mapping/e-2/0") in scheduler.queue("ordered-mapping").pending.map { it.name })
            val mappingsBefore = ordered.mappings.get()
            scheduler.queue("ordered-mapping").deliverAll()
            assertEquals(listOf("o-1#2"), ordered.handled)
            assertEquals(mappingsBefore, ordered.mappings.get())
            assertEquals(emptyList(), rows.list("ordered-mapping"))
            assertEquals(emptyList(), scheduler.queue("ordered-mapping").pending)
        }

    @Test
    fun `skipParked on an unordered policy deletes the kept row, and its pending task finishes without running`(): Unit =
        runBlocking {
            val (_, unordered) = parkMappings()

            operations().skipParked("unordered-mapping", EventId("e-1"))

            assertEquals(emptyList(), operations().parkedMappings("unordered-mapping"))
            val mappingsBefore = unordered.mappings.get()
            scheduler.queue("unordered-mapping").deliverAll()
            assertEquals(mappingsBefore, unordered.mappings.get())
            assertEquals(listOf("o-1#2"), unordered.handled)
            assertEquals(emptyList(), rows.list("unordered-mapping"))
        }

    /**
     * Holds [policy]'s parked mapping of e-1 mid-run (fixed, so it will succeed) through task [task], runs [during],
     * then lets it finish.
     */
    private suspend fun whileMappingRuns(
        policy: Mapping,
        task: String,
        during: suspend () -> Unit,
    ) = coroutineScope {
        val running = CountDownLatch(1)
        val release = CountDownLatch(1)
        policy.broken = false
        policy.onMap = {
            running.countDown()
            release.await(10, TimeUnit.SECONDS)
        }
        val delivery = async(Dispatchers.IO) { scheduler.queue(policy.name).deliver(task) }
        try {
            assertTrue(running.await(10, TimeUnit.SECONDS), "the mapping started")
            during()
        } finally {
            release.countDown()
        }
        delivery.await()
        policy.onMap = {}
    }

    @Test
    fun `skipParked refuses a parked mapping that is running`(): Unit =
        runBlocking {
            val (ordered, unordered) = parkMappings()
            val ops = operations()

            whileMappingRuns(ordered, front("Order/o-1", "ordered-mapping/e-1/mapping")) {
                val refused = assertFailsWith<IllegalStateException> { ops.skipParked("ordered-mapping", EventId("e-1")) }
                assertTrue("running" in refused.message.orEmpty())
            }
            whileMappingRuns(unordered, "unordered-mapping/e-1/mapping") {
                assertFailsWith<IllegalStateException> { ops.skipParked("unordered-mapping", EventId("e-1")) }
            }

            // Both mappings recovered, so the events' work runs once each, in order.
            scheduler.queue("ordered-mapping").deliverAll()
            scheduler.queue("unordered-mapping").deliverAll()
            assertEquals(listOf("o-1#1", "o-1#2"), ordered.handled)
            assertEquals(setOf("o-1#1", "o-1#2"), unordered.handled.toSet())
            assertEquals(2, unordered.handled.size)
            assertEquals(emptyList(), ops.parkedMappings("ordered-mapping"))
            assertEquals(emptyList(), ops.parkedMappings("unordered-mapping"))
        }

    @Test
    fun `skipParked refuses an event with no parked mapping`(): Unit =
        runBlocking {
            parkMappings()
            val ops = operations()

            assertFailsWith<IllegalArgumentException> { ops.skipParked("ordered-mapping", EventId("e-2")) }
            assertFailsWith<IllegalArgumentException> { ops.skipParked("unordered-mapping", EventId("e-9")) }
            assertFailsWith<IllegalArgumentException> { ops.skipParked("other", EventId("e-1")) }
            assertEquals(1, ops.parkedMappings("ordered-mapping").size)
            assertEquals(1, ops.parkedMappings("unordered-mapping").size)
        }

    @Test
    fun `a queue whose policy is no longer registered can still be listed and skipped`(): Unit =
        runBlocking {
            blockOrderOne()
            parkMappings(order = "o-3", first = "e-4", second = "e-5")
            // A later deployment, or an admin script: no reactor, no policies, a scheduler nothing subscribes to.
            val elsewhere = ManualTaskScheduler()
            val ops = operations(on = elsewhere)

            assertEquals(1, ops.blockedReactions("blocking").size)
            assertEquals(1, ops.parkedMappings("ordered-mapping").size)
            assertEquals(1, ops.parkedMappings("unordered-mapping").size)

            ops.skipBlocked("blocking", EventReactionId("blocking/e-1/0"))
            ops.skipParked("ordered-mapping", EventId("e-4"))
            ops.skipParked("unordered-mapping", EventId("e-4"))

            assertEquals(listOf(front("Order/o-1", "blocking/e-2/0")), elsewhere.queue("blocking").pending.map { it.name })
            assertEquals(listOf(front("Order/o-3", "ordered-mapping/e-5/0")), elsewhere.queue("ordered-mapping").pending.map { it.name })
            assertEquals(emptyList(), elsewhere.queue("unordered-mapping").pending)
            assertEquals(emptyList(), ops.blockedReactions("blocking"))
            assertEquals(emptyList(), ops.parkedMappings("ordered-mapping"))
            assertEquals(emptyList(), ops.parkedMappings("unordered-mapping"))
        }
}

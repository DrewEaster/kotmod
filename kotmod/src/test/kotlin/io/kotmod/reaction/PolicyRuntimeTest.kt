package io.kotmod.reaction

import io.kotmod.event.reaction.InMemoryReactionRows
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionTasks
import io.kotmod.event.reaction.RowKind
import io.kotmod.event.reaction.TaskPayload
import io.kotmod.scheduling.ManualTaskScheduler
import io.kotmod.scheduling.TaskOutcome
import io.kotmod.support.OrderPlaced
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerializationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class PolicyRuntimeTest {
    private var now = Instant.parse("2026-10-06T10:00:00Z")
    private val scheduler = ManualTaskScheduler()
    private val rows = InMemoryReactionRows { now }

    private fun runtime(policy: EventPolicy<Notice>) = PolicyRuntime(policy, scheduler, rows, readEvent = { null }) { now }.also { it.start() }

    private fun queue(name: String = "confirmations") = scheduler.queue(name)

    /** Queues [notices] as the policy's triggers for event [eventId] of order [orderId], as its mapping would. */
    private suspend fun PolicyRuntime<Notice>.send(
        vararg notices: Notice,
        eventId: String = "e-1",
        orderId: String = "o-1",
        sequence: Long = 1,
        notBefore: Instant? = null,
    ) = mapAndPublish(orderEvent(OrderPlaced("book"), eventId, orderId, sequence).metadata, policy.sources.single()) {
        notices.map { ProducedTrigger(it, notBefore) }
    }

    private suspend fun deliverNext(name: String = "confirmations"): Duration? = queue(name).deliverNext().againAfter(now)

    @Test
    fun `each event policy runs on its own task queue, named after it, and an ordered one keeps its work in its aggregate's line`() =
        runBlocking<Unit> {
            runtime(RecordingPolicy(name = "a")).send(Confirm("o-1"))
            runtime(RecordingPolicy(name = "b", ordering = ReactionOrdering.PerAggregate())).send(Confirm("o-1"))

            assertEquals(listOf("a/e-1/0"), queue("a").unordered().map { it.reactionId })
            assertTrue(rows.rows("a").isEmpty())
            assertEquals(listOf("Order/o-1" to "b/e-1/0"), rows.rows("b").map { it.key to it.reactionId })
            assertEquals(listOf(ReactionTasks.frontName("Order/o-1", "b/e-1/0")), queue("b").names())
        }

    @Test
    fun `a handled trigger finishes and onCompletion sees Completed`() =
        runBlocking {
            val policy = RecordingPolicy()
            runtime(policy).send(Confirm("o-1"))

            assertEquals(listOf<TaskOutcome>(TaskOutcome.Done), queue().deliverAll())
            assertEquals(listOf<Pair<Notice, ReactionContext>>(Confirm("o-1") to ReactionContext("confirmations/e-1/0", 0)), policy.handled)
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.Completed), policy.completions)
        }

    @Test
    fun `a failure is retried with capped backoff by default, and attempt counts the retries`() =
        runBlocking {
            val policy = RecordingPolicy().apply { failWith = { _, context -> if (context.attempt < 2) RuntimeException("down") else null } }
            runtime(policy).send(Confirm("o-1"))

            val outcomes = (1..3).map { deliverNext() }

            assertEquals(listOf(1.seconds, 2.seconds, null), outcomes)
            assertEquals(listOf(0, 1, 2), policy.handled.map { it.second.attempt })
            assertEquals(listOf(0, 1), policy.failures.map { it.first })
        }

    @Test
    fun `the reaction id is stable across retries and redeliveries`() =
        runBlocking {
            val policy = RecordingPolicy().apply { failWith = { _, context -> if (context.attempt == 0) RuntimeException("flaky") else null } }
            runtime(policy).send(Confirm("o-1"))

            deliverNext()
            queue().deliverDuplicate("confirmations/e-1/0")
            deliverNext()

            assertEquals(3, policy.handled.size)
            assertEquals(listOf("confirmations/e-1/0"), policy.handled.map { it.second.reactionId }.distinct())
        }

    @Test
    fun `onFailure returning GiveUp finishes the reaction as given up, and onCompletion sees the error`() =
        runBlocking {
            val boom = RuntimeException("card declined")
            val policy =
                RecordingPolicy().apply {
                    failWith = { _, _ -> boom }
                    decide = { _, attempt, _ -> if (attempt < 1) Retry(1.seconds) else GiveUp }
                }
            runtime(policy).send(Confirm("o-1"))

            assertEquals(listOf(1.seconds, null), listOf(deliverNext(), deliverNext()))
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.GaveUp(boom)), policy.completions)
            assertSame(boom, policy.failures.first().second)
            assertTrue(queue().pending.isEmpty())
        }

    @Test
    fun `running past the timeout is a failure passed to onFailure as ReactionTimeoutException`() =
        runBlocking<Unit> {
            val policy =
                RecordingPolicy(timeout = 50.milliseconds).apply {
                    work = { delay(5.seconds) }
                    decide = { _, _, _ -> GiveUp }
                }
            runtime(policy).send(Confirm("o-1"))

            assertEquals(listOf<TaskOutcome>(TaskOutcome.Done), queue().deliverAll())
            assertEquals(50.milliseconds, assertIs<ReactionTimeoutException>(policy.failures.single().second).timeout)
            assertIs<ReactionResult.GaveUp>(policy.completions.single().second)
        }

    @Test
    fun `a failing reaction doesn't hold back another`() =
        runBlocking {
            val policy = RecordingPolicy().apply { failWith = { notice, _ -> if (notice == Confirm("o-1")) RuntimeException("down") else null } }
            val runtime = runtime(policy)
            runtime.send(Confirm("o-1"), eventId = "e-1", orderId = "o-1")
            runtime.send(Confirm("o-2"), eventId = "e-2", orderId = "o-2")

            assertEquals(listOf(1.seconds, null), listOf(deliverNext(), deliverNext()))
            assertEquals(listOf<Notice>(Confirm("o-1"), Confirm("o-2")), policy.handled.map { it.first })
        }

    @Test
    fun `with ordering, a failing reaction holds back only its own aggregate's later reactions, in its own event policy`() =
        runBlocking {
            val ordered =
                RecordingPolicy(name = "ordered", ordering = ReactionOrdering.PerAggregate()).apply {
                    failWith = { notice, _ -> if (notice == Confirm("a1")) RuntimeException("down") else null }
                }
            val other = RecordingPolicy(name = "other", ordering = ReactionOrdering.PerAggregate())
            val orderedRuntime = runtime(ordered)
            val otherRuntime = runtime(other)
            orderedRuntime.send(Confirm("a1"), eventId = "a1", orderId = "a", sequence = 1)
            orderedRuntime.send(Confirm("a2"), eventId = "a2", orderId = "a", sequence = 2)
            orderedRuntime.send(Confirm("b1"), eventId = "b1", orderId = "b", sequence = 1)
            otherRuntime.send(Confirm("a1"), eventId = "a1", orderId = "a", sequence = 1)

            repeat(3) { queue("ordered").deliverNext() }
            queue("other").deliverAll()

            assertEquals(listOf<Notice>(Confirm("a1"), Confirm("b1"), Confirm("a1")), ordered.handled.map { it.first })
            assertEquals(listOf<Notice>(Confirm("a1")), other.handled.map { it.first })
            assertEquals(listOf("ordered/a1/0", "ordered/a2/0"), rows.rows("ordered").map { it.reactionId })
        }

    @Test
    fun `an ordered policy's BlockAggregate give-up blocks its line`() =
        runBlocking {
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(OnGiveUp.BlockAggregate)).apply {
                    failWith = { notice, _ -> if (notice == Confirm("a1")) RuntimeException("down") else null }
                    decide = { _, _, _ -> GiveUp }
                }
            val runtime = runtime(policy)
            runtime.send(Confirm("a1"), eventId = "a1", orderId = "a", sequence = 1)
            runtime.send(Confirm("a2"), eventId = "a2", orderId = "a", sequence = 2)

            assertEquals(listOf<TaskOutcome>(TaskOutcome.Done), queue().deliverAll())

            assertEquals(listOf<Notice>(Confirm("a1")), policy.handled.map { it.first })
            assertIs<ReactionResult.GaveUp>(policy.completions.single().second)
            assertEquals(
                listOf("confirmations/a1/0" to true, "confirmations/a2/0" to false),
                rows.rows("confirmations").map { it.reactionId to it.blocked },
            )
            assertTrue(queue().pending.isEmpty())
        }

    @Test
    fun `a policy switched from unordered to BlockAggregate gives up on its pending unordered work once`() =
        runBlocking {
            val before = runtime(RecordingPolicy())
            before.send(Confirm("a1"))
            before.stop()
            val after =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(OnGiveUp.BlockAggregate)).apply {
                    failWith = { _, _ -> RuntimeException("down") }
                    decide = { _, _, _ -> GiveUp }
                }
            runtime(after)

            assertEquals(listOf<TaskOutcome>(TaskOutcome.Done), queue().deliverAll())
            assertEquals(1, after.handled.size)
            assertIs<ReactionResult.GaveUp>(after.completions.single().second)
            assertTrue(queue().pending.isEmpty())
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `an ordered policy's ContinueWithNext give-up moves on to its line's next work`() =
        runBlocking {
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate(OnGiveUp.ContinueWithNext)).apply {
                    failWith = { notice, _ -> if (notice == Confirm("a1")) RuntimeException("down") else null }
                    decide = { _, _, _ -> GiveUp }
                }
            val runtime = runtime(policy)
            runtime.send(Confirm("a1"), eventId = "a1", orderId = "a", sequence = 1)
            runtime.send(Confirm("a2"), eventId = "a2", orderId = "a", sequence = 2)

            queue().deliverAll()

            assertEquals(listOf<Notice>(Confirm("a1"), Confirm("a2")), policy.handled.map { it.first })
            assertEquals(listOf(true, false), policy.completions.map { it.second is ReactionResult.GaveUp })
            assertTrue(rows.rows("confirmations").isEmpty())
        }

    @Test
    fun `a trigger delivered before notBefore waits without running, then runs once due`() =
        runBlocking {
            val policy = RecordingPolicy()
            runtime(policy).send(Confirm("o-1"), notBefore = now + 1.hours)

            assertEquals(1.hours, deliverNext())
            assertTrue(policy.handled.isEmpty())
            now += 1.hours
            assertEquals(null, deliverNext())
            assertEquals(0, policy.handled.single().second.attempt)
        }

    @Test
    fun `onFailure throwing retries the reaction after a backoff`() =
        runBlocking {
            val policy =
                RecordingPolicy().apply {
                    failWith = { _, _ -> RuntimeException("down") }
                    decide = { _, _, _ -> error("broken onFailure") }
                }
            runtime(policy).send(Confirm("o-1"))

            assertEquals(1.seconds, deliverNext())
            assertEquals(1, queue().pending.size)
        }

    @Test
    fun `onCompletion throwing retries the reaction after a backoff`() =
        runBlocking {
            val policy = RecordingPolicy().apply { onCompleted = { error("audit log down") } }
            runtime(policy).send(Confirm("o-1"))

            assertEquals(1.seconds, deliverNext())
            assertEquals(1, queue().pending.size)
        }

    @Test
    fun `an ordered policy's onCompletion throwing retries the reaction and keeps its place in line`() =
        runBlocking {
            var failing = true
            val policy = RecordingPolicy(ordering = ReactionOrdering.PerAggregate()).apply { onCompleted = { if (failing) error("audit log down") } }
            val runtime = runtime(policy)
            runtime.send(Confirm("a1"), eventId = "a1", orderId = "a", sequence = 1)
            runtime.send(Confirm("a2"), eventId = "a2", orderId = "a", sequence = 2)

            assertEquals(1.seconds, deliverNext())
            assertEquals(listOf("confirmations/a1/0", "confirmations/a2/0"), rows.rows("confirmations").map { it.reactionId })
            failing = false
            queue().deliverAll()

            assertEquals(listOf<Notice>(Confirm("a1"), Confirm("a1"), Confirm("a2")), policy.handled.map { it.first })
        }

    @Test
    fun `a stored trigger that can't be decoded goes back to the backend and never reaches handle or onFailure`() =
        runBlocking {
            val policy = RecordingPolicy()
            runtime(policy)
            val stored = """{"type":"trigger","trigger":"{\"type\":\"io.kotmod.reaction.Renamed\"}"}"""
            queue().schedule("r-1", ReactionTasks.encode(TaskPayload.Unordered("r-1", stored)), now)

            assertFailsWith<SerializationException> { queue().deliverNext() }
            assertTrue(policy.handled.isEmpty())
            assertTrue(policy.failures.isEmpty())
            assertEquals(listOf("r-1"), queue().names())
        }

    @Test
    fun `a CancellationException thrown by handle while the reaction is still running is a failure like any other`() =
        runBlocking<Unit> {
            val declined = java.util.concurrent.CancellationException("payment request cancelled by the provider")
            val policy =
                RecordingPolicy().apply {
                    failWith = { _, _ -> declined }
                    decide = { _, attempt, _ -> if (attempt < 1) Retry(1.seconds) else GiveUp }
                }
            runtime(policy).send(Confirm("o-1"))

            assertEquals(listOf(1.seconds, null), listOf(deliverNext(), deliverNext()))
            assertEquals(listOf(0, 1), policy.failures.map { it.first })
            assertSame(declined, policy.failures.last().second)
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.GaveUp(declined)), policy.completions)
            assertTrue(queue().pending.isEmpty())
        }

    @Test
    fun `onFailure or onCompletion throwing a CancellationException while running retries the reaction after a backoff`() =
        runBlocking<Unit> {
            val rethrowing =
                RecordingPolicy().apply {
                    failWith = { _, _ -> java.util.concurrent.CancellationException("declined") }
                    decide = { _, _, error -> throw error }
                }
            runtime(rethrowing).send(Confirm("o-1"))
            assertEquals(1.seconds, deliverNext())

            val completing = RecordingPolicy(name = "audits").apply { onCompleted = { throw java.util.concurrent.CancellationException("audit") } }
            runtime(completing).send(Confirm("o-2"))
            assertEquals(1.seconds, deliverNext("audits"))
        }

    @Test
    fun `a reaction cancelled by a shutdown goes back to the backend and is not reported as a failure`() =
        runBlocking<Unit> {
            val started = CompletableDeferred<Unit>()
            val policy =
                RecordingPolicy().apply {
                    work = {
                        started.complete(Unit)
                        awaitCancellation()
                    }
                }
            runtime(policy).send(Confirm("o-1"))

            val delivery = async { queue().deliverNext() }
            started.await()
            delivery.cancelAndJoin()

            assertTrue(delivery.isCancelled)
            assertFailsWith<CancellationException> { delivery.await() }
            assertTrue(policy.failures.isEmpty())
            assertTrue(policy.completions.isEmpty())
            assertEquals(1, queue().pending.size)
        }

    @Test
    fun `an ordered reaction cancelled by a shutdown keeps its place and its attempt count`() =
        runBlocking<Unit> {
            val started = CompletableDeferred<Unit>()
            val policy =
                RecordingPolicy(ordering = ReactionOrdering.PerAggregate()).apply {
                    work = {
                        started.complete(Unit)
                        awaitCancellation()
                    }
                }
            runtime(policy).send(Confirm("o-1"))

            val delivery = async { queue().deliverNext() }
            started.await()
            delivery.cancelAndJoin()

            assertTrue(policy.failures.isEmpty())
            val row = rows.rows("confirmations").single()
            assertEquals(RowKind.ORDERED to 0, row.kind to row.attempts)
            assertEquals(1, queue().pending.size)
        }
}

package io.kotmod.reaction

import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.process.ManualQueues
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
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class PolicyRuntimeTest {
    private var now = Instant.parse("2026-10-06T10:00:00Z")
    private val queues = ManualQueues(enforceOrdering = true)

    private fun runtime(policy: EventPolicy<Notice>) = PolicyRuntime(policy, queues, readEvent = { null }, clock = { now }).also { it.start() }

    private suspend fun PolicyRuntime<Notice>.send(
        id: String,
        notice: Notice,
        key: String? = null,
        sequence: Long = 1,
        notBefore: Instant? = null,
    ) = publish(EventReactionId(id), notice, key?.let { DispatchOrdering(it, sequence, 0, OnGiveUp.ContinueWithNext) }, notBefore)

    @Test
    fun `the queue is named after the event policy and ordered with it`() {
        runtime(RecordingPolicy(name = "a"))
        runtime(RecordingPolicy(name = "b", ordering = ReactionOrdering.PerAggregate()))

        assertEquals(listOf("a" to false, "b" to true), queues.channels)
    }

    @Test
    fun `an ordered event policy needs a queue that supports ordering`() {
        assertFailsWith<IllegalArgumentException> {
            PolicyRuntime(RecordingPolicy(ordering = ReactionOrdering.PerAggregate()), ManualQueues(supportsOrdering = false), readEvent = { null }, clock = { now })
        }
    }

    @Test
    fun `a handled trigger finishes and onCompletion sees Completed`() =
        runBlocking {
            val policy = RecordingPolicy()
            runtime(policy).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = false)), queues.deliver("confirmations"))
            assertEquals(listOf<Pair<Notice, ReactionContext>>(Confirm("o-1") to ReactionContext("r-1", 0)), policy.handled)
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.Completed), policy.completions)
        }

    @Test
    fun `a failure is retried with capped backoff by default, and attempt counts the retries`() =
        runBlocking {
            val policy = RecordingPolicy().apply { failWith = { _, context -> if (context.attempt < 2) RuntimeException("down") else null } }
            runtime(policy).send("r-1", Confirm("o-1"))

            val outcomes = (1..3).flatMap { queues.deliver("confirmations") }

            assertEquals(
                listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Retry(2.seconds), ReactionOutcome.Finished(gaveUp = false)),
                outcomes,
            )
            assertEquals(listOf(0, 1, 2), policy.handled.map { it.second.attempt })
            assertEquals(listOf(0, 1), policy.failures.map { it.first })
        }

    @Test
    fun `the reaction id is stable across retries and redeliveries`() =
        runBlocking {
            val policy = RecordingPolicy().apply { failWith = { _, context -> if (context.attempt == 0) RuntimeException("flaky") else null } }
            runtime(policy).send("confirmations/e-1/0", Confirm("o-1"))

            queues.deliver("confirmations")
            queues.deliver("confirmations")
            queues.redeliver(queues.published.single())
            queues.deliver("confirmations")

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
            runtime(policy).send("r-1", Confirm("o-1"))

            val outcomes = queues.deliver("confirmations") + queues.deliver("confirmations")

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Finished(gaveUp = true)), outcomes)
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.GaveUp(boom)), policy.completions)
            assertSame(boom, policy.failures.first().second)
            assertTrue(queues.pending("confirmations").isEmpty())
        }

    @Test
    fun `running past the timeout is a failure passed to onFailure as ReactionTimeoutException`() =
        runBlocking<Unit> {
            val policy =
                RecordingPolicy(timeout = 50.milliseconds).apply {
                    work = { delay(5.seconds) }
                    decide = { _, _, _ -> GiveUp }
                }
            runtime(policy).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = true)), queues.deliver("confirmations"))
            assertEquals(50.milliseconds, assertIs<ReactionTimeoutException>(policy.failures.single().second).timeout)
            assertIs<ReactionResult.GaveUp>(policy.completions.single().second)
        }

    @Test
    fun `a failing reaction doesn't hold back another`() =
        runBlocking {
            val policy = RecordingPolicy().apply { failWith = { notice, _ -> if (notice == Confirm("o-1")) RuntimeException("down") else null } }
            val runtime = runtime(policy)
            runtime.send("r-1", Confirm("o-1"))
            runtime.send("r-2", Confirm("o-2"))

            assertEquals(
                listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Finished(gaveUp = false)),
                queues.deliver("confirmations"),
            )
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
            orderedRuntime.send("a1", Confirm("a1"), key = "Order/a", sequence = 1)
            orderedRuntime.send("a2", Confirm("a2"), key = "Order/a", sequence = 2)
            orderedRuntime.send("b1", Confirm("b1"), key = "Order/b", sequence = 1)
            otherRuntime.send("a1", Confirm("a1"), key = "Order/a", sequence = 1)

            queues.deliver("ordered")
            queues.deliver("ordered")
            queues.deliver("other")

            assertEquals(listOf<Notice>(Confirm("a1"), Confirm("b1"), Confirm("a1")), ordered.handled.map { it.first })
            assertEquals(listOf<Notice>(Confirm("a1")), other.handled.map { it.first })
        }

    @Test
    fun `a trigger delivered before notBefore waits without running, then runs once due`() =
        runBlocking {
            val policy = RecordingPolicy()
            runtime(policy).send("r-1", Confirm("o-1"), notBefore = now + 1.hours)

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Wait(1.hours)), queues.deliver("confirmations"))
            assertTrue(policy.handled.isEmpty())
            now += 1.hours
            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = false)), queues.deliver("confirmations"))
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
            runtime(policy).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("confirmations"))
            assertEquals(1, queues.pending("confirmations").size)
        }

    @Test
    fun `onCompletion throwing retries the reaction after a backoff`() =
        runBlocking {
            val policy = RecordingPolicy().apply { onCompleted = { error("audit log down") } }
            runtime(policy).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("confirmations"))
            assertEquals(1, queues.pending("confirmations").size)
        }

    @Test
    fun `a stored trigger that can't be decoded goes back to the queue and never reaches handle or onFailure`() =
        runBlocking {
            val policy = RecordingPolicy()
            runtime(policy)
            queues.redeliver(
                ManualQueues.Published("confirmations", EventReactionId("r-1"), TriggerItem("""{"type":"io.kotmod.reaction.Renamed"}"""), null, null),
            )

            assertFailsWith<SerializationException> { queues.deliver("confirmations") }
            assertTrue(policy.handled.isEmpty())
            assertTrue(policy.failures.isEmpty())
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
            runtime(policy).send("r-1", Confirm("o-1"))

            val outcomes = queues.deliver("confirmations") + queues.deliver("confirmations")

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Finished(gaveUp = true)), outcomes)
            assertEquals(listOf(0, 1), policy.failures.map { it.first })
            assertSame(declined, policy.failures.last().second)
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.GaveUp(declined)), policy.completions)
            assertTrue(queues.pending("confirmations").isEmpty())
        }

    @Test
    fun `onFailure or onCompletion throwing a CancellationException while running retries the reaction after a backoff`() =
        runBlocking<Unit> {
            val rethrowing =
                RecordingPolicy().apply {
                    failWith = { _, _ -> java.util.concurrent.CancellationException("declined") }
                    decide = { _, _, error -> throw error }
                }
            runtime(rethrowing).send("r-1", Confirm("o-1"))
            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("confirmations"))

            val completing = RecordingPolicy(name = "audits").apply { onCompleted = { throw java.util.concurrent.CancellationException("audit") } }
            runtime(completing).send("r-2", Confirm("o-2"))
            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("audits"))
        }

    @Test
    fun `a reaction cancelled by a shutdown goes back to the queue and is not reported as a failure`() =
        runBlocking<Unit> {
            val started = CompletableDeferred<Unit>()
            val policy =
                RecordingPolicy().apply {
                    work = {
                        started.complete(Unit)
                        awaitCancellation()
                    }
                }
            runtime(policy).send("r-1", Confirm("o-1"))

            val delivery = async { queues.deliver("confirmations") }
            started.await()
            delivery.cancelAndJoin()

            assertTrue(delivery.isCancelled)
            assertFailsWith<CancellationException> { delivery.await() }
            assertTrue(policy.failures.isEmpty())
            assertTrue(policy.completions.isEmpty())
            assertEquals(1, queues.pending("confirmations").size)
        }
}

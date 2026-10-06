package io.kotmod.reaction

import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.process.ManualQueues
import kotlinx.coroutines.CancellationException
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

class UseCaseRuntimeTest {
    private var now = Instant.parse("2026-10-06T10:00:00Z")
    private val queues = ManualQueues(enforceOrdering = true)

    private fun runtime(useCase: Reactions<Notice>) = UseCaseRuntime(useCase, queues, readEvent = { null }, clock = { now }).also { it.start() }

    private suspend fun UseCaseRuntime<Notice>.send(
        id: String,
        notice: Notice,
        key: String? = null,
        sequence: Long = 1,
        notBefore: Instant? = null,
    ) = publish(EventReactionId(id), notice, key?.let { DispatchOrdering(it, sequence, 0, OnGiveUp.ContinueWithNext) }, notBefore)

    @Test
    fun `the queue is named after the use case and ordered with it`() {
        runtime(RecordingUseCase(name = "a"))
        runtime(RecordingUseCase(name = "b", ordering = ReactionOrdering.PerAggregate()))

        assertEquals(listOf("a" to false, "b" to true), queues.channels)
    }

    @Test
    fun `an ordered use case needs a queue that supports ordering`() {
        assertFailsWith<IllegalArgumentException> {
            UseCaseRuntime(RecordingUseCase(ordering = ReactionOrdering.PerAggregate()), ManualQueues(supportsOrdering = false), readEvent = { null }, clock = { now })
        }
    }

    @Test
    fun `a handled trigger finishes and onCompletion sees Completed`() =
        runBlocking {
            val useCase = RecordingUseCase()
            runtime(useCase).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = false)), queues.deliver("confirmations"))
            assertEquals(listOf<Pair<Notice, ReactionContext>>(Confirm("o-1") to ReactionContext("r-1", 0)), useCase.handled)
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.Completed), useCase.completions)
        }

    @Test
    fun `a failure is retried with capped backoff by default, and attempt counts the retries`() =
        runBlocking {
            val useCase = RecordingUseCase().apply { failWith = { _, context -> if (context.attempt < 2) RuntimeException("down") else null } }
            runtime(useCase).send("r-1", Confirm("o-1"))

            val outcomes = (1..3).flatMap { queues.deliver("confirmations") }

            assertEquals(
                listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Retry(2.seconds), ReactionOutcome.Finished(gaveUp = false)),
                outcomes,
            )
            assertEquals(listOf(0, 1, 2), useCase.handled.map { it.second.attempt })
            assertEquals(listOf(0, 1), useCase.failures.map { it.first })
        }

    @Test
    fun `the reaction id is stable across retries and redeliveries`() =
        runBlocking {
            val useCase = RecordingUseCase().apply { failWith = { _, context -> if (context.attempt == 0) RuntimeException("flaky") else null } }
            runtime(useCase).send("confirmations/e-1/0", Confirm("o-1"))

            queues.deliver("confirmations")
            queues.deliver("confirmations")
            queues.redeliver(queues.published.single())
            queues.deliver("confirmations")

            assertEquals(3, useCase.handled.size)
            assertEquals(listOf("confirmations/e-1/0"), useCase.handled.map { it.second.reactionId }.distinct())
        }

    @Test
    fun `onFailure returning GiveUp finishes the reaction as given up, and onCompletion sees the error`() =
        runBlocking {
            val boom = RuntimeException("card declined")
            val useCase =
                RecordingUseCase().apply {
                    failWith = { _, _ -> boom }
                    decide = { _, attempt, _ -> if (attempt < 1) Retry(1.seconds) else GiveUp }
                }
            runtime(useCase).send("r-1", Confirm("o-1"))

            val outcomes = queues.deliver("confirmations") + queues.deliver("confirmations")

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Finished(gaveUp = true)), outcomes)
            assertEquals(listOf<Pair<Notice, ReactionResult>>(Confirm("o-1") to ReactionResult.GaveUp(boom)), useCase.completions)
            assertSame(boom, useCase.failures.first().second)
            assertTrue(queues.pending("confirmations").isEmpty())
        }

    @Test
    fun `running past the timeout is a failure passed to onFailure as ReactionTimeoutException`() =
        runBlocking {
            val useCase =
                RecordingUseCase(timeout = 50.milliseconds).apply {
                    work = { delay(5.seconds) }
                    decide = { _, _, _ -> GiveUp }
                }
            runtime(useCase).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = true)), queues.deliver("confirmations"))
            assertEquals(50.milliseconds, assertIs<ReactionTimeoutException>(useCase.failures.single().second).timeout)
            assertIs<ReactionResult.GaveUp>(useCase.completions.single().second)
        }

    @Test
    fun `a failing reaction doesn't hold back another`() =
        runBlocking {
            val useCase = RecordingUseCase().apply { failWith = { notice, _ -> if (notice == Confirm("o-1")) RuntimeException("down") else null } }
            val runtime = runtime(useCase)
            runtime.send("r-1", Confirm("o-1"))
            runtime.send("r-2", Confirm("o-2"))

            assertEquals(
                listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds), ReactionOutcome.Finished(gaveUp = false)),
                queues.deliver("confirmations"),
            )
        }

    @Test
    fun `with ordering, a failing reaction holds back only its own aggregate's later reactions, in its own use case`() =
        runBlocking {
            val ordered =
                RecordingUseCase(name = "ordered", ordering = ReactionOrdering.PerAggregate()).apply {
                    failWith = { notice, _ -> if (notice == Confirm("a1")) RuntimeException("down") else null }
                }
            val other = RecordingUseCase(name = "other", ordering = ReactionOrdering.PerAggregate())
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
            val useCase = RecordingUseCase()
            runtime(useCase).send("r-1", Confirm("o-1"), notBefore = now + 1.hours)

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Wait(1.hours)), queues.deliver("confirmations"))
            assertTrue(useCase.handled.isEmpty())
            now += 1.hours
            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Finished(gaveUp = false)), queues.deliver("confirmations"))
            assertEquals(0, useCase.handled.single().second.attempt)
        }

    @Test
    fun `onFailure throwing retries the reaction after a backoff`() =
        runBlocking {
            val useCase =
                RecordingUseCase().apply {
                    failWith = { _, _ -> RuntimeException("down") }
                    decide = { _, _, _ -> error("broken policy") }
                }
            runtime(useCase).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("confirmations"))
            assertEquals(1, queues.pending("confirmations").size)
        }

    @Test
    fun `onCompletion throwing retries the reaction after a backoff`() =
        runBlocking {
            val useCase = RecordingUseCase().apply { onCompleted = { error("audit log down") } }
            runtime(useCase).send("r-1", Confirm("o-1"))

            assertEquals(listOf<ReactionOutcome>(ReactionOutcome.Retry(1.seconds)), queues.deliver("confirmations"))
            assertEquals(1, queues.pending("confirmations").size)
        }

    @Test
    fun `a stored trigger that can't be decoded goes back to the queue and never reaches handle or onFailure`() =
        runBlocking {
            val useCase = RecordingUseCase()
            runtime(useCase)
            queues.redeliver(
                ManualQueues.Published("confirmations", EventReactionId("r-1"), TriggerItem("""{"type":"io.kotmod.reaction.Renamed"}"""), null, null),
            )

            assertFailsWith<SerializationException> { queues.deliver("confirmations") }
            assertTrue(useCase.handled.isEmpty())
            assertTrue(useCase.failures.isEmpty())
        }

    @Test
    fun `a reaction cancelled by a shutdown goes back to the queue and is not reported as a failure`() =
        runBlocking {
            val useCase = RecordingUseCase().apply { work = { throw CancellationException("scheduler stopping") } }
            runtime(useCase).send("r-1", Confirm("o-1"))

            assertFailsWith<CancellationException> { queues.deliver("confirmations") }
            assertTrue(useCase.failures.isEmpty())
            assertTrue(useCase.completions.isEmpty())
        }
}

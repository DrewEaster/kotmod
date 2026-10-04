package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionCompletionResult
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.RetrySignal
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class OrderedReactionsIntegrationTest : IntegrationTest() {
    private fun reactions(name: String = "ordered") =
        DbSchedulerEventReactions(name, TestTriggerSerializer, jdbc = jdbc, orderedRecheckDelay = 200.milliseconds)

    private fun ordering(
        key: String,
        sequence: Long,
        onGiveUp: OnGiveUp = OnGiveUp.ContinueWithNext,
    ) = DispatchOrdering(key, sequence, 0, onGiveUp)

    private class Log {
        val events = CopyOnWriteArrayList<String>() // "start:<name>" / "end:<name>"
    }

    private fun executor(
        reactions: DbSchedulerEventReactions<TestTrigger>,
        scheduler: com.github.kagkarlsson.scheduler.Scheduler,
        log: Log,
        execute: suspend (TestTrigger, Int) -> EventReactionExecutionResult = { _, _ ->
            delay(50)
            EventReactionExecutionResult.EventReactionExecutionCompleted
        },
        giveUp: Boolean = false,
    ) = EventReactionExecutor<TestTrigger, Unit>(
        sink = reactions.sink(scheduler),
        source = reactions.source,
        createExecutionContext = { _, _ -> },
        execute = { _, _, trigger, retryCount, _ ->
            log.events += "start:${trigger.name}"
            try {
                execute(trigger, retryCount)
            } finally {
                log.events += "end:${trigger.name}"
            }
        },
        failureRetryHandler = { _, _, _, _, _, _ ->
            if (giveUp) RetrySignal.DoNotRetry(EventReactionCompletionResult.EventReactionFailed("gave up", allowManualRetry = true))
            else RetrySignal.Retry(100.milliseconds)
        },
        timeoutRetryHandler = { _, _, _, _, _ -> RetrySignal.Retry(100.milliseconds) },
        onCompletion = { _, _, _, _, _, _ -> },
    )

    private fun neverOverlap(log: Log): Boolean {
        var running = 0
        for (e in log.events) {
            running += if (e.startsWith("start:")) 1 else -1
            if (running > 1) return false
        }
        return true
    }

    @Test
    fun `reactions for one aggregate run one at a time in sequence order`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log)
            (1..8L).forEach { executor.dispatch(EventReactionId("r-$it"), TestTrigger("A$it"), ordering("Order/a", it)) }

            running(scheduler, executor) { eventually(20.seconds) { log.events.count { it.startsWith("end:") } == 8 } }

            assertTrue(neverOverlap(log), log.events.toString())
            assertEquals((1..8).map { "A$it" }, log.events.filter { it.startsWith("start:") }.map { it.removePrefix("start:") })
        }

    @Test
    fun `different aggregates run in parallel`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, execute = { _, _ ->
                delay(500)
                EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-a"), TestTrigger("A1"), ordering("Order/a", 1))
            executor.dispatch(EventReactionId("r-b"), TestTrigger("B1"), ordering("Order/b", 1))

            running(scheduler, executor) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `a retrying head holds back later reactions`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, execute = { trigger, retryCount ->
                if (trigger.name == "A1" && retryCount < 2) EventReactionExecutionResult.EventReactionFailed(RuntimeException("flaky"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2))

            running(scheduler, executor) { eventually(15.seconds) { log.events.contains("end:A2") } }

            val starts = log.events.filter { it.startsWith("start:") }
            assertEquals(listOf("start:A1", "start:A1", "start:A1", "start:A2"), starts)
        }

    @Test
    fun `ContinueWithNext moves on after a give-up`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, giveUp = true, execute = { trigger, _ ->
                if (trigger.name == "A1") EventReactionExecutionResult.EventReactionFailed(RuntimeException("poison"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.ContinueWithNext))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.ContinueWithNext))

            running(scheduler, executor) { eventually(10.seconds) { log.events.contains("end:A2") } }
        }

    @Test
    fun `BlockAggregate parks the aggregate until retried or skipped, and is never nudged`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val failures = AtomicInteger()
            val executor = executor(reactions, scheduler, log, giveUp = true, execute = { trigger, _ ->
                if (trigger.name == "A1" && failures.getAndIncrement() == 0) EventReactionExecutionResult.EventReactionFailed(RuntimeException("poison"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.BlockAggregate))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.BlockAggregate))

            running(scheduler, executor) {
                eventually { reactions.blockedReactions(scheduler).isNotEmpty() }
                delay(1000)
                assertTrue("start:A2" !in log.events, "A2 must wait behind the blocked A1")
                assertEquals(listOf(EventReactionId("r-1")), reactions.blockedReactions(scheduler).map { it.reactionId })

                reactions.retryBlocked(scheduler, EventReactionId("r-1"))
                eventually(10.seconds) { log.events.contains("end:A2") }
            }
            assertEquals(listOf("start:A1", "start:A1", "start:A2"), log.events.filter { it.startsWith("start:") })
        }

    @Test
    fun `skipBlocked releases the aggregate without running the blocked reaction again`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, giveUp = true, execute = { trigger, _ ->
                if (trigger.name == "A1") EventReactionExecutionResult.EventReactionFailed(RuntimeException("poison"))
                else EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.BlockAggregate))
            executor.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.BlockAggregate))

            running(scheduler, executor) {
                eventually { reactions.blockedReactions(scheduler).isNotEmpty() }
                reactions.skipBlocked(scheduler, EventReactionId("r-1"))
                eventually(10.seconds) { log.events.contains("end:A2") }
            }
            assertEquals(1, log.events.count { it == "start:A1" })
        }

    @Test
    fun `duplicate ordered dispatch is absorbed`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log)
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))
            executor.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))

            running(scheduler, executor) {
                eventually { log.events.contains("end:A1") }
                delay(500)
            }
            assertEquals(1, log.events.count { it == "start:A1" })
        }

    @Test
    fun `aggregate ids with separators never share ordering`() =
        runBlocking {
            val reactions = reactions()
            val scheduler = testScheduler(dataSource, *reactions.tasks.toTypedArray())
            val log = Log()
            val executor = executor(reactions, scheduler, log, execute = { _, _ ->
                delay(400)
                EventReactionExecutionResult.EventReactionExecutionCompleted
            })
            executor.dispatch(EventReactionId("r-a"), TestTrigger("X"), ordering("Order/a", 2))
            executor.dispatch(EventReactionId("r-b"), TestTrigger("Y"), ordering("Order/a#0000000000000000001", 1))

            running(scheduler, executor) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            // Different aggregates: both start before either ends.
            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `two executors handling the same aggregate do not wait on each other`() =
        runBlocking {
            val first = reactions("first")
            val second = reactions("second")
            val scheduler = testScheduler(dataSource, *(first.tasks + second.tasks).toTypedArray())
            val log = Log()
            val slow: suspend (TestTrigger, Int) -> EventReactionExecutionResult = { _, _ ->
                delay(500)
                EventReactionExecutionResult.EventReactionExecutionCompleted
            }
            val e1 = executor(first, scheduler, log, execute = slow)
            val e2 = executor(second, scheduler, log, execute = slow)
            e1.dispatch(EventReactionId("r-1"), TestTrigger("first"), ordering("Order/a", 1))
            e2.dispatch(EventReactionId("r-1"), TestTrigger("second"), ordering("Order/a", 1))

            running(scheduler, e1, e2) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }
}

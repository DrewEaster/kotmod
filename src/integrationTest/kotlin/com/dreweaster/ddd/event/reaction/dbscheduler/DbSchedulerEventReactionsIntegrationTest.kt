package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionCompletionResult
import com.dreweaster.ddd.event.reaction.EventReactionExecutionResult
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.RetrySignal
import com.dreweaster.ddd.postgres.support.IntegrationTest
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DbSchedulerEventReactionsIntegrationTest : IntegrationTest() {
    private fun rowExists(
        reactions: DbSchedulerEventReactions<TestTrigger>,
        scheduler: com.github.kagkarlsson.scheduler.Scheduler,
        id: String,
    ) = scheduler.getScheduledExecution(reactions.task.instanceId(id)).isPresent

    @Test
    fun `dispatched reaction executes once, completes and its row is removed`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("hello"))
                eventually { recorder.completions.isNotEmpty() && !rowExists(reactions, scheduler, "r-1") }
            }

            assertEquals(1, recorder.attempts.size)
            assertEquals(TestTrigger("hello"), recorder.attempts.single().trigger)
            assertEquals(0, recorder.attempts.single().retryCount)
            assertEquals(
                listOf(EventReactionId("r-1") to EventReactionCompletionResult.EventReactionCompleted),
                recorder.completions.toList(),
            )
        }

    @Test
    fun `duplicate dispatch while pending results in one row and one execution`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)

            // Scheduler not started yet, so the first dispatch is still pending when the second arrives.
            executor.dispatch(EventReactionId("r-1"), TestTrigger("first"))
            executor.dispatch(EventReactionId("r-1"), TestTrigger("second"))
            assertEquals(1, scheduler.getScheduledExecutionsForTask("test-reactions", String::class.java).size)

            running(scheduler, executor) {
                eventually { recorder.completions.isNotEmpty() }
                delay(500) // give a hypothetical duplicate time to show up
            }

            assertEquals(listOf(TestTrigger("first")), recorder.attempts.map { it.trigger })
        }

    @Test
    fun `retries carry an increasing retryCount and fresh execution ids until success`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor =
                testExecutor(
                    reactions,
                    scheduler,
                    recorder,
                    execute = { attempt ->
                        if (attempt.retryCount < 2) {
                            EventReactionExecutionResult.EventReactionFailed(RuntimeException("attempt ${attempt.retryCount}"))
                        } else {
                            EventReactionExecutionResult.EventReactionExecutionCompleted
                        }
                    },
                )

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("flaky"))
                eventually { recorder.completions.isNotEmpty() && !rowExists(reactions, scheduler, "r-1") }
            }

            assertEquals(listOf(0, 1, 2), recorder.attempts.map { it.retryCount })
            assertEquals(3, recorder.attempts.map { it.executionId }.toSet().size)
            assertEquals(EventReactionCompletionResult.EventReactionCompleted, recorder.completions.single().second)
        }

    @Test
    fun `DoNotRetry passes the failure to onCompletion and removes the row`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val failure = EventReactionCompletionResult.EventReactionFailed("boom", allowManualRetry = false)
            val executor =
                testExecutor(
                    reactions,
                    scheduler,
                    recorder,
                    execute = { EventReactionExecutionResult.EventReactionFailed(RuntimeException("boom")) },
                    failureRetryHandler = { _, _ -> RetrySignal.DoNotRetry(failure) },
                )

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("doomed"))
                eventually { recorder.completions.isNotEmpty() && !rowExists(reactions, scheduler, "r-1") }
            }

            assertEquals(1, recorder.attempts.size)
            assertEquals(listOf(EventReactionId("r-1") to failure), recorder.completions.toList())
            assertTrue(scheduler.getScheduledExecutionsForTask("test-reactions", String::class.java).isEmpty())
        }
}

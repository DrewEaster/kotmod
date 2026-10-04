package io.kotmod.event.reaction.dbscheduler

import io.kotmod.AggregateId
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.EventId
import io.kotmod.EventMetadata
import io.kotmod.PendingEvent
import io.kotmod.event.reaction.EventReaction
import io.kotmod.event.reaction.EventReactionCompletionResult
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.RetrySignal
import io.kotmod.outbox.AggregateEventOutbox
import io.kotmod.postgres.PostgresDomainPersistenceBackend
import io.kotmod.postgres.PostgresDomainPollingBackend
import io.kotmod.postgres.PostgresOffsetManager
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.postgres.support.orderEventSerialization
import io.kotmod.support.OrderPlaced
import com.github.kagkarlsson.scheduler.task.TaskInstance
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

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

    @Test
    fun `reaction picked up before the executor subscribes is rescheduled and runs once it does`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer, unsubscribedRetryDelay = 300.milliseconds)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)
            val dispatchedAt = Instant.now()
            executor.dispatch(EventReactionId("r-1"), TestTrigger("early"))

            scheduler.start() // wrong order on purpose: no executor subscribed yet
            try {
                eventually {
                    val execution = scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get()
                    execution.executionTime.isAfter(dispatchedAt.plusMillis(250))
                }
                val execution = scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get()
                assertEquals(0, execution.consecutiveFailures)
                assertTrue(recorder.attempts.isEmpty())

                executor.start()
                eventually { recorder.completions.isNotEmpty() }
            } finally {
                scheduler.stop()
                executor.stop()
            }

            assertEquals(listOf(0), recorder.attempts.map { it.retryCount })
        }

    @Test
    fun `undecodable task data or an undeserializable trigger is retried by the failure handler without reaching the executor`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)

            // Garbage envelope, written directly with db-scheduler's own API.
            scheduler.schedule(TaskInstance("test-reactions", "garbage", "not json"), Instant.now())
            // Valid envelope, but TestTriggerSerializer rejects triggers starting with "poison".
            executor.dispatch(EventReactionId("poisoned"), TestTrigger("poison-pill"))

            running(scheduler, executor) {
                eventually {
                    listOf("garbage", "poisoned").all { id ->
                        scheduler.getScheduledExecution(reactions.task.instanceId(id)).get().consecutiveFailures >= 1
                    }
                }
            }

            assertTrue(recorder.attempts.isEmpty())
            assertTrue(recorder.completions.isEmpty())
        }

    @Test
    fun `createExecutionContext throwing goes to the failure handler without consuming a retry`() =
        runBlocking {
            val reactions = DbSchedulerEventReactions("test-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor =
                testExecutor(
                    reactions,
                    scheduler,
                    recorder,
                    createExecutionContext = { _, _ -> error("context unavailable") },
                )

            running(scheduler, executor) {
                executor.dispatch(EventReactionId("r-1"), TestTrigger("needs-context"))
                eventually { scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get().consecutiveFailures >= 1 }
            }

            val data = scheduler.getScheduledExecution(reactions.task.instanceId("r-1")).get().data as String
            assertTrue(data.contains("\"retryCount\":0"), "retry count should be untouched, was $data")
            assertTrue(recorder.attempts.isEmpty())
        }

    @Test
    fun `two executors on one scheduler each receive only their own reactions`() =
        runBlocking {
            val billing = DbSchedulerEventReactions("billing-reactions", TestTriggerSerializer)
            val notifications = DbSchedulerEventReactions("notification-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, billing.task, notifications.task)
            val billingRecorder = ReactionRecorder()
            val notificationRecorder = ReactionRecorder()
            val billingExecutor = testExecutor(billing, scheduler, billingRecorder)
            val notificationExecutor = testExecutor(notifications, scheduler, notificationRecorder)

            running(scheduler, billingExecutor, notificationExecutor) {
                billingExecutor.dispatch(EventReactionId("charge-e-1"), TestTrigger("charge"))
                notificationExecutor.dispatch(EventReactionId("confirm-e-1"), TestTrigger("confirm"))
                // Same id under a different task name is a different reaction.
                notificationExecutor.dispatch(EventReactionId("charge-e-1"), TestTrigger("charge-receipt"))
                eventually { billingRecorder.completions.size == 1 && notificationRecorder.completions.size == 2 }
            }

            assertEquals(listOf(TestTrigger("charge")), billingRecorder.attempts.map { it.trigger })
            assertEquals(
                setOf(TestTrigger("confirm"), TestTrigger("charge-receipt")),
                notificationRecorder.attempts.map { it.trigger }.toSet(),
            )
        }

    @Test
    fun `outbox delivers reactions end to end through db-scheduler and saves its offset`() =
        runBlocking {
            val persistence = PostgresDomainPersistenceBackend(jdbc, orderEventSerialization())
            val offsets = PostgresOffsetManager(jdbc)
            listOf("e-1", "e-2").forEachIndexed { index, eventId ->
                persistence.appendEvents(
                    listOf(
                        PendingEvent(
                            metadata =
                                EventMetadata(
                                    eventId = EventId(eventId),
                                    aggregateType = AggregateType("Order"),
                                    aggregateId = AggregateId("o-$index"),
                                    causationId = CommandId("cmd-$eventId"),
                                    correlationId = null,
                                    timestamp = kotlin.time.Instant.parse("2026-10-03T10:00:00Z"),
                                ),
                            event = OrderPlaced("widgets-$index"),
                        ),
                    ),
                )
            }

            val reactions = DbSchedulerEventReactions("order-reactions", TestTriggerSerializer)
            val scheduler = testScheduler(dataSource, reactions.task)
            val recorder = ReactionRecorder()
            val executor = testExecutor(reactions, scheduler, recorder)
            val outbox =
                AggregateEventOutbox(
                    backend = PostgresDomainPollingBackend(jdbc),
                    executor = executor,
                    eventToReactions = { event ->
                        listOf(
                            EventReaction(
                                id = EventReactionId("charge-${event.metadata.eventId.value}"),
                                trigger = TestTrigger("charge-${event.metadata.aggregateId.value}"),
                            ),
                        )
                    },
                    getPosition = { offsets.getPosition("order-outbox") },
                    savePosition = { offsets.savePosition("order-outbox", it) },
                    isLeader = { true },
                    pollInterval = 50.milliseconds,
                )

            running(scheduler, executor) {
                outbox.start()
                try {
                    eventually { recorder.completions.size == 2 }
                } finally {
                    outbox.stop()
                }
            }

            assertEquals(
                setOf(EventReactionId("charge-e-1"), EventReactionId("charge-e-2")),
                recorder.completions.map { it.first }.toSet(),
            )
            assertEquals(
                setOf(TestTrigger("charge-o-0"), TestTrigger("charge-o-1")),
                recorder.attempts.map { it.trigger }.toSet(),
            )
            assertEquals(2L, offsets.getPosition("order-outbox").globalOffset)
            assertTrue(scheduler.getScheduledExecutionsForTask("order-reactions", String::class.java).isEmpty())
        }
}

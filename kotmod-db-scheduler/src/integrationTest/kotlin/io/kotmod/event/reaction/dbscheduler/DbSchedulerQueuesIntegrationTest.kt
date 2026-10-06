package io.kotmod.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.task.TaskInstance
import com.github.kagkarlsson.scheduler.task.TaskInstanceId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class DbSchedulerQueuesIntegrationTest : IntegrationTest() {
    private fun queues(unsubscribedRetryDelay: Duration = 5.seconds) = DbSchedulerQueues(jdbc, unsubscribedRetryDelay = unsubscribedRetryDelay)

    private fun DbSchedulerQueues.queue(name: String = "test-reactions") = channel(name, TestTriggerSerializer, ordered = false)

    /** Builds the scheduler for [queues] (after all their queues exist) and binds them to it. */
    private fun scheduler(queues: DbSchedulerQueues): Scheduler = testScheduler(dataSource, *queues.tasks.toTypedArray()).also { queues.bind(it) }

    private fun Scheduler.row(
        id: String,
        task: String = "test-reactions",
    ) = getScheduledExecution(TaskInstanceId.of(task, id))

    @Test
    fun `a published reaction runs once, finishes and its row is removed`() =
        runBlocking {
            val queues = queues()
            val consumer = TestConsumer(queues.queue())
            val scheduler = scheduler(queues)

            running(scheduler, consumer) {
                consumer.dispatch(EventReactionId("r-1"), TestTrigger("hello"))
                eventually { consumer.finished.isNotEmpty() && !scheduler.row("r-1").isPresent }
            }

            assertEquals(listOf(TestTrigger("hello")), consumer.attempts.map { it.trigger })
            assertEquals(0, consumer.attempts.single().retryCount)
            assertEquals(listOf(EventReactionId("r-1") to false), consumer.finished.toList())
        }

    @Test
    fun `a duplicate publish while pending results in one row and one run`() =
        runBlocking {
            val queues = queues()
            val consumer = TestConsumer(queues.queue())
            val scheduler = scheduler(queues)

            // Scheduler not started yet, so the first publish is still pending when the second arrives.
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("first"))
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("second"))
            assertEquals(1, scheduler.getScheduledExecutionsForTask("test-reactions", String::class.java).size)

            running(scheduler, consumer) {
                eventually { consumer.finished.isNotEmpty() }
                delay(500) // give a hypothetical duplicate time to show up
            }

            assertEquals(listOf(TestTrigger("first")), consumer.attempts.map { it.trigger })
        }

    @Test
    fun `retries carry an increasing retry count and fresh execution ids until success`() =
        runBlocking {
            val queues = queues()
            val consumer =
                TestConsumer(queues.queue()) { attempt ->
                    if (attempt.retryCount < 2) ReactionOutcome.Retry(100.milliseconds) else ReactionOutcome.Finished(gaveUp = false)
                }
            val scheduler = scheduler(queues)

            running(scheduler, consumer) {
                consumer.dispatch(EventReactionId("r-1"), TestTrigger("flaky"))
                eventually { consumer.finished.isNotEmpty() && !scheduler.row("r-1").isPresent }
            }

            assertEquals(listOf(0, 1, 2), consumer.attempts.map { it.retryCount })
            assertEquals(3, consumer.attempts.map { it.executionId }.toSet().size)
        }

    @Test
    fun `a reaction that finishes as given up is removed`() =
        runBlocking {
            val queues = queues()
            val consumer = TestConsumer(queues.queue()) { ReactionOutcome.Finished(gaveUp = true) }
            val scheduler = scheduler(queues)

            running(scheduler, consumer) {
                consumer.dispatch(EventReactionId("r-1"), TestTrigger("doomed"))
                eventually { consumer.finished.isNotEmpty() && !scheduler.row("r-1").isPresent }
            }

            assertEquals(listOf(EventReactionId("r-1") to true), consumer.finished.toList())
            assertTrue(scheduler.getScheduledExecutionsForTask("test-reactions", String::class.java).isEmpty())
        }

    @Test
    fun `a reaction picked up before anything subscribes is rescheduled and runs once something does`() =
        runBlocking {
            val queues = queues(unsubscribedRetryDelay = 300.milliseconds)
            val consumer = TestConsumer(queues.queue())
            val scheduler = scheduler(queues)
            val publishedAt = java.time.Instant.now()
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("early"))

            scheduler.start() // wrong order on purpose: nothing subscribed yet
            try {
                eventually { scheduler.row("r-1").get().executionTime.isAfter(publishedAt.plusMillis(250)) }
                assertEquals(0, scheduler.row("r-1").get().consecutiveFailures)
                assertTrue(consumer.attempts.isEmpty())

                consumer.start()
                eventually { consumer.finished.isNotEmpty() }
            } finally {
                scheduler.stop()
                consumer.stop()
            }

            assertEquals(listOf(0), consumer.attempts.map { it.retryCount })
        }

    @Test
    fun `undecodable task data or an undeserializable trigger is retried by the failure handler without reaching the consumer`() =
        runBlocking {
            val queues = queues()
            val consumer = TestConsumer(queues.queue())
            val scheduler = scheduler(queues)

            // Garbage task data, written directly with db-scheduler's own API.
            scheduler.schedule(TaskInstance("test-reactions", "garbage", "not json"), java.time.Instant.now())
            // Valid task data, but TestTriggerSerializer rejects triggers starting with "poison".
            consumer.dispatch(EventReactionId("poisoned"), TestTrigger("poison-pill"))

            running(scheduler, consumer) {
                eventually { listOf("garbage", "poisoned").all { id -> scheduler.row(id).get().consecutiveFailures >= 1 } }
            }

            assertTrue(consumer.attempts.isEmpty())
        }

    @Test
    fun `a consumer that throws hands the row to the failure handler without counting a retry`() =
        runBlocking {
            val queues = queues()
            val consumer = TestConsumer(queues.queue()) { error("context unavailable") }
            val scheduler = scheduler(queues)

            running(scheduler, consumer) {
                consumer.dispatch(EventReactionId("r-1"), TestTrigger("needs-context"))
                eventually { scheduler.row("r-1").get().consecutiveFailures >= 1 }
            }

            val data = scheduler.row("r-1").get().data as String
            assertTrue(data.contains("\"retryCount\":0"), "retry count should be untouched, was $data")
            assertTrue(consumer.finished.isEmpty())
        }

    @Test
    fun `two queues on one scheduler each receive only their own reactions`() =
        runBlocking {
            val queues = queues()
            val billing = TestConsumer(queues.queue("billing"))
            val notifications = TestConsumer(queues.queue("notifications"))
            val scheduler = scheduler(queues)

            running(scheduler, billing, notifications) {
                billing.dispatch(EventReactionId("charge-e-1"), TestTrigger("charge"))
                notifications.dispatch(EventReactionId("confirm-e-1"), TestTrigger("confirm"))
                // The same id in a different queue is a different reaction.
                notifications.dispatch(EventReactionId("charge-e-1"), TestTrigger("charge-receipt"))
                eventually { billing.finished.size == 1 && notifications.finished.size == 2 }
            }

            assertEquals(listOf(TestTrigger("charge")), billing.attempts.map { it.trigger })
            assertEquals(setOf(TestTrigger("confirm"), TestTrigger("charge-receipt")), notifications.attempts.map { it.trigger }.toSet())
        }

    @Test
    fun `a delayed reaction does not run before notBefore, then runs once with retry count 0`() =
        runBlocking {
            val queues = queues()
            val consumer = TestConsumer(queues.queue())
            val scheduler = scheduler(queues)
            val notBefore = Clock.System.now() + 3.seconds

            running(scheduler, consumer) {
                consumer.dispatch(EventReactionId("later"), TestTrigger("first"), notBefore = notBefore)
                // A second publish of the pending reaction, with a different time, is ignored.
                consumer.dispatch(EventReactionId("later"), TestTrigger("second"), notBefore = Clock.System.now())
                delay(1_500)
                assertTrue(consumer.attempts.isEmpty(), "ran before notBefore")
                eventually { consumer.finished.isNotEmpty() }
            }

            assertEquals(listOf(TestTrigger("first")), consumer.attempts.map { it.trigger })
            assertEquals(0, consumer.attempts.single().retryCount)
            assertTrue(Clock.System.now() >= notBefore)
        }
}

package io.kotmod.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.OnGiveUp
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class OrderedReactionsIntegrationTest : IntegrationTest() {
    private val queues = DbSchedulerQueues(jdbc, orderedRecheckDelay = 200.milliseconds)

    private fun ordering(
        key: String,
        sequence: Long,
        onGiveUp: OnGiveUp = OnGiveUp.ContinueWithNext,
    ) = DispatchOrdering(key, sequence, 0, onGiveUp)

    private class Log {
        val events = CopyOnWriteArrayList<String>() // "start:<name>" / "end:<name>"
    }

    /** A consumer of ordered queue [name]: [succeeds] decides each attempt; a failure retries, or gives up with [giveUp]. */
    private fun consumer(
        log: Log,
        name: String = "ordered",
        giveUp: Boolean = false,
        succeeds: suspend (TestTrigger, Int) -> Boolean = { _, _ ->
            delay(50)
            true
        },
    ) = TestConsumer(queues.channel(name, TestTriggerSerializer, ordered = true)) { attempt ->
        log.events += "start:${attempt.trigger.name}"
        val ok =
            try {
                succeeds(attempt.trigger, attempt.retryCount)
            } finally {
                log.events += "end:${attempt.trigger.name}"
            }
        when {
            ok -> ReactionOutcome.Finished(gaveUp = false)
            giveUp -> ReactionOutcome.Finished(gaveUp = true)
            else -> ReactionOutcome.Retry(100.milliseconds)
        }
    }

    private fun scheduler(): Scheduler = testScheduler(dataSource, *queues.tasks.toTypedArray()).also { queues.bind(it) }

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
            val log = Log()
            val consumer = consumer(log)
            val scheduler = scheduler()
            (1..8L).forEach { consumer.dispatch(EventReactionId("r-$it"), TestTrigger("A$it"), ordering("Order/a", it)) }

            running(scheduler, consumer) { eventually(20.seconds) { log.events.count { it.startsWith("end:") } == 8 } }

            assertTrue(neverOverlap(log), log.events.toString())
            assertEquals((1..8).map { "A$it" }, log.events.filter { it.startsWith("start:") }.map { it.removePrefix("start:") })
        }

    @Test
    fun `different aggregates run in parallel`() =
        runBlocking {
            val log = Log()
            val consumer =
                consumer(log) { _, _ ->
                    delay(500)
                    true
                }
            val scheduler = scheduler()
            consumer.dispatch(EventReactionId("r-a"), TestTrigger("A1"), ordering("Order/a", 1))
            consumer.dispatch(EventReactionId("r-b"), TestTrigger("B1"), ordering("Order/b", 1))

            running(scheduler, consumer) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `a retrying head holds back later reactions`() =
        runBlocking {
            val log = Log()
            val consumer = consumer(log) { trigger, retryCount -> !(trigger.name == "A1" && retryCount < 2) }
            val scheduler = scheduler()
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))
            consumer.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2))

            running(scheduler, consumer) { eventually(15.seconds) { log.events.contains("end:A2") } }

            assertEquals(listOf("start:A1", "start:A1", "start:A1", "start:A2"), log.events.filter { it.startsWith("start:") })
        }

    @Test
    fun `ContinueWithNext moves on after a give-up`() =
        runBlocking {
            val log = Log()
            val consumer = consumer(log, giveUp = true) { trigger, _ -> trigger.name != "A1" }
            val scheduler = scheduler()
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.ContinueWithNext))
            consumer.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.ContinueWithNext))

            running(scheduler, consumer) { eventually(10.seconds) { log.events.contains("end:A2") } }
        }

    @Test
    fun `BlockAggregate parks the aggregate until retried, and is never nudged`() =
        runBlocking {
            val log = Log()
            val failures = AtomicInteger()
            val consumer = consumer(log, giveUp = true) { trigger, _ -> !(trigger.name == "A1" && failures.getAndIncrement() == 0) }
            val scheduler = scheduler()
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.BlockAggregate))
            consumer.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.BlockAggregate))

            running(scheduler, consumer) {
                eventually { queues.blockedReactions(scheduler, "ordered").isNotEmpty() }
                delay(1000)
                assertTrue("start:A2" !in log.events, "A2 must wait behind the blocked A1")
                assertEquals(listOf(EventReactionId("r-1")), queues.blockedReactions(scheduler, "ordered").map { it.reactionId })

                queues.retryBlocked(scheduler, "ordered", EventReactionId("r-1"))
                eventually(10.seconds) { log.events.contains("end:A2") }
            }
            assertEquals(listOf("start:A1", "start:A1", "start:A2"), log.events.filter { it.startsWith("start:") })
        }

    @Test
    fun `skipBlocked releases the aggregate without running the blocked reaction again`() =
        runBlocking {
            val log = Log()
            val consumer = consumer(log, giveUp = true) { trigger, _ -> trigger.name != "A1" }
            val scheduler = scheduler()
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1, OnGiveUp.BlockAggregate))
            consumer.dispatch(EventReactionId("r-2"), TestTrigger("A2"), ordering("Order/a", 2, OnGiveUp.BlockAggregate))

            running(scheduler, consumer) {
                eventually { queues.blockedReactions(scheduler, "ordered").isNotEmpty() }
                queues.skipBlocked(scheduler, "ordered", EventReactionId("r-1"))
                eventually(10.seconds) { log.events.contains("end:A2") }
            }
            assertEquals(1, log.events.count { it == "start:A1" })
        }

    @Test
    fun `a duplicate ordered publish is absorbed`() =
        runBlocking {
            val log = Log()
            val consumer = consumer(log)
            val scheduler = scheduler()
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))
            consumer.dispatch(EventReactionId("r-1"), TestTrigger("A1"), ordering("Order/a", 1))

            running(scheduler, consumer) {
                eventually { log.events.contains("end:A1") }
                delay(500)
            }
            assertEquals(1, log.events.count { it == "start:A1" })
        }

    @Test
    fun `aggregate ids with separators never share ordering`() =
        runBlocking {
            val log = Log()
            val consumer =
                consumer(log) { _, _ ->
                    delay(400)
                    true
                }
            val scheduler = scheduler()
            consumer.dispatch(EventReactionId("r-a"), TestTrigger("X"), ordering("Order/a", 2))
            consumer.dispatch(EventReactionId("r-b"), TestTrigger("Y"), ordering("Order/a#0000000000000000001", 1))

            running(scheduler, consumer) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            // Different aggregates: both start before either ends.
            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }

    @Test
    fun `two queues handling the same aggregate do not wait on each other`() =
        runBlocking {
            val log = Log()
            val slow: suspend (TestTrigger, Int) -> Boolean = { _, _ ->
                delay(500)
                true
            }
            val first = consumer(log, name = "first", succeeds = slow)
            val second = consumer(log, name = "second", succeeds = slow)
            val scheduler = scheduler()
            first.dispatch(EventReactionId("r-1"), TestTrigger("first"), ordering("Order/a", 1))
            second.dispatch(EventReactionId("r-1"), TestTrigger("second"), ordering("Order/a", 1))

            running(scheduler, first, second) { eventually(10.seconds) { log.events.count { it.startsWith("end:") } == 2 } }

            assertEquals(listOf("start", "start", "end", "end"), log.events.map { it.substringBefore(':') })
        }
}

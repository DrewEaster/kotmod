package io.kotmod.scheduling.dbscheduler

import io.kotmod.postgres.support.IntegrationTest
import io.kotmod.postgres.support.eventually
import io.kotmod.scheduling.TaskOutcome
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** What the db-scheduler adapter does beyond the [io.kotmod.scheduling.TaskSchedulerContract]. */
class DbSchedulerTaskSchedulerIntegrationTest : IntegrationTest() {
    @Test
    fun `a task executed while nothing is subscribed is kept, and delivered once with its payload when something subscribes`() =
        runBlocking<Unit> {
            val backend = DbSchedulerTaskScheduler(unsubscribedRetryDelay = 300.milliseconds)
            val queue = backend.queue("unsubscribed")
            val scheduler = testScheduler(dataSource, backend.tasks).also { backend.bind(it) }
            queue.schedule("t-1", "payload-1", Clock.System.now())
            scheduler.start()
            try {
                // db-scheduler ran it at least once with nothing subscribed, and it is still pending.
                eventually {
                    scheduler.getScheduledExecutionsForTask("unsubscribed", String::class.java).any {
                        it.taskInstance.id == "t-1" && it.lastSuccess != null
                    }
                }
                val seen = CopyOnWriteArrayList<Pair<String, String>>()
                queue.subscribe { name, payload ->
                    seen += name to payload
                    TaskOutcome.Done
                }
                eventually(5.seconds) { seen.isNotEmpty() }
                delay(1.seconds)
                assertEquals(listOf("t-1" to "payload-1"), seen.toList())
                assertEquals(emptyList(), scheduler.instanceIds("unsubscribed"))
            } finally {
                scheduler.stop()
            }
        }
}

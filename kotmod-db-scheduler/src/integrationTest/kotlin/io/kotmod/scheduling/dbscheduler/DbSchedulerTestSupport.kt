package io.kotmod.scheduling.dbscheduler

import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.task.Task
import io.kotmod.postgres.support.IntegrationTest
import javax.sql.DataSource

/** The shared test database, for tests that can't extend [IntegrationTest] (it keeps its data source protected). */
class SharedDatabase : IntegrationTest() {
    fun dataSourceForTests(): DataSource = dataSource
}

/** A db-scheduler `Scheduler` for tests: polls often, runs work scheduled for now at once, stops quickly. */
fun testScheduler(
    dataSource: DataSource,
    tasks: List<Task<*>>,
): Scheduler =
    Scheduler
        .create(dataSource, tasks)
        .pollingInterval(java.time.Duration.ofMillis(100))
        .enableImmediateExecution()
        .threads(4)
        .shutdownMaxWait(java.time.Duration.ofSeconds(2))
        .build()

/** The names of the pending instances of [task] in `scheduled_tasks`. */
fun Scheduler.instanceIds(task: String): List<String> = getScheduledExecutionsForTask(task, String::class.java).map { it.taskInstance.id }

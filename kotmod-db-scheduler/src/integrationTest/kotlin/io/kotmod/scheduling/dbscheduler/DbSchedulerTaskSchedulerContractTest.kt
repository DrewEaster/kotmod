package io.kotmod.scheduling.dbscheduler

import com.github.kagkarlsson.scheduler.Scheduler
import io.kotmod.scheduling.TaskScheduler
import io.kotmod.scheduling.TaskSchedulerContract
import kotlin.test.BeforeTest

class DbSchedulerTaskSchedulerContractTest : TaskSchedulerContract() {
    private val database = SharedDatabase()
    private val backend = DbSchedulerTaskScheduler().also { it.queue(QUEUE_A); it.queue(QUEUE_B) }

    // Built (unstarted) and bound on first use, since the contract schedules before it starts delivery.
    private val built: Scheduler by lazy { testScheduler(database.dataSourceForTests(), backend.tasks).also { backend.bind(it) } }

    @BeforeTest
    fun truncate() = database.truncateDddTables()

    override fun scheduler(): TaskScheduler = backend.also { built }

    override fun start() = built.start()

    override fun stop() = built.stop()
}

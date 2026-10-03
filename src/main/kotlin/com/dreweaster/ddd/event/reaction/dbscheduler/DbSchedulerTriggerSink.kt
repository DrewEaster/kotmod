package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSerializer
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSink
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.TaskInstance
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.time.Instant

internal class DbSchedulerTriggerSink<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    private val client: SchedulerClient,
    private val clock: () -> Instant = Instant::now,
) : EventReactionTriggerSink<T> {
    private val log = LoggerFactory.getLogger(DbSchedulerTriggerSink::class.java)

    override suspend fun publish(
        id: EventReactionId,
        trigger: T,
    ) {
        val taskData = ReactionTaskData(trigger = triggerSerializer.serialize(trigger), retryCount = 0).encode()
        val scheduled =
            withContext(Dispatchers.IO) {
                client.scheduleIfNotExists(TaskInstance(taskName, id.value, taskData), clock())
            }
        if (!scheduled) {
            log.debug("Event reaction {} already scheduled for task {}; ignoring duplicate dispatch", id.value, taskName)
        }
    }
}

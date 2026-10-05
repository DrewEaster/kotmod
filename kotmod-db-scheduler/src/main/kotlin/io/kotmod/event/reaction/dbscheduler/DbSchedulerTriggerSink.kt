package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import com.github.kagkarlsson.scheduler.SchedulerClient
import com.github.kagkarlsson.scheduler.task.TaskInstance
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.toJavaInstant

/**
 * Schedules each dispatched reaction as a db-scheduler task instance, unless one with the same id is already pending.
 * A delayed reaction is scheduled at its `notBefore`.
 * Ordered reactions get an instance id that sorts by aggregate, sequence and ordinal (see [orderedInstanceId]).
 */
internal class DbSchedulerTriggerSink<T : EventReactionTrigger>(
    private val taskName: String,
    private val triggerSerializer: EventReactionTriggerSerializer<T>,
    private val client: SchedulerClient,
    private val clock: () -> Instant = Instant::now,
    override val supportsOrdering: Boolean = false,
) : EventReactionTriggerSink<T> {
    private val log = LoggerFactory.getLogger(DbSchedulerTriggerSink::class.java)

    override suspend fun publish(
        id: EventReactionId,
        trigger: T,
        ordering: DispatchOrdering?,
        notBefore: kotlin.time.Instant?,
    ) {
        require(ordering == null || supportsOrdering) {
            "Ordered reactions need DbSchedulerEventReactions to be created with a JdbcContext"
        }
        val stamp = ordering?.let { OrderingStamp(it.key, it.sequence, it.ordinal, it.onGiveUp.name, id.value) }
        val instanceId = stamp?.let { orderedInstanceId(it.key, it.sequence, it.ordinal, id.value) } ?: id.value
        val taskData = ReactionTaskData(trigger = triggerSerializer.serialize(trigger), retryCount = 0, ordering = stamp, notBeforeEpochMillis = notBefore?.toEpochMilliseconds()).encode()
        val scheduled =
            withContext(Dispatchers.IO) {
                client.scheduleIfNotExists(TaskInstance(taskName, instanceId, taskData), notBefore?.toJavaInstant() ?: clock())
            }
        if (!scheduled) {
            log.debug("Event reaction {} already scheduled for task {}; ignoring duplicate dispatch", instanceId, taskName)
        }
    }
}

package com.dreweaster.ddd.event.reaction.dbscheduler

import com.dreweaster.ddd.event.reaction.Cancellable
import com.dreweaster.ddd.event.reaction.EventReactionExecutionId
import com.dreweaster.ddd.event.reaction.EventReactionId
import com.dreweaster.ddd.event.reaction.EventReactionTrigger
import com.dreweaster.ddd.event.reaction.EventReactionTriggerSource
import com.dreweaster.ddd.event.reaction.RetryCount
import com.dreweaster.ddd.event.reaction.RetrySignal

internal typealias ReactionHandler<T> = suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> RetrySignal.Retry?

/** Holds the handler of the one executor subscribed to a db-scheduler task; the task reads it on every execution. */
internal class DbSchedulerTriggerSource<T : EventReactionTrigger> : EventReactionTriggerSource<T> {
    @Volatile
    var handler: ReactionHandler<T>? = null
        private set

    override fun subscribe(block: ReactionHandler<T>): Cancellable {
        synchronized(this) {
            check(handler == null) { "An event reaction executor is already subscribed to this source" }
            handler = block
        }
        return object : Cancellable {
            override fun cancel() {
                synchronized(this@DbSchedulerTriggerSource) {
                    if (handler === block) handler = null
                }
            }
        }
    }
}

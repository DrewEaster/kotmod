package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.EventReactionExecutionId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.RetryCount
import io.kotmod.event.reaction.ReactionOutcome

internal typealias ReactionHandler<T> = suspend (EventReactionId, EventReactionExecutionId, T, RetryCount) -> ReactionOutcome

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

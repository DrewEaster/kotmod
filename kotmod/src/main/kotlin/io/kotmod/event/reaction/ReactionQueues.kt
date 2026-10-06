package io.kotmod.event.reaction

/**
 * Provides the queues kotmod runs reactions on: one per event policy (named after it) and one per process manager channel.
 * Names are stable across restarts. `kotmod-db-scheduler` provides `DbSchedulerQueues`; any queue that implements an
 * [EventReactionTriggerSink] and an [EventReactionTriggerSource] works.
 */
interface ReactionQueues {
    /**
     * Returns the queue named [name], whose items are stored with [triggerSerializer]. An [ordered] queue's sink must
     * support ordering. kotmod asks for each name once.
     */
    fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ReactionChannel<T>
}

/** One queue: where reactions are published ([sink]), and where they are delivered from ([source]). */
class ReactionChannel<T : EventReactionTrigger>(
    val sink: EventReactionTriggerSink<T>,
    val source: EventReactionTriggerSource<T>,
)

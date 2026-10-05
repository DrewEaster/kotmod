package io.kotmod.process

import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionExecutionId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.event.reaction.RetryCount
import kotlin.time.Instant

/**
 * In-memory queues for tests: a publish of an id that is still pending is ignored, as real queues do. [deliver] runs
 * every pending reaction of a channel once and removes the ones that finish.
 */
class ManualQueues(
    private val supportsOrdering: Boolean = true,
) : ProcessManagerQueues {
    data class Published(
        val channel: String,
        val id: EventReactionId,
        val trigger: EventReactionTrigger,
        val ordering: DispatchOrdering?,
        val notBefore: Instant?,
    )

    val published = mutableListOf<Published>()
    private val pending = linkedMapOf<Pair<String, EventReactionId>, Published>()
    private val handlers =
        mutableMapOf<String, suspend (EventReactionId, EventReactionExecutionId, EventReactionTrigger, RetryCount, Instant?) -> ReactionOutcome>()
    val channels = mutableListOf<Pair<String, Boolean>>()

    override fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ProcessChannel<T> {
        channels += name to ordered
        val sink =
            object : EventReactionTriggerSink<T> {
                override val supportsOrdering = this@ManualQueues.supportsOrdering

                override suspend fun publish(
                    id: EventReactionId,
                    trigger: T,
                    ordering: DispatchOrdering?,
                    notBefore: Instant?,
                ) {
                    val entry = Published(name, id, trigger, ordering, notBefore)
                    published += entry
                    pending.putIfAbsent(name to id, entry)
                }
            }
        val source =
            object : EventReactionTriggerSource<T> {
                override fun subscribe(
                    block: suspend (EventReactionId, EventReactionExecutionId, T, RetryCount, Instant?) -> ReactionOutcome,
                ): Cancellable {
                    handlers[name] = { id, executionId, trigger, retryCount, notBefore ->
                        @Suppress("UNCHECKED_CAST")
                        block(id, executionId, trigger as T, retryCount, notBefore)
                    }
                    return object : Cancellable {
                        override fun cancel() {
                            handlers.remove(name)
                        }
                    }
                }
            }
        return ProcessChannel(sink, source)
    }

    fun pending(channel: String): List<Published> = pending.values.filter { it.channel == channel }

    /** Delivers every pending reaction of [channel] once, in publish order; finished ones are removed. */
    suspend fun deliver(channel: String): List<ReactionOutcome> =
        pending(channel).map { entry ->
            val handler = checkNotNull(handlers[channel]) { "no executor subscribed to $channel" }
            val outcome = handler(entry.id, EventReactionExecutionId("x-${entry.id.value}"), entry.trigger, 0, entry.notBefore)
            if (outcome is ReactionOutcome.Finished) pending.remove(channel to entry.id)
            outcome
        }

    /** Puts a finished reaction back, as a queue redelivering it after a crash would. */
    fun redeliver(entry: Published) {
        pending[entry.channel to entry.id] = entry
    }
}

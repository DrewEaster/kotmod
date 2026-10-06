package io.kotmod.process

import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionExecutionId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.ReactionChannel
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.event.reaction.ReactionQueues
import io.kotmod.event.reaction.RetryCount
import kotlin.time.Instant

/**
 * In-memory queues for tests: a publish of an id that is still pending is ignored, as real queues do. [deliver] runs
 * every pending reaction of a channel once and removes the ones that finish.
 *
 * As in a real queue, a reaction's retry count goes up by one per [ReactionOutcome.Retry], and starts again at 0 when
 * it is redelivered after finishing. Unlike a real queue: a [ReactionOutcome.Wait] or retry delay is ignored (the
 * reaction just stays pending for the next [deliver]); ordering stamps are enforced only with [enforceOrdering] (a
 * stamped reaction is then skipped while an earlier one of its key is pending in its channel); and [deliver] works on a
 * snapshot, so reactions published while it runs wait for the next call.
 */
class ManualQueues(
    private val supportsOrdering: Boolean = true,
    private val enforceOrdering: Boolean = false,
) : ReactionQueues {
    data class Published(
        val channel: String,
        val id: EventReactionId,
        val trigger: EventReactionTrigger,
        val ordering: DispatchOrdering?,
        val notBefore: Instant?,
    )

    val published = mutableListOf<Published>()
    private val pending = linkedMapOf<Pair<String, EventReactionId>, Published>()
    private val retries = mutableMapOf<Pair<String, EventReactionId>, Int>()
    private val handlers =
        mutableMapOf<String, suspend (EventReactionId, EventReactionExecutionId, EventReactionTrigger, RetryCount, Instant?) -> ReactionOutcome>()
    val channels = mutableListOf<Pair<String, Boolean>>()

    /** How many times an executor subscribed to any channel. */
    var subscriptions = 0
        private set

    override fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ReactionChannel<T> {
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
                    subscriptions++
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
        return ReactionChannel(sink, source)
    }

    fun pending(channel: String): List<Published> = pending.values.filter { it.channel == channel }

    /** How many times reaction [id] of [channel] has been retried since it was published or redelivered. */
    fun retries(
        channel: String,
        id: EventReactionId,
    ): Int = retries[channel to id] ?: 0

    /**
     * Delivers every pending reaction of [channel] once, in publish order, skipping any that must wait for an earlier one
     * of its key (with [enforceOrdering]); finished ones are removed.
     */
    suspend fun deliver(channel: String): List<ReactionOutcome> =
        pending(channel).mapNotNull { entry ->
            if (enforceOrdering && waitsForEarlier(entry)) return@mapNotNull null
            val key = channel to entry.id
            val handler = checkNotNull(handlers[channel]) { "no executor subscribed to $channel" }
            val outcome = handler(entry.id, EventReactionExecutionId("x-${entry.id.value}"), entry.trigger, retries[key] ?: 0, entry.notBefore)
            when (outcome) {
                is ReactionOutcome.Finished -> {
                    pending.remove(key)
                    retries.remove(key)
                }
                is ReactionOutcome.Retry -> retries[key] = (retries[key] ?: 0) + 1
                is ReactionOutcome.Wait -> Unit
            }
            outcome
        }

    /** Puts a finished reaction back, as a queue redelivering it after a crash would. */
    fun redeliver(entry: Published) {
        pending[entry.channel to entry.id] = entry
    }

    private fun waitsForEarlier(entry: Published): Boolean {
        val stamp = entry.ordering ?: return false
        return pending.values.any { other ->
            val otherStamp = other.ordering
            other.channel == entry.channel &&
                other.id != entry.id &&
                otherStamp != null &&
                otherStamp.key == stamp.key &&
                compareValuesBy(other, entry, { it.ordering!!.sequence }, { it.ordering!!.ordinal }, { it.id.value }) < 0
        }
    }
}

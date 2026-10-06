package io.kotmod.reaction

import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.ReactionOrdering
import io.kotmod.event.reaction.ReactionOutcome
import io.kotmod.event.reaction.ReactionQueues
import io.kotmod.process.JsonTriggerSerializer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Instant

/** What a use case's queue holds. kotmod's own envelope around the app's triggers; the queue only stores it. */
@Serializable
internal sealed interface UseCaseItem : EventReactionTrigger

/** One of the use case's triggers, as JSON written with its `triggers` serializer. */
@Serializable
@SerialName("trigger")
internal data class TriggerItem(
    val trigger: String,
    @Transient override val timeout: Duration? = null,
) : UseCaseItem

private val log = LoggerFactory.getLogger("io.kotmod.reaction.UseCaseRuntime")

/**
 * Runs one use case on its own queue, named after it and ordered when the use case is: publishes its triggers, and
 * handles each delivered trigger with the use case's timeout and failure policy.
 */
internal class UseCaseRuntime<T : Any>(
    val useCase: Reactions<T>,
    queues: ReactionQueues,
    private val clock: () -> Instant = { Clock.System.now() },
) {
    private val ordered = useCase.ordering is ReactionOrdering.PerAggregate
    private val channel = queues.channel(useCase.name, JsonTriggerSerializer(UseCaseItem.serializer()), ordered)
    private val backoff = BackoffStrategy()

    @Volatile
    private var subscription: Cancellable? = null

    init {
        require(!ordered || channel.sink.supportsOrdering) { "Use case ${useCase.name} is ordered, so its queue must support ordering" }
    }

    /** Starts handling the queue's deliveries. Does nothing if already started. */
    fun start() {
        if (subscription != null) return
        subscription = channel.source.subscribe { id, _, item, attempt, notBefore -> deliver(id, item, attempt, notBefore) }
    }

    /** Stops handling deliveries. */
    fun stop() {
        subscription?.cancel()
        subscription = null
    }

    /** Queues [trigger] as reaction [id]. */
    suspend fun publish(
        id: EventReactionId,
        trigger: T,
        ordering: DispatchOrdering?,
        notBefore: Instant?,
    ) = channel.sink.publish(id, TriggerItem(encode(trigger)), ordering, notBefore)

    private fun encode(trigger: T): String = Json.encodeToString(useCase.triggers, trigger)

    private suspend fun deliver(
        id: EventReactionId,
        item: UseCaseItem,
        attempt: Int,
        notBefore: Instant?,
    ): ReactionOutcome {
        val now = clock()
        if (notBefore != null && now < notBefore) return ReactionOutcome.Wait(notBefore - now)
        return when (item) {
            // A trigger that can't be decoded throws here, back to the queue, which retries it until a fix is deployed.
            is TriggerItem -> runTrigger(id, Json.decodeFromString(useCase.triggers, item.trigger), attempt)
        }
    }

    private suspend fun runTrigger(
        id: EventReactionId,
        trigger: T,
        attempt: Int,
    ): ReactionOutcome {
        // handle's exception is caught inside withTimeout and carried out as a value, so only a genuine timeout or
        // cancellation crosses the timeout boundary (which keeps the app's exception from being copied by stack-trace recovery).
        val error: Throwable? =
            try {
                withTimeout(useCase.timeout) {
                    try {
                        useCase.handle(trigger, ReactionContext(id.value, attempt))
                        null
                    } catch (e: CancellationException) {
                        // Not a failure: the attempt was interrupted (e.g. the scheduler is stopping); the queue redelivers it.
                        throw e
                    } catch (e: Throwable) {
                        e
                    }
                }
            } catch (e: TimeoutCancellationException) {
                ReactionTimeoutException(useCase.timeout)
            }
        if (error == null) return finish(id, trigger, ReactionResult.Completed, attempt)
        log.error("Reaction {} of use case {} failed [attempt={}]", id.value, useCase.name, attempt, error)
        val decision =
            try {
                useCase.onFailure(trigger, attempt, error)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                log.error("onFailure of use case {} threw for reaction {}; retrying the reaction", useCase.name, id.value, e)
                return ReactionOutcome.Retry(backoff.calculateBackoff(attempt))
            }
        return when (decision) {
            is Retry -> ReactionOutcome.Retry(decision.delay)
            GiveUp -> {
                log.warn("Use case {} gave up on reaction {} [attempt={}]", useCase.name, id.value, attempt)
                finish(id, trigger, ReactionResult.GaveUp(error), attempt)
            }
        }
    }

    private suspend fun finish(
        id: EventReactionId,
        trigger: T,
        result: ReactionResult,
        attempt: Int,
    ): ReactionOutcome =
        try {
            useCase.onCompletion(trigger, result)
            ReactionOutcome.Finished(gaveUp = result is ReactionResult.GaveUp)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            log.error("onCompletion of use case {} threw for reaction {}; retrying the reaction", useCase.name, id.value, e)
            ReactionOutcome.Retry(backoff.calculateBackoff(attempt))
        }
}

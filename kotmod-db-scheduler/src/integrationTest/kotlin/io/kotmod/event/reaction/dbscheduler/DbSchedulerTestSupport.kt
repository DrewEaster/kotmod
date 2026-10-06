package io.kotmod.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.Scheduler
import com.github.kagkarlsson.scheduler.task.Task
import io.kotmod.event.reaction.Cancellable
import io.kotmod.event.reaction.DispatchOrdering
import io.kotmod.event.reaction.EventReactionExecutionId
import io.kotmod.event.reaction.EventReactionId
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.ReactionChannel
import io.kotmod.event.reaction.ReactionOutcome
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Instant

data class TestTrigger(
    val name: String,
    override val timeout: Duration? = null,
) : EventReactionTrigger

/** Serializes a trigger as its name. Names starting with "poison" are rejected on deserialize. */
object TestTriggerSerializer : EventReactionTriggerSerializer<TestTrigger> {
    override suspend fun serialize(trigger: TestTrigger): String = trigger.name

    override suspend fun deserialize(serializedTrigger: String): TestTrigger {
        require(!serializedTrigger.startsWith("poison")) { "cannot deserialize $serializedTrigger" }
        return TestTrigger(serializedTrigger)
    }
}

data class Attempt(
    val id: EventReactionId,
    val executionId: EventReactionExecutionId,
    val trigger: TestTrigger,
    val retryCount: Int,
)

/**
 * Consumes one queue through the queue SPI, as kotmod's own runtimes do: [run] decides each attempt's outcome. A
 * delivery before its notBefore waits without running. Thread-safe records of what it saw.
 */
class TestConsumer(
    private val channel: ReactionChannel<TestTrigger>,
    private val run: suspend (Attempt) -> ReactionOutcome = { ReactionOutcome.Finished(gaveUp = false) },
) {
    val attempts = CopyOnWriteArrayList<Attempt>()
    val finished = CopyOnWriteArrayList<Pair<EventReactionId, Boolean>>()

    @Volatile
    private var subscription: Cancellable? = null

    suspend fun dispatch(
        id: EventReactionId,
        trigger: TestTrigger,
        ordering: DispatchOrdering? = null,
        notBefore: Instant? = null,
    ) = channel.sink.publish(id, trigger, ordering, notBefore)

    fun start() {
        subscription =
            channel.source.subscribe { id, executionId, trigger, retryCount, notBefore ->
                val now = Clock.System.now()
                if (notBefore != null && now < notBefore) {
                    ReactionOutcome.Wait(notBefore - now)
                } else {
                    val attempt = Attempt(id, executionId, trigger, retryCount)
                    attempts += attempt
                    run(attempt).also { if (it is ReactionOutcome.Finished) finished += id to it.gaveUp }
                }
            }
    }

    fun stop() {
        subscription?.cancel()
        subscription = null
    }
}

fun testScheduler(
    dataSource: DataSource,
    vararg tasks: Task<*>,
): Scheduler =
    Scheduler
        .create(dataSource, *tasks)
        .pollingInterval(java.time.Duration.ofMillis(100))
        .enableImmediateExecution()
        .threads(4)
        .shutdownMaxWait(java.time.Duration.ofSeconds(2))
        .build()

/** Starts consumers before the scheduler and stops them after it — the documented lifecycle order. */
suspend fun <R> running(
    scheduler: Scheduler,
    vararg consumers: TestConsumer,
    block: suspend () -> R,
): R {
    consumers.forEach { it.start() }
    scheduler.start()
    try {
        return block()
    } finally {
        scheduler.stop()
        consumers.forEach { it.stop() }
    }
}

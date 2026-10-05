package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CommandResult
import io.kotmod.CorrelationId
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.EventReactionExecutionResult
import io.kotmod.event.reaction.EventReactionExecutor
import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.EventReactionTriggerSerializer
import io.kotmod.event.reaction.EventReactionTriggerSink
import io.kotmod.event.reaction.EventReactionTriggerSource
import io.kotmod.event.reaction.RetrySignal
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Instant

/**
 * Provides the queues a [ProcessManager]'s channels run on. A process manager asks for a channel per name; names are
 * stable across restarts. `kotmod-db-scheduler` provides `DbSchedulerProcessManagerQueues`; any queue that implements
 * an event reaction sink and source works.
 */
interface ProcessManagerQueues {
    /** Returns the queue for channel [name]. An [ordered] channel's sink must support ordering. */
    fun <T : EventReactionTrigger> channel(
        name: String,
        triggerSerializer: EventReactionTriggerSerializer<T>,
        ordered: Boolean,
    ): ProcessChannel<T>
}

/** One queue for a process manager channel: where reactions are published, and where they are delivered from. */
class ProcessChannel<T : EventReactionTrigger>(
    val sink: EventReactionTriggerSink<T>,
    val source: EventReactionTriggerSource<T>,
)

/** Delivers [input] (JSON) to process [processId], recognised by [inputId] if delivered again. */
@Serializable
internal data class InputTrigger(
    val processId: String,
    val input: String,
    val inputId: String,
    @Transient override val timeout: Duration? = null,
) : EventReactionTrigger

/** Runs [command] (JSON) against aggregate [targetType]/[targetId] with command id [commandId], for process [processId]. */
@Serializable
internal data class CommandTrigger(
    val processId: String,
    val targetType: String,
    val targetId: String,
    val command: String,
    val commandId: String,
    @Transient override val timeout: Duration? = null,
) : EventReactionTrigger

internal class JsonTriggerSerializer<T : EventReactionTrigger>(
    private val serializer: KSerializer<T>,
) : EventReactionTriggerSerializer<T> {
    override suspend fun serialize(trigger: T): String = Json.encodeToString(serializer, trigger)

    override suspend fun deserialize(serializedTrigger: String): T = Json.decodeFromString(serializer, serializedTrigger)
}

/** An aggregate a process manager may send commands to, with how its rejections come back as inputs. Build it with [target]. */
class ProcessTarget<out I : Any> internal constructor(
    internal val type: AggregateType,
    internal val send: suspend (targetId: AggregateId, command: String, commandId: CommandId, correlationId: CorrelationId) -> I?,
)

/**
 * Lets a process manager send commands to [manager]'s aggregates. When one of them rejects a command, [onRejected] turns
 * the command and its typed rejection into an input in the process's own language, which is delivered back to the
 * process instance that asked.
 */
fun <C : Any, R : Any, I : Any> target(
    manager: AggregateManager<*, C, *, R>,
    onRejected: (command: C, rejection: R) -> I,
): ProcessTarget<I> =
    ProcessTarget(manager.kind.type) { targetId, json, commandId, correlationId ->
        val command = Json.decodeFromString(manager.kind.commandSerializer, json)
        when (val result = manager.handle(targetId, command, commandId, correlationId)) {
            is CommandResult.Accepted -> null
            is CommandResult.Rejected -> onRejected(command, result.rejection)
        }
    }

private val log = LoggerFactory.getLogger("io.kotmod.process.ProcessManager")

/**
 * An executor for one process manager channel: [run] does the work; any exception, and any timeout (60 seconds unless
 * the trigger sets one), is retried with capped backoff and never given up, so an input or command is never silently
 * dropped.
 */
internal fun <T : EventReactionTrigger> processExecutor(
    channel: ProcessChannel<T>,
    clock: () -> Instant,
    run: suspend (T) -> Unit,
): EventReactionExecutor<T, Unit> {
    val backoff = BackoffStrategy()
    return EventReactionExecutor(
        sink = channel.sink,
        source = channel.source,
        createExecutionContext = { _, _ -> },
        execute = { _, _, trigger, _, _ ->
            run(trigger)
            EventReactionExecutionResult.EventReactionExecutionCompleted
        },
        failureRetryHandler = { id, _, _, retryCount, _, ex ->
            log.error("Process manager reaction ${id.value} failed and will be retried [ totalRetries=$retryCount ]", ex)
            RetrySignal.Retry(backoff.calculateBackoff(retryCount))
        },
        timeoutRetryHandler = { _, _, _, retryCount, _ -> RetrySignal.Retry(backoff.calculateBackoff(retryCount)) },
        onCompletion = { _, _, _, _, _, _ -> },
        clock = clock,
    )
}

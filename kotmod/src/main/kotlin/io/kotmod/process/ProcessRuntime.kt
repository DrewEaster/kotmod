package io.kotmod.process

import io.kotmod.AggregateId
import io.kotmod.AggregateManager
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.CommandResult
import io.kotmod.CorrelationId
import io.kotmod.event.reaction.BackoffStrategy
import io.kotmod.event.reaction.ItemResult
import io.kotmod.event.reaction.LEASE_MARGIN
import io.kotmod.event.reaction.ReactionQueue
import io.kotmod.event.reaction.ReactionRows
import io.kotmod.event.reaction.rethrowIfCancelled
import io.kotmod.scheduling.TaskScheduler
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/** Delivers [input] (JSON) to process [processId], recognised by [inputId] if delivered again. */
@Serializable
internal data class InputTrigger(
    val processId: String,
    val input: String,
    val inputId: String,
)

/** Runs [command] (JSON) against aggregate [targetType]/[targetId] with command id [commandId], for process [processId]. */
@Serializable
internal data class CommandTrigger(
    val processId: String,
    val targetType: String,
    val targetId: String,
    val command: String,
    val commandId: String,
)

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

/** How long one process manager input or command may run before it is retried. */
internal val PROCESS_TIMEOUT: Duration = 60.seconds

/**
 * One process manager queue, [name], on the [scheduler]'s queue of that name. An ordered item is leased for
 * [PROCESS_TIMEOUT] plus [LEASE_MARGIN].
 */
internal fun <T : Any> processQueue(
    name: String,
    serializer: KSerializer<T>,
    scheduler: TaskScheduler,
    rows: ReactionRows,
    clock: () -> Instant,
): ReactionQueue<T> = ReactionQueue(name, serializer, scheduler.queue(name), rows, PROCESS_TIMEOUT + LEASE_MARGIN, clock)

/**
 * Starts running [run] for each item; any failure, and any run longer than [PROCESS_TIMEOUT], is retried with capped
 * backoff and never given up, so an input or command is never silently dropped.
 */
internal fun <T : Any> ReactionQueue<T>.startProcess(run: suspend (T) -> Unit) {
    val backoff = BackoffStrategy()
    start { delivery ->
        try {
            withTimeout(PROCESS_TIMEOUT) { run(delivery.item) }
            ItemResult.Completed
        } catch (e: TimeoutCancellationException) {
            log.error("Process manager reaction {} timed out and will be retried [attempt={}]", delivery.id.value, delivery.attempt)
            ItemResult.Retry(backoff.calculateBackoff(delivery.attempt))
        } catch (e: Throwable) {
            rethrowIfCancelled(e)
            log.error("Process manager reaction {} failed and will be retried [attempt={}]", delivery.id.value, delivery.attempt, e)
            ItemResult.Retry(backoff.calculateBackoff(delivery.attempt))
        }
    }
}

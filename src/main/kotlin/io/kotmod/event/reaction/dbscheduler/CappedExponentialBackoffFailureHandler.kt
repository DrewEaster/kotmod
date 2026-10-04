package io.kotmod.event.reaction.dbscheduler

import com.github.kagkarlsson.scheduler.task.ExecutionComplete
import com.github.kagkarlsson.scheduler.task.ExecutionOperations
import com.github.kagkarlsson.scheduler.task.FailureHandler
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Retries task executions that failed outside the reaction handler (e.g. unreadable task data), doubling
 * the delay from [initialDelay] up to [maximumDelay] and never giving up.
 */
internal class CappedExponentialBackoffFailureHandler(
    private val initialDelay: Duration,
    private val maximumDelay: Duration,
) : FailureHandler<String> {
    private val log = LoggerFactory.getLogger(CappedExponentialBackoffFailureHandler::class.java)

    override fun onFailure(
        executionComplete: ExecutionComplete,
        executionOperations: ExecutionOperations<String>,
    ) {
        val execution = executionComplete.execution
        val delay = delayFor(execution.consecutiveFailures)
        log.error(
            "Event reaction task {} instance {} failed outside its handler; retrying in {} [consecutiveFailures={}]",
            execution.taskName,
            execution.id,
            delay,
            execution.consecutiveFailures + 1,
            executionComplete.cause.orElse(null),
        )
        executionOperations.reschedule(executionComplete, executionComplete.timeDone.plus(delay.toJavaDuration()))
    }

    internal fun delayFor(previousConsecutiveFailures: Int): Duration {
        val exponent = previousConsecutiveFailures.coerceIn(0, 30)
        return minOf(initialDelay * (1 shl exponent), maximumDelay)
    }
}

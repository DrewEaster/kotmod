package io.kotmod.outbox

import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
import io.kotmod.SequenceCheck
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import kotlin.time.Duration

/**
 * The polling loop shared by [io.kotmod.reaction.EventReactor], [io.kotmod.contract.PublicEventContract] and [io.kotmod.process.ProcessManager]:
 * reads events after the saved position, hands each one to [handleEvent], and saves the position after each
 * event. It delivers each aggregate's events in sequence order (pulling an earlier event forward when the log
 * has it later, and skipping it when reached). An exception stops the current batch; the next poll resumes from the last saved position.
 */
internal class DomainEventPoller(
    private val backend: DomainEventPollingBackend,
    private val getPosition: () -> EventLogPosition,
    private val savePosition: (EventLogPosition) -> Unit,
    private val isLeader: () -> Boolean,
    private val pollInterval: Duration,
    private val batchSize: Int,
    private val loggerName: String,
    private val handleEvent: suspend (PersistedEvent) -> Unit,
) {
    private val log = LoggerFactory.getLogger(loggerName)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private var job: Job? = null

    fun start() {
        if (job != null) return
        log.info("Starting {} poller", loggerName)
        job =
            scope.launch {
                while (isActive) {
                    try {
                        tick()
                    } catch (ex: CancellationException) {
                        throw ex
                    } catch (ex: Exception) {
                        log.error("Error processing {} tick", loggerName, ex)
                    }
                    delay(pollInterval)
                }
            }
    }

    suspend fun stop() {
        log.info("Stopping {} poller", loggerName)
        job?.cancelAndJoin()
        job = null
    }

    internal suspend fun tickForTest() {
        tick()
    }

    private suspend fun tick() {
        if (!isLeader()) return

        var position = withContext(Dispatchers.IO) { getPosition() }
        val rows = withContext(Dispatchers.IO) { backend.readEventsAfter(position, batchSize) }

        for (envelope in rows) {
            when (val check = withContext(Dispatchers.IO) { backend.checkSequence(envelope, position) }) {
                SequenceCheck.WaitForEarlier -> return // an earlier event of this aggregate isn't readable yet
                SequenceCheck.AlreadyHandled -> Unit // handled early, when a later event pulled it forward
                SequenceCheck.InOrder -> handle(envelope)
                is SequenceCheck.HandleEarlierFirst -> {
                    check.earlier.forEach { handle(it) }
                    handle(envelope)
                }
            }
            position = envelope.position
            withContext(Dispatchers.IO) { savePosition(position) }
        }
    }

    private suspend fun handle(envelope: PersistedEvent) {
        log.debug("Handling DDD event {} [position={}]", envelope.metadata.eventId.value, envelope.position)
        handleEvent(envelope)
    }
}

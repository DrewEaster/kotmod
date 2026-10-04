package io.kotmod.outbox

import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventLogPosition
import io.kotmod.PersistedEvent
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
 * The polling loop shared by [AggregateEventOutbox] and [io.kotmod.contract.PublicEventContract]:
 * reads events after the saved position, hands each one to [handleEvent], and saves the position after each
 * event. An exception stops the current batch; the next poll resumes from the last saved position.
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

        val position =
            withContext(Dispatchers.IO) {
                getPosition()
            }
        val rows =
            withContext(Dispatchers.IO) {
                backend.readEventsAfter(position, batchSize)
            }

        for (envelope in rows) {
            log.debug(
                "Handling DDD event {} [position={}]",
                envelope.metadata.eventId.value,
                envelope.position
            )
            handleEvent(envelope)
            withContext(Dispatchers.IO) {
                savePosition(envelope.position)
            }
        }
    }
}

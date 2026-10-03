package com.dreweaster.ddd.outbox

import com.dreweaster.ddd.DomainEventPollingBackend
import com.dreweaster.ddd.PersistedEvent
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

internal class DomainEventPoller(
    private val backend: DomainEventPollingBackend,
    private val getOffset: () -> Long,
    private val saveOffset: (Long) -> Unit,
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

        val lastOffset =
            withContext(Dispatchers.IO) {
                getOffset()
            }
        val rows =
            withContext(Dispatchers.IO) {
                backend.readEventsAfter(lastOffset, batchSize)
            }

        for (envelope in rows) {
            log.debug(
                "Handling DDD event {} [offset={}]",
                envelope.metadata.eventId.value,
                envelope.globalOffset
            )
            handleEvent(envelope)
            withContext(Dispatchers.IO) {
                saveOffset(envelope.globalOffset)
            }
        }
    }
}

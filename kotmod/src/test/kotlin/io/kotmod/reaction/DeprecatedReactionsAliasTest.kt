package io.kotmod.reaction

import io.kotmod.EventLogPosition
import io.kotmod.process.ManualQueues
import io.kotmod.support.OrderPlaced
import io.kotmod.support.testOrders
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

/** 0.3.0's `Reactions<T>` is a deprecated alias of [EventPolicy]: code written against it still compiles and runs. */
@Suppress("DEPRECATION")
class DeprecatedReactionsAliasTest {
    private class Legacy : Reactions<Notice>("legacy", Notice.serializer()) {
        init {
            on(testOrders) { _, metadata -> trigger(Confirm(metadata.aggregateId.value)) }
        }

        override suspend fun handle(
            trigger: Notice,
            context: ReactionContext,
        ) = Unit
    }

    @Test
    fun `a class extending the deprecated Reactions alias registers and queues its triggers`() =
        runBlocking {
            val log = InMemoryLog()
            val queues = ManualQueues()
            val reactor =
                EventReactor(
                    queues = queues,
                    polling = log,
                    readEvent = log::readEvent,
                    getPosition = { EventLogPosition.START },
                    savePosition = {},
                    isLeader = { true },
                    name = "reactor",
                    pollInterval = 50.milliseconds,
                    batchSize = 100,
                    clock = { Instant.parse("2026-10-06T10:00:00Z") },
                )
            reactor.register(Legacy())
            log.add(orderEvent(OrderPlaced("book")))

            reactor.tickForTest()

            assertEquals(listOf<Notice>(Confirm("o-1")), queues.pending("legacy").map { it.notice() })
        }
}

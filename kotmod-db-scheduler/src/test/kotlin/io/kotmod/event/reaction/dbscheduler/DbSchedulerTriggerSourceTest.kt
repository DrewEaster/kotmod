package io.kotmod.event.reaction.dbscheduler

import io.kotmod.event.reaction.EventReactionTrigger
import io.kotmod.event.reaction.ReactionOutcome
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.time.Duration

class DbSchedulerTriggerSourceTest {
    private data class FakeTrigger(
        override val timeout: Duration? = null,
    ) : EventReactionTrigger

    private val source = DbSchedulerTriggerSource<FakeTrigger>()
    private val first: ReactionHandler<FakeTrigger> = { _, _, _, _ -> ReactionOutcome.Finished(false) }
    private val second: ReactionHandler<FakeTrigger> = { _, _, _, _ -> ReactionOutcome.Finished(false) }

    @Test
    fun `subscribe stores the handler`() {
        source.subscribe(first)
        assertSame(first, source.handler)
    }

    @Test
    fun `second subscribe while subscribed throws`() {
        source.subscribe(first)
        assertFailsWith<IllegalStateException> { source.subscribe(second) }
        assertSame(first, source.handler)
    }

    @Test
    fun `cancel clears the handler and allows re-subscription`() {
        source.subscribe(first).cancel()
        assertNull(source.handler)

        source.subscribe(second)
        assertSame(second, source.handler)
    }

    @Test
    fun `stale cancel from an earlier subscription does not clear a newer handler`() {
        val stale = source.subscribe(first)
        stale.cancel()
        source.subscribe(second)

        stale.cancel()

        assertSame(second, source.handler)
    }
}

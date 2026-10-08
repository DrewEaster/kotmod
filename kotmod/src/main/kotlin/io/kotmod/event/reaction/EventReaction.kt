package io.kotmod.event.reaction

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Identifies one reaction across all of its retries. kotmod derives it deterministically (for an event policy,
 * `<policy>/<eventId>/<n>`), so queuing the same work again is recognised as a duplicate while it is pending.
 */
@JvmInline
value class EventReactionId(
    val value: String,
)

/** Exponential backoff for retries kotmod schedules itself: 1s, 2s, 4s… capped at [maximumDuration]. */
internal class BackoffStrategy(
    private val maximumDuration: Duration = 600.seconds,
) {
    /** Returns the delay before retry number [retryCount] + 1. */
    fun calculateBackoff(retryCount: Int): Duration {
        // 2^30 seconds is decades — far beyond any sensible cap — and keeps the shift from overflowing.
        val exponent = retryCount.coerceIn(0, 30)
        return minOf((1L shl exponent).seconds, maximumDuration)
    }
}

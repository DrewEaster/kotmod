package io.kotmod.postgres.support

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Polls [condition] every 50ms until it is true, failing after [timeout]. */
suspend fun eventually(
    timeout: Duration = 10.seconds,
    condition: () -> Boolean,
) {
    withTimeout(timeout) {
        while (!condition()) delay(50)
    }
}

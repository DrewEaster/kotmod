package io.kotmod

import com.aventrix.jnanoid.jnanoid.NanoIdUtils
import java.security.SecureRandom

/** Identifies one aggregate instance within its [AggregateType]. Must not be blank. */
@JvmInline
value class AggregateId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "AggregateId must not be blank" }
    }

    override fun toString(): String = value
}

/** Names a kind of aggregate, e.g. `Order`. Must not be blank. */
@JvmInline
value class AggregateType(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "AggregateType must not be blank" }
    }

    override fun toString(): String = value
}

/**
 * Identifies one command. Commands are idempotent per aggregate: re-running a command with the same id
 * returns the result of the first run instead of applying it twice. Must not be blank.
 */
@JvmInline
value class CommandId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "CommandId must not be blank" }
    }

    override fun toString(): String = value
}

/** Ties together events that belong to the same wider flow, e.g. one inbound request. Must not be blank. */
@JvmInline
value class CorrelationId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "CorrelationId must not be blank" }
    }

    override fun toString(): String = value
}

/** Uniquely identifies one persisted domain event. Must not be blank. */
@JvmInline
value class EventId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "EventId must not be blank" }
    }

    override fun toString(): String = value
}

private val random = SecureRandom()
private val alphabet = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray()
private val defaultLengthIdRegex = Regex("^[0-9a-z]{21}$")

/** Generates a random, URL-safe id of [length] lowercase letters and digits (a NanoID, 21 characters by default). */
fun randomId(length: Int = 21): String = NanoIdUtils.randomNanoId(random, alphabet, length)

/** Returns whether [value] has the shape of a default-length id produced by [randomId]. */
fun isValidRandomId(value: String): Boolean = defaultLengthIdRegex.matches(value)

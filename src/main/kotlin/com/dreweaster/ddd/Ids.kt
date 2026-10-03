package com.dreweaster.ddd

import com.aventrix.jnanoid.jnanoid.NanoIdUtils
import java.security.SecureRandom

@JvmInline
value class AggregateId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "AggregateId must not be blank" }
    }

    override fun toString(): String = value
}

@JvmInline
value class AggregateType(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "AggregateType must not be blank" }
    }

    override fun toString(): String = value
}

@JvmInline
value class CommandId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "CommandId must not be blank" }
    }

    override fun toString(): String = value
}

@JvmInline
value class CorrelationId(
    val value: String,
) {
    init {
        require(value.isNotBlank()) { "CorrelationId must not be blank" }
    }

    override fun toString(): String = value
}

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

fun randomId(length: Int = 21): String = NanoIdUtils.randomNanoId(random, alphabet, length)

fun isValidRandomId(value: String): Boolean = defaultLengthIdRegex.matches(value)

package io.kotmod.support

import io.kotmod.AggregateAlreadyExistsException
import io.kotmod.AggregateId
import io.kotmod.AggregateMeta
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DomainPersistenceBackend
import io.kotmod.DomainEvent
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.PendingEvent
import kotlin.time.toKotlinInstant

class StubPersistenceBackend<E : DomainEvent> : DomainPersistenceBackend<E> {
    data class Key(
        val type: AggregateType,
        val id: AggregateId,
    )

    data class CommandKey(
        val type: AggregateType,
        val id: AggregateId,
        val commandId: CommandId,
    )

    val metas = mutableMapOf<Key, AggregateMeta>()
    val events = mutableListOf<PendingEvent<E>>()
    val commands = mutableSetOf<CommandKey>()

    private var transactionDepth = 0
    var transactionsCommitted = 0
        private set
    val writesOutsideTransaction = mutableListOf<String>()

    override fun <R> inTransaction(block: () -> R): R {
        transactionDepth++
        try {
            val result = block()
            if (transactionDepth == 1) transactionsCommitted++
            return result
        } finally {
            transactionDepth--
        }
    }

    private fun recordWrite(name: String) {
        if (transactionDepth == 0) writesOutsideTransaction += name
    }

    override fun loadMeta(
        type: AggregateType,
        id: AggregateId,
    ): AggregateMeta? = metas[Key(type, id)]

    private val lastSequences = mutableMapOf<Key, Long>()

    override fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
        eventCount: Int,
    ): Long {
        recordWrite("saveMeta")
        val key = Key(type, id)
        val now =
            java.time.Instant
                .now()
                .toKotlinInstant()
        if (expectedVersion == null) {
            if (metas.containsKey(key)) throw AggregateAlreadyExistsException(type, id)
            metas[key] = AggregateMeta(version = 1, createdAt = now, updatedAt = now)
        } else {
            val current = metas[key]
            if (current == null || current.version != expectedVersion) {
                throw OptimisticConcurrencyException(type, id, expectedVersion)
            }
            metas[key] = current.copy(version = expectedVersion + 1, updatedAt = now)
        }
        val last = (lastSequences[key] ?: 0L) + eventCount
        lastSequences[key] = last
        return last
    }

    override fun appendEvents(events: List<PendingEvent<E>>) {
        recordWrite("appendEvents")
        this.events.addAll(events)
    }

    override fun wasCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): Boolean = CommandKey(type, id, commandId) in commands

    override fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ) {
        recordWrite("recordCommandHandled")
        commands.add(CommandKey(type, id, commandId))
    }
}

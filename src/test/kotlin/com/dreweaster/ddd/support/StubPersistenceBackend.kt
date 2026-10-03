package com.dreweaster.ddd.support

import com.dreweaster.ddd.AggregateAlreadyExistsException
import com.dreweaster.ddd.AggregateId
import com.dreweaster.ddd.AggregateMeta
import com.dreweaster.ddd.AggregateType
import com.dreweaster.ddd.CommandId
import com.dreweaster.ddd.DomainPersistenceBackend
import com.dreweaster.ddd.DomainEvent
import com.dreweaster.ddd.OptimisticConcurrencyException
import com.dreweaster.ddd.PendingEvent
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

    override fun loadMeta(
        type: AggregateType,
        id: AggregateId,
    ): AggregateMeta? = metas[Key(type, id)]

    override fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
    ) {
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
    }

    override fun appendEvents(events: List<PendingEvent<E>>) {
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
        commands.add(CommandKey(type, id, commandId))
    }
}

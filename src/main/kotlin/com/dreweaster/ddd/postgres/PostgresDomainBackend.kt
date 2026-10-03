package com.dreweaster.ddd.postgres

import com.dreweaster.ddd.AggregateAlreadyExistsException
import com.dreweaster.ddd.AggregateId
import com.dreweaster.ddd.AggregateMeta
import com.dreweaster.ddd.AggregateType
import com.dreweaster.ddd.CommandId
import com.dreweaster.ddd.DataSerializationContext
import com.dreweaster.ddd.DomainPersistenceBackend
import com.dreweaster.ddd.DomainEvent
import com.dreweaster.ddd.OptimisticConcurrencyException
import com.dreweaster.ddd.PendingEvent
import app.cash.sqldelight.driver.jdbc.JdbcDriver
import com.dreweaster.ddd.CorrelationId
import com.dreweaster.ddd.DomainEventPollingBackend
import com.dreweaster.ddd.EventId
import com.dreweaster.ddd.EventMetadata
import com.dreweaster.ddd.PersistedEvent
import com.dreweaster.ddd.SerializedEvent
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.time.toJavaInstant
import kotlin.time.toKotlinInstant
import kotlin.use

/**
 * Postgres implementation of [DomainPersistenceBackend], using the tables in [DddSchema] and plain JDBC.
 *
 * Every call borrows a connection from [driver], so calls made inside a SQLDelight transaction on that
 * driver share the transaction. Events are serialized with [serialization] as they are appended.
 */
class PostgresDomainPersistenceBackend<E : DomainEvent>(
    private val driver: JdbcDriver,
    private val serialization: DataSerializationContext<E>,
) : DomainPersistenceBackend<E> {
    override fun loadMeta(
        type: AggregateType,
        id: AggregateId,
    ): AggregateMeta? =
        useConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT aggregate_version, created_at, updated_at " +
                        "FROM ddd_aggregate_root WHERE aggregate_type = ? AND aggregate_id = ?",
                ).use { ps ->
                    ps.setString(1, type.value)
                    ps.setString(2, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) return@useConnection null
                        AggregateMeta(
                            version = rs.getLong("aggregate_version"),
                            createdAt =
                                rs
                                    .getObject("created_at", OffsetDateTime::class.java)
                                    .toInstant()
                                    .toKotlinInstant(),
                            updatedAt =
                                rs
                                    .getObject("updated_at", OffsetDateTime::class.java)
                                    .toInstant()
                                    .toKotlinInstant(),
                        )
                    }
                }
        }

    override fun saveMeta(
        type: AggregateType,
        id: AggregateId,
        expectedVersion: Long?,
    ) {
        useConnection { conn ->
            val now = OffsetDateTime.now(ZoneOffset.UTC)
            if (expectedVersion == null) {
                try {
                    conn
                        .prepareStatement(
                            "INSERT INTO ddd_aggregate_root " +
                                "(aggregate_type, aggregate_id, aggregate_version, created_at, updated_at) " +
                                "VALUES (?, ?, 1, ?, ?)",
                        ).use { ps ->
                            ps.setString(1, type.value)
                            ps.setString(2, id.value)
                            ps.setObject(3, now)
                            ps.setObject(4, now)
                            ps.executeUpdate()
                        }
                } catch (e: SQLException) {
                    if (e.sqlState == UNIQUE_VIOLATION) throw AggregateAlreadyExistsException(type, id)
                    throw e
                }
            } else {
                val newVersion = expectedVersion + 1
                val rows =
                    conn
                        .prepareStatement(
                            "UPDATE ddd_aggregate_root " +
                                "SET aggregate_version = ?, updated_at = ? " +
                                "WHERE aggregate_type = ? AND aggregate_id = ? AND aggregate_version = ?",
                        ).use { ps ->
                            ps.setLong(1, newVersion)
                            ps.setObject(2, now)
                            ps.setString(3, type.value)
                            ps.setString(4, id.value)
                            ps.setLong(5, expectedVersion)
                            ps.executeUpdate()
                        }
                if (rows == 0) throw OptimisticConcurrencyException(type, id, expectedVersion)
            }
        }
    }

    override fun appendEvents(events: List<PendingEvent<E>>) {
        if (events.isEmpty()) return
        useConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO ddd_domain_event " +
                        "(aggregate_type, aggregate_id, causation_id, correlation_id, event_id, " +
                        " event_type, event_version, event_payload, event_timestamp) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ).use { ps ->
                    for (pending in events) {
                        val metadata = pending.metadata
                        val serialized = serialization.serialize(pending.event)
                        ps.setString(1, metadata.aggregateType.value)
                        ps.setString(2, metadata.aggregateId.value)
                        ps.setString(3, metadata.causationId.value)
                        val correlationId = metadata.correlationId
                        if (correlationId != null) {
                            ps.setString(4, correlationId.value)
                        } else {
                            ps.setNull(4, Types.VARCHAR)
                        }
                        ps.setString(5, metadata.eventId.value)
                        ps.setString(6, serialized.type)
                        ps.setInt(7, serialized.version)
                        ps.setString(8, serialized.payload)
                        ps.setObject(
                            9,
                            OffsetDateTime.ofInstant(metadata.timestamp.toJavaInstant(), ZoneOffset.UTC),
                        )
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
        }
    }

    override fun wasCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): Boolean =
        useConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT 1 FROM ddd_command_history " +
                        "WHERE aggregate_type = ? AND aggregate_id = ? AND command_id = ?",
                ).use { ps ->
                    ps.setString(1, type.value)
                    ps.setString(2, id.value)
                    ps.setString(3, commandId.value)
                    ps.executeQuery().use { rs -> rs.next() }
                }
        }

    override fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ) {
        useConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO ddd_command_history (aggregate_type, aggregate_id, command_id) " +
                        "VALUES (?, ?, ?)",
                ).use { ps ->
                    ps.setString(1, type.value)
                    ps.setString(2, id.value)
                    ps.setString(3, commandId.value)
                    ps.executeUpdate()
                }
        }
    }

    private inline fun <R> useConnection(block: (Connection) -> R): R {
        val (conn, close) = driver.connectionAndClose()
        try {
            return block(conn)
        } finally {
            close()
        }
    }

    private companion object {
        const val UNIQUE_VIOLATION = "23505"
    }
}

/** Postgres implementation of [DomainEventPollingBackend], reading `ddd_domain_event` in `global_offset` order. */
class PostgresDomainPollingBackend(
    private val driver: JdbcDriver,
    private val eventAttributeColumns: Set<String> = setOf(),
): DomainEventPollingBackend {

    override fun readEventsAfter(
        lastOffset: Long,
        limit: Int
    ): List<PersistedEvent> {
        val (conn, close) = driver.connectionAndClose()
        try {
            return conn
                .prepareStatement(
                    "SELECT global_offset, aggregate_type, aggregate_id, " +
                            "causation_id, correlation_id, event_id, event_type, event_version, " +
                            "event_payload, event_timestamp " +
                            "FROM ddd_domain_event " +
                            "WHERE global_offset > ? " +
                            "ORDER BY global_offset ASC " +
                            "LIMIT ?",
                ).use { ps ->
                    ps.setLong(1, lastOffset)
                    ps.setInt(2, limit)
                    ps.executeQuery().use { rs ->
                        val result = mutableListOf<PersistedEvent>()
                        while (rs.next()) result += rs.toPublishedEvent()
                        result
                    }
                }
        } finally {
            close()
        }
    }

    private fun ResultSet.toPublishedEvent(): PersistedEvent {
        // PostgreSQL JDBC returns null for SQL NULL values, so we don't need wasNull()
        // here — and calling it after unrelated getXxx() calls would reflect the wrong
        // column's null state anyway.
        val correlation: String? = getString("correlation_id")
        return PersistedEvent(
            globalOffset = getLong("global_offset"),
            metadata =
                EventMetadata(
                    eventId = EventId(getString("event_id")),
                    aggregateType = AggregateType(getString("aggregate_type")),
                    aggregateId = AggregateId(getString("aggregate_id")),
                    causationId = CommandId(getString("causation_id")),
                    correlationId = correlation?.let(::CorrelationId),
                    timestamp =
                        getObject("event_timestamp", OffsetDateTime::class.java)
                            .toInstant()
                            .toKotlinInstant(),
                ),
            serialized =
                SerializedEvent(
                    type = getString("event_type"),
                    version = getInt("event_version"),
                    payload = getString("event_payload"),
                ),
        )
    }
}



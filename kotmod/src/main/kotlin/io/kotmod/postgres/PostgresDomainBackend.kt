package io.kotmod.postgres

import io.kotmod.AggregateAlreadyExistsException
import io.kotmod.AggregateId
import io.kotmod.AggregateMeta
import io.kotmod.AggregateType
import io.kotmod.CommandId
import io.kotmod.DataSerializationContext
import io.kotmod.DomainPersistenceBackend
import io.kotmod.DomainEvent
import io.kotmod.OptimisticConcurrencyException
import io.kotmod.PendingEvent
import io.kotmod.jdbc.JdbcContext
import io.kotmod.CorrelationId
import io.kotmod.DomainEventPollingBackend
import io.kotmod.EventId
import io.kotmod.EventLogPosition
import io.kotmod.EventMetadata
import io.kotmod.PersistedEvent
import io.kotmod.SerializedEvent
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
 * Every call borrows a connection from [jdbc], so calls made inside one of its transactions share that
 * transaction. Events are serialized with [serialization] as they are appended.
 */
class PostgresDomainPersistenceBackend<E : DomainEvent>(
    private val jdbc: JdbcContext,
    private val serialization: DataSerializationContext<E>,
) : DomainPersistenceBackend<E> {
    override fun <R> inTransaction(block: () -> R): R = jdbc.inTransaction(block)

    override fun isInTransaction(): Boolean = jdbc.isInTransaction()

    override fun loadMeta(
        type: AggregateType,
        id: AggregateId,
    ): AggregateMeta? =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT aggregate_version, created_at, updated_at " +
                        "FROM ddd_aggregate_root WHERE aggregate_type = ? AND aggregate_id = ?",
                ).use { ps ->
                    ps.setString(1, type.value)
                    ps.setString(2, id.value)
                    ps.executeQuery().use { rs ->
                        if (!rs.next()) return@withConnection null
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
        eventCount: Int,
    ): Long =
        jdbc.withConnection { conn ->
            val now = OffsetDateTime.now(ZoneOffset.UTC)
            if (expectedVersion == null) {
                try {
                    conn
                        .prepareStatement(
                            "INSERT INTO ddd_aggregate_root " +
                                "(aggregate_type, aggregate_id, aggregate_version, last_sequence, created_at, updated_at) " +
                                "VALUES (?, ?, 1, ?, ?, ?) RETURNING last_sequence",
                        ).use { ps ->
                            ps.setString(1, type.value)
                            ps.setString(2, id.value)
                            ps.setLong(3, eventCount.toLong())
                            ps.setObject(4, now)
                            ps.setObject(5, now)
                            ps.executeQuery().use { rs ->
                                rs.next()
                                rs.getLong(1)
                            }
                        }
                } catch (e: SQLException) {
                    if (e.sqlState == UNIQUE_VIOLATION) throw AggregateAlreadyExistsException(type, id)
                    throw e
                }
            } else {
                conn
                    .prepareStatement(
                        "UPDATE ddd_aggregate_root " +
                            "SET aggregate_version = ?, last_sequence = last_sequence + ?, updated_at = ? " +
                            "WHERE aggregate_type = ? AND aggregate_id = ? AND aggregate_version = ? " +
                            "RETURNING last_sequence",
                    ).use { ps ->
                        ps.setLong(1, expectedVersion + 1)
                        ps.setLong(2, eventCount.toLong())
                        ps.setObject(3, now)
                        ps.setString(4, type.value)
                        ps.setString(5, id.value)
                        ps.setLong(6, expectedVersion)
                        ps.executeQuery().use { rs ->
                            if (!rs.next()) throw OptimisticConcurrencyException(type, id, expectedVersion)
                            rs.getLong(1)
                        }
                    }
            }
        }

    override fun appendEvents(events: List<PendingEvent<E>>) {
        if (events.isEmpty()) return
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "INSERT INTO ddd_domain_event " +
                        "(aggregate_type, aggregate_id, aggregate_sequence, causation_id, correlation_id, event_id, " +
                        " event_type, event_version, event_payload, event_timestamp) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ).use { ps ->
                    for (pending in events) {
                        val metadata = pending.metadata
                        val serialized = serialization.serialize(pending.event)
                        ps.setString(1, metadata.aggregateType.value)
                        ps.setString(2, metadata.aggregateId.value)
                        ps.setLong(3, metadata.sequence)
                        ps.setString(4, metadata.causationId.value)
                        val correlationId = metadata.correlationId
                        if (correlationId != null) {
                            ps.setString(5, correlationId.value)
                        } else {
                            ps.setNull(5, Types.VARCHAR)
                        }
                        ps.setString(6, metadata.eventId.value)
                        ps.setString(7, serialized.type)
                        ps.setInt(8, serialized.version)
                        ps.setString(9, serialized.payload)
                        ps.setObject(
                            10,
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
        jdbc.withConnection { conn ->
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
        jdbc.withConnection { conn ->
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


    private companion object {
        const val UNIQUE_VIOLATION = "23505"
    }
}

/** Postgres implementation of [DomainEventPollingBackend], reading `ddd_domain_event` in
 * `(transaction_id, global_offset)` order, only past transactions that have finished. */
class PostgresDomainPollingBackend(
    private val jdbc: JdbcContext,
    private val eventAttributeColumns: Set<String> = setOf(),
): DomainEventPollingBackend {

    override fun readEventsAfter(
        position: EventLogPosition,
        limit: Int,
    ): List<PersistedEvent> =
        jdbc.withConnection { conn ->
            requirePositionNotAhead(conn, position)
            conn
                .prepareStatement(
                    "SELECT global_offset, transaction_id::text::bigint AS transaction_id_value, " +
                        "aggregate_type, aggregate_id, aggregate_sequence, causation_id, correlation_id, event_id, event_type, " +
                        "event_version, event_payload, event_timestamp " +
                        "FROM ddd_domain_event " +
                        "WHERE (transaction_id, global_offset) > (?::text::xid8, ?) " +
                        "AND transaction_id < pg_snapshot_xmin(pg_current_snapshot()) " +
                        "ORDER BY transaction_id, global_offset " +
                        "LIMIT ?",
                ).use { ps ->
                    ps.setLong(1, position.transactionId)
                    ps.setLong(2, position.globalOffset)
                    ps.setInt(3, limit)
                    ps.executeQuery().use { rs ->
                        val result = mutableListOf<PersistedEvent>()
                        while (rs.next()) result += rs.toPublishedEvent()
                        result
                    }
                }
        }

    /**
     * A saved position whose transaction id is at or beyond the server's next transaction id can only come from
     * a different server — e.g. after restoring the database with pg_dump or logical replication. Reading on
     * would silently skip every new event, so fail loudly instead.
     */
    private fun requirePositionNotAhead(
        conn: Connection,
        position: EventLogPosition,
    ) {
        val nextTransactionId =
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint").use { rs ->
                    rs.next()
                    rs.getLong(1)
                }
            }
        check(position.transactionId < nextTransactionId) {
            "Saved event log position $position is ahead of this Postgres server's transaction counter " +
                "($nextTransactionId). This happens after restoring the database onto a new server (pg_dump or " +
                "logical replication); see the README's guidance on moving the database before resuming."
        }
    }

    private fun ResultSet.toPublishedEvent(): PersistedEvent {
        // PostgreSQL JDBC returns null for SQL NULL values, so we don't need wasNull()
        // here — and calling it after unrelated getXxx() calls would reflect the wrong
        // column's null state anyway.
        val correlation: String? = getString("correlation_id")
        return PersistedEvent(
            position = EventLogPosition(getLong("transaction_id_value"), getLong("global_offset")),
            metadata =
                EventMetadata(
                    eventId = EventId(getString("event_id")),
                    aggregateType = AggregateType(getString("aggregate_type")),
                    aggregateId = AggregateId(getString("aggregate_id")),
                    causationId = CommandId(getString("causation_id")),
                    correlationId = correlation?.let(::CorrelationId),
                    sequence = getLong("aggregate_sequence"),
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



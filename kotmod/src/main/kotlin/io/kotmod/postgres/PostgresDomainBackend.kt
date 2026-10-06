package io.kotmod.postgres

import io.kotmod.AggregateAlreadyExistsException
import io.kotmod.AggregateId
import io.kotmod.AggregateMeta
import io.kotmod.AggregateType
import io.kotmod.CommandAlreadyRecordedException
import io.kotmod.CommandId
import io.kotmod.HandledCommand
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
import io.kotmod.SequenceCheck
import io.kotmod.SerializedEvent
import io.kotmod.process.ProcessEventSerialization
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
                                "(aggregate_type, aggregate_id, aggregate_version, last_sequence, last_transaction_id, " +
                                "created_at, updated_at) " +
                                "VALUES (?, ?, 1, ?, pg_current_xact_id(), ?, ?) RETURNING last_sequence",
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
                // Flag the aggregate when this writer's transaction id is below the last writer's. The row lock
                // means the last writer has committed. While unflagged, writers' ids rise with the sequence, so
                // the last writer's id is the highest; an event read before an earlier-sequence event of the same
                // aggregate can only come from a writer whose id is below it. So any event that needs more than
                // InOrder from checkSequence was preceded by a committed flagging write. (SET sees old values.)
                conn
                    .prepareStatement(
                        "UPDATE ddd_aggregate_root " +
                            "SET aggregate_version = ?, last_sequence = last_sequence + ?, updated_at = ?, " +
                            "has_out_of_order_events = has_out_of_order_events OR pg_current_xact_id() < last_transaction_id, " +
                            "last_transaction_id = pg_current_xact_id() " +
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

    override fun findHandledCommand(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ): HandledCommand? =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement(
                    "SELECT rejection_type, rejection_payload FROM ddd_command_history " +
                        "WHERE aggregate_type = ? AND aggregate_id = ? AND command_id = ?",
                ).use { ps ->
                    ps.setString(1, type.value)
                    ps.setString(2, id.value)
                    ps.setString(3, commandId.value)
                    ps.executeQuery().use { rs ->
                        when {
                            !rs.next() -> null
                            rs.getString(1) == null -> HandledCommand.Accepted
                            else -> HandledCommand.Rejected(rs.getString(1), rs.getString(2))
                        }
                    }
                }
        }

    override fun recordCommandHandled(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
    ) = insertCommand(type, id, commandId, rejectionType = null, payload = null)

    override fun recordCommandRejected(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
        rejectionType: String,
        payload: String,
    ) = insertCommand(type, id, commandId, rejectionType, payload)

    private fun insertCommand(
        type: AggregateType,
        id: AggregateId,
        commandId: CommandId,
        rejectionType: String?,
        payload: String?,
    ) {
        jdbc.withConnection { conn ->
            try {
                conn
                    .prepareStatement(
                        "INSERT INTO ddd_command_history " +
                            "(aggregate_type, aggregate_id, command_id, rejection_type, rejection_payload) VALUES (?, ?, ?, ?, ?)",
                    ).use { ps ->
                        ps.setString(1, type.value)
                        ps.setString(2, id.value)
                        ps.setString(3, commandId.value)
                        ps.setString(4, rejectionType)
                        ps.setString(5, payload)
                        ps.executeUpdate()
                    }
            } catch (e: SQLException) {
                if (e.sqlState == UNIQUE_VIOLATION) throw CommandAlreadyRecordedException(type, id, commandId)
                throw e
            }
        }
    }

    private companion object {
        const val UNIQUE_VIOLATION = "23505"
    }
}

/**
 * Postgres implementation of [DomainEventPollingBackend], reading `ddd_domain_event` in
 * `(transaction_id, global_offset)` order, only past transactions that have finished.
 *
 * kotmod's own internal events (the commands a process manager requests and the inputs it schedules, stored in its
 * stream) are never returned: not by [readEventsAfter], and not by [checkSequence], which neither pulls one forward nor
 * waits for one. Only a process manager's own poller reads them.
 */
class PostgresDomainPollingBackend private constructor(
    private val jdbc: JdbcContext,
    private val eventAttributeColumns: Set<String>,
    private val includeProcessEnvelopes: Boolean,
): DomainEventPollingBackend {
    constructor(
        jdbc: JdbcContext,
        eventAttributeColumns: Set<String> = setOf(),
    ) : this(jdbc, eventAttributeColumns, includeProcessEnvelopes = false)

    /** The process manager's own backend, which also reads its internal events. */
    internal constructor(jdbc: JdbcContext, includeProcessEnvelopes: Boolean) :
        this(jdbc, setOf(), includeProcessEnvelopes)

    /**
     * The condition that hides kotmod's internal events, applied to every query that returns events or decides what
     * has been passed, so a consumer reads a log in which they don't exist (see [checkSequence]).
     */
    private fun visible(eventType: String = "event_type"): String =
        if (includeProcessEnvelopes) "" else "AND $eventType NOT IN ($ENVELOPE_TYPE_LITERALS) "

    /** Returns up to [limit] events after [position]; kotmod's own internal events are never returned. */
    override fun readEventsAfter(
        position: EventLogPosition,
        limit: Int,
    ): List<PersistedEvent> =
        jdbc.withConnection { conn ->
            requirePositionNotAhead(conn, position)
            conn
                .prepareStatement(
                    "SELECT $EVENT_COLUMNS " +
                        "FROM ddd_domain_event " +
                        "WHERE (transaction_id, global_offset) > (?::text::xid8, ?) " +
                        "AND transaction_id < pg_snapshot_xmin(pg_current_snapshot()) " +
                        visible() +
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
     * Returns the event with [eventId], or `null` if there is none. A parked use-case mapping reads its event again with
     * it. kotmod's own internal events are never returned, unless this is a process manager's own backend.
     */
    internal fun readEvent(eventId: EventId): PersistedEvent? =
        jdbc.withConnection { conn ->
            conn
                .prepareStatement("SELECT $EVENT_COLUMNS FROM ddd_domain_event WHERE event_id = ? " + visible())
                .use { ps ->
                    ps.setString(1, eventId.value)
                    ps.executeQuery().use { rs -> if (rs.next()) rs.toPublishedEvent() else null }
                }
        }

    override fun checkSequence(
        event: PersistedEvent,
        position: EventLogPosition,
    ): SequenceCheck =
        jdbc.withConnection { conn ->
            // Fast path: an aggregate never written out of transaction order (see saveMeta) is read in sequence
            // order, so this primary-key lookup is all most events need.
            if (!hasOutOfOrderEvents(conn, event)) return@withConnection SequenceCheck.InOrder
            // Hidden internal events are left out of both parts below, so this works on the log the consumer reads:
            // their sequence numbers are just gaps. Counting one in highest_passed would skip a fact never pulled
            // forward (the hidden event was never checked); listing one would deliver it or wait for it.
            // One statement: the highest sequence already passed (always returned, even with no earlier events
            // ahead) left-joined to the earlier events of the aggregate that sit after the saved position.
            conn
                .prepareStatement(
                    "SELECT $EVENT_COLUMNS, " +
                        "e.transaction_id < pg_snapshot_xmin(pg_current_snapshot()) AS readable, " +
                        "hp.highest_passed " +
                        "FROM (SELECT max(aggregate_sequence) AS highest_passed FROM ddd_domain_event " +
                        "WHERE aggregate_type = ? AND aggregate_id = ? " +
                        "AND (transaction_id, global_offset) <= (?::text::xid8, ?) " +
                        visible() +
                        ") hp " +
                        "LEFT JOIN ddd_domain_event e ON e.aggregate_type = ? AND e.aggregate_id = ? " +
                        "AND e.aggregate_sequence < ? AND (e.transaction_id, e.global_offset) > (?::text::xid8, ?) " +
                        visible("e.event_type") +
                        "ORDER BY e.aggregate_sequence",
                ).use { ps ->
                    val m = event.metadata
                    ps.setString(1, m.aggregateType.value)
                    ps.setString(2, m.aggregateId.value)
                    ps.setLong(3, position.transactionId)
                    ps.setLong(4, position.globalOffset)
                    ps.setString(5, m.aggregateType.value)
                    ps.setString(6, m.aggregateId.value)
                    ps.setLong(7, m.sequence)
                    ps.setLong(8, position.transactionId)
                    ps.setLong(9, position.globalOffset)
                    ps.executeQuery().use { rs ->
                        var highestPassed = 0L
                        val candidates = mutableListOf<Pair<PersistedEvent, Boolean>>()
                        while (rs.next()) {
                            highestPassed = rs.getLong("highest_passed") // NULL (nothing passed) reads as 0
                            if (rs.getString("event_id") != null) {
                                candidates += rs.toPublishedEvent() to rs.getBoolean("readable")
                            }
                        }
                        // Earlier events at or below the highest sequence already passed were handled when
                        // that event was reached; pulling them forward again would re-deliver them.
                        val earlier = candidates.filter { it.first.metadata.sequence > highestPassed }
                        when {
                            highestPassed > m.sequence -> SequenceCheck.AlreadyHandled
                            earlier.isEmpty() -> SequenceCheck.InOrder
                            earlier.any { !it.second } -> SequenceCheck.WaitForEarlier
                            else -> SequenceCheck.HandleEarlierFirst(earlier.map { it.first })
                        }
                    }
                }
        }

    private fun hasOutOfOrderEvents(
        conn: Connection,
        event: PersistedEvent,
    ): Boolean =
        conn
            .prepareStatement(
                "SELECT has_out_of_order_events FROM ddd_aggregate_root WHERE aggregate_type = ? AND aggregate_id = ?",
            ).use { ps ->
                ps.setString(1, event.metadata.aggregateType.value)
                ps.setString(2, event.metadata.aggregateId.value)
                ps.executeQuery().use { rs -> rs.next() && rs.getBoolean(1) }
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

    private companion object {
        val ENVELOPE_TYPE_LITERALS = ProcessEventSerialization.ENVELOPE_TYPES.joinToString { "'$it'" }

        const val EVENT_COLUMNS =
            "global_offset, transaction_id::text::bigint AS transaction_id_value, aggregate_type, aggregate_id, " +
                "aggregate_sequence, causation_id, correlation_id, event_id, event_type, event_version, " +
                "event_payload, event_timestamp"
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



package io.kotmod.postgres

/**
 * The Postgres tables the library's Postgres classes rely on: aggregate bookkeeping, the domain event
 * log, handled-command history and consumer offsets. Requires PostgreSQL 13 or later. Apps copy [ddl] into their own migrations; the
 * library's integration tests apply it directly.
 */
object DddSchema {
    /** The `CREATE TABLE` / `CREATE INDEX` statements, as one script. */
    val ddl: String =
        """
        CREATE TABLE ddd_aggregate_root (
            aggregate_type    VARCHAR(72)  NOT NULL,
            aggregate_id      VARCHAR(72)  NOT NULL,
            aggregate_version BIGINT       NOT NULL,
            last_sequence     BIGINT       NOT NULL,
            created_at        TIMESTAMPTZ  NOT NULL,
            updated_at        TIMESTAMPTZ  NOT NULL,
            PRIMARY KEY (aggregate_type, aggregate_id)
        );

        CREATE TABLE ddd_domain_event (
            global_offset     BIGSERIAL    PRIMARY KEY,
            transaction_id    XID8         NOT NULL DEFAULT pg_current_xact_id(),
            aggregate_type    VARCHAR(72)  NOT NULL,
            aggregate_id      VARCHAR(72)  NOT NULL,
            aggregate_sequence BIGINT      NOT NULL,
            causation_id      VARCHAR(72)  NOT NULL,
            correlation_id    VARCHAR(72),
            event_id          VARCHAR(72)  NOT NULL UNIQUE,
            event_type        VARCHAR(255) NOT NULL,
            event_version     INTEGER      NOT NULL,
            event_payload     TEXT         NOT NULL,
            event_timestamp   TIMESTAMPTZ  NOT NULL
        );
        CREATE INDEX idx_ddd_domain_event_aggregate
            ON ddd_domain_event (aggregate_type, aggregate_id, global_offset);
        CREATE INDEX idx_ddd_domain_event_position ON ddd_domain_event (transaction_id, global_offset);
        CREATE UNIQUE INDEX idx_ddd_domain_event_sequence
            ON ddd_domain_event (aggregate_type, aggregate_id, aggregate_sequence);

        CREATE TABLE ddd_command_history (
            aggregate_type VARCHAR(72) NOT NULL,
            aggregate_id   VARCHAR(72) NOT NULL,
            command_id     VARCHAR(72) NOT NULL,
            PRIMARY KEY (aggregate_type, aggregate_id, command_id)
        );

        CREATE TABLE ddd_consumer_offset (
            consumer_name       VARCHAR(255) PRIMARY KEY,
            last_transaction_id BIGINT       NOT NULL,
            last_offset         BIGINT       NOT NULL,
            updated_at          TIMESTAMPTZ  NOT NULL
        );
        """.trimIndent()
}

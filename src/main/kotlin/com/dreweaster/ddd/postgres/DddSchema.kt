package com.dreweaster.ddd.postgres

/**
 * SQL schema required by [PostgresDomainPersistenceBackend] and [PostgresOffsetManager]. Consuming apps copy these
 * statements verbatim into their own Flyway migrations.
 *
 * The library's integration tests use this string directly to set up their
 * test databases.
 */
object DddSchema {
    val ddl: String =
        """
        CREATE TABLE ddd_aggregate_root (
            aggregate_type    VARCHAR(72)  NOT NULL,
            aggregate_id      VARCHAR(72)  NOT NULL,
            aggregate_version BIGINT       NOT NULL,
            created_at        TIMESTAMPTZ  NOT NULL,
            updated_at        TIMESTAMPTZ  NOT NULL,
            PRIMARY KEY (aggregate_type, aggregate_id)
        );

        CREATE TABLE ddd_domain_event (
            global_offset     BIGSERIAL    PRIMARY KEY,
            aggregate_type    VARCHAR(72)  NOT NULL,
            aggregate_id      VARCHAR(72)  NOT NULL,
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

        CREATE TABLE ddd_command_history (
            aggregate_type VARCHAR(72) NOT NULL,
            aggregate_id   VARCHAR(72) NOT NULL,
            command_id     VARCHAR(72) NOT NULL,
            PRIMARY KEY (aggregate_type, aggregate_id, command_id)
        );

        CREATE TABLE ddd_consumer_offset (
            consumer_name VARCHAR(255) PRIMARY KEY,
            last_offset   BIGINT       NOT NULL,
            updated_at    TIMESTAMPTZ  NOT NULL
        );
        """.trimIndent()
}

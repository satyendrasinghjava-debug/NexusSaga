-- =============================================================================
-- © 2026 Satyendra Singh. All Rights Reserved.
-- PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tenants allowed to call the API. The HMAC secret is stored AES-256-GCM encrypted.
-- -----------------------------------------------------------------------------
CREATE TABLE api_tenants (
    tenant_id              VARCHAR(64)    PRIMARY KEY,
    tenant_name            VARCHAR(128)   NOT NULL,
    api_key                VARCHAR(128)   NOT NULL,
    hmac_secret_encrypted  VARCHAR(1024)  NOT NULL,
    active                 BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at             TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    CONSTRAINT uq_api_tenants_api_key UNIQUE (api_key)
);
CREATE INDEX idx_api_tenants_key_active ON api_tenants (api_key, active);

-- -----------------------------------------------------------------------------
-- Workflow definitions. Every registration creates a new immutable version.
-- -----------------------------------------------------------------------------
CREATE TABLE saga_definitions (
    id             BIGSERIAL      PRIMARY KEY,
    workflow_name  VARCHAR(128)   NOT NULL,
    version        INT            NOT NULL,
    description    VARCHAR(512),
    steps          JSONB          NOT NULL,
    created_by     VARCHAR(64)    NOT NULL,
    created_at     TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    CONSTRAINT uq_saga_definitions_name_version UNIQUE (workflow_name, version)
);
CREATE INDEX idx_saga_definitions_name_version_desc ON saga_definitions (workflow_name, version DESC);

-- -----------------------------------------------------------------------------
-- Saga instances. "version" backs JPA @Version optimistic locking.
-- -----------------------------------------------------------------------------
CREATE TABLE saga_instances (
    saga_id             VARCHAR(36)    PRIMARY KEY,
    correlation_id      VARCHAR(128)   NOT NULL,
    tenant_id           VARCHAR(64)    NOT NULL,
    workflow_name       VARCHAR(128)   NOT NULL,
    definition_version  INT            NOT NULL,
    current_step_index  INT            NOT NULL,
    status              VARCHAR(32)    NOT NULL,
    retry_count         INT            NOT NULL DEFAULT 0,
    last_command_id     VARCHAR(36),
    payload             JSONB          NOT NULL,
    failure_reason      VARCHAR(1024),
    started_at          TIMESTAMPTZ(6) NOT NULL,
    updated_at          TIMESTAMPTZ(6) NOT NULL,
    step_started_at     TIMESTAMPTZ(6) NOT NULL,
    completed_at        TIMESTAMPTZ(6),
    version             BIGINT         NOT NULL DEFAULT 0,
    CONSTRAINT uq_saga_instances_tenant_wf_corr UNIQUE (tenant_id, workflow_name, correlation_id)
);
-- Zombie reaper scan: WHERE status IN (...) AND step_started_at < ?
CREATE INDEX idx_saga_instances_status_step_started ON saga_instances (status, step_started_at);
-- Tenant listing / dashboards
CREATE INDEX idx_saga_instances_tenant_wf_started ON saga_instances (tenant_id, workflow_name, started_at DESC);
CREATE INDEX idx_saga_instances_tenant_status_updated ON saga_instances (tenant_id, status, updated_at DESC);

-- -----------------------------------------------------------------------------
-- Transactional outbox, written in the same transaction as the saga state change.
-- -----------------------------------------------------------------------------
CREATE TABLE outbox_events (
    id            BIGSERIAL      PRIMARY KEY,
    aggregate_id  VARCHAR(36)    NOT NULL,
    topic         VARCHAR(255)   NOT NULL,
    message_key   VARCHAR(128)   NOT NULL,
    event_type    VARCHAR(64)    NOT NULL,
    payload       JSONB          NOT NULL,
    status        VARCHAR(16)    NOT NULL,
    attempts      INT            NOT NULL DEFAULT 0,
    last_error    VARCHAR(1024),
    created_at    TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ(6)
);
-- Relay poll: WHERE status = 'PENDING' ORDER BY id
CREATE INDEX idx_outbox_events_status_id ON outbox_events (status, id);
CREATE INDEX idx_outbox_events_aggregate_created ON outbox_events (aggregate_id, created_at);

-- -----------------------------------------------------------------------------
-- Dead letter queue for sagas whose compensation failed.
-- -----------------------------------------------------------------------------
CREATE TABLE saga_dlq (
    id                 BIGSERIAL      PRIMARY KEY,
    saga_id            VARCHAR(36)    NOT NULL,
    tenant_id          VARCHAR(64)    NOT NULL,
    workflow_name      VARCHAR(128)   NOT NULL,
    failed_step_index  INT            NOT NULL,
    failed_step_name   VARCHAR(128)   NOT NULL,
    reason             VARCHAR(1024)  NOT NULL,
    payload_snapshot   JSONB          NOT NULL,
    status             VARCHAR(32)    NOT NULL,
    replay_count       INT            NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ(6) NOT NULL DEFAULT now(),
    resolved_at        TIMESTAMPTZ(6),
    resolved_by        VARCHAR(64)
);
CREATE INDEX idx_saga_dlq_saga_status ON saga_dlq (saga_id, status);
CREATE INDEX idx_saga_dlq_tenant_status_created ON saga_dlq (tenant_id, status, created_at DESC);

-- -----------------------------------------------------------------------------
-- Hash-chained audit log used to build tamper-evident audit receipts.
-- -----------------------------------------------------------------------------
CREATE TABLE saga_audit_log (
    id           BIGSERIAL      PRIMARY KEY,
    saga_id      VARCHAR(36)    NOT NULL,
    seq          INT            NOT NULL,
    event_type   VARCHAR(64)    NOT NULL,
    step_index   INT            NOT NULL,
    step_name    VARCHAR(128),
    status       VARCHAR(32)    NOT NULL,
    detail       JSONB          NOT NULL,
    event_time   TIMESTAMPTZ(6) NOT NULL,
    prev_hash    VARCHAR(64)    NOT NULL,
    entry_hash   VARCHAR(64)    NOT NULL,
    CONSTRAINT uq_saga_audit_log_saga_seq UNIQUE (saga_id, seq)
);

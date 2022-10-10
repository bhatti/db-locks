-- Your SQL goes here
CREATE TABLE IF NOT EXISTS semaphores
(
    semaphore_key     VARCHAR(128) NOT NULL,
    tenant_id         VARCHAR(128) NOT NULL,
    version           VARCHAR(64)  NOT NULL,
    max_size          INTEGER      NOT NULL DEFAULT 1,
    lease_duration_ms BIGINT       NOT NULL DEFAULT 0,
    busy_count        INTEGER,
    fair_semaphore    BOOLEAN,
    created_by        VARCHAR(36),
    created_at        TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    updated_by        VARCHAR(36),
    updated_at        TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (semaphore_key, tenant_id)
);

CREATE INDEX semaphores_tenant_id_ndx ON semaphores (semaphore_key, tenant_id);

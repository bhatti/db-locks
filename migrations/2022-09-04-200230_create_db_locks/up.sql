-- Your SQL goes here
CREATE TABLE IF NOT EXISTS mutexes
(
    mutex_key         VARCHAR(128) NOT NULL,
    tenant_id         VARCHAR(128) NOT NULL,
    version           VARCHAR(64)  NOT NULL,
    lease_duration_ms BIGINT       NOT NULL DEFAULT 0,
    semaphore_key     VARCHAR(128),
    data              TEXT,
    delete_on_release BOOLEAN,
    locked            BOOLEAN,
    expires_at        TIMESTAMP,
    created_by        VARCHAR(36),
    created_at        TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    updated_by        VARCHAR(36),
    updated_at        TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (mutex_key, tenant_id)
);

CREATE INDEX mutexes_tenant_id_ndx ON mutexes (mutex_key, tenant_id);
CREATE INDEX mutexes_semaphore_ndx ON mutexes (semaphore_key);
CREATE INDEX mutexes_expires_locked_ndx ON mutexes (expires_at, locked);

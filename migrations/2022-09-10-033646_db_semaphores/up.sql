-- Your SQL goes here
CREATE TABLE IF NOT EXISTS semaphores
(
    key                                 VARCHAR(128) NOT NULL PRIMARY KEY,
    version                             VARCHAR(64),
    data                                TEXT,
    replace_data                        BOOLEAN,
    delete_on_release                   BOOLEAN,
    owner                               VARCHAR(128),
    max_size                            INTEGER      NOT NULL DEFAULT 1,
    lease_duration_ms                   BIGINT       NOT NULL DEFAULT 0,
    reentrant                           BOOLEAN,
    refresh_period_ms                   BIGINT,
    additional_time_to_wait_for_lock_ms BIGINT,
    created_by                          VARCHAR(36),
    created_at                          TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    updated_by                          VARCHAR(36),
    updated_at                          TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX semaphores_owner_ndx ON semaphores (key, owner);

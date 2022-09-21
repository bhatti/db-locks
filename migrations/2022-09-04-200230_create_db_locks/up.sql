-- Your SQL goes here
CREATE TABLE IF NOT EXISTS lock_items
(
    key                                 VARCHAR(128) NOT NULL PRIMARY KEY,
    version                             VARCHAR(64),
    semaphore_key                       VARCHAR(128),
    data                                TEXT,
    replace_data                        BOOLEAN,
    delete_on_release                   BOOLEAN,
    owner                               VARCHAR(128),
    locked                              BOOLEAN,
    lease_duration_ms                   BIGINT       NOT NULL DEFAULT 0,
    reentrant                           BOOLEAN,
    acquire_only_if_already_exists      BOOLEAN,
    refresh_period_ms                   BIGINT,
    additional_time_to_wait_for_lock_ms BIGINT,
    override_time_to_wait_for_lock_ms   BIGINT,
    expires_at                          TIMESTAMP,
    created_by                          VARCHAR(36),
    created_at                          TIMESTAMP             DEFAULT CURRENT_TIMESTAMP,
    updated_by                          VARCHAR(36),
    updated_at                          TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX lock_items_owner_ndx ON lock_items (key, owner);
CREATE INDEX lock_items_semaphore_ndx ON lock_items (semaphore_key);

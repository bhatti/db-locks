use std::cmp;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Add;

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::models::{DEFAULT_LEASE_PERIOD, DEFAULT_SEMAPHORE_KEY, MutexLock, Semaphore};

// Used as a default buffer for how long extra to wait when querying database for a
// lock in acquireLock (can be overridden by specifying a timeout when calling acquireLock)
const DEFAULT_BUFFER_MS: i64 = 500;

/// AcquireLockOptions defines abstraction for acquiring lock
#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
pub struct AcquireLockOptions {
    // The key representing the lock
    pub key: String,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
    // The data to be stored alongside the lock (can be empty)
    // It can be null if no data is needed to be stored there. If null
    // with replace_data = true, the data will be removed.
    pub data: Option<String>,
    // Whether or not to replace data when acquiring lock that already exists in the database
    pub replace_data: Option<bool>,
    // Whether or not to delete the lock item when releasing it
    pub delete_on_release: Option<bool>,
    // If reentrant is set to true, the lock client will check first if it already owns the lock.
    // If it already owns the lock and the lock is not expired, it will return the lock immediately.
    // If this is set to false and the client already owns the lock, the call to acquireLock will block.
    pub reentrant: Option<bool>,
    // The acquire_only_if_already_exists whether or not to allow acquiring locks if the lock
    // does not exist already.
    pub acquire_only_if_already_exists: Option<bool>,
    // How long to wait before trying to get the lock again.
    pub refresh_period_ms: Option<i64>,
    //  How long to wait in addition to the lease duration.
    pub additional_time_to_wait_for_lock_ms: Option<i64>,
    //  Override time to wait to the lease duration.
    pub override_time_to_wait_for_lock_ms: Option<i64>,
    // The max lock size if using semaphore
    pub semaphore_max_size: Option<i32>,
}

impl PartialEq for AcquireLockOptions {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Hash for AcquireLockOptions {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.key.hash(hasher);
    }
}

impl Display for AcquireLockOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl AcquireLockOptions {
    pub fn to_unlocked_mutex(&self, tenant_id: &str) -> MutexLock {
        MutexLock {
            mutex_key: self.key.clone(),
            tenant_id: tenant_id.to_string(),
            version: Uuid::new_v4().to_string(),
            semaphore_key: if self.get_semaphore_max_size() > 1 { Some(self.key.clone()) } else { Some(DEFAULT_SEMAPHORE_KEY.to_string()) },
            data: self.data.clone(),
            delete_on_release: self.delete_on_release,
            locked: Some(false),
            lease_duration_ms: self.lease_duration_ms,
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn to_semaphore(&self, tenant_id: &str) -> Semaphore {
        Semaphore {
            semaphore_key: self.key.clone(),
            tenant_id: tenant_id.to_string(),
            version: Uuid::new_v4().to_string(),
            max_size: cmp::max(1, self.get_semaphore_max_size()),
            lease_duration_ms: self.lease_duration_ms,
            busy_count: None,
            fair_semaphore: None,
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn to_locked_mutex(&self, tenant_id: &str) -> MutexLock {
        MutexLock {
            mutex_key: self.key.clone(),
            tenant_id: tenant_id.to_string(),
            version: Uuid::new_v4().to_string(),
            lease_duration_ms: self.lease_duration_ms,
            semaphore_key: if self.get_semaphore_max_size() > 1 { Some(self.key.clone()) } else { Some(DEFAULT_SEMAPHORE_KEY.to_string()) },
            data: self.data.clone(),
            delete_on_release: self.delete_on_release,
            locked: Some(true),
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn is_replace_data(&self) -> bool {
        self.replace_data.unwrap_or(false)
    }

    pub fn is_acquire_only_if_already_exists(&self) -> bool {
        self.acquire_only_if_already_exists.unwrap_or(false)
    }

    pub fn get_additional_time_to_wait_for_lock_ms(&self) -> i64 {
        self.additional_time_to_wait_for_lock_ms.unwrap_or(100)
    }

    pub fn get_override_time_to_wait_for_lock_ms(&self) -> i64 {
        self.override_time_to_wait_for_lock_ms.unwrap_or(self.lease_duration_ms)
    }

    pub fn get_refresh_period_ms(&self) -> i64 {
        self.refresh_period_ms.unwrap_or(DEFAULT_BUFFER_MS)
    }

    pub fn get_semaphore_max_size(&self) -> i32 {
        self.semaphore_max_size.unwrap_or(0)
    }

    pub fn is_reentrant(&self) -> bool {
        self.reentrant.unwrap_or(false)
    }
}

/// AcquireLockOptionsBuilder builds AcquireLockOptions
#[derive(Debug, Clone)]
pub struct AcquireLockOptionsBuilder {
    // The key representing the lock
    pub key: String,
    // The data to be stored alongside the lock (can be empty)
    // It can be null if no data is needed to be stored there. If null
    // with replace_data = true, the data will be removed.
    pub data: Option<String>,
    // Whether or not to replace data when acquiring lock that already exists in the database
    pub replace_data: Option<bool>,
    // Whether or not to delete the lock item when releasing it
    pub delete_on_release: Option<bool>,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
    // If reentrant is set to true, the lock client will check first if it already owns the lock.
    // If it already owns the lock and the lock is not expired, it will return the lock immediately.
    // If this is set to false and the client already owns the lock, the call to acquireLock will block.
    pub reentrant: Option<bool>,
    // The acquire_only_if_already_exists whether or not to allow acquiring locks if the lock
    // does not exist already.
    pub acquire_only_if_already_exists: Option<bool>,
    // How long to wait before trying to get the lock again.
    pub refresh_period_ms: Option<i64>,
    //  How long to wait in addition to the lease duration.
    pub additional_time_to_wait_for_lock_ms: Option<i64>,
    //  How long to wait to the lease duration.
    pub override_time_to_wait_for_lock_ms: Option<i64>,
    // The max lock size if using semaphore
    pub semaphore_max_size: Option<i32>,
}

impl Display for AcquireLockOptionsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl AcquireLockOptionsBuilder {
    pub fn new(key: &str) -> Self {
        AcquireLockOptionsBuilder {
            key: key.to_string(),
            data: None,
            replace_data: None,
            delete_on_release: None,
            lease_duration_ms: DEFAULT_LEASE_PERIOD,
            reentrant: None,
            acquire_only_if_already_exists: None,
            refresh_period_ms: None,
            additional_time_to_wait_for_lock_ms: None,
            override_time_to_wait_for_lock_ms: None,
            semaphore_max_size: None,
        }
    }

    pub fn build(&self) -> AcquireLockOptions {
        AcquireLockOptions {
            key: self.key.clone(),
            data: self.data.clone(),
            replace_data: self.replace_data,
            delete_on_release: self.delete_on_release,
            lease_duration_ms: self.lease_duration_ms,
            reentrant: self.reentrant,
            acquire_only_if_already_exists: self.acquire_only_if_already_exists,
            refresh_period_ms: self.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: self.additional_time_to_wait_for_lock_ms,
            override_time_to_wait_for_lock_ms: self.override_time_to_wait_for_lock_ms,
            semaphore_max_size: self.semaphore_max_size,
        }
    }

    pub fn with_lease_duration_millis(&mut self, val: i64) -> &mut Self {
        self.lease_duration_ms = val;
        self
    }

    pub fn with_lease_duration_secs(&mut self, val: i64) -> &mut Self {
        self.lease_duration_ms = val * 1000;
        self
    }

    pub fn with_lease_duration_minutes(&mut self, val: i64) -> &mut Self {
        self.lease_duration_ms = val * 1000 * 60;
        self
    }

    pub fn with_data(&mut self, data: &str) -> &mut Self {
        self.data = Some(data.to_string());
        self
    }

    pub fn with_opt_semaphore_max_size(&mut self, max_size: &Option<i32>) -> &mut Self {
        self.semaphore_max_size = max_size.as_ref().cloned();
        self
    }

    pub fn with_opt_data(&mut self, data: &Option<String>) -> &mut Self {
        self.data = data.as_ref().cloned();
        self
    }

    pub fn with_replace_data(&mut self, replace_data: bool) -> &mut Self {
        self.replace_data = Some(replace_data);
        self
    }

    pub fn with_delete_on_release(&mut self, delete_on_release: bool) -> &mut Self {
        self.delete_on_release = Some(delete_on_release);
        self
    }

    pub fn with_reentrant(&mut self, reentrant: bool) -> &mut Self {
        self.reentrant = Some(reentrant);
        self
    }

    pub fn with_acquire_only_if_already_exists(&mut self, acquire_only_if_already_exists: bool) -> &mut Self {
        self.acquire_only_if_already_exists = Some(acquire_only_if_already_exists);
        self
    }

    pub fn with_refresh_period_ms(&mut self, refresh_period_ms: i64) -> &mut Self {
        self.refresh_period_ms = Some(refresh_period_ms);
        self
    }

    pub fn with_additional_time_to_wait_for_lock_ms(&mut self, additional_time_to_wait_for_lock_ms: i64) -> &mut Self {
        self.additional_time_to_wait_for_lock_ms = Some(additional_time_to_wait_for_lock_ms);
        self
    }
    pub fn with_override_time_to_wait_for_lock_ms(&mut self, override_time_to_wait_for_lock_ms: i64) -> &mut Self {
        self.override_time_to_wait_for_lock_ms = Some(override_time_to_wait_for_lock_ms);
        self
    }
    pub fn with_semaphore_max_size(&mut self, semaphore_max_size: i32) -> &mut Self {
        self.semaphore_max_size = Some(semaphore_max_size);
        self
    }
}


// Provides options for releasing a lock when calling the release_lock() method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseLockOptions {
    // The key representing the lock
    pub key: String,

    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: String,

    // Whether or not to delete the lock when releasing it. If set to false, the
    //  lock row will continue to be in database, but it will be marked as released.
    pub delete_lock: Option<bool>,

    // Optional link to the semaphore for multiple resource locks
    pub semaphore_key: Option<String>,

    // New data to persist to the lock (only used if delete_ock=false.) If the
    // data is null, then the lock client will keep the data as-is and not change it.
    pub data: Option<String>,
}

impl ReleaseLockOptions {
    pub fn new(
        key: &str,
        version: &str,
        delete_lock: Option<bool>,
        semaphore_key: Option<String>,
        data: Option<String>) -> Self {
        ReleaseLockOptions {
            key: key.to_string(),
            version: version.to_string(),
            delete_lock,
            semaphore_key,
            data,
        }
    }

    pub fn is_delete_lock(&self) -> bool {
        self.delete_lock.unwrap_or(false)
    }

    pub fn get_semaphore_key(&self) -> String {
        self.semaphore_key.clone().unwrap_or_else(|| DEFAULT_SEMAPHORE_KEY.to_string())
    }

    pub fn data_or(&self, other: &MutexLock) -> Option<String> {
        let mut data = other.data.clone();
        if self.data != None {
            data = self.data.clone();
        }
        data
    }
}

/// ReleaseLockOptionsBuilder builds ReleaseLockOptions
#[derive(Debug, Clone)]
pub struct ReleaseLockOptionsBuilder {
    // The key representing the lock
    pub key: String,

    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: String,

    // Whether or not to delete the lock item when releasing it
    pub delete_lock: Option<bool>,

    // Optional link to the semaphore for multiple resource locks
    pub semaphore_key: Option<String>,

    // The data to be stored alongside the lock (can be empty)
    // It can be null if no data is needed to be stored there. If null
    // with replace_data = true, the data will be removed.
    pub data: Option<String>,

}

impl ReleaseLockOptionsBuilder {
    pub fn new(key: &str, version: &str) -> Self {
        ReleaseLockOptionsBuilder {
            key: key.to_string(),
            version: version.to_string(),
            delete_lock: None,
            semaphore_key: None,
            data: None,
        }
    }

    pub fn build(&self) -> ReleaseLockOptions {
        ReleaseLockOptions {
            key: self.key.clone(),
            version: self.version.clone(),
            delete_lock: self.delete_lock,
            semaphore_key: self.semaphore_key.clone(),
            data: self.data.clone(),
        }
    }

    pub fn with_delete_lock(&mut self, delete_lock: bool) -> &mut Self {
        self.delete_lock = Some(delete_lock);
        self
    }

    pub fn with_semaphore_key(&mut self, semaphore_key: &str) -> &mut Self {
        self.semaphore_key = Some(semaphore_key.to_string());
        self
    }

    pub fn with_opt_semaphore_key(&mut self, semaphore_key: &Option<String>) -> &mut Self {
        self.semaphore_key = semaphore_key.as_ref().cloned();
        self
    }

    pub fn with_data(&mut self, data: &str) -> &mut Self {
        self.data = Some(data.to_string());
        self
    }

    pub fn with_opt_data(&mut self, data: &Option<String>) -> &mut Self {
        self.data = data.as_ref().cloned();
        self
    }
}


// Provides options for deleting locks or semaphores
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteLockOptions {
    // The key representing the lock
    pub key: String,

    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: String,
}

// It defines abstraction for sending lock heartbeats or updating the lock data with
// various combinations of overrides of the default behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendHeartbeatOptions {
    // The key representing the lock
    pub key: String,
    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: String,
    // The delete_data is true if the extra data associated with the lock should be deleted.
    // Must be null or false if data is non-null.
    pub delete_data: Option<bool>,
    // The new duration of the lease after the heartbeat is sent
    pub lease_duration_to_ensure_ms: i64,

    // Optional link to the semaphore for multiple resource locks
    pub semaphore_key: Option<String>,

    // New data to update in the lock item. If null, will delete the data from the item.
    pub data: Option<String>,
}

impl SendHeartbeatOptions {
    pub fn new(
        key: &str,
        version: &str,
        delete_data: Option<bool>,
        lease_duration_to_ensure_ms: i64,
        semaphore_key: Option<String>,
        data: Option<String>) -> Self {
        SendHeartbeatOptions {
            key: key.to_string(),
            version: version.to_string(),
            delete_data,
            lease_duration_to_ensure_ms,
            semaphore_key,
            data,
        }
    }

    pub fn get_semaphore_key(&self) -> String {
        self.semaphore_key.clone().unwrap_or_else(|| DEFAULT_SEMAPHORE_KEY.to_string())
    }

    pub fn is_delete_data(&self) -> bool {
        self.delete_data.unwrap_or(false)
    }

    pub fn to_mutex(&self, tenant_id: &str, locked: bool) -> MutexLock {
        MutexLock {
            mutex_key: self.key.clone(),
            tenant_id: tenant_id.to_string(),
            version: self.version.clone(),
            semaphore_key: self.semaphore_key.clone(),
            data: None,
            delete_on_release: Some(false),
            locked: Some(locked),
            lease_duration_ms: self.lease_duration_to_ensure_ms,
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_to_ensure_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }
}

/// SendHeartbeatOptionsBuilder builds SendHeartbeatOptions
#[derive(Debug, Clone)]
pub struct SendHeartbeatOptionsBuilder {
    // The key representing the lock
    pub key: String,

    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: String,

    // The new duration of the lease after the heartbeat is sent
    pub lease_duration_to_ensure_ms: i64,

    // The delete_data is true if the extra data associated with the lock should be deleted.
    // Must be null or false if data is non-null.
    pub delete_data: Option<bool>,

    // Optional link to the semaphore for multiple resource locks
    pub semaphore_key: Option<String>,

    // The data to be stored alongside the lock (can be empty)
    // It can be null if no data is needed to be stored there. If null
    // with replace_data = true, the data will be removed.
    pub data: Option<String>,

}

impl SendHeartbeatOptionsBuilder {
    pub fn new(key: &str, version: &str) -> Self {
        SendHeartbeatOptionsBuilder {
            key: key.to_string(),
            version: version.to_string(),
            lease_duration_to_ensure_ms: DEFAULT_LEASE_PERIOD,
            delete_data: None,
            semaphore_key: None,
            data: None,
        }
    }

    pub fn build(&self) -> SendHeartbeatOptions {
        SendHeartbeatOptions {
            key: self.key.clone(),
            version: self.version.clone(),
            lease_duration_to_ensure_ms: self.lease_duration_to_ensure_ms,
            delete_data: self.delete_data,
            semaphore_key: self.semaphore_key.clone(),
            data: self.data.clone(),
        }
    }

    pub fn with_delete_data(&mut self, delete_data: bool) -> &mut Self {
        self.delete_data = Some(delete_data);
        self
    }

    pub fn with_lease_duration_millis(&mut self, val: i64) -> &mut Self {
        self.lease_duration_to_ensure_ms = val;
        self
    }

    pub fn with_lease_duration_secs(&mut self, val: i64) -> &mut Self {
        self.lease_duration_to_ensure_ms = val * 1000;
        self
    }

    pub fn with_lease_duration_minutes(&mut self, val: i64) -> &mut Self {
        self.lease_duration_to_ensure_ms = val * 1000 * 60;
        self
    }

    pub fn with_semaphore_key(&mut self, semaphore_key: &str) -> &mut Self {
        self.semaphore_key = Some(semaphore_key.to_string());
        self
    }

    pub fn with_opt_semaphore_key(&mut self, semaphore_key: &Option<String>) -> &mut Self {
        self.semaphore_key = semaphore_key.as_ref().cloned();
        self
    }

    pub fn with_data(&mut self, data: &str) -> &mut Self {
        self.data = Some(data.to_string());
        self
    }

    pub fn with_opt_data(&mut self, data: &Option<String>) -> &mut Self {
        self.data = data.as_ref().cloned();
        self
    }
}

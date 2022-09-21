use std::{error::Error, fmt};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Add;

use chrono::{Duration, NaiveDateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::schema::*;

#[path = "../utils.rs"]
mod utils;

// Used as a default buffer for how long extra to wait when querying DynamoDB for a lock in acquireLock (can be overriden by
// specifying a timeout when calling acquireLock)
const DEFAULT_BUFFER_MS: i64 = 1000;

//
#[derive(Debug)]
pub enum LockError {
    Database {
        message: String,
        reason_code: Option<String>,
        transient: bool,
    },
    NotGranted {
        message: String,
        reason_code: Option<String>,
    },
    NotFound {
        message: String,
    },

    // This is a retry-able error, which indicates that the lock being requested has already been
// held by another worker and has not been released yet and the lease duration has not expired
// since the lock was last updated by the current owner.
// The caller can retry acquiring the lock with or without a backoff.
    CurrentlyUnavailable {
        message: String,
        reason_code: Option<String>,
        transient: bool,
    },
    Validation {
        message: String,
        reason_code: Option<String>,
    },
}

impl LockError {
    pub fn database(message: String, reason_code: Option<String>, transient: bool) -> LockError {
        LockError::Database { message, reason_code, transient }
    }
    pub fn not_granted(message: String, reason_code: Option<String>) -> LockError {
        LockError::NotGranted { message, reason_code }
    }
    pub fn not_found(message: String) -> LockError {
        LockError::NotFound { message }
    }
    pub fn unavailable(message: String, reason_code: Option<String>) -> LockError {
        LockError::CurrentlyUnavailable { message, reason_code, transient: true }
    }
    pub fn validation(message: String, reason_code: Option<String>) -> LockError {
        LockError::Validation { message, reason_code }
    }
}

impl std::convert::From<prometheus::Error> for LockError {
    fn from(err: prometheus::Error) -> Self {
        LockError::validation(format!("prometheus validation {:?}", err), None)
    }
}

impl Display for LockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LockError::Database { message, reason_code, transient } => {
                write!(f, "{} {:?} {}", message, reason_code, transient)
            }
            LockError::NotGranted { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
            LockError::NotFound { message } => {
                write!(f, "{}", message)
            }
            LockError::CurrentlyUnavailable { message, reason_code, transient } => {
                write!(f, "{} {:?} {}", message, reason_code, transient)
            }
            LockError::Validation { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
        }
    }
}

impl Error for LockError {}

/// A specialized Result type for Lock Result.
pub type LockResult<T> = Result<T, LockError>;

/// LockItem defines abstraction for mutex based locking
#[derive(Debug, Clone, Serialize, Deserialize, Eq, Queryable, Identifiable, Insertable, AsChangeset, Associations)]
#[diesel(table_name = lock_items)]
#[diesel(primary_key(key))]
#[diesel(belongs_to(Semaphore, foreign_key = semaphore_key))]
pub struct LockItem {
    // The key representing the lock
    pub key: String,
    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: Option<String>,
    // Optional link to the semaphore for multiple resource locks
    pub semaphore_key: Option<String>,
    // The data to be stored alongside the lock (can be empty)
    // It can be null if no data is needed to be stored there. If null
    // with replace_data = true, the data will be removed.
    pub data: Option<String>,
    // Whether or not to replace data when acquiring lock that already exists in the database
    pub replace_data: Option<bool>,
    // Whether or not to delete the lock item when releasing it
    pub delete_on_release: Option<bool>,
    // The owner associated with the lock
    pub owner: Option<String>,
    // Whether the item in database is marked as locked
    pub locked: Option<bool>,
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
    //  Override How long to wait in addition to the lease duration.
    pub override_time_to_wait_for_lock_ms: Option<i64>,
    // expiration time
    pub expires_at: Option<NaiveDateTime>,
    pub created_by: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub updated_by: Option<String>,
    pub updated_at: Option<NaiveDateTime>,
}

impl PartialEq for LockItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Hash for LockItem {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.key.hash(hasher);
    }
}

impl std::fmt::Display for LockItem {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "key={} version={:?} owner={:?} locked={:?} expired={}", self.key, self.version, self.owner, self.locked, self.expired())
    }
}

impl LockItem {
    pub fn clone_with_options(&self, options: &AcquireLockOptions, owner: Option<String>) -> Self {
        LockItem {
            key: self.key.clone(),
            version: Some(Uuid::new_v4().to_string()),
            semaphore_key: self.semaphore_key.clone(),
            data: if options.is_replace_data() { options.data.clone() } else { self.data.clone() },
            replace_data: options.replace_data,
            delete_on_release: options.delete_on_release,
            owner,
            locked: Some(true),
            lease_duration_ms: options.lease_duration_ms,
            reentrant: options.reentrant,
            acquire_only_if_already_exists: options.acquire_only_if_already_exists,
            refresh_period_ms: options.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: options.additional_time_to_wait_for_lock_ms,
            override_time_to_wait_for_lock_ms: options.override_time_to_wait_for_lock_ms,
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(options.lease_duration_ms))),
            created_at: self.created_at.clone(),
            created_by: self.created_by.clone(),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    // Clone Lock object with new the record version of the lock. This method is package private --
    // it should only be called by the lock client.
    pub fn update_version(&mut self, options: &SendHeartbeatOptions) {
        self.version = Some(Uuid::new_v4().to_string());
        if options.is_delete_data() {
            self.data = None;
        } else if options.data != None {
            self.data = options.data.clone();
        }
        self.lease_duration_ms = options.lease_duration_to_ensure_ms;
        self.expires_at = Some(Utc::now().naive_utc().add(Duration::milliseconds(options.lease_duration_to_ensure_ms)));
    }

    pub fn expired(self: &Self) -> bool {
        if let (Some(locked), Some(expires_at)) = (self.locked, self.expires_at) {
            !locked || expires_at.timestamp_millis() < utils::current_time_ms()
        } else {
            false
        }
    }

    pub fn key_rank(&self) -> Option<i32> {
        if self.semaphore_key != None {
            let parts: Vec<&str> = self.key.split('_').collect();
            if parts.len() > 1 {
                match parts[parts.len() - 1].parse() {
                    Ok(n) => {
                        return Some(n);
                    }
                    Err(_) => {}
                }
            }
        }
        None
    }

    pub fn to_release_options(&self) -> ReleaseLockOptions {
        ReleaseLockOptions::new(
            self.key.clone(),
            self.version.clone(),
            Some(false),
            self.data.clone())
    }

    pub fn to_heartbeat_options(&self) -> SendHeartbeatOptions {
        SendHeartbeatOptions::new(
            self.key.clone(),
            self.version.clone(),
            Some(false),
            self.lease_duration_ms,
            self.data.clone())
    }
}

/// Semaphore represents a count of locks for managing resources
#[derive(Debug, Clone, Queryable, Identifiable, Insertable, AsChangeset, Serialize, Deserialize)]
#[diesel(table_name = semaphores)]
#[diesel(primary_key(key))]
pub struct Semaphore {
    // The key representing the semaphore
    pub key: String,
    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: Option<String>,
    // The data stored in the lock (can be empty)
    pub data: Option<String>,
    // Whether or not to replace data when acquiring lock that already exists in the database
    pub replace_data: Option<bool>,
    // Whether or not to delete the lock item when releasing it
    pub delete_on_release: Option<bool>,
    // The owner associated with the lock
    pub owner: Option<String>,
    // Whether the item in database is marked as locked
    pub max_size: i32,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
    // If reentrant is set to true, the lock client will check first if it already owns the lock.
    // If it already owns the lock and the lock is not expired, it will return the lock immediately.
    // If this is set to false and the client already owns the lock, the call to acquireLock will block.
    pub reentrant: Option<bool>,
    // How long to wait before trying to get the lock again.
    pub refresh_period_ms: Option<i64>,
    //  How long to wait in addition to the lease duration.
    pub additional_time_to_wait_for_lock_ms: Option<i64>,
    pub created_by: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub updated_by: Option<String>,
    pub updated_at: Option<NaiveDateTime>,
}

impl Semaphore {
    pub fn is_replace_data(&self) -> bool {
        self.replace_data.unwrap_or_else(|| false)
    }

    pub fn get_additional_time_to_wait_for_lock_ms(&self) -> i64 {
        self.additional_time_to_wait_for_lock_ms.unwrap_or_else(|| 100)
    }

    pub fn get_refresh_period_ms(&self) -> i64 {
        self.refresh_period_ms.unwrap_or_else(|| DEFAULT_BUFFER_MS)
    }

    pub fn is_reentrant(&self) -> bool {
        self.reentrant.unwrap_or_else(|| false)
    }

    pub fn generate_lock_items(&self, from: i32) -> Vec<LockItem> {
        let items = (from..self.max_size).map(|i| self.to_lock(i, self.owner.clone())).collect();
        items
    }

    pub(crate) fn clone_with_owner(&self, other: &Semaphore, owner: Option<String>) -> Semaphore {
        Semaphore {
            key: self.key.clone(),
            version: self.version.clone().or_else(|| Some(Uuid::new_v4().to_string())),
            data: other.data.clone(),
            replace_data: other.replace_data,
            delete_on_release: other.delete_on_release,
            owner,
            max_size: other.max_size,
            lease_duration_ms: other.lease_duration_ms,
            reentrant: other.reentrant,
            refresh_period_ms: other.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: other.additional_time_to_wait_for_lock_ms,
            created_at: other.created_at.clone(),
            created_by: other.created_by.clone(),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    fn to_lock(&self, rank: i32, owner: Option<String>) -> LockItem {
        let key = format!("{}_{:0width$}", self.key, rank, width = 10);
        LockItem {
            key,
            version: Some(Uuid::new_v4().to_string()),
            semaphore_key: Some(self.key.clone()),
            data: self.data.clone(),
            replace_data: self.replace_data,
            delete_on_release: self.delete_on_release,
            owner: owner,
            locked: Some(false),
            lease_duration_ms: self.lease_duration_ms,
            reentrant: self.reentrant,
            acquire_only_if_already_exists: Some(true),
            refresh_period_ms: self.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: self.additional_time_to_wait_for_lock_ms,
            override_time_to_wait_for_lock_ms: Some(self.lease_duration_ms),
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }
}


impl PartialEq for Semaphore {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Hash for Semaphore {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.key.hash(hasher);
    }
}

impl std::fmt::Display for Semaphore {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "key={} version={:?} owner={:?}", self.key, self.version, self.owner)
    }
}

#[derive(Debug, Clone)]
pub struct SemaphoreBuilder {
    // The key representing the semaphore
    pub key: String,
    // The data stored in the lock (can be empty)
    pub data: Option<String>,
    // Whether or not to replace data when acquiring lock that already exists in the database
    pub replace_data: Option<bool>,
    // Whether or not to delete the lock item when releasing it
    pub delete_on_release: Option<bool>,
    // Whether the item in database is marked as locked
    pub max_size: i32,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
    // If reentrant is set to true, the lock client will check first if it already owns the lock.
    // If it already owns the lock and the lock is not expired, it will return the lock immediately.
    // If this is set to false and the client already owns the lock, the call to acquireLock will block.
    pub reentrant: Option<bool>,
    // How long to wait before trying to get the lock again.
    pub refresh_period_ms: Option<i64>,
    //  How long to wait in addition to the lease duration.
    pub additional_time_to_wait_for_lock_ms: Option<i64>,
}

impl SemaphoreBuilder {
    pub fn new(key: String, lease_duration_ms: i64, max_size: i32) -> Self {
        SemaphoreBuilder {
            key: key.clone(),
            data: None,
            replace_data: None,
            delete_on_release: None,
            max_size,
            lease_duration_ms,
            reentrant: None,
            refresh_period_ms: None,
            additional_time_to_wait_for_lock_ms: None,
        }
    }

    pub fn build(&self) -> Semaphore {
        Semaphore {
            key: self.key.clone(),
            version: Some(Uuid::new_v4().to_string()),
            data: self.data.clone(),
            replace_data: self.replace_data,
            delete_on_release: self.delete_on_release,
            owner: None,
            max_size: self.max_size,
            lease_duration_ms: self.lease_duration_ms,
            reentrant: self.reentrant,
            refresh_period_ms: self.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: self.additional_time_to_wait_for_lock_ms,
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn with_data(&mut self, data: String) -> &mut Self {
        self.data = Some(data);
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

    pub fn with_refresh_period_ms(&mut self, refresh_period_ms: i64) -> &mut Self {
        self.refresh_period_ms = Some(refresh_period_ms);
        self
    }

    pub fn with_additional_time_to_wait_for_lock_ms(&mut self, additional_time_to_wait_for_lock_ms: i64) -> &mut Self {
        self.additional_time_to_wait_for_lock_ms = Some(additional_time_to_wait_for_lock_ms);
        self
    }
}

// LocksConfig represents configuration for lock service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocksConfig {
    // How often to update database to note that the instance is still running (recommendation
    // is to make this at least 3 times smaller than the leaseDuration -- for example
    // heartbeat_period_ms = 1000, lease_duration_ms=10000 could be a reasonable configuration,
    // make sure to include a buffer for network latency.
    pub heatbeat_period_ms: Option<i64>,
    // The owner associated with the lock
    pub owner: Option<String>,
}

const DEFAULT_HEARTBEAT_PERIOD: i64 = 5000;

impl LocksConfig {
    pub fn get_heartbeat_period_ms(&self) -> i64 {
        self.heatbeat_period_ms.unwrap_or_else(|| DEFAULT_HEARTBEAT_PERIOD)
    }
}

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
    // The lock uses semaphore
    pub uses_semaphore: Option<bool>,
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

impl std::fmt::Display for AcquireLockOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl AcquireLockOptions {
    pub fn to_lock(&self) -> LockItem {
        LockItem {
            key: self.key.clone(),
            version: Some(Uuid::new_v4().to_string()),
            semaphore_key: if self.does_use_semaphore() { Some(self.key.clone()) } else { None },
            data: self.data.clone(),
            replace_data: self.replace_data,
            delete_on_release: self.delete_on_release,
            owner: None,
            locked: Some(false),
            lease_duration_ms: self.lease_duration_ms,
            reentrant: self.reentrant,
            acquire_only_if_already_exists: if self.does_use_semaphore() { Some(true) } else { self.acquire_only_if_already_exists },
            refresh_period_ms: self.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: self.additional_time_to_wait_for_lock_ms,
            override_time_to_wait_for_lock_ms: self.override_time_to_wait_for_lock_ms,
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn to_semaphore(&self, max_size: i32) -> Semaphore {
        Semaphore {
            key: self.key.clone(),
            version: Some(Uuid::new_v4().to_string()),
            data: self.data.clone(),
            replace_data: self.replace_data,
            delete_on_release: self.delete_on_release,
            owner: None,
            max_size,
            lease_duration_ms: self.lease_duration_ms,
            reentrant: self.reentrant,
            refresh_period_ms: self.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: self.additional_time_to_wait_for_lock_ms,
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn to_locked(&self, owner: Option<String>) -> LockItem {
        LockItem {
            key: self.key.clone(),
            version: Some(Uuid::new_v4().to_string()),
            semaphore_key: if self.does_use_semaphore() { Some(self.key.clone()) } else { None },
            data: self.data.clone(),
            replace_data: self.replace_data,
            delete_on_release: self.delete_on_release,
            owner,
            locked: Some(true),
            lease_duration_ms: self.lease_duration_ms,
            reentrant: self.reentrant,
            acquire_only_if_already_exists: if self.does_use_semaphore() { Some(true) } else { self.acquire_only_if_already_exists },
            refresh_period_ms: self.refresh_period_ms,
            additional_time_to_wait_for_lock_ms: self.additional_time_to_wait_for_lock_ms,
            override_time_to_wait_for_lock_ms: self.override_time_to_wait_for_lock_ms,
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn is_replace_data(&self) -> bool {
        self.replace_data.unwrap_or_else(|| false)
    }

    pub fn is_acquire_only_if_already_exists(&self) -> bool {
        self.uses_semaphore.unwrap_or_else(|| false) || self.acquire_only_if_already_exists.unwrap_or_else(|| false)
    }

    pub fn get_additional_time_to_wait_for_lock_ms(&self) -> i64 {
        self.additional_time_to_wait_for_lock_ms.unwrap_or_else(|| 100)
    }

    pub fn get_override_time_to_wait_for_lock_ms(&self) -> i64 {
        self.override_time_to_wait_for_lock_ms.unwrap_or_else(|| self.lease_duration_ms)
    }

    pub fn get_refresh_period_ms(&self) -> i64 {
        self.refresh_period_ms.unwrap_or_else(|| DEFAULT_BUFFER_MS)
    }

    pub fn does_use_semaphore(&self) -> bool {
        self.uses_semaphore.unwrap_or_else(|| false)
    }

    pub fn is_reentrant(&self) -> bool {
        self.reentrant.unwrap_or_else(|| false)
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
    // The lock uses semaphore
    pub uses_semaphore: Option<bool>,
}

impl std::fmt::Display for AcquireLockOptionsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl AcquireLockOptionsBuilder {
    pub fn new(key: String, lease_duration_ms: i64) -> Self {
        AcquireLockOptionsBuilder {
            key: key.clone(),
            data: None,
            replace_data: None,
            delete_on_release: None,
            lease_duration_ms,
            reentrant: None,
            acquire_only_if_already_exists: None,
            refresh_period_ms: None,
            additional_time_to_wait_for_lock_ms: None,
            override_time_to_wait_for_lock_ms: None,
            uses_semaphore: None,
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
            uses_semaphore: self.uses_semaphore,
        }
    }

    pub fn with_data(&mut self, data: String) -> &mut Self {
        self.data = Some(data);
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
    pub fn with_uses_semaphore(&mut self, uses_semaphore: bool) -> &mut Self {
        self.uses_semaphore = Some(uses_semaphore);
        self
    }
}


// Provides options for releasing a lock when calling the release_lock() method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseLockOptions {
    // The key representing the lock
    pub key: String,

    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: Option<String>,

    // Whether or not to delete the lock when releasing it. If set to false, the
    //  lock row will continue to be in database, but it will be marked as released.
    pub delete_lock: Option<bool>,

    // New data to persist to the lock (only used if delete_ock=false.) If the
    // data is null, then the lock client will keep the data as-is and not change it.
    pub data: Option<String>,
}

impl ReleaseLockOptions {
    pub fn new(key: String, version: Option<String>, delete_lock: Option<bool>, data: Option<String>) -> Self {
        ReleaseLockOptions {
            key,
            version,
            delete_lock,
            data,
        }
    }

    pub fn is_delete_lock(&self) -> bool {
        self.delete_lock.unwrap_or_else(|| false)
    }

    pub fn data_or(&self, other: Option<LockItem>) -> Option<String> {
        let mut data = None;
        if let Some(old) = other {
            data = old.data;
        }
        if self.data != None {
            data = self.data.clone();
        }
        data
    }
}

// Provides options for deleting locks or semaphores
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteLockOptions {
    // The key representing the lock
    pub key: String,

    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: Option<String>,
}

// It defines abstraction for sending lock heartbeats or updating the lock data with
// various combinations of overrides of the default behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendHeartbeatOptions {
    // The key representing the lock
    pub key: String,
    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: Option<String>,
    // The delete_data is true if the extra data associated with the lock should be deleted.
    // Must be null or false if data is non-null.
    pub delete_data: Option<bool>,
    // The new duration of the lease after the heartbeat is sent
    pub lease_duration_to_ensure_ms: i64,
    // New data to update in the lock item. If null, will delete the data from the item.
    pub data: Option<String>,
}


impl SendHeartbeatOptions {
    pub fn new(key: String, version: Option<String>, delete_data: Option<bool>, lease_duration_to_ensure_ms: i64, data: Option<String>) -> Self {
        SendHeartbeatOptions {
            key,
            version,
            delete_data,
            lease_duration_to_ensure_ms,
            data,
        }
    }

    pub fn is_delete_data(&self) -> bool {
        self.delete_data.unwrap_or_else(|| false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_create_database_error() {
        let err = LockError::database("test error".to_string(), None, false);
        let err_msg = format!("{}", err);
        assert_eq!("test error None false", err_msg);
    }

    #[test]
    fn test_should_create_not_granted_error() {
        let err = LockError::not_granted("test error".to_string(), None);
        let err_msg = format!("{}", err);
        assert_eq!("test error None", err_msg);
    }

    #[test]
    fn test_should_create_not_found_error() {
        let err = LockError::not_found("test error".to_string());
        let err_msg = format!("{}", err);
        assert_eq!("test error", err_msg);
        // pub fn unavailable(message: String, reason_code: Option<String>) -> LockError {
        // pub fn validation(message: String, reason_code: Option<String>) -> LockError {
    }

    #[test]
    fn test_should_create_unavailable_error() {
        let err = LockError::unavailable("test error".to_string(), None);
        let err_msg = format!("{}", err);
        assert_eq!("test error None true", err_msg);
    }

    #[test]
    fn test_should_create_validation_error() {
        let err = LockError::validation("test error".to_string(), None);
        let err_msg = format!("{}", err);
        assert_eq!("test error None", err_msg);
    }

    #[test]
    fn test_should_build_lock_item() {
        let lock = AcquireLockOptionsBuilder::new("key1".to_string(), 100)
            .with_data("data1".to_string())
            .with_replace_data(true)
            .with_delete_on_release(true)
            .with_reentrant(true)
            .with_acquire_only_if_already_exists(true)
            .with_refresh_period_ms(10)
            .with_additional_time_to_wait_for_lock_ms(5)
            .with_override_time_to_wait_for_lock_ms(5)
            .build();
        assert_eq!("key1", lock.key);
        assert_eq!(true, lock.is_reentrant());
        assert_eq!(true, lock.is_acquire_only_if_already_exists());
        assert_eq!(100, lock.lease_duration_ms);
        assert_eq!(10, lock.get_refresh_period_ms());
        assert_eq!(5, lock.get_additional_time_to_wait_for_lock_ms());
        assert_eq!(5, lock.get_override_time_to_wait_for_lock_ms());
        assert_eq!(true, lock.delete_on_release.unwrap());
        assert_eq!("data1", lock.data.unwrap());
    }

    #[test]
    fn test_should_build_release_options_from_lock_item() {
        let lock = AcquireLockOptionsBuilder::new("key2".to_string(), 20)
            .with_data("data2".to_string())
            .with_replace_data(true)
            .with_delete_on_release(true)
            .with_reentrant(true)
            .with_acquire_only_if_already_exists(true)
            .with_refresh_period_ms(10)
            .with_additional_time_to_wait_for_lock_ms(5)
            .build().to_locked(None);
        let options = lock.to_release_options();
        assert_eq!("key2", options.key);
        assert_eq!(false, options.is_delete_lock());
        assert_eq!("data2", options.data.unwrap());
    }

    #[test]
    fn test_should_build_heartbeat_options_from_lock_item() {
        let lock = AcquireLockOptionsBuilder::new("key3".to_string(), 30)
            .with_data("data3".to_string())
            .with_replace_data(true)
            .with_delete_on_release(true)
            .with_reentrant(true)
            .with_acquire_only_if_already_exists(true)
            .with_refresh_period_ms(10)
            .with_additional_time_to_wait_for_lock_ms(5)
            .build().to_locked(None);
        let options = lock.to_heartbeat_options();
        assert_eq!("key3", options.key);
        assert_eq!(false, options.is_delete_data());
        assert_eq!(30, options.lease_duration_to_ensure_ms);
        assert_eq!("data3", options.data.unwrap());
    }

    #[test]
    fn test_should_build_semaphore() {
        let lock = SemaphoreBuilder::new("key5".to_string(), 200, 50)
            .with_data("data1".to_string())
            .with_replace_data(true)
            .with_delete_on_release(true)
            .with_reentrant(true)
            .with_refresh_period_ms(10)
            .with_additional_time_to_wait_for_lock_ms(5)
            .build();
        assert_eq!("key5", lock.key);
        assert_eq!(true, lock.is_reentrant());
        assert_eq!(200, lock.lease_duration_ms);
        assert_eq!(10, lock.get_refresh_period_ms());
        assert_eq!(5, lock.get_additional_time_to_wait_for_lock_ms());
        assert_eq!(true, lock.delete_on_release.unwrap());
        assert_eq!(50, lock.max_size);
        assert_eq!("data1", lock.data.unwrap());
    }
}

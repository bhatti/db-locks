use std::{env, error::Error, fmt};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Add};
use std::path::PathBuf;

use aws_sdk_dynamodb::model::AttributeValue;
use chrono::{Duration, NaiveDateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::schema::*;
use crate::utils;

// Used as a default buffer for how long extra to wait when querying database for a
// lock in acquireLock (can be overriden by specifying a timeout when calling acquireLock)
const DEFAULT_BUFFER_MS: i64 = 1000;

const DEFAULT_HEARTBEAT_PERIOD: i64 = 5000;

const DEFAULT_LEASE_PERIOD: i64 = 15000;

pub(crate) const DEFAULT_SEMAPHORE_KEY: &str = "DEFAULT";

#[derive(Debug, Copy, Clone, PartialOrd, Ord, ValueEnum)]
pub enum RepositoryProvider {
    Rdb,
    Ddb,
    Redis,
}

impl PartialEq for RepositoryProvider {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RepositoryProvider::Rdb, RepositoryProvider::Rdb) => { true }
            (RepositoryProvider::Ddb, RepositoryProvider::Ddb) => { true }
            (RepositoryProvider::Redis, RepositoryProvider::Redis) => { true }
            _ => false
        }
    }
}

impl Eq for RepositoryProvider {}

impl Hash for RepositoryProvider {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        match self {
            RepositoryProvider::Rdb => {
                0.hash(hasher);
            }
            RepositoryProvider::Ddb => {
                1.hash(hasher);
            }
            RepositoryProvider::Redis => {
                2.hash(hasher);
            }
        }
    }
}

impl std::fmt::Display for RepositoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RepositoryProvider::Rdb => {
                write!(f, "rdb")
            }
            RepositoryProvider::Ddb => {
                write!(f, "ddb")
            }
            RepositoryProvider::Redis => {
                write!(f, "redis")
            }
        }
    }
}

//
#[derive(Debug)]
pub enum LockError {
    Database {
        message: String,
        reason_code: Option<String>,
        retryable: bool,
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
    // since the lock was last updated by the current tenant_id.
    // The caller can retry acquiring the lock with or without a backoff.
    CurrentlyUnavailable {
        message: String,
        reason_code: Option<String>,
        retryable: bool,
    },
    Validation {
        message: String,
        reason_code: Option<String>,
    },
    Serialization {
        message: String,
    },
    Runtime {
        message: String,
        reason_code: Option<String>,
    },
}

impl LockError {
    pub fn database(message: &str, reason_code: Option<String>, retryable: bool) -> LockError {
        LockError::Database { message: message.to_string(), reason_code, retryable }
    }

    pub fn not_granted(message: &str, reason_code: Option<String>) -> LockError {
        LockError::NotGranted { message: message.to_string(), reason_code }
    }

    pub fn not_found(message: &str) -> LockError {
        LockError::NotFound { message: message.to_string() }
    }

    pub fn unavailable(message: &str, reason_code: Option<String>, retryable: bool) -> LockError {
        LockError::CurrentlyUnavailable { message: message.to_string(), reason_code, retryable }
    }

    pub fn validation(message: &str, reason_code: Option<String>) -> LockError {
        LockError::Validation { message: message.to_string(), reason_code }
    }

    pub fn serialization(message: &str) -> LockError {
        LockError::Serialization { message: message.to_string() }
    }

    pub fn runtime(message: &str, reason_code: Option<String>) -> LockError {
        LockError::Runtime { message: message.to_string(), reason_code }
    }

    pub fn retryable(&self) -> bool {
        match self {
            LockError::Database { retryable, .. } => { *retryable }
            LockError::NotGranted { .. } => { false }
            LockError::NotFound { .. } => { false }
            LockError::CurrentlyUnavailable { retryable, .. } => { *retryable }
            LockError::Validation { .. } => { false }
            LockError::Serialization { .. } => { false }
            LockError::Runtime { .. } => { false }
        }
    }
}

impl std::convert::From<std::io::Error> for LockError {
    fn from(err: std::io::Error) -> Self {
        LockError::runtime(
            format!("serde validation {:?}", err).as_str(), None)
    }
}

impl std::convert::From<serde_json::Error> for LockError {
    fn from(err: serde_json::Error) -> Self {
        LockError::serialization(
            format!("serde validation {:?}", err).as_str())
    }
}

impl std::convert::From<prometheus::Error> for LockError {
    fn from(err: prometheus::Error) -> Self {
        LockError::validation(
            format!("prometheus validation {:?}", err).as_str(), None)
    }
}

impl Display for LockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LockError::Database { message, reason_code, retryable } => {
                write!(f, "{} {:?} {}", message, reason_code, retryable)
            }
            LockError::NotGranted { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
            LockError::NotFound { message } => {
                write!(f, "{}", message)
            }
            LockError::CurrentlyUnavailable { message, reason_code, retryable } => {
                write!(f, "{} {:?} {}", message, reason_code, retryable)
            }
            LockError::Validation { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
            LockError::Serialization { message } => {
                write!(f, "{}", message)
            }
            LockError::Runtime { message, reason_code } => {
                write!(f, "{} {:?}", message, reason_code)
            }
        }
    }
}

impl Error for LockError {}

/// A specialized Result type for Lock Result.
pub type LockResult<T> = Result<T, LockError>;

// It defines abstraction for paginated result
#[derive(Debug, Clone)]
pub struct PaginatedResult<T> {
    // The page number or token
    pub page: Option<String>,
    // Next page if available
    pub next_page: Option<String>,
    // page size
    pub page_size: usize,
    // total records matching filtering
    pub total_records: u64,
    // list of records
    pub records: Vec<T>,
}


impl<T> PaginatedResult<T> {
    pub fn from_rdb(
        page: Option<&str>,
        page_size: usize,
        records: Vec<T>) -> Self {
        let page_num: usize = page.clone().unwrap_or_else(|| "0").parse().unwrap_or_else(|_| 0);
        let next_page = if records.len() < page_size { None } else { Some((page_num + 1).to_string()) };
        PaginatedResult::new(page, next_page, page_size, 0, records)
    }

    pub fn to_ddb_page(tenant_id: &str, page: Option<&str>) -> Option<HashMap<String, AttributeValue>> {
        if let Some(page) = page {
            if let Ok(str_map) = serde_json::from_str::<HashMap<String, String>>(page) {
                let mut attr_map = HashMap::new();
                for (k, v) in str_map {
                    attr_map.insert(k, AttributeValue::S(v));
                }
                attr_map.insert("tenant_id".to_string(), AttributeValue::S(tenant_id.to_string()));
                return Some(attr_map);
            }
        }
        None
    }

    pub fn from_ddb(
        page: Option<&str>,
        last_evaluated_key: Option<&HashMap<String, AttributeValue>>,
        page_size: usize,
        records: Vec<T>) -> Self {
        let mut next_page: Option<String> = None;
        if let Some(attr_map) = last_evaluated_key {
            let mut str_map = HashMap::new();
            for (k, v) in attr_map {
                if let AttributeValue::S(val) = v {
                    str_map.insert(k.clone(), val.to_string());
                }
            }
            if let Ok(j) = serde_json::to_string(&str_map) {
                next_page = Some(j);
            }
        }
        PaginatedResult::new(page, next_page, page_size, 0, records)
    }

    pub fn new(page: Option<&str>, next_page: Option<String>, page_size: usize, total_records: u64, records: Vec<T>) -> Self {
        PaginatedResult {
            page: page.map(str::to_string),
            page_size,
            next_page,
            total_records,
            records,
        }
    }
}

/// LockItem defines abstraction for mutex based locking
#[derive(Debug, Clone, Serialize, Deserialize, Eq, Queryable, Identifiable, Insertable, AsChangeset, Associations)]
#[diesel(table_name = mutexes)]
#[diesel(primary_key(mutex_key, tenant_id))]
#[diesel(belongs_to(Semaphore, foreign_key = semaphore_key))]
pub struct MutexLock {
    // The key representing the lock
    pub mutex_key: String,
    // The tenant_id associated with the lock
    pub tenant_id: String,
    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: String,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
    // Optional link to the semaphore for multiple resource locks
    pub semaphore_key: Option<String>,
    // The data to be stored alongside the lock (can be empty)
    // It can be null if no data is needed to be stored there. If null
    // with replace_data = true, the data will be removed.
    pub data: Option<String>,
    // Whether or not to delete the lock item when releasing it
    pub delete_on_release: Option<bool>,
    // Whether the item in database is marked as locked
    pub locked: Option<bool>,
    // expiration time
    pub expires_at: Option<NaiveDateTime>,
    pub created_by: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub updated_by: Option<String>,
    pub updated_at: Option<NaiveDateTime>,
}

impl PartialEq for MutexLock {
    fn eq(&self, other: &Self) -> bool {
        self.mutex_key == other.mutex_key
    }
}

impl Hash for MutexLock {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.mutex_key.hash(hasher);
    }
}

impl Display for MutexLock {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "key={} version={} tenant_id={} locked={:?} expired={}, lease_duration_ms={}, expires={:?}",
               self.mutex_key, self.version, self.tenant_id, self.locked, self.expired(), self.lease_duration_ms, self.expires_at_string())
    }
}

impl MutexLock {
    pub fn clone_with_options(&self, options: &AcquireLockOptions, tenant_id: &str) -> Self {
        MutexLock {
            mutex_key: self.mutex_key.clone(),
            tenant_id: tenant_id.to_string(),
            version: Uuid::new_v4().to_string(),
            semaphore_key: self.semaphore_key.clone(),
            data: if options.is_replace_data() { options.data.clone() } else { self.data.clone() },
            delete_on_release: options.delete_on_release,
            locked: Some(true),
            lease_duration_ms: options.lease_duration_ms,
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
        self.version = Uuid::new_v4().to_string();
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
            let parts: Vec<&str> = self.mutex_key.split('_').collect();
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
            self.mutex_key.as_str(),
            self.version.as_str(),
            Some(false),
            self.semaphore_key.clone(),
            self.data.clone())
    }

    pub fn to_heartbeat_options(&self) -> SendHeartbeatOptions {
        SendHeartbeatOptions::new(
            self.mutex_key.as_str(),
            self.version.as_str(),
            Some(false),
            self.lease_duration_ms,
            self.semaphore_key.clone(),
            self.data.clone())
    }

    pub fn full_key(&self) -> String {
        format!("{}_{}", self.mutex_key, self.tenant_id)
    }

    pub fn expires_at_string(&self) -> String {
        let date = self.expires_at.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        date.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
    }

    pub fn updated_at_string(&self) -> String {
        let date = self.updated_at.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        date.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
    }

    pub fn get_semaphore_key(&self) -> String {
        self.semaphore_key.clone().unwrap_or_else(|| DEFAULT_SEMAPHORE_KEY.to_string())
    }

    pub fn belongs_to_semaphore(&self) -> bool {
        if let Some(sem_key) = &self.semaphore_key {
            return sem_key.as_str() != DEFAULT_SEMAPHORE_KEY && sem_key.as_str() != "";
        }
        false
    }
}

/// Semaphore represents a count of locks for managing resources
#[derive(Debug, Clone, Queryable, Identifiable, Insertable, AsChangeset, Serialize, Deserialize)]
#[diesel(table_name = semaphores)]
#[diesel(primary_key(semaphore_key, tenant_id))]
pub struct Semaphore {
    // The key representing the semaphore
    pub semaphore_key: String,
    // The tenant_id associated with the lock
    pub tenant_id: String,
    // record version of the lock in database. This is what tells the lock client when the lock is stale.
    pub version: String,
    // Whether the item in database is marked as locked
    pub max_size: i32,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
    // The data stored in the lock (can be empty)
    pub data: Option<String>,
    pub created_by: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub updated_by: Option<String>,
    pub updated_at: Option<NaiveDateTime>,
}

impl Semaphore {
    pub(crate) fn clone_with_tenant_id(&self, other: &Semaphore, tenant_id: &str) -> Semaphore {
        Semaphore {
            semaphore_key: self.semaphore_key.clone(),
            tenant_id: tenant_id.to_string(),
            version: self.version.clone(),
            data: other.data.clone(),
            max_size: other.max_size,
            lease_duration_ms: other.lease_duration_ms,
            created_at: other.created_at.clone(),
            created_by: other.created_by.clone(),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    fn to_lock(&self, rank: i32, tenant_id: &str) -> MutexLock {
        let key = format!("{}_{:0width$}", self.semaphore_key, rank, width = 10);
        MutexLock {
            mutex_key: key,
            tenant_id: tenant_id.to_string(),
            version: Uuid::new_v4().to_string(),
            lease_duration_ms: self.lease_duration_ms,
            semaphore_key: Some(self.semaphore_key.clone()),
            data: self.data.clone(),
            delete_on_release: Some(false),
            locked: Some(false),
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn generate_mutexes(&self, from: i32) -> Vec<MutexLock> {
        let items = (from..self.max_size).map(|i| self.to_lock(i, self.tenant_id.as_str())).collect();
        items
    }

    pub fn updated_at_string(&self) -> String {
        let date = self.updated_at.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        date.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
    }
}

impl PartialEq for Semaphore {
    fn eq(&self, other: &Self) -> bool {
        self.semaphore_key == other.semaphore_key
    }
}

impl Hash for Semaphore {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.semaphore_key.hash(hasher);
    }
}

impl Display for Semaphore {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "key={} version={} tenant_id={}, lease_duration_ms={}",
               self.semaphore_key, self.version, self.tenant_id, self.lease_duration_ms)
    }
}

#[derive(Debug, Clone)]
pub struct SemaphoreBuilder {
    // The key representing the semaphore
    pub semaphore_key: String,
    // The data stored in the lock (can be empty)
    pub data: Option<String>,
    // Whether the item in database is marked as locked
    pub max_size: i32,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
}

impl SemaphoreBuilder {
    pub fn new(key: &str, max_size: i32) -> Self {
        SemaphoreBuilder {
            semaphore_key: key.to_string(),
            data: None,
            max_size,
            lease_duration_ms: DEFAULT_LEASE_PERIOD,
        }
    }

    pub fn build(&self) -> Semaphore {
        Semaphore {
            semaphore_key: self.semaphore_key.clone(),
            tenant_id: "".to_string(),
            version: Uuid::new_v4().to_string(),
            max_size: self.max_size,
            lease_duration_ms: self.lease_duration_ms,
            data: self.data.clone(),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
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

    pub fn with_opt_data(&mut self, data: &Option<String>) -> &mut Self {
        self.data = data.as_ref().map(|d| d.clone());
        self
    }

    pub fn with_data(&mut self, data: &str) -> &mut Self {
        self.data = Some(data.to_string());
        self
    }
}

// LocksConfig represents configuration for lock manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocksConfig {
    // The tenant_id associated with the lock
    pub tenant_id: Option<String>,
    // How often to update database to note that the instance is still running (recommendation
    // is to make this at least 3 times smaller than the leaseDuration -- for example
    // heartbeat_period_ms = 1000, lease_duration_ms=10000 could be a reasonable configuration,
    // make sure to include a buffer for network latency.
    pub heartbeat_period_ms: Option<i64>,
    // The name of table for locks_table when using DDB or Redis
    pub locks_table_name: Option<String>,
    // The name of table for semaphores_table when using DDB or Redis
    pub semaphores_table_name: Option<String>,

    pub use_cache: Option<bool>,

    // The max size of semaphores
    pub max_semaphore_size: Option<usize>,

    // database-url for diesel
    pub database_url: Option<String>,

    // database-pool-size for diesel
    pub database_pool_size: Option<u32>,

    // flag to indicate database migrations
    pub run_database_migrations: Option<bool>,

    // aws region for dynamo DDB
    pub aws_region: Option<String>,

    // use read consistency for DDB
    pub ddb_read_consistency: Option<bool>,

    // max attempts to retry server error such as quota exceeded
    pub server_error_retries_limit: Option<u32>,

    // wait between retries with exponential backoff
    pub wait_before_server_error_retries_ms: Option<u64>,

}

impl LocksConfig {
    pub fn new(tenant_id: &str) -> Self {
        LocksConfig {
            tenant_id: Some(tenant_id.to_string()),
            heartbeat_period_ms: None,
            locks_table_name: None,
            semaphores_table_name: None,
            use_cache: None,
            max_semaphore_size: None,
            database_url: None,
            database_pool_size: None,
            run_database_migrations: Some(true),
            aws_region: None,
            ddb_read_consistency: None,
            server_error_retries_limit: None,
            wait_before_server_error_retries_ms: None,
        }
    }

    pub fn get_tenant_id(&self) -> String {
        self.tenant_id.clone().unwrap_or_else(|| "test_tenant_id".to_string())
    }

    pub fn should_run_database_migrations(&self) -> bool {
        self.run_database_migrations.unwrap_or_else(|| false)
    }

    pub fn should_use_cache(&self) -> bool {
        self.use_cache.unwrap_or_else(|| true)
    }

    pub fn get_database_url(&self) -> String {
        self.database_url.clone().unwrap_or_else(|| env::var("DATABASE_URL").unwrap_or_else(|_| "test_db.sqlite".to_string()))
    }

    pub fn get_database_pool_size(&self) -> u32 {
        self.database_pool_size.unwrap_or_else(|| 32)
    }

    pub fn get_max_semaphore_size(&self) -> usize {
        self.max_semaphore_size.unwrap_or_else(|| 1000)
    }

    pub fn get_heartbeat_period_ms(&self) -> i64 {
        self.heartbeat_period_ms.unwrap_or_else(|| DEFAULT_HEARTBEAT_PERIOD)
    }

    pub fn get_locks_table_name(&self) -> String {
        self.locks_table_name.clone().unwrap_or_else(|| "mutexes".to_string())
    }

    pub fn get_semaphores_table_name(&self) -> String {
        self.locks_table_name.clone().unwrap_or_else(|| "semaphores".to_string())
    }

    pub fn get_aws_region(&self) -> String {
        self.aws_region.clone().unwrap_or_else(|| "us-west-2".to_string())
    }

    pub fn has_ddb_read_consistency(&self) -> bool {
        self.ddb_read_consistency.unwrap_or_else(|| true)
    }

    pub fn get_server_error_retries_limit(&self) -> u32 {
        self.server_error_retries_limit.unwrap_or_else(|| 20)
    }

    pub fn get_wait_before_server_error_retries_ms(&self) -> u64 {
        self.wait_before_server_error_retries_ms.unwrap_or_else(|| 250)
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
    pub requires_semaphore: Option<bool>,
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
            semaphore_key: if self.does_use_semaphore() { Some(self.key.clone()) } else { Some(DEFAULT_SEMAPHORE_KEY.to_string()) },
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

    pub fn to_semaphore(&self, tenant_id: &str, max_size: i32) -> Semaphore {
        Semaphore {
            semaphore_key: self.key.clone(),
            tenant_id: tenant_id.to_string(),
            version: Uuid::new_v4().to_string(),
            data: self.data.clone(),
            max_size,
            lease_duration_ms: self.lease_duration_ms,
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
            semaphore_key: if self.does_use_semaphore() { Some(self.key.clone()) } else { Some(DEFAULT_SEMAPHORE_KEY.to_string()) },
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
        self.replace_data.unwrap_or_else(|| false)
    }

    pub fn is_acquire_only_if_already_exists(&self) -> bool {
        self.requires_semaphore.unwrap_or_else(|| false) || self.acquire_only_if_already_exists.unwrap_or_else(|| false)
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
        self.requires_semaphore.unwrap_or_else(|| false)
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
    pub requires_semaphore: Option<bool>,
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
            requires_semaphore: None,
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
            requires_semaphore: self.requires_semaphore,
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

    pub fn with_opt_data(&mut self, data: &Option<String>) -> &mut Self {
        self.data = data.as_ref().map(|d| d.clone());
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
    pub fn with_requires_semaphore(&mut self, requires_semaphore: bool) -> &mut Self {
        self.requires_semaphore = Some(requires_semaphore);
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
        self.delete_lock.unwrap_or_else(|| false)
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
        self.semaphore_key = semaphore_key.as_ref().map(|k| k.clone());
        self
    }

    pub fn with_data(&mut self, data: &str) -> &mut Self {
        self.data = Some(data.to_string());
        self
    }

    pub fn with_opt_data(&mut self, data: &Option<String>) -> &mut Self {
        self.data = data.as_ref().map(|d| d.clone());
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
        self.delete_data.unwrap_or_else(|| false)
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
        self.semaphore_key = semaphore_key.as_ref().map(|k| k.clone());
        self
    }

    pub fn with_data(&mut self, data: &str) -> &mut Self {
        self.data = Some(data.to_string());
        self
    }

    pub fn with_opt_data(&mut self, data: &Option<String>) -> &mut Self {
        self.data = data.as_ref().map(|d| d.clone());
        self
    }
}


#[derive(Subcommand, Debug, Clone)]
pub enum CommandActions {
    Acquire {
        /// key of mutex or semaphore to acquire
        #[arg(short, long)]
        key: String,

        // How long the lease for the lock is (in seconds)
        #[arg(short, long, default_value_t = 15)]
        lease: i64,

        // If this requires a semaphore previously created
        #[arg(short, long)]
        semaphore: Option<bool>,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    Heartbeat {
        /// key of mutex to renew lease
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,

        // How long the lease for the lock is (in seconds)
        #[arg(short, long, default_value_t = 15)]
        lease: i64,

        // If this requires a semaphore previously created
        #[arg(short, long)]
        semaphore_key: Option<String>,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    Release {
        /// key of mutex to release
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,

        // If this requires a semaphore previously created
        #[arg(short, long)]
        semaphore_key: Option<String>,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    GetMutex {
        /// key of mutex to retrieve
        #[arg(short, long)]
        key: String,
    },
    DeleteMutex {
        /// key of mutex to delete
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,

        // If this requires a semaphore previously created
        #[arg(short, long)]
        semaphore_key: Option<String>,
    },
    CreateSemaphore {
        /// key of semaphore to create
        #[arg(short, long)]
        key: String,

        // The number of locks in semaphores
        #[arg(short, long)]
        max_size: i64,

        // How long the lease for the lock is (in seconds)
        #[arg(short, long, default_value_t = 15)]
        lease: i64,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    GetSemaphore {
        /// key of semaphore to retrieve
        #[arg(short, long)]
        key: String,
    },
    DeleteSemaphore {
        /// key of semaphore to delete
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,
    },
    GetSemaphoreMutexes {
        /// key of semaphore for retrieving mutexes
        #[arg(short, long)]
        key: String,
    },
}

/// Database based Mutexes and Semaphores
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(next_line_help = true)]
pub struct Args {
    /// Action to perform
    #[command(subcommand)]
    pub action: CommandActions,

    /// Database provider
    #[arg(value_enum, default_value = "rdb")]
    pub provider: RepositoryProvider,

    /// tentant-id for the database
    #[arg(short, long, default_value = "default")]
    pub tenant: String,

    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_create_database_error() {
        let err = LockError::database("test error", None, false);
        let err_msg = format!("{}", err);
        assert_eq!("test error None false", err_msg);
    }

    #[test]
    fn test_should_create_not_granted_error() {
        let err = LockError::not_granted("test error", None);
        let err_msg = format!("{}", err);
        assert_eq!("test error None", err_msg);
    }

    #[test]
    fn test_should_create_not_found_error() {
        let err = LockError::not_found("test error");
        let err_msg = format!("{}", err);
        assert_eq!("test error", err_msg);
    }

    #[test]
    fn test_should_create_unavailable_error() {
        let err = LockError::unavailable("test error", None, true);
        let err_msg = format!("{}", err);
        assert_eq!("test error None true", err_msg);
    }

    #[test]
    fn test_should_create_validation_error() {
        let err = LockError::validation("test error", None);
        let err_msg = format!("{}", err);
        assert_eq!("test error None", err_msg);
    }

    #[test]
    fn test_should_build_lock_item() {
        let lock = AcquireLockOptionsBuilder::new("key1")
            .with_lease_duration_secs(100)
            .with_data("data1")
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
        assert_eq!(100000, lock.lease_duration_ms);
        assert_eq!(10, lock.get_refresh_period_ms());
        assert_eq!(5, lock.get_additional_time_to_wait_for_lock_ms());
        assert_eq!(5, lock.get_override_time_to_wait_for_lock_ms());
        assert_eq!(true, lock.delete_on_release.unwrap());
        assert_eq!("data1", lock.data.unwrap());
    }

    #[test]
    fn test_should_build_release_options_from_lock_item() {
        let lock = AcquireLockOptionsBuilder::new("key2")
            .with_lease_duration_millis(20)
            .with_data("data2")
            .with_replace_data(true)
            .with_delete_on_release(true)
            .with_reentrant(true)
            .with_acquire_only_if_already_exists(true)
            .with_refresh_period_ms(10)
            .with_additional_time_to_wait_for_lock_ms(5)
            .build().to_locked_mutex("tenant_id1");
        let options = lock.to_release_options();
        assert_eq!("key2", options.key);
        assert_eq!(false, options.is_delete_lock());
        assert_eq!("data2", options.data.unwrap());
    }

    #[test]
    fn test_should_build_heartbeat_options_from_lock_item() {
        let lock = AcquireLockOptionsBuilder::new("key3")
            .with_lease_duration_millis(20)
            .with_data("data3")
            .with_replace_data(true)
            .with_delete_on_release(true)
            .with_reentrant(true)
            .with_acquire_only_if_already_exists(true)
            .with_refresh_period_ms(10)
            .with_additional_time_to_wait_for_lock_ms(5)
            .build().to_locked_mutex("tenant_id2");
        let options = lock.to_heartbeat_options();
        assert_eq!("key3", options.key);
        assert_eq!(false, options.is_delete_data());
        assert_eq!(20, options.lease_duration_to_ensure_ms);
        assert_eq!("data3", options.data.unwrap());
    }

    #[test]
    fn test_should_build_semaphore() {
        let semaphore = SemaphoreBuilder::new("key5", 200)
            .with_lease_duration_millis(20)
            .with_data("data1")
            .build();
        assert_eq!("key5", semaphore.semaphore_key);
        assert_eq!(20, semaphore.lease_duration_ms);
        assert_eq!(200, semaphore.max_size);
        assert_eq!("data1", semaphore.data.unwrap());
    }

    #[test]
    fn test_should_serialize_semaphore() {
        let semaphore = SemaphoreBuilder::new("key5", 200)
            .with_lease_duration_millis(20)
            .with_data("data1")
            .build();
        let s: String = serde_json::to_string(&semaphore).unwrap();
        assert!(s.as_str().contains("data1"));
        let v = serde_json::to_value(&semaphore).unwrap();
        assert!(v.to_string().as_str().contains("data1"));
    }
}



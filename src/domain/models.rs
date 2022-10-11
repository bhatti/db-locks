use std::env;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Add;
use clap::ValueEnum;

use aws_sdk_dynamodb::model::AttributeValue;
use chrono::{Duration, NaiveDateTime, Utc};
use diesel::prelude::*;
use gethostname::gethostname;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::domain::error::LockError;
use crate::domain::options::{AcquireLockOptions, ReleaseLockOptions, SendHeartbeatOptions};

use crate::domain::schema::*;
use crate::utils;

pub(crate) const DEFAULT_LEASE_PERIOD: i64 = 15000;

const DEFAULT_HEARTBEAT_PERIOD: i64 = 5000;

pub(crate) const DEFAULT_SEMAPHORE_KEY: &str = "DEFAULT";

#[derive(Debug, Copy, Clone, PartialOrd, Ord, ValueEnum)]
pub enum RepositoryProvider {
    Rdb,
    Ddb,
    Redis,
}

impl PartialEq for RepositoryProvider {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (RepositoryProvider::Rdb, RepositoryProvider::Rdb) |
            (RepositoryProvider::Ddb, RepositoryProvider::Ddb) |
            (RepositoryProvider::Redis, RepositoryProvider::Redis))
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
        let page_num: usize = page.unwrap_or("0").parse().unwrap_or(0);
        let next_page = if records.len() < page_size { None } else { Some((page_num + 1).to_string()) };
        PaginatedResult::new(page, next_page, page_size, 0, records)
    }

    pub fn from_redis(
        next_token: &str,
        page: Option<&str>,
        page_size: usize,
        records: Vec<T>) -> Self {
        if page != None && next_token == "0" {
            PaginatedResult::new(page, Some("-1".to_string()), page_size, 0, records)
        } else {
            PaginatedResult::new(page, Some(next_token.to_string()), page_size, 0, records)
        }
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
            created_at: self.created_at,
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

    pub fn locked_clone(&self) -> Self {
        MutexLock {
            mutex_key: self.mutex_key.clone(),
            tenant_id: self.tenant_id.clone(),
            version: self.version.clone(),
            semaphore_key: self.semaphore_key.clone(),
            data: self.data.clone(),
            delete_on_release: self.delete_on_release,
            locked: Some(true),
            lease_duration_ms: self.lease_duration_ms,
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: self.created_at,
            created_by: self.created_by.clone(),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }


    pub fn get_lease_duration_secs(&self) -> usize {
        (self.lease_duration_ms / 1000) as usize
    }

    pub fn expired(&self) -> bool {
        if let (Some(locked), Some(expires_at)) = (self.locked, self.expires_at) {
            return !locked || expires_at.timestamp_millis() < utils::current_time_ms();
        }
        self.locked == None || self.expires_at == None
    }

    pub fn key_rank(&self) -> Option<i32> {
        if self.semaphore_key != None {
            let parts: Vec<&str> = self.mutex_key.split('_').collect();
            if parts.len() > 1 {
                if let Ok(n) = parts[parts.len() - 1].parse() {
                    return Some(n);
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

    pub fn full_key(&self, prefix: &str) -> String {
        MutexLock::build_full_key(prefix, self.mutex_key.as_str(), self.tenant_id.as_str())
    }

    pub fn build_full_key(prefix: &str, other_key: &str, other_tenant_key: &str) -> String {
        format!("{}{}_{}", prefix, other_key, other_tenant_key)
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
            return !Semaphore::is_default_semaphore(sem_key.as_str());
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
    // Number of busy locks
    pub busy_count: Option<i32>,
    // fair semaphore lock
    pub fair_semaphore: Option<bool>,
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
            max_size: other.max_size,
            lease_duration_ms: other.lease_duration_ms,
            busy_count: other.busy_count,
            fair_semaphore: None,
            created_at: other.created_at,
            created_by: other.created_by.clone(),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub(crate) fn build_key_rank(key: &str, rank: i32) -> String {
        format!("{}_{:0width$}", key, rank, width = 10)
    }

    pub fn is_default_semaphore(sem_key: &str) -> bool {
        sem_key == DEFAULT_SEMAPHORE_KEY || sem_key.is_empty()
    }

    fn to_mutex_with_rank(&self, rank: i32, tenant_id: &str) -> MutexLock {
        let key = Semaphore::build_key_rank(self.semaphore_key.as_str(), rank);
        MutexLock {
            mutex_key: key,
            tenant_id: tenant_id.to_string(),
            version: Uuid::new_v4().to_string(),
            lease_duration_ms: self.lease_duration_ms,
            semaphore_key: Some(self.semaphore_key.clone()),
            data: None,
            delete_on_release: Some(false),
            locked: Some(false),
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub(crate) fn to_mutex_with_key_version(&self, key: &str, version: &str, locked: bool) -> MutexLock {
        MutexLock {
            mutex_key: key.to_string(),
            tenant_id: self.tenant_id.to_string(),
            version: version.to_string(),
            lease_duration_ms: self.lease_duration_ms,
            semaphore_key: Some(self.semaphore_key.clone()),
            data: None,
            delete_on_release: Some(false),
            locked: Some(locked),
            expires_at: Some(Utc::now().naive_utc().add(Duration::milliseconds(self.lease_duration_ms))),
            created_at: Some(Utc::now().naive_utc()),
            created_by: Some(String::from("")),
            updated_at: Some(Utc::now().naive_utc()),
            updated_by: Some(String::from("")),
        }
    }

    pub fn generate_mutexes(&self, from: i32) -> Vec<MutexLock> {
        (from..self.max_size).map(|i| self.to_mutex_with_rank(i, self.tenant_id.as_str())).collect()
    }

    pub fn updated_at_string(&self) -> String {
        let date = self.updated_at.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        date.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
    }

    pub fn is_fair_semaphore(&self) -> bool {
        self.fair_semaphore.unwrap_or(false)
    }

    pub fn full_key(&self, prefix: &str) -> String {
        MutexLock::build_full_key(prefix, self.semaphore_key.as_str(), self.tenant_id.as_str())
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
        write!(f, "key={} version={} tenant_id={}, lease_duration_ms={}, max_size={}, busy={:?}, fair={:?}",
               self.semaphore_key, self.version, self.tenant_id,
               self.lease_duration_ms, self.max_size, self.busy_count, self.fair_semaphore)
    }
}

#[derive(Debug, Clone)]
pub struct SemaphoreBuilder {
    // The key representing the semaphore
    pub semaphore_key: String,
    // Whether the item in database is marked as locked
    pub max_size: i32,
    // How long the lease for the lock is (in milliseconds)
    pub lease_duration_ms: i64,
    // fair semaphore lock
    pub fair_semaphore: Option<bool>,
}

impl SemaphoreBuilder {
    pub fn new(key: &str, max_size: i32) -> Self {
        SemaphoreBuilder {
            semaphore_key: key.to_string(),
            max_size,
            lease_duration_ms: DEFAULT_LEASE_PERIOD,
            fair_semaphore: None,
        }
    }

    pub fn build(&self) -> Semaphore {
        Semaphore {
            semaphore_key: self.semaphore_key.clone(),
            tenant_id: get_default_tenant(),
            version: Uuid::new_v4().to_string(),
            max_size: self.max_size,
            lease_duration_ms: self.lease_duration_ms,
            busy_count: None,
            fair_semaphore: self.fair_semaphore,
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

    // fair semaphore lock
    pub fn with_fair_semaphore(&mut self, fair_semaphore: bool) -> &mut Self {
        self.fair_semaphore = Some(fair_semaphore);
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
    // The name of table for mutexes when using DDB or Redis
    pub mutexes_table_name: Option<String>,
    // The name of table for semaphores when using DDB or Redis
    pub semaphores_table_name: Option<String>,

    pub fair_semaphore: Option<bool>,

    pub cache_enabled: Option<bool>,

    // The max size of semaphores
    pub max_semaphore_size: Option<i32>,

    // database-url for diesel
    pub database_url: Option<String>,

    // redis url, e.g. redis://127.0.0.1 or rediss://127.0.0.1/
    // redis://:password@127.0.0.1:6379
    // cluster not supported but you will need to specify list of ip-addresses
    pub redis_url: Option<String>,

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
            mutexes_table_name: None,
            semaphores_table_name: None,
            fair_semaphore: None,
            cache_enabled: None,
            max_semaphore_size: None,
            database_url: None,
            redis_url: None,
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
        self.run_database_migrations.unwrap_or(false)
    }

    pub fn is_cache_enabled(&self) -> bool {
        self.cache_enabled.unwrap_or(true)
    }

    pub fn is_fair_semaphore(&self) -> bool {
        self.fair_semaphore.unwrap_or(false)
    }

    pub fn get_database_url(&self) -> String {
        self.database_url.clone().unwrap_or_else(|| env::var("DATABASE_URL").unwrap_or_else(|_| "test_db.sqlite".to_string()))
    }

    pub fn get_database_pool_size(&self) -> u32 {
        self.database_pool_size.unwrap_or(32)
    }

    pub fn get_max_semaphore_size(&self) -> i32 {
        self.max_semaphore_size.unwrap_or(1000)
    }

    pub fn get_heartbeat_period_ms(&self) -> i64 {
        self.heartbeat_period_ms.unwrap_or(DEFAULT_HEARTBEAT_PERIOD)
    }

    pub fn get_mutexes_table_name(&self) -> String {
        self.mutexes_table_name.clone().unwrap_or_else(|| "mutexes".to_string())
    }

    pub fn get_redis_url(&self) -> String {
        self.redis_url.clone().unwrap_or_else(|| env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1".to_string()))
    }

    pub fn get_semaphores_table_name(&self) -> String {
        self.mutexes_table_name.clone().unwrap_or_else(|| "semaphores".to_string())
    }

    pub fn get_aws_region(&self) -> String {
        self.aws_region.clone().unwrap_or_else(|| "us-west-2".to_string())
    }

    pub fn has_ddb_read_consistency(&self) -> bool {
        self.ddb_read_consistency.unwrap_or(true)
    }

    pub fn get_server_error_retries_limit(&self) -> u32 {
        self.server_error_retries_limit.unwrap_or(20)
    }

    pub fn get_wait_before_server_error_retries_ms(&self) -> u64 {
        self.wait_before_server_error_retries_ms.unwrap_or(250)
    }
}

pub(crate) fn get_default_tenant() -> String {
    gethostname().to_str().unwrap_or("localhost").to_string()
}

#[cfg(test)]
mod tests {
    use crate::domain::options::{AcquireLockOptionsBuilder, ReleaseLockOptionsBuilder, SendHeartbeatOptionsBuilder};
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
            .with_opt_semaphore_max_size(&Some(1))
            .with_semaphore_max_size(2)
            .build();
        assert_eq!("key1", lock.key);
        assert!(lock.is_reentrant());
        assert!(lock.is_acquire_only_if_already_exists());
        assert_eq!(100000, lock.lease_duration_ms);
        assert_eq!(10, lock.get_refresh_period_ms());
        assert_eq!(5, lock.get_additional_time_to_wait_for_lock_ms());
        assert_eq!(5, lock.get_override_time_to_wait_for_lock_ms());
        assert!(lock.delete_on_release.unwrap());
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
        assert!(!options.is_delete_lock());
        assert_eq!("data2", options.data.unwrap());
    }

    #[test]
    fn test_should_build_release_options() {
        let opts = ReleaseLockOptionsBuilder::new("key", "version")
            .with_opt_data(&Some("1".to_string()))
            .with_opt_semaphore_key(&Some("sem".to_string()))
            .with_semaphore_key("sem1")
            .with_delete_lock(true)
            .with_data("data2")
            .build();
        assert_eq!("key", opts.key);
        assert_eq!("sem1", opts.get_semaphore_key());
        assert!(opts.is_delete_lock());
        assert_eq!("data2", opts.data.unwrap());
    }

    #[test]
    fn test_should_build_heartbeat_options_from_lock_item() {
        let lock = AcquireLockOptionsBuilder::new("key3")
            .with_lease_duration_millis(20)
            .with_opt_data(&Some("".to_string()))
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
        assert!(!options.is_delete_data());
        assert_eq!(20, options.lease_duration_to_ensure_ms);
        assert_eq!("data3", options.data.unwrap());
    }

    #[test]
    fn test_should_build_heartbeat_options() {
        let opts = SendHeartbeatOptionsBuilder::new("key1", "version1")
            .with_delete_data(true)
            .with_lease_duration_millis(0)
            .with_lease_duration_secs(1)
            .with_lease_duration_minutes(1)
            .with_lease_duration_secs(1)
            .with_opt_semaphore_key(&Some("key".to_string()))
            .with_semaphore_key("key2")
            .with_opt_data(&Some("data".to_string()))
            .with_data("data1").build();
        assert_eq!("key1", opts.key);
        assert_eq!("version1", opts.version);
        assert_eq!("key2", opts.get_semaphore_key());
    }


    #[test]
    fn test_should_build_semaphore() {
        let semaphore = SemaphoreBuilder::new("key5", 200)
            .with_lease_duration_millis(20)
            .build();
        assert_eq!("key5", semaphore.semaphore_key);
        assert_eq!(20, semaphore.lease_duration_ms);
        assert_eq!(200, semaphore.max_size);
    }

    #[test]
    fn test_should_serialize_semaphore() {
        let semaphore = SemaphoreBuilder::new("key5", 200)
            .with_lease_duration_millis(20)
            .with_lease_duration_secs(2)
            .with_lease_duration_minutes(2)
            .with_fair_semaphore(true)
            .build();
        let s: String = serde_json::to_string(&semaphore).unwrap();
        assert!(s.as_str().contains("key5"));
        let v = serde_json::to_value(&semaphore).unwrap();
        assert!(v.to_string().as_str().contains("key5"));
        assert_eq!(semaphore.semaphore_key, semaphore.to_mutex_with_key_version("id", "ver", true).get_semaphore_key());
        assert!(semaphore.fair_semaphore.unwrap());
    }

    #[test]
    fn test_should_create_config() {
        let config = LocksConfig::new("id");
        assert_eq!(config.get_tenant_id().as_str(), "id");
        assert!(config.should_run_database_migrations());
        assert!(config.is_cache_enabled());
        assert_eq!("test_db.sqlite", config.get_database_url().as_str());
        assert_eq!(32, config.get_database_pool_size());
        assert_eq!(1000, config.get_max_semaphore_size());
        assert_eq!(DEFAULT_HEARTBEAT_PERIOD, config.get_heartbeat_period_ms());
        assert_eq!("mutexes", config.get_mutexes_table_name());
        assert_eq!("redis://127.0.0.1", config.get_redis_url());
        assert_eq!("semaphores", config.get_semaphores_table_name());
        assert_eq!("us-west-2", config.get_aws_region());
        assert!(config.has_ddb_read_consistency());
        assert_eq!(20, config.get_server_error_retries_limit());
        assert_eq!(250, config.get_wait_before_server_error_retries_ms());
        assert_eq!(DEFAULT_HEARTBEAT_PERIOD, None.unwrap_or(DEFAULT_HEARTBEAT_PERIOD));
    }
}


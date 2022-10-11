use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use prometheus::default_registry;

use crate::domain::models::{LockResult, LocksConfig, MutexLock, PaginatedResult, RepositoryProvider, Semaphore};
use crate::utils::lock_metrics::LockMetrics;

pub mod factory;
pub mod rdb_common;
pub mod orm_mutex_repository;
pub mod orm_semaphore_repository;
pub mod ddb_mutex_repository;
pub mod ddb_semaphore_repository;
pub mod redis_mutex_repository;
pub mod redis_semaphore_repository;
pub mod ddb_common;
pub mod redis_fair_semaphore_repository;
mod mutex_repository_tests;
mod semaphore_repository_tests;
mod redis_common;

macro_rules! pool_decl {
    () => {
        Pool<ConnectionManager<SqliteConnection>>
        //Pool<ConnectionManager<PgConnection>>
    };
}

macro_rules! build_pool {
    ($database_url:expr) => {
        rdb_common::build_sqlite_pool($database_url).expect("failed to create db")
        //rdb_common::build_pg_pool($database_url).expect("failed to create db")
    };
}

// allow access to macros - must be below macro definition
pub(crate) use build_pool;
pub(crate) use pool_decl;
use crate::domain::error::LockError;

#[async_trait]
pub trait MutexRepository {
    // run migrations or create table if necessary
    async fn setup_database(&self, recreate: bool) -> LockResult<()>;

    // create lock item
    async fn create(&self, mutex: &MutexLock) -> LockResult<usize>;

    // updates existing lock item for acquire
    async fn acquire_update(&self,
                            old_version: &str,
                            mutex: &MutexLock) -> LockResult<usize>;

    // updates existing lock item for heartbeat
    async fn heartbeat_update(&self,
                              old_version: &str,
                              mutex: &MutexLock) -> LockResult<usize>;

    // updates existing lock item for release
    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            other_data: Option<&str>) -> LockResult<usize>;

    // get lock by key
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<MutexLock>;

    // delete lock
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str) -> LockResult<usize>;

    // delete expired lock
    async fn delete_expired_lock(&self, other_key: &str,
                                 other_tenant_id: &str) -> LockResult<usize>;

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>>;

    // find by semaphore
    async fn find_by_semaphore(&self,
                               semaphore_key: &str,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>>;

    async fn ping(&self) -> LockResult<()>;

    fn eventually_consistent(&self) -> bool;
}

#[async_trait]
pub trait SemaphoreRepository {
    // run migrations or create table if necessary
    async fn setup_database(&self, recreate: bool) -> LockResult<()>;

    // create semaphore
    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize>;

    // updates existing semaphore item
    async fn update(&self,
                    other_version: &str,
                    semaphore: &Semaphore) -> LockResult<usize>;

    // find by key and tenant_id
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<Semaphore>;

    // delete semaphore
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str) -> LockResult<usize>;

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<Semaphore>>;

    async fn ping(&self) -> LockResult<()>;

    fn eventually_consistent(&self) -> bool;
}

#[async_trait]
pub trait FairSemaphoreRepository {
    // run migrations or create table if necessary
    async fn setup_database(&self, recreate: bool) -> LockResult<()>;

    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize>;

    // find by key and tenant_id
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<Semaphore>;

    // delete semaphore
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str) -> LockResult<usize>;

    // updates existing lock item for acquire
    async fn acquire_update(&self,
                            semaphore: &Semaphore) -> LockResult<MutexLock>;

    // updates existing lock item for heartbeat
    async fn heartbeat_update(&self,
                              other_key: &str,
                              other_tenant_id: &str,
                              other_version: &str,
                              lease_duration_ms: i64) -> LockResult<usize>;

    // updates existing lock item for release
    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            other_data: Option<&str>) -> LockResult<usize>;

    // find locks by semaphore
    async fn get_semaphore_mutexes(&self,
                                   other_key: &str,
                                   other_tenant_id: &str,
    ) -> LockResult<Vec<MutexLock>>;

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<Semaphore>>;

    async fn ping(&self) -> LockResult<()>;

    fn eventually_consistent(&self) -> bool;
}

async fn invoke_with_retry_attempts<T, F: Fn() -> R, R>(config: &LocksConfig, msg: &str, f: F) -> LockResult<T>
    where
        R: Future<Output=LockResult<T>> {
    for i in 0..(config.get_server_error_retries_limit()) {
        match f().await {
            Ok(k) => {
                return Ok(k);
            }
            Err(err) => {
                if !err.retryable() || i == config.get_server_error_retries_limit() - 1 {
                    if let LockError::NotFound { .. } = err {} else {
                        log::warn!("error while invoking not retryable function {} due to {:?}", msg, err);
                    }
                    return Err(err);
                }
                let delay = Duration::from_millis(config.get_wait_before_server_error_retries_ms() * (i + 1) as u64);
                log::warn!("error while invoking retryable function {} due to {:?}, will retry {}th time with delay {:?}", msg, err, i, delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(LockError::runtime("error retrying", None))
}

pub(crate) struct RetryableMutexRepository {
    config: LocksConfig,
    delegate: Box<dyn MutexRepository + Send + Sync>,
}

impl RetryableMutexRepository {
    fn new(config: &LocksConfig, delegate: Box<dyn MutexRepository + Send + Sync>) -> Self {
        RetryableMutexRepository {
            config: config.clone(),
            delegate,
        }
    }
}

#[async_trait]
impl MutexRepository for RetryableMutexRepository {
    async fn setup_database(&self, recreate: bool) -> LockResult<()> {
        invoke_with_retry_attempts(&self.config, "setup_database", || async {
            self.delegate.setup_database(recreate).await
        }).await
    }

    async fn create(&self, mutex: &MutexLock) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "create", || async {
            self.delegate.create(mutex).await
        }).await
    }

    async fn acquire_update(&self, old_version: &str, mutex: &MutexLock) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "acquire_update", || async {
            self.delegate.acquire_update(old_version, mutex).await
        }).await
    }

    async fn heartbeat_update(&self, old_version: &str, mutex: &MutexLock) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "heartbeat_update", || async {
            self.delegate.heartbeat_update(old_version, mutex).await
        }).await
    }

    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            other_data: Option<&str>) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "release_update", || async {
            self.delegate.release_update(other_key, other_tenant_id, other_version, other_data).await
        }).await
    }

    async fn get(&self, other_key: &str, other_tenant_id: &str) -> LockResult<MutexLock> {
        invoke_with_retry_attempts(&self.config, "get", || async {
            self.delegate.get(other_key, other_tenant_id).await
        }).await
    }

    async fn delete(&self, other_key: &str, other_tenant_id: &str, other_version: &str) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "delete", || async {
            self.delegate.delete(other_key, other_tenant_id, other_version).await
        }).await
    }

    async fn delete_expired_lock(&self, other_key: &str, other_tenant_id: &str) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "delete_expired_lock", || async {
            self.delegate.delete_expired_lock(other_key, other_tenant_id).await
        }).await
    }

    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize) -> LockResult<PaginatedResult<MutexLock>> {
        invoke_with_retry_attempts(&self.config, "find_by_tenant_id", || async {
            self.delegate.find_by_tenant_id(other_tenant_id, page, page_size).await
        }).await
    }

    async fn find_by_semaphore(&self,
                               semaphore_key: &str,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize) -> LockResult<PaginatedResult<MutexLock>> {
        invoke_with_retry_attempts(&self.config, "find_by_semaphore", || async {
            self.delegate.find_by_semaphore(semaphore_key, other_tenant_id, page, page_size).await
        }).await
    }

    // no retry
    async fn ping(&self) -> LockResult<()> {
        self.delegate.ping().await
    }

    fn eventually_consistent(&self) -> bool {
        self.delegate.eventually_consistent()
    }
}

pub(crate) struct RetryableFairSemaphoreRepository {
    config: LocksConfig,
    delegate: Box<dyn FairSemaphoreRepository + Send + Sync>,
}

impl RetryableFairSemaphoreRepository {
    fn new(config: &LocksConfig, delegate: Box<dyn FairSemaphoreRepository + Send + Sync>) -> Self {
        RetryableFairSemaphoreRepository {
            config: config.clone(),
            delegate,
        }
    }
}

#[async_trait]
impl FairSemaphoreRepository for RetryableFairSemaphoreRepository {
    async fn setup_database(&self, recreate: bool) -> LockResult<()> {
        invoke_with_retry_attempts(&self.config, "setup_database", || async {
            self.delegate.setup_database(recreate).await
        }).await
    }

    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "create", || async {
            self.delegate.create(semaphore).await
        }).await
    }

    async fn get(&self, other_key: &str, other_tenant_id: &str) -> LockResult<Semaphore> {
        invoke_with_retry_attempts(&self.config, "get", || async {
            self.delegate.get(other_key, other_tenant_id).await
        }).await
    }

    async fn delete(&self, other_key: &str, other_tenant_id: &str, other_version: &str) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "delete", || async {
            self.delegate.delete(other_key, other_tenant_id, other_version).await
        }).await
    }

    async fn acquire_update(&self, semaphore: &Semaphore) -> LockResult<MutexLock> {
        invoke_with_retry_attempts(&self.config, "acquire_update", || async {
            self.delegate.acquire_update(semaphore).await
        }).await
    }

    async fn heartbeat_update(&self,
                              other_key: &str,
                              other_tenant_id: &str,
                              other_version: &str,
                              lease_duration_ms: i64) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "heartbeat_update", || async {
            self.delegate.heartbeat_update(other_key, other_tenant_id, other_version, lease_duration_ms).await
        }).await
    }

    async fn release_update(&self, other_key: &str, other_tenant_id: &str, other_version: &str, other_data: Option<&str>) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "release_update", || async {
            self.delegate.release_update(other_key, other_tenant_id, other_version, other_data).await
        }).await
    }

    async fn get_semaphore_mutexes(&self, other_key: &str, other_tenant_id: &str) -> LockResult<Vec<MutexLock>> {
        invoke_with_retry_attempts(&self.config, "get_semaphore_mutexes", || async {
            self.delegate.get_semaphore_mutexes(other_key, other_tenant_id).await
        }).await
    }

    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize) -> LockResult<PaginatedResult<Semaphore>> {
        invoke_with_retry_attempts(&self.config, "find_by_tenant_id", || async {
            self.delegate.find_by_tenant_id(other_tenant_id, page, page_size).await
        }).await
    }

    // No retry
    async fn ping(&self) -> LockResult<()> {
        self.delegate.ping().await
    }

    fn eventually_consistent(&self) -> bool {
        self.delegate.eventually_consistent()
    }

}

pub(crate) struct RetryableSemaphoreRepository {
    config: LocksConfig,
    delegate: Box<dyn SemaphoreRepository + Send + Sync>,
}

impl RetryableSemaphoreRepository {
    fn new(config: &LocksConfig, delegate: Box<dyn SemaphoreRepository + Send + Sync>) -> Self {
        RetryableSemaphoreRepository {
            config: config.clone(),
            delegate,
        }
    }
}

#[async_trait]
impl SemaphoreRepository for RetryableSemaphoreRepository {
    async fn setup_database(&self, recreate: bool) -> LockResult<()> {
        invoke_with_retry_attempts(&self.config, "setup_database", || async {
            self.delegate.setup_database(recreate).await
        }).await
    }

    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "create", || async {
            self.delegate.create(semaphore).await
        }).await
    }

    async fn update(&self, other_version: &str, semaphore: &Semaphore) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "update", || async {
            self.delegate.update(other_version, semaphore).await
        }).await
    }

    async fn get(&self, other_key: &str, other_tenant_id: &str) -> LockResult<Semaphore> {
        invoke_with_retry_attempts(&self.config, "get", || async {
            self.delegate.get(other_key, other_tenant_id).await
        }).await
    }

    async fn delete(&self, other_key: &str, other_tenant_id: &str, other_version: &str) -> LockResult<usize> {
        invoke_with_retry_attempts(&self.config, "delete", || async {
            self.delegate.delete(other_key, other_tenant_id, other_version).await
        }).await
    }

    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize) -> LockResult<PaginatedResult<Semaphore>> {
        invoke_with_retry_attempts(&self.config, "find_by_tenant_id", || async {
            self.delegate.find_by_tenant_id(other_tenant_id, page, page_size).await
        }).await
    }

    // No retry
    async fn ping(&self) -> LockResult<()> {
        self.delegate.ping().await
    }

    fn eventually_consistent(&self) -> bool {
        self.delegate.eventually_consistent()
    }
}

pub(crate) struct MeasurableMutexRepository {
    provider: RepositoryProvider,
    delegate: Box<dyn MutexRepository + Send + Sync>,
    metrics: LockMetrics,
}

impl MeasurableMutexRepository {
    fn new(provider: RepositoryProvider, delegate: Box<dyn MutexRepository + Send + Sync>) -> Self {
        MeasurableMutexRepository {
            provider,
            delegate,
            metrics: LockMetrics::new(format!("{}__", provider).as_str(),
                                      default_registry()).unwrap(),
        }
    }

    fn metrics_summary(&self) -> HashMap<String, f64> {
        let mut result = HashMap::new();
        for (k, v) in self.metrics.summary() {
            result.insert(k, v);
        }
        result
    }

    fn dump_metrics(&self) -> String {
        log::info!("xxxmetrics {:?}", self.metrics.summary());
        let mut out = format!("# MeasurableMutexRepository metrics dump for {}", self.provider);
        out.push_str(self.metrics.dump().as_str());
        out
    }
}

#[async_trait]
impl MutexRepository for MeasurableMutexRepository {
    async fn setup_database(&self, recreate: bool) -> LockResult<()> {
        let _metric = self.metrics.new_metric("setup_database");
        self.delegate.setup_database(recreate).await
    }

    async fn create(&self, mutex: &MutexLock) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("create");
        self.delegate.create(mutex).await
    }

    async fn acquire_update(&self, old_version: &str, mutex: &MutexLock) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("acquire_update");
        self.delegate.acquire_update(old_version, mutex).await
    }

    async fn heartbeat_update(&self, old_version: &str, mutex: &MutexLock) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("heartbeat_update");
        self.delegate.heartbeat_update(old_version, mutex).await
    }

    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            other_data: Option<&str>) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("release_update");
        self.delegate.release_update(other_key, other_tenant_id, other_version, other_data).await
    }

    async fn get(&self, other_key: &str, other_tenant_id: &str) -> LockResult<MutexLock> {
        let _metric = self.metrics.new_metric("get");
        self.delegate.get(other_key, other_tenant_id).await
    }

    async fn delete(&self, other_key: &str, other_tenant_id: &str, other_version: &str) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("delete");
        self.delegate.delete(other_key, other_tenant_id, other_version).await
    }

    async fn delete_expired_lock(&self, other_key: &str, other_tenant_id: &str) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("delete_expired_lock");
        self.delegate.delete_expired_lock(other_key, other_tenant_id).await
    }

    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize) -> LockResult<PaginatedResult<MutexLock>> {
        let _metric = self.metrics.new_metric("find_by_tenant_id");
        self.delegate.find_by_tenant_id(other_tenant_id, page, page_size).await
    }

    async fn find_by_semaphore(&self,
                               semaphore_key: &str,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize) -> LockResult<PaginatedResult<MutexLock>> {
        let _metric = self.metrics.new_metric("find_by_semaphore");
        self.delegate.find_by_semaphore(semaphore_key, other_tenant_id, page, page_size).await
    }

    async fn ping(&self) -> LockResult<()> {
        self.delegate.ping().await
    }

    fn eventually_consistent(&self) -> bool {
        self.delegate.eventually_consistent()
    }
}

pub(crate) struct MeasurableSemaphoreRepository {
    provider: RepositoryProvider,
    delegate: Box<dyn SemaphoreRepository + Send + Sync>,
    mutex_repository: MeasurableMutexRepository,
    metrics: LockMetrics,
}

impl MeasurableSemaphoreRepository {
    fn new(
        provider: RepositoryProvider,
        delegate: Box<dyn SemaphoreRepository + Send + Sync>,
        mutex_repository: MeasurableMutexRepository) -> Self {
        MeasurableSemaphoreRepository {
            provider,
            delegate,
            mutex_repository,
            metrics: LockMetrics::new(format!("{}__", provider).as_str(),
                                      default_registry()).unwrap(),
        }
    }

    fn metrics_summary(&self) -> HashMap<String, f64> {
        let mut result = HashMap::new();
        for (k, v) in self.metrics.summary() {
            result.insert(k, v);
        }
        for (k, v) in self.mutex_repository.metrics.summary() {
            result.insert(k, v);
        }
        result
    }

    fn dump_metrics(&self) -> String {
        log::info!(">>>xxxmetrics {:?}", self.metrics.summary());
        let mut out = format!("# MeasurableSemaphoreRepository metrics dump for {}", self.provider);
        out.push_str(self.metrics.dump().as_str());
        out.push_str(self.mutex_repository.dump_metrics().as_str());
        out
    }
}

#[async_trait]
impl SemaphoreRepository for MeasurableSemaphoreRepository {
    async fn setup_database(&self, recreate: bool) -> LockResult<()> {
        let _metric = self.metrics.new_metric("setup_database");
        self.delegate.setup_database(recreate).await
    }

    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("create");
        self.delegate.create(semaphore).await
    }

    async fn update(&self, other_version: &str, semaphore: &Semaphore) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("update");
        self.delegate.update(other_version, semaphore).await
    }

    async fn get(&self, other_key: &str, other_tenant_id: &str) -> LockResult<Semaphore> {
        let _metric = self.metrics.new_metric("get");
        self.delegate.get(other_key, other_tenant_id).await
    }

    async fn delete(&self, other_key: &str, other_tenant_id: &str, other_version: &str) -> LockResult<usize> {
        let _metric = self.metrics.new_metric("delete");
        self.delegate.delete(other_key, other_tenant_id, other_version).await
    }

    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>, page_size: usize) -> LockResult<PaginatedResult<Semaphore>> {
        let _metric = self.metrics.new_metric("find_by_tenant_id");
        self.delegate.find_by_tenant_id(other_tenant_id, page, page_size).await
    }

    async fn ping(&self) -> LockResult<()> {
        let _metric = self.metrics.new_metric("ping");
        self.delegate.ping().await
    }

    fn eventually_consistent(&self) -> bool {
        self.delegate.eventually_consistent()
    }
}

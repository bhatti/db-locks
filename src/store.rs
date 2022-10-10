pub mod default_lock_store;
mod lock_cache;
pub mod fair_lock_store;


use async_trait::async_trait;

use crate::domain::models::{LockResult, MutexLock, Semaphore};
use crate::domain::options::{AcquireLockOptions, ReleaseLockOptions, SendHeartbeatOptions};

#[async_trait]
pub trait LockStore {
    async fn try_acquire_lock(&self, opts: &AcquireLockOptions) -> LockResult<Option<MutexLock>>;

    async fn try_release_lock(&self, opts: &ReleaseLockOptions) -> LockResult<bool>;

    async fn try_send_heartbeat(&self, opts: &SendHeartbeatOptions) -> LockResult<MutexLock>;

    // Creates mutex if doesn't exist
    async fn create_mutex(&self, mutex: &MutexLock) -> LockResult<usize>;

    async fn get_mutex(&self, mutex_key: &str) -> LockResult<MutexLock>;

    async fn delete_mutex(&self, other_key: &str, other_version: &str, other_semaphore_key: Option<String>) -> LockResult<usize>;

    async fn get_semaphore(&self, semaphore_key: &str) -> LockResult<Semaphore>;

    async fn delete_semaphore(&self, other_key: &str, other_version: &str) -> LockResult<usize>;

    async fn get_semaphore_mutexes(&self, semaphore_key: &str) -> LockResult<Vec<MutexLock>>;

    // Creates or update semaphore
    async fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize>;

    async fn validate_semaphore_size(&self, semaphore_key: &str, max_size: i32) -> LockResult<i32>;

    async fn ping(&self) -> LockResult<()>;

}

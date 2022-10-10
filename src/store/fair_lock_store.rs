use async_trait::async_trait;
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, LocksConfig, MutexLock, Semaphore};
use crate::domain::options::{AcquireLockOptions, ReleaseLockOptions, SendHeartbeatOptions};
use crate::repository::{FairSemaphoreRepository};
use crate::store::lock_cache::LockCache;
use crate::store::LockStore;

pub struct FairLockStore {
    cache: LockCache,
    tenant_id: String,
    fair_semaphore_repository: Box<dyn FairSemaphoreRepository + Send + Sync>,
}

impl FairLockStore {
    pub fn new(
        config: &LocksConfig,
        fair_semaphore_repository: Box<dyn FairSemaphoreRepository + Send + Sync>,
    ) -> Self {
        FairLockStore {
            cache: LockCache::new(config.is_cache_enabled()),
            tenant_id: config.get_tenant_id(),
            fair_semaphore_repository,
        }
    }

    async fn acquire_update(&self, opts: &AcquireLockOptions) -> Option<MutexLock> {
        match self.fair_semaphore_repository.acquire_update(
            &opts.to_semaphore(self.tenant_id.as_str())).await {
            Ok(saving) => {
                self.cache.put_cached_mutex(&saving);
                Some(saving)
            }
            Err(err) => {
                log::debug!("update_lock:: failed to update lock for {:?} due to {}", opts, err);
                None
            }
        }
    }

    async fn heartbeat_update(&self,
                              other_key: &str,
                              other_version: &str,
                              lease_duration_ms: i64) -> LockResult<usize> {
        self.fair_semaphore_repository.heartbeat_update(
            other_key,
            self.tenant_id.as_str(),
            other_version,
            lease_duration_ms).await
    }

    // update data for release
    async fn release_update(&self, opts: &ReleaseLockOptions) -> LockResult<()> {
        // will not check for version mismatch
        let _ = self.fair_semaphore_repository.release_update(
            opts.key.as_str(),
            self.tenant_id.as_str(),
            opts.version.as_str(),
            opts.data.as_deref()).await?;
        Ok(())
    }

    fn remove_cached_mutex(&self, semaphore_key: &str, mutex_key: &str) -> Option<MutexLock> {
        self.cache.remove_cached_mutex(semaphore_key, mutex_key)
    }
}

#[async_trait]
impl LockStore for FairLockStore {
    async fn try_acquire_lock(&self, opts: &AcquireLockOptions) -> LockResult<Option<MutexLock>> {
        if opts.get_semaphore_max_size() <= 0 {
            return Err(LockError::validation("semaphore max_size must be greater than 0", None));
        }
        Ok(self.acquire_update(opts).await)
    }

    async fn try_release_lock(&self, opts: &ReleaseLockOptions) -> LockResult<bool> {
        // Always remove the heartbeat for the lock. The caller's intention is to release the lock.
        // Stopping the heartbeat alone will do that regardless of whether the database
        // write succeeds or fails.
        let _ = self.remove_cached_mutex(
            opts.get_semaphore_key().as_str(), &opts.key);
        let _ = self.release_update(opts).await?;
        Ok(true)
    }

    async fn try_send_heartbeat(&self, opts: &SendHeartbeatOptions) -> LockResult<MutexLock> {
        self.heartbeat_update(
            opts.key.as_str(),
            opts.version.as_str(),
            opts.lease_duration_to_ensure_ms)
            .await.map(|_| opts.to_mutex(self.tenant_id.as_str(), true))
    }

    async fn create_mutex(&self, _mutex: &MutexLock) -> LockResult<usize> {
        return Err(LockError::runtime("create_mutex method is not supported for fair-semaphore", None));
    }

    async fn get_mutex(&self, _mutex_key: &str) -> LockResult<MutexLock> {
        return Err(LockError::runtime("get_mutex method is not supported for fair-semaphore", None));
    }

    async fn delete_mutex(&self,
                          _other_key: &str,
                          _other_version: &str,
                          _other_semaphore_key: Option<String>) -> LockResult<usize> {
        return Err(LockError::runtime("delete_mutex method is not supported for fair-semaphore", None));
    }

    // Returns semaphore for the key
    async fn get_semaphore(&self, semaphore_key: &str) -> LockResult<Semaphore> {
        self.fair_semaphore_repository.get(semaphore_key, self.tenant_id.as_str()).await
    }

    // Deletes semaphore if all associated locks are not locked
    async fn delete_semaphore(&self,
                              other_key: &str,
                              other_version: &str,
    ) -> LockResult<usize> {
        self.cache.remove_all_cached_mutexes_by_semaphore_key(other_key);
        self.fair_semaphore_repository.delete(
            other_key,
            self.tenant_id.as_str(),
            other_version,
        ).await
    }

    async fn get_semaphore_mutexes(&self, semaphore_key: &str) -> LockResult<Vec<MutexLock>> {
        self.fair_semaphore_repository.get_semaphore_mutexes(semaphore_key, self.tenant_id.as_str()).await
    }

    // No need to pre-create semaphore as it's managed as a set
    async fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        if semaphore.max_size <= 0 {
            return Err(LockError::validation("semaphore max_size must be greater than 0", None));
        }
        self.fair_semaphore_repository.create(semaphore).await
    }

    // No need to verify size as it will be dynamically adjusted by sorted-set in Redis
    async fn validate_semaphore_size(&self, _semaphore_key: &str, max_size: i32) -> LockResult<i32> {
        Ok(max_size)
    }

    async fn ping(&self) -> LockResult<()> {
        self.fair_semaphore_repository.ping().await
    }
}

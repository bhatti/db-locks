use async_trait::async_trait;
use crate::domain::error::LockError;
use crate::domain::models;

use crate::domain::models::{LockResult, LocksConfig, MutexLock, PaginatedResult, Semaphore, SemaphoreBuilder};
use crate::domain::options::{AcquireLockOptions, ReleaseLockOptions, SendHeartbeatOptions};
use crate::repository::MutexRepository;
use crate::repository::SemaphoreRepository;
use crate::store::lock_cache::LockCache;
use crate::store::LockStore;

pub struct DefaultLockStore {
    cache: LockCache,
    tenant_id: String,
    max_semaphore_mutexes: i32,
    mutex_repository: Box<dyn MutexRepository + Send + Sync>,
    semaphore_repository: Box<dyn SemaphoreRepository + Send + Sync>,
}

impl DefaultLockStore {
    pub fn new(
        config: &LocksConfig,
        mutex_repository: Box<dyn MutexRepository + Send + Sync>,
        semaphore_repository: Box<dyn SemaphoreRepository + Send + Sync>,
    ) -> Self {
        DefaultLockStore {
            cache: LockCache::new(config.is_cache_enabled()),
            tenant_id: config.get_tenant_id(),
            max_semaphore_mutexes: config.get_max_semaphore_size(),
            mutex_repository,
            semaphore_repository,
        }
    }

    async fn acquire_update(&self, old_version: &str, existing_lock: &MutexLock, opts: &AcquireLockOptions) -> Option<MutexLock> {
        let saving = existing_lock.clone_with_options(
            opts, self.tenant_id.as_str());
        match self.mutex_repository.acquire_update(old_version, &saving).await {
            Ok(_) => {
                self.cache.put_cached_mutex(&saving);
                Some(saving)
            }
            Err(err) => {
                log::warn!("update_lock:: failed to update mutex lock for {} due to {}", saving.mutex_key, err);
                None
            }
        }
    }

    async fn heartbeat_update(&self, old_version: String, saving: &MutexLock) -> Result<(), LockError> {
        match self.mutex_repository.heartbeat_update(
            old_version.as_str(), saving).await {
            Ok(_) => {
                self.cache.put_cached_mutex(saving);
                Ok(())
            }
            Err(err) => {
                // check for retryable error
                Err(LockError::not_granted(
                    format!("lock could not be updated {}", err).as_str(), None))
            }
        }
    }

    // update data for release
    async fn release_update(&self, opts: &ReleaseLockOptions, data: Option<String>) -> LockResult<()> {
        // will check for version mismatch
        let _ = self.mutex_repository.release_update(
            opts.key.as_str(),
            self.tenant_id.as_str(),
            opts.version.as_str(),
            data.as_deref()).await?;
        Ok(())
    }

    // find regular mutexes or semaphore locks
    async fn get_all_mutexes_by_key(&self, key: &str, semaphore_max_size: i32) -> Vec<MutexLock> {
        if semaphore_max_size > 1 {
            if let Ok(mutexes) = self.get_cached_or_db_semaphore_mutexes(key, semaphore_max_size).await {
                return mutexes;
            }
        }
        if let Ok(lock) = self.get_cached_or_db_mutex(models::DEFAULT_SEMAPHORE_KEY, key).await {
            return vec![lock];
        }
        vec![]
    }

    async fn populate_semaphore_busy_count(&self, semaphore: &mut Semaphore) -> LockResult<()> {
        let mut busy_count = 0;
        for m in self.get_cached_or_db_semaphore_mutexes(
            semaphore.semaphore_key.as_str(), semaphore.max_size).await? {
            if !m.expired() {
                busy_count += 1;
            }
        }
        semaphore.busy_count = Some(busy_count);
        semaphore.fair_semaphore = Some(false);
        Ok(())
    }

    fn remove_cached_mutex(&self, semaphore_key: &str, mutex_key: &str) -> Option<MutexLock> {
        self.cache.remove_cached_mutex(semaphore_key, mutex_key)
    }

    async fn get_cached_or_db_mutex(&self, semaphore_key: &str, mutex_key: &str) -> LockResult<MutexLock> {
        if let Some(lock) = self.cache.get_cached_mutex(semaphore_key, mutex_key) {
            return Ok(lock);
        }
        self.fetch_db_mutex(mutex_key).await
    }

    // Retrieves the lock item from database . Note that this will return a
    // LockItem even if it was released -- do NOT use this method if your goal
    // is to acquire a lock for doing work.
    async fn fetch_db_mutex(&self, mutex_key: &str) -> LockResult<MutexLock> {
        self.mutex_repository.get(mutex_key, self.tenant_id.as_str()).await
    }

    async fn fetch_db_semaphore(&self, semaphore_key: &str) -> LockResult<Semaphore> {
        self.semaphore_repository.get(semaphore_key, self.tenant_id.as_str())
            .await.map(|semaphore| {
            self.cache.put_cached_semaphore(semaphore.clone());
            semaphore
        })
    }

    async fn fetch_db_semaphore_mutexes(&self, semaphore_key: &str) -> LockResult<Vec<MutexLock>> {
        self.mutex_repository.find_by_semaphore(
            semaphore_key,
            self.tenant_id.as_str(),
            None,
            self.max_semaphore_mutexes as usize)
            .await.map(|res| res.records)
    }

    async fn get_cached_or_db_semaphore_mutexes(&self,
                                                semaphore_key: &str, max_size: i32) -> LockResult<Vec<MutexLock>> {
        if let Some(locks) = self.cache.get_all_cached_mutexes(semaphore_key) {
            let mut count_expired = 0;
            let mut total: i32 = 0;
            for lock in &locks {
                if lock.expired() {
                    count_expired += 1;
                }
                total += 1;
            }
            if count_expired > 0 || total == max_size {
                return Ok(locks);
            }
        }
        let mutexes = self.fetch_db_semaphore_mutexes(semaphore_key).await?;
        if mutexes.len() != max_size as usize {
            return Err(LockError::runtime(
                format!("unexpected number of semaphore locks for {}, expected {}, actual {}",
                        semaphore_key, max_size, mutexes.len()).as_str(), None));
        }
        let mutexes = self.fetch_paginated_db_semaphore_mutexes(
            semaphore_key,
            None, 1000).await?.records;
        if mutexes.len() != max_size as usize {
            return Err(LockError::runtime(
                format!("unexpected number of semaphore locks for {}, expected {}, actual {}",
                        semaphore_key, max_size, mutexes.len()).as_str(), None));
        }
        Ok(mutexes)
    }

    async fn fetch_paginated_db_semaphore_mutexes(&self,
                                                  semaphore_key: &str,
                                                  page: Option<&str>,
                                                  page_size: usize) -> LockResult<PaginatedResult<MutexLock>> {
        self.mutex_repository.find_by_semaphore(
            semaphore_key, self.tenant_id.as_str(), page, page_size).await
    }
}

#[async_trait]
impl LockStore for DefaultLockStore {
    async fn try_acquire_lock(&self, opts: &AcquireLockOptions) -> LockResult<Option<MutexLock>> {
        let existing_locks = self.get_all_mutexes_by_key(
            &opts.key, opts.get_semaphore_max_size()).await;
        let not_found = existing_locks.is_empty();

        for existing_lock in existing_locks {
            if opts.is_reentrant() && !existing_lock.expired() {
                return Ok(Some(existing_lock));
            }

            log::debug!("acquire_lock:: checking existing lock {}", existing_lock);

            if existing_lock.expired() {
                if let Some(saving) = self.acquire_update(
                    existing_lock.version.as_str(),
                    &existing_lock,
                    opts).await {
                    log::debug!("lock-manager acquire_lock returning lock {}", existing_lock.mutex_key.as_str());
                    return Ok(Some(saving));
                }
            }
        }

        if not_found {
            if opts.is_acquire_only_if_already_exists() {
                return Err(LockError::not_granted(format!("lock {} does not already exist.", opts.key).as_str(), None));
            }

            // add new lock except for semaphores
            let saving = opts.to_locked_mutex(self.tenant_id.as_str());
            if let Ok(_) = self.create_mutex(&saving).await {
                return Ok(Some(saving.clone()));
            }
        }
        Ok(None)
    }

    async fn try_release_lock(&self, opts: &ReleaseLockOptions) -> LockResult<bool> {
        // Always remove the heartbeat for the lock. The caller's intention is to release the lock.
        // Stopping the heartbeat alone will do that regardless of whether the database
        // write succeeds or fails.
        let mut old_opts = self.remove_cached_mutex(
            opts.get_semaphore_key().as_str(), &opts.key);
        if old_opts == None {
            old_opts = Some(self.get_mutex(opts.key.as_str()).await?);
        }
        let old = old_opts.expect("should have mutex lock");

        if old.version != opts.version {
            return Err(LockError::not_found(format!("old version {} didn't match {}", old.version, opts.version).as_str()));
        }

        if !old.belongs_to_semaphore() && opts.is_delete_lock() {
            let _ = self.delete_mutex(
                opts.key.as_str(),
                opts.version.as_str(),
                opts.semaphore_key.clone(),
            ).await?;
        } else {
            let data = opts.data_or(&old);
            let _ = self.release_update(opts, data).await?;
        }
        Ok(true)
    }

    async fn try_send_heartbeat(&self, opts: &SendHeartbeatOptions) -> LockResult<MutexLock> {
        return if let Ok(mut lock) = self.get_cached_or_db_mutex(
            opts.get_semaphore_key().as_str(), opts.key.as_str()).await {
            if lock.expired() || lock.tenant_id != self.tenant_id {
                let _ = self.remove_cached_mutex(
                    opts.get_semaphore_key().as_str(), &opts.key);
                return Err(LockError::not_granted(
                    "cannot send heartbeat because lock is not granted", None));
            }

            let old_version = lock.version.clone();
            lock.update_version(opts);
            self.heartbeat_update(old_version, &lock).await?;
            Ok(lock)
        } else {
            Err(LockError::not_granted(
                format!("cannot find lock for {:?}", opts).as_str(), None))
        };
    }

    async fn create_mutex(&self, mutex: &MutexLock) -> LockResult<usize> {
        match self.mutex_repository.create(mutex).await {
            Ok(size) => {
                self.cache.put_cached_mutex(mutex);
                Ok(size)
            }
            Err(err) => {
                log::warn!("insert_lock:: failed to insert mutex lock for {} due to {}", mutex.mutex_key, err);
                Err(err)
            }
        }
    }

    async fn get_mutex(&self, mutex_key: &str) -> LockResult<MutexLock> {
        self.get_cached_or_db_mutex(models::DEFAULT_SEMAPHORE_KEY, mutex_key).await
    }

    async fn delete_mutex(&self,
                          other_key: &str,
                          other_version: &str,
                          other_semaphore_key: Option<String>) -> LockResult<usize> {
        let semaphore_key = other_semaphore_key.unwrap_or_else(|| models::DEFAULT_SEMAPHORE_KEY.to_string());
        if !Semaphore::is_default_semaphore(semaphore_key.as_str()) {
            return Err(LockError::validation(
                format!("cannot delete semaphore mutexes for {}",
                        semaphore_key.as_str()).as_str(), None));
        }
        let _ = self.remove_cached_mutex(semaphore_key.as_str(), other_key);
        self.mutex_repository.delete(
            other_key,
            self.tenant_id.as_str(),
            other_version,
        ).await
    }

    // Returns semaphore for the key
    async fn get_semaphore(&self, semaphore_key: &str) -> LockResult<Semaphore> {
        match self.cache.get_cached_semaphore(semaphore_key) {
            None => {
                let mut semaphore = self.fetch_db_semaphore(semaphore_key).await?;
                let _ = self.populate_semaphore_busy_count(&mut semaphore).await;
                Ok(semaphore)
            }
            Some(mut semaphore) => {
                let _ = self.populate_semaphore_busy_count(&mut semaphore).await;
                Ok(semaphore)
            }
        }
    }

    // Deletes semaphore if all associated locks are not locked
    async fn delete_semaphore(&self,
                              other_key: &str,
                              other_version: &str,
    ) -> LockResult<usize> {
        self.cache.remove_all_cached_mutexes_by_semaphore_key(other_key);
        self.semaphore_repository.delete(
            other_key,
            self.tenant_id.as_str(),
            other_version,
        ).await
    }

    // find locks by semaphore
    async fn get_semaphore_mutexes(&self, semaphore_key: &str) -> LockResult<Vec<MutexLock>> {
        let semaphore = self.get_semaphore(semaphore_key).await?;
        self.get_cached_or_db_semaphore_mutexes(semaphore_key, semaphore.max_size).await
    }

    // Creates or updates semaphore with given max size
    async fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        match self.get_semaphore(&semaphore.semaphore_key).await {
            Ok(old) => {
                let saving = old.clone_with_tenant_id(semaphore, self.tenant_id.as_str());
                self.semaphore_repository.update(old.version.as_str(), &saving)
                    .await.map(|size| {
                    self.cache.put_cached_semaphore(saving);
                    size
                })
            }
            Err(err) => {
                log::warn!("failed to fetch semaphore {} -- {}", semaphore, err);
                let saving = semaphore.clone_with_tenant_id(semaphore, self.tenant_id.as_str());
                self.semaphore_repository.create(&saving)
                    .await.map(|size| {
                    self.cache.put_cached_semaphore(saving);
                    size
                })
            }
        }
    }

    // Verifies semaphore size and upsert if needed
    async fn validate_semaphore_size(&self, semaphore_key: &str, max_size: i32) -> LockResult<i32> {
        match self.get_semaphore(semaphore_key).await {
            Ok(mut semaphore) => {
                if semaphore.max_size != max_size {
                    semaphore.max_size = max_size;
                    self.create_semaphore(&semaphore).await?;
                }
                Ok(semaphore.max_size)
            }
            Err(_) => {
                let semaphore = SemaphoreBuilder::new(semaphore_key, max_size).build();
                self.create_semaphore(&semaphore).await?;
                Ok(semaphore.max_size)
            }
        }
    }

    async fn ping(&self) -> LockResult<()> {
        self.semaphore_repository.ping().await
    }
}

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use prometheus::Registry;
use crate::domain::models;

use crate::domain::models::{AcquireLockOptions, LockError, LockResult, LocksConfig, MutexLock, PaginatedResult, ReleaseLockOptions, Semaphore, SendHeartbeatOptions};
use crate::manager::LocksManager;
use crate::repository::MutexRepository;
use crate::repository::SemaphoreRepository;
use crate::utils;
use crate::utils::lock_metrics::LockMetrics;

pub struct LocksManagerImpl {
    locks_by_semaphore_key: Arc<RwLock<HashMap<String, HashMap<String, MutexLock>>>>,
    config: LocksConfig,
    mutex_repository: Box<dyn MutexRepository + Send + Sync>,
    semaphore_repository: Box<dyn SemaphoreRepository + Send + Sync>,
    metrics: LockMetrics,
}

impl LocksManagerImpl {
    pub fn new(
        config: &LocksConfig,
        mutex_repository: Box<dyn MutexRepository + Send + Sync>,
        semaphore_repository: Box<dyn SemaphoreRepository + Send + Sync>,
        registry: &Registry) -> LockResult<LocksManagerImpl> {
        Ok(LocksManagerImpl {
            locks_by_semaphore_key: Arc::new(RwLock::new(HashMap::new())),
            config: config.clone(),
            mutex_repository,
            semaphore_repository,
            metrics: LockMetrics::new("locks_manager", registry)?,
        })
    }

    async fn insert_lock(&self, saving: &MutexLock) -> bool {
        match self.mutex_repository.create(&saving).await {
            Ok(_) => {
                self.put_cached_mutex(saving);
                return true;
            }
            Err(err) => {
                log::warn!("insert_lock:: failed to insert lock for {} due to {}", saving.mutex_key, err);
                false
            }
        }
    }

    async fn acquire_update(&self, old_version: &str, saving: &MutexLock) -> bool {
        match self.mutex_repository.acquire_update(old_version.clone(), &saving).await {
            Ok(_) => {
                self.put_cached_mutex(saving);
                return true;
            }
            Err(err) => {
                log::warn!("update_lock:: failed to update lock for {} due to {}", saving.mutex_key, err);
                false
            }
        }
    }

    async fn heartbeat_update(&self, old_version: String, saving: &MutexLock) -> Result<(), LockError> {
        match self.mutex_repository.heartbeat_update(
            old_version.as_str(), &saving).await {
            Ok(_) => {
                self.put_cached_mutex(saving);
                Ok(())
            }
            Err(err) => {
                // check for retryable error
                Err(LockError::not_granted(
                    format!("lock could not be updated {}", err).as_str(), None))
            }
        }
    }

    // find regular mutexes or semaphore locks
    async fn get_all_mutexes_by_key(&self, key: &str, semaphore: &Option<Semaphore>) -> Vec<MutexLock> {
        if let Some(semaphore) = semaphore {
            if let Ok(mutexes) = self.get_cached_or_db_semaphore_mutexes(key, semaphore.max_size).await {
                return mutexes;
            }
        } else {
            if let Ok(lock) = self.get_cached_or_db_mutex(models::DEFAULT_SEMAPHORE_KEY, key).await {
                return vec![lock];
            }
        }
        vec![]
    }

    async fn get_by_semaphore(&self,
                              semaphore_key: &str,
                              page: Option<&str>,
                              page_size: usize) -> LockResult<PaginatedResult<MutexLock>> {
        self.mutex_repository.find_by_semaphore(
            semaphore_key, self.config.get_tenant_id().as_str(), page, page_size).await
    }

    // Retrieves the lock item from database . Note that this will return a
    // LockItem even if it was released -- do NOT use this method if your goal
    // is to acquire a lock for doing work.
    async fn fetch_db_mutex(&self, mutex_key: &str) -> LockResult<MutexLock> {
        self.mutex_repository.get(mutex_key, self.config.get_tenant_id().as_str()).await
    }

    async fn fetch_db_semaphore_mutexes(&self, semaphore_key: &str) -> LockResult<Vec<MutexLock>> {
        self.mutex_repository.find_by_semaphore(
            semaphore_key,
            self.config.get_tenant_id().as_str(),
            None,
            self.config.get_max_semaphore_size()).await.and_then(|res| Ok(res.records))
    }

    async fn get_cached_or_db_semaphore_mutexes(&self,
                                                semaphore_key: &str, max_size: i32) -> LockResult<Vec<MutexLock>> {
        if let Some(locks) = self.get_all_cached_mutexes(semaphore_key) {
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
        self.fetch_db_semaphore_mutexes(semaphore_key).await
    }

    async fn get_cached_or_db_mutex(&self, semaphore_key: &str, mutex_key: &str) -> LockResult<MutexLock> {
        if let Some(lock) = self.get_cached_mutex(semaphore_key, mutex_key) {
            return Ok(lock.clone());
        }
        self.fetch_db_mutex(mutex_key).await
    }

    fn put_cached_mutex(&self, mutex: &MutexLock) {
        if !self.config.should_use_cache() {
            return;
        }
        if let Ok(mut outer) = self.locks_by_semaphore_key.write() {
            if let Some(inner) = outer.get_mut(mutex.get_semaphore_key().as_str()) {
                inner.insert(mutex.mutex_key.clone(), mutex.clone());
            } else {
                // if let Err(err) =
                // self.locks_by_semaphore_key.lock().unwrap().try_insert(
                //     lock.get_semaphore_key().clone(),
                //     Mutex::new(HashMap::from([(lock.mutex_key.clone(), lock.clone())]))) {
                //     err.value.lock().unwrap().insert(lock.mutex_key.clone(), lock.clone());
                // }
                outer.insert(
                    mutex.get_semaphore_key().clone(),
                    HashMap::from([(mutex.mutex_key.clone(), mutex.clone())]));
            }
        }
    }

    fn get_cached_mutex(&self, semaphore_key: &str, mutex_key: &str) -> Option<MutexLock> {
        if !self.config.should_use_cache() {
            return None;
        }
        if let Ok(outer) = self.locks_by_semaphore_key.read() {
            if let Some(inner) = outer.get(semaphore_key) {
                return inner.get(mutex_key).cloned();
            }
        }
        None
    }

    fn remove_cached_mutex(&self, semaphore_key: &str, mutex_key: &str) -> Option<MutexLock> {
        if !self.config.should_use_cache() {
            return None;
        }
        if let Ok(mut outer) = self.locks_by_semaphore_key.write() {
            if let Some(inner) = outer.get_mut(semaphore_key) {
                return inner.remove(mutex_key);
            }
        }
        None
    }

    fn count_cached_map(&self) -> usize {
        if let Ok(outer) = self.locks_by_semaphore_key.write() {
            return outer.len();
        }
        0
    }

    fn count_cached_semaphore(&self, semaphore_key: &str) -> usize {
        if let Ok(outer) = self.locks_by_semaphore_key.write() {
            if let Some(inner) = outer.get(semaphore_key) {
                return inner.len();
            }
        }
        0
    }

    fn remove_cached_semaphore(&self, semaphore_key: &str) {
        if !self.config.should_use_cache() {
            return;
        }
        if let Ok(mut outer) = self.locks_by_semaphore_key.write() {
            let _ = outer.remove(semaphore_key);
        }
    }

    fn get_all_cached_mutexes(&self, semaphore_key: &str) -> Option<Vec<MutexLock>> {
        if !self.config.should_use_cache() {
            return None;
        }
        if let Ok(outer) = self.locks_by_semaphore_key.read() {
            if let Some(inner) = outer.get(semaphore_key) {
                let mut result = vec![];
                for (_, v) in inner.iter() {
                    result.push(v.clone());
                }
                return Some(result);
            }
        }
        None
    }
}

#[async_trait]
impl LocksManager for LocksManagerImpl {
    // Attempts to acquire a lock until it either acquires the lock, or a specified additional_time_to_wait_for_lock_ms is
    // reached. This method will poll database based on the refresh_period. If it does not see the lock in database, it
    // will immediately return the lock to the caller. If it does see the lock, it will note the lease expiration on the lock. If
    // the lock is deemed stale, (that is, there is no heartbeat on it for at least the length of its lease duration) then this
    // will acquire and return it. Otherwise, if it waits for as long as additional_time_to_wait_for_lock_ms without acquiring the
    // lock, then it will return LockError::NotGranted.
    //
    // Note that this method will wait for at least as long as the lease_duration_ms in order to acquire a lock that already
    // exists. If the lock is not acquired in that time, it will wait an additional amount of time specified in
    // additional_time_to_wait_for_lock_ms before giving up.
    async fn acquire_lock(&self, options: &AcquireLockOptions) -> LockResult<MutexLock> {
        let _metric = self.metrics.new_metric("acquire_lock");

        let wait_ms = options.get_additional_time_to_wait_for_lock_ms() + options.get_override_time_to_wait_for_lock_ms();
        let refresh_period_ms = options.get_refresh_period_ms();

        let started = utils::current_time_ms();

        let mut semaphore: Option<Semaphore> = None;

        if options.does_use_semaphore() {
            match self.get_semaphore(options.key.as_str()).await {
                Ok(sem) => { semaphore = Option::from(sem); }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        loop {
            let existing_locks = self.get_all_mutexes_by_key(
                &options.key, &semaphore).await;
            let not_found = existing_locks.len() == 0;

            for existing_lock in existing_locks {
                if options.is_reentrant() {
                    if !existing_lock.expired() {
                        return Ok(existing_lock);
                    }
                }

                log::debug!("acquire_lock:: checking existing lock {}", existing_lock.mutex_key);

                if existing_lock.expired() {
                    let saving = existing_lock.clone_with_options(&options, self.config.get_tenant_id().as_str());
                    if self.acquire_update(existing_lock.version.as_str(), &saving).await {
                        return Ok(saving.clone());
                    }
                }
            }

            if not_found {
                if options.is_acquire_only_if_already_exists() || options.does_use_semaphore() {
                    return Err(LockError::not_granted(format!("lock {} does not already exist.", options.key).as_str(), None));
                }
                // add new lock except for semaphores
                let saving = options.to_locked_mutex(self.config.get_tenant_id().as_str());
                if self.insert_lock(&saving).await && !options.does_use_semaphore() {
                    return Ok(saving.clone());
                }
            }

            // check for no wait
            if options.get_override_time_to_wait_for_lock_ms() == 0 {
                return Err(LockError::not_granted("didn't acquire lock with no wait time",
                                                  Some("NO_WAIT".to_string())));
            }

            // timed out
            if utils::current_time_ms() > started + wait_ms {
                return Err(LockError::not_granted(
                    format!("didn't acquire lock after sleeping for {} ms, override-timeout {}",
                            wait_ms, options.get_override_time_to_wait_for_lock_ms()).as_str(), Some("TIMED_OUT".to_string())));
            }

            self.metrics.inc_retry_wait();

            thread::sleep(Duration::from_millis(refresh_period_ms as u64));
        }
    }

    // Releases the given lock if the current user still has it, returning true if the lock was
    // successfully released, and false if someone else already stole the lock. Deletes the
    // lock item if it is released and delete_lock_item_on_close is set.
    async fn release_lock(&self, options: &ReleaseLockOptions) -> LockResult<bool> {
        let _metric = self.metrics.new_metric("release_lock");

        // Always remove the heartbeat for the lock. The caller's intention is to release the lock.
        // Stopping the heartbeat alone will do that regardless of whether the database
        // write succeeds or fails.
        let mut old_opt = self.remove_cached_mutex(options.get_semaphore_key().as_str(), &options.key);
        if old_opt == None {
            old_opt = Some(self.mutex_repository.get(options.key.as_str(), self.config.get_tenant_id().as_str()).await?);
        }
        let old = old_opt.unwrap();
        if old.version != options.version {
            return Err(LockError::not_found(format!("old version {} didn't match {}", old.version, options.version).as_str()));
        }

        if !old.belongs_to_semaphore() && options.is_delete_lock() {
            let _ = self.delete_mutex(
                options.key.as_str(),
                options.version.as_str(),
                options.semaphore_key.clone(),
            ).await?;
        } else {
            // update data for release
            let data = options.data_or(&old);
            let _ = self.mutex_repository.release_update(
                options.key.as_str(),
                self.config.get_tenant_id().as_str(),
                options.version.as_str(),
                data.as_deref()).await?;
        }

        // Only remove the session monitor if no exception thrown above.
        // While moving the heartbeat removal before the database call should not cause existing
        // clients problems, there may be existing clients that depend on the monitor firing if they
        // get exceptions from this method.
        // this.removeKillSessionMonitor(lockItem.getUniqueIdentifier());
        Ok(true)
    }

    // Sends a heartbeat to indicate that the given lock is still being worked on.
    // This method will also set the lease duration of the lock to the given value.
    // This will also either update or delete the data from the lock, as specified in the options
    async fn send_heartbeat(&self, options: &SendHeartbeatOptions) -> LockResult<MutexLock> {
        let _metric = self.metrics.new_metric("send_heartbeat");

        if options.is_delete_data() && options.delete_data != None {
            return Err(LockError::not_granted(
                "data must not be present if delete_data is true", None));
        }

        return if let Ok(mut lock) = self.get_cached_or_db_mutex(
            options.get_semaphore_key().as_str(), options.key.as_str()).await {
            if lock.expired() || lock.tenant_id != self.config.get_tenant_id() {
                let _ = self.remove_cached_mutex(options.get_semaphore_key().as_str(), &options.key);
                return Err(LockError::not_granted(
                    "cannot send heartbeat because lock is not granted", None));
            }

            let old_version = lock.version.clone();
            lock.update_version(&options);

            self.heartbeat_update(old_version, &lock).await?;
            Ok(lock)
        } else {
            Err(LockError::not_granted(
                format!("cannot find lock for {:?}", options).as_str(), None))
        };
    }

    // Deletes lock if not locked
    async fn delete_mutex(&self,
                          other_key: &str,
                          other_version: &str,
                          other_semaphore_key: Option<String>) -> LockResult<usize> {
        let semaphore_key = other_semaphore_key.unwrap_or_else(|| models::DEFAULT_SEMAPHORE_KEY.to_string());
        let _ = self.remove_cached_mutex(semaphore_key.as_str(), other_key);
        self.mutex_repository.delete(
            other_key,
            self.config.get_tenant_id().as_str(),
            other_version,
        ).await
    }

    // Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
    // given lock. If the client currently has the lock, it will return the lock, and operations such as releaseLock will work.
    // However, if the client does not have the lock, then operations like releaseLock will not work (after calling get_lock, the
    // caller should check lockItem.isExpired() to figure out if it currently has the lock.)
    async fn get_mutex(&self, mutex_key: &str) -> LockResult<MutexLock> {
        self.get_cached_or_db_mutex(models::DEFAULT_SEMAPHORE_KEY, mutex_key).await
    }

    // Creates or updates semaphore with given max size
    async fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        if let Ok(lock) = self.get_mutex(semaphore.semaphore_key.as_str()).await {
            return Err(LockError::validation(
                format!("cannot create semaphore for {} as lock with the key already exists {}",
                        semaphore.semaphore_key.clone(), lock).as_str(), None));
        }
        if semaphore.max_size > self.config.get_max_semaphore_size() as i32 {
            return Err(LockError::validation(
                format!("cannot create semaphore for {}, max allowed size {}",
                        semaphore, self.config.get_max_semaphore_size()).as_str(), None));
        }

        match self.get_semaphore(&semaphore.semaphore_key).await {
            Ok(old) => {
                let saving = old.clone_with_tenant_id(semaphore, self.config.get_tenant_id().as_str());
                self.semaphore_repository.update(old.version.as_str(), &saving).await
            }
            Err(err) => {
                log::warn!("failed to fetch semaphore {} -- {}", semaphore, err);
                let saving = semaphore.clone_with_tenant_id(semaphore, self.config.get_tenant_id().as_str());
                self.semaphore_repository.create(&saving).await
            }
        }
    }

    // Returns semaphore for the key
    async fn get_semaphore(&self, semaphore_key: &str) -> LockResult<Semaphore> {
        self.semaphore_repository.get(semaphore_key.clone(), self.config.get_tenant_id().as_str()).await
    }

    // find locks by semaphore
    async fn get_mutexes_for_semaphore(&self,
                                       semaphore_key: &str,
    ) -> LockResult<Vec<MutexLock>> {
        let semaphore = self.get_semaphore(semaphore_key).await?;
        self.get_cached_or_db_semaphore_mutexes(semaphore_key, semaphore.max_size).await
    }

    // Deletes semaphore if all associated locks are not locked
    async fn delete_semaphore(&self,
                              other_key: &str,
                              other_version: &str,
    ) -> LockResult<usize> {
        self.remove_cached_semaphore(other_key);
        self.semaphore_repository.delete(
            other_key,
            self.config.get_tenant_id().as_str(),
            other_version,
        ).await
    }
}


#[cfg(test)]
mod tests {
    use env_logger::Env;
    use futures::future::join_all;

    use prometheus::default_registry;
    use rand::Rng;
    use uuid::Uuid;

    use crate::domain::models::{AcquireLockOptionsBuilder, RepositoryProvider};
    use crate::repository::factory;

    use super::*;

    #[tokio::test]
    async fn test_should_auto_create_and_acquire_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test1_{}", Uuid::new_v4().to_string());
            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_refresh_period_ms(10).build();
            // WHEN acquiring a non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options).await.expect("should acquire lock");
            // THEN it should succeed
            assert_eq!(Some(true), lock.locked);

            // WHEN releasing the same lock
            // THEN it should succeed
            let mut release_options = lock.to_release_options();
            release_options.data = Some("new_data".to_string());
            assert_eq!(true, lock_manager.release_lock(&release_options).await.unwrap());

            let loaded = lock_manager.get_mutex(mutex_key.as_str()).await.unwrap();
            assert_eq!(release_options.data, loaded.data);
            assert_eq!(Some(false), loaded.locked);
        }
    }

    #[tokio::test]
    async fn test_should_not_acquire_already_acquired_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test1_{}", Uuid::new_v4().to_string());
            let mut lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .build();
            // WHEN acquiring a non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options).await.expect("should acquire lock");
            // THEN it should succeed
            assert_eq!(Some(true), lock.locked);

            // WHEN trying to lock it again
            lock_options.override_time_to_wait_for_lock_ms = Some(0);
            assert_eq!(true, lock_manager.acquire_lock(&lock_options).await.is_err());

            // WHEN releasing the same lock
            // THEN it should succeed
            assert_eq!(true, lock_manager.release_lock(&lock.to_release_options()).await.unwrap());

            // AND we should be able to lock it again
            assert_eq!(lock, lock_manager.acquire_lock(&lock_options).await.expect("should acquire lock again"));
        }
    }

    #[tokio::test]
    async fn test_should_acquire_reentrant_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test1_{}", Uuid::new_v4().to_string());
            let mut lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .build();
            // WHEN acquiring a non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options).await.expect("should acquire lock");
            // THEN it should succeed
            assert_eq!(Some(true), lock.locked);

            // WHEN trying to lock it again with reentrant flag
            lock_options.reentrant = Some(true);
            assert_eq!(lock, lock_manager.acquire_lock(&lock_options).await.expect("should acquire lock again"));
        }
    }

    #[tokio::test]
    async fn test_should_delete_lock_with_release() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test2_{}", Uuid::new_v4().to_string());
            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_acquire_only_if_already_exists(false)
                .with_data("0")
                .build();
            // WHEN acquiring a non-existing lock
            // THEN it should succeed
            let lock = lock_manager.acquire_lock(&lock_options).await.unwrap();
            let mut release_opts = lock.to_release_options();

            // WHEN releasing with delete lock
            release_opts.delete_lock = Some(true);
            // THEN it should delete
            assert_eq!(true, lock_manager.release_lock(&release_opts).await.unwrap());

            // AND it should not find it
            assert!(lock_manager.get_mutex(&lock.mutex_key).await.is_err());
        }
    }


    #[tokio::test]
    async fn test_should_fail_to_acquire_lock_with_acquire_only_if_already_exists_true_for_nonexistant_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options with acquire_only_if_already_exists is true for_nonexistant_lock
            let mutex_key = format!("test2_{}", Uuid::new_v4().to_string());
            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_acquire_only_if_already_exists(true)
                .with_lease_duration_secs(1)
                .build();
            // should have failed because we didn't inserted lock before and have with_acquire_only_if_already_exists true
            assert_eq!(true, lock_manager.acquire_lock(&lock_options).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_should_create_and_get_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // WHEN creating a semaphore
            let sem_key = format!("SEM_{}", Uuid::new_v4().to_string());
            let lock_options = AcquireLockOptionsBuilder::new(sem_key.as_str())
                .with_acquire_only_if_already_exists(false)
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3)
                .with_requires_semaphore(true)
                .with_override_time_to_wait_for_lock_ms(10)
                .build();

            // THEN it should succeed
            let semaphore = lock_options.to_semaphore(tenant_id.as_str(), 100);
            assert_eq!(1, lock_manager.create_semaphore(&semaphore).await.unwrap());

            // WHEN finding semaphore by key
            // THEN it should succeed
            assert_eq!(semaphore, lock_manager.get_semaphore(sem_key.as_str()).await.unwrap());

            // WHEN finding mutexes by semaphore
            // THEN it should succeed
            assert_eq!(100, lock_manager.get_mutexes_for_semaphore(sem_key.as_str()).await.unwrap().len());
        }
    }

    #[tokio::test]
    async fn test_should_create_and_acquire_and_release_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // WHEN creating a semaphore
            let sem_key = format!("SEM_{}", Uuid::new_v4().to_string());
            let lock_options = AcquireLockOptionsBuilder::new(sem_key.as_str())
                .with_acquire_only_if_already_exists(false)
                .with_lease_duration_secs(10)
                .with_additional_time_to_wait_for_lock_ms(3)
                .with_requires_semaphore(true)
                .with_override_time_to_wait_for_lock_ms(10)
                .with_data("abc")
                .build();

            // THEN it should succeed
            assert_eq!(1, lock_manager.create_semaphore(&lock_options.to_semaphore(tenant_id.as_str(), 10)).await.unwrap());

            // WHEN acquiring all locks for semaphore
            let mut acquired = vec![];
            for _i in 0..10 {
                // THEN it should succeed
                let next = lock_manager.acquire_lock(&lock_options).await.expect("should acquire semaphore lock");
                acquired.push(next);
            }

            // BUT WHEN acquiring next lock
            // THEN it should fail
            assert_eq!(true, lock_manager.acquire_lock(&lock_options).await.is_err());

            let mut renewed = vec![];
            // WHEN sending heartbeat for the lock
            for lock in acquired {
                // THEN it should succeed
                let updated = lock_manager.send_heartbeat(&lock.to_heartbeat_options()).await.expect("should renew semaphore lock");
                renewed.push(updated.clone());

                // WHEN releasing with old versions
                // THEN it should fail
                assert_eq!(true, lock_manager.release_lock(&lock.to_release_options()).await.is_err());
            }

            // WHEN releasing the lock
            for lock in renewed {
                // THEN it should succeed
                assert_eq!(true, lock_manager.release_lock(&lock.to_release_options()).await.unwrap());
                // AND it should acquire it again
                assert_eq!(lock.data, lock_manager.acquire_lock(&lock_options).await.expect("should acquire semaphore lock").data);
            }
        }
    }

    #[tokio::test]
    async fn test_should_cache_mutexes_and_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // WHEN creating a semaphore
            for i in 0..10 {
                let sem_key = format!("SEM_{}_{}", i, Uuid::new_v4().to_string());

                let lock_options = AcquireLockOptionsBuilder::new(sem_key.as_str())
                    .with_acquire_only_if_already_exists(false)
                    .with_lease_duration_secs(1)
                    .with_additional_time_to_wait_for_lock_ms(3)
                    .with_requires_semaphore(true)
                    .with_override_time_to_wait_for_lock_ms(10)
                    .with_data("abc")
                    .build();

                // THEN it should succeed
                let semaphore = lock_options.to_semaphore(tenant_id.as_str(), 100);
                assert_eq!(1, lock_manager.create_semaphore(&semaphore).await.unwrap());

                let mutexes = semaphore.generate_mutexes(0);
                for (j, mutex) in mutexes.iter().enumerate() {
                    lock_manager.put_cached_mutex(mutex);
                    assert_eq!(i + 1, lock_manager.count_cached_map());
                    assert_eq!(j + 1, lock_manager.count_cached_semaphore(mutex.get_semaphore_key().as_str()));
                    let mut loaded1 = lock_manager.get_cached_mutex(
                        mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str()).unwrap();
                    assert_eq!(*mutex, loaded1);

                    // WHEN updating mutex
                    // THEN it should update it
                    loaded1.data = Some(j.to_string());
                    lock_manager.put_cached_mutex(&loaded1);

                    let loaded2 = lock_manager.get_cached_mutex(
                        mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str()).unwrap();
                    assert_eq!(loaded1, loaded2);
                    assert_eq!(loaded1.data, loaded2.data);

                    // WHEN removing mutex
                    // THEN it should remove it
                    lock_manager.remove_cached_mutex(mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str());

                    assert_eq!(None, lock_manager.get_cached_mutex(
                        mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str()));

                    loaded1.data = Some("123".to_string());
                    lock_manager.put_cached_mutex(&loaded1);
                }
                let mutexes = lock_manager.get_all_cached_mutexes(semaphore.semaphore_key.as_str()).unwrap();
                assert_eq!(100, mutexes.len());
            }
        }
    }

    #[tokio::test]
    async fn test_should_acquire_lock_concurrently() {
        let repetition_count = 10;
        let thread_count = 2;
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            let mutex_key = Uuid::new_v4().to_string();
            // AND GIVEN options
            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_millis(500)
                .with_additional_time_to_wait_for_lock_ms(30)
                .with_acquire_only_if_already_exists(false)
                .with_data("0")
                .with_refresh_period_ms(10).build();

            // WHEN acquiring non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options).await.unwrap();
            // THEN it should succeed
            let mut release_opts = lock.to_release_options();
            release_opts.data = None;
            release_opts.delete_lock = Some(false);
            assert_eq!(true, lock_manager.release_lock(&release_opts).await.unwrap());

            // WHEN updating locks concurrently
            let mut tasks = vec![];
            for _i in 0..thread_count {
                tasks.push(repeat_acquire_release_locks(
                    &lock_manager, repetition_count, mutex_key.as_str()));
            }

            // THEN it should succeed
            let failed: i32 = join_all(tasks).await.iter().sum();
            // TODO max timeout 5 min
            assert_eq!(0, failed);
            assert_eq!(Some("20".to_string()), lock_manager.get_mutex(&lock.mutex_key).await.unwrap().data);

            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_acquire_only_if_already_exists(false)
                .with_refresh_period_ms(10).build();
            let lock = lock_manager.acquire_lock(&lock_options).await.unwrap();
            assert_eq!(Some("20".to_string()), lock.data);
            assert_eq!(true, lock_manager.release_lock(&lock.to_release_options()).await.unwrap());
            assert_eq!(22, lock_manager.metrics.get_request_total("acquire_lock"));
            assert_eq!(22, lock_manager.metrics.get_request_total("release_lock"));

            for (k, v) in lock_manager.metrics.summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    async fn repeat_acquire_release_locks(lock_manager: &LocksManagerImpl, repetition_count: i32, mutex_key: &str) -> i32 {
        let max_done_ms = 100;
        let mut failed = 0;
        for _j in 0..repetition_count {
            let lock_options = AcquireLockOptionsBuilder::new(
                mutex_key)
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_acquire_only_if_already_exists(true)
                .with_refresh_period_ms(10).build();
            match lock_manager.acquire_lock(&lock_options).await {
                Ok(mut lock) => {
                    let count: u32 = lock.data.unwrap_or_else(|| "0".to_string()).trim().parse().expect("could not parse");
                    lock.data = Some(format!("{}", count + 1));
                    let mut rng = rand::thread_rng();
                    let sleep_ms = rng.gen_range(1..max_done_ms);
                    thread::sleep(Duration::from_millis(sleep_ms as u64));
                    if lock_manager.release_lock(&lock.to_release_options()).await.is_err() {
                        failed += 1;
                        log::error!("failed to release lock for {}", lock);
                    }
                }
                Err(err) => {
                    failed += 1;
                    log::error!("failed to acquire lock for {} due to {}", lock_options, err);
                }
            }
        }
        failed
    }

    async fn build_test_lock_manager(read_consistency: bool) -> Vec<LocksManagerImpl> {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or(
            "info,aws_config=warn,aws_smithy_http=warn,aws_config=warn,aws_sigv4=warn,aws_smithy_http_tower=warn")).is_test(true).try_init();
        let mut config = LocksConfig::new(format!("test_tenant_{}", Uuid::new_v4().to_string()).as_str());
        config.ddb_read_consistency = Some(read_consistency);
        let mut managers = vec![];
        for provider in vec![RepositoryProvider::Rdb, RepositoryProvider::Ddb] {
            let mutex_repo = build_test_mutex_repo(&config, provider.clone()).await;
            let semaphore_repo = build_test_semaphore_repo(&config, provider.clone()).await;
            managers.push(LocksManagerImpl::new(
                &config,
                mutex_repo,
                semaphore_repo,
                &default_registry(),
            ).expect("failed to initialize lock manager"))
        }
        managers
    }

    async fn build_test_mutex_repo(config: &LocksConfig, provider: RepositoryProvider) -> Box<dyn MutexRepository + Send + Sync> {
        factory::build_mutex_repository(
            provider,
            &config)
            .await.expect("failed to build mutex repository")
    }

    async fn build_test_semaphore_repo(config: &LocksConfig, provider: RepositoryProvider) -> Box<dyn SemaphoreRepository + Send + Sync> {
        factory::build_semaphore_repository(
            provider,
            &config)
            .await.expect("failed to build semaphore repository")
    }
}

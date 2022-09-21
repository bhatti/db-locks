use std::collections::HashMap;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use futures::future::join_all;
use prometheus::Registry;

use crate::domain::models::{AcquireLockOptions, DeleteLockOptions, LockError, LockItem, LockResult, LocksConfig, ReleaseLockOptions, Semaphore, SendHeartbeatOptions};
use crate::repository::LockRepository;
use crate::repository::SemaphoreRepository;
use crate::service::lock_metrics::LockMetrics;
use crate::service::LockService;
use crate::utils;
use crate::repository::build_pool;

pub(crate) struct LockServiceImpl {
    locks: Mutex<HashMap<String, LockItem>>,
    config: LocksConfig,
    locks_repository: Box<dyn LockRepository + Send + Sync>,
    semaphore_repository: Box<dyn SemaphoreRepository + Send + Sync>,
    metrics: LockMetrics,
}

impl LockService for LockServiceImpl {
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
    fn acquire_lock(&self, options: &AcquireLockOptions) -> LockResult<LockItem> {
        let _metric = self.metrics.new_metric("acquire_lock");

        let wait_ms = options.get_additional_time_to_wait_for_lock_ms() + options.get_override_time_to_wait_for_lock_ms();
        let refresh_period_ms = options.get_refresh_period_ms();

        let started = utils::current_time_ms();

        loop {
            let existing_locks = self.get_locks_by_key(&options.key, options.does_use_semaphore());
            let not_found = existing_locks.len() == 0;
            for existing_lock in existing_locks {
                if options.is_reentrant() {
                    if !existing_lock.expired() {
                        return Ok(existing_lock);
                    }
                }
                log::info!("acquire_lock:: checking existing lock {}", existing_lock.key);
                if existing_lock.expired() {
                    let saving = existing_lock.clone_with_options(&options, self.config.owner.clone());
                    if self.acquire_update(existing_lock.version.clone(), &saving) {
                        return Ok(saving.clone());
                    }
                }
            }

            if not_found {
                if options.is_acquire_only_if_already_exists() {
                    return Err(LockError::not_granted(format!("lock does not exist."), None));
                }
                let saving = options.to_locked(self.config.owner.clone());
                if self.insert_lock(&saving) {
                    return Ok(saving.clone());
                }
            }

            // timed out
            if utils::current_time_ms() > started + wait_ms {
                return Err(LockError::not_granted(format!("didn't acquire lock after sleeping for {} ms", wait_ms), Some("TIMED_OUT".to_string())));
            }

            self.metrics.inc_retry_wait();

            thread::sleep(Duration::from_millis(refresh_period_ms as u64));
        }
    }

    // Releases the given lock if the current user still has it, returning true if the lock was
    // successfully released, and false if someone else already stole the lock. Deletes the
    // lock item if it is released and delete_lock_item_on_close is set.
    fn release_lock(&self, options: &ReleaseLockOptions) -> LockResult<bool> {
        let _metric = self.metrics.new_metric("release_lock");

        // Always remove the heartbeat for the lock. The caller's intention is to release the lock.
        // Stopping the heartbeat alone will do that regardless of whether the database
        // write succeeds or fails.
        let old_opt = self.locks.lock().unwrap().remove(&options.key);
        if options.is_delete_lock() {
            let _ = self.locks_repository.delete(
                options.key.clone(),
                options.version.clone(),
                self.config.owner.clone())?;
        } else {
            let data = options.data_or(old_opt);
            let _ = self.locks_repository.release_update(
                options.key.clone(),
                options.version.clone(),
                self.config.owner.clone(),
                data)?;
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
    fn send_heartbeat(&self, options: &SendHeartbeatOptions) -> LockResult<()> {
        let _metric = self.metrics.new_metric("send_heartbeat");

        if options.is_delete_data() && options.delete_data != None {
            return Err(LockError::not_granted(format!("data must not be present if delete_data is true"), None));
        }
        return if let Ok(mut lock) = self.get_lock(&options.key) {
            if lock.expired() || lock.owner != self.config.owner {
                let _ = self.locks.lock().unwrap().remove(&options.key);
                return Err(LockError::not_granted(format!("cannot send heartbeat because lock is not granted"), None));
            }
            let old_version = lock.version.clone();
            lock.update_version(&options);
            match self.locks_repository.heartbeat_update(
                old_version.clone(), &lock) {
                Ok(_) => {
                    Ok(())
                }
                Err(err) => {
                    // check for retryable error
                    Err(LockError::not_granted(format!("lock could not be updated {}", err), None))
                }
            }
        } else {
            Err(LockError::not_granted(format!("lock not found"), None))
        };
    }

    // Creates or updates semaphore with given max size
    fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        if let Ok(lock) = self.get_lock(&semaphore.key.clone()) {
            return Err(LockError::validation(format!("cannot create semaphore for {} as lock with the key already exists {}", semaphore.key.clone(), lock), None));
        }
        match self.get_semaphore(&semaphore.key) {
            Ok(old) => {
                let saving = old.clone_with_owner(semaphore, self.config.owner.clone());
                self.semaphore_repository.update(&saving)
            }
            Err(err) => {
                log::warn!("failed to fetch semaphore {} -- {}", semaphore, err);
                let saving = semaphore.clone_with_owner(semaphore, self.config.owner.clone());
                self.semaphore_repository.create(&saving)
            }
        }
    }

    // Deletes semaphore if all associated locks are not locked
    fn delete_semaphore(&self, options: &DeleteLockOptions) -> LockResult<usize> {
        self.semaphore_repository.delete(options.key.clone(), options.version.clone(), self.config.owner.clone())
    }

    // Deletes lock if not locked
    fn delete_lock(&self, options: &DeleteLockOptions) -> LockResult<usize> {
        self.locks_repository.delete(options.key.clone(), options.version.clone(), self.config.owner.clone())
    }

    // Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
    // given lock. If the client currently has the lock, it will return the lock, and operations such as releaseLock will work.
    // However, if the client does not have the lock, then operations like releaseLock will not work (after calling get_lock, the
    // caller should check lockItem.isExpired() to figure out if it currently has the lock.)
    fn get_lock(&self, key: &String) -> LockResult<LockItem> {
        if let Some(lock) = self.locks.lock().unwrap().get(key) {
            return Ok(lock.clone());
        }
        self.fetch_db_lock(key)
    }

    // Returns semaphore for the key
    fn get_semaphore(&self, key: &String) -> LockResult<Semaphore> {
        self.semaphore_repository.get(key.clone(), self.config.owner.clone())
    }

    // find locks by semaphore
    fn find_by_semaphore(&self, key: &String) -> LockResult<Vec<LockItem>> {
        self.locks_repository.find_by_semaphore(key.clone(), self.config.owner.clone())
    }
}

impl LockServiceImpl {
    pub fn new(
        config: LocksConfig,
        locks_repository: Box<dyn LockRepository + Send + Sync>,
        semaphore_repository: Box<dyn SemaphoreRepository + Send + Sync>,
        registry: Registry) -> LockResult<LockServiceImpl> {
        Ok(LockServiceImpl {
            locks: Mutex::new(HashMap::new()),
            config,
            locks_repository,
            semaphore_repository,
            metrics: LockMetrics::new(registry)?,
        })
    }

    // Retrieves the lock item from database . Note that this will return a
    // LockItem even if it was released -- do NOT use this method if your goal
    // is to acquire a lock for doing work.
    fn fetch_db_lock(&self, key: &String) -> LockResult<LockItem> {
        self.locks_repository.get(key.clone(), self.config.owner.clone())
    }

    fn insert_lock(&self, saving: &LockItem) -> bool {
        match self.locks_repository.create(&saving) {
            Ok(_) => {
                self.locks.lock().unwrap().insert(saving.key.clone(), saving.clone());
                return true;
            }
            Err(err) => {
                log::warn!("insert_lock:: failed to insert lock for {} due to {}", saving.key, err);
                false
            }
        }
    }

    fn acquire_update(&self, old_version: Option<String>, saving: &LockItem) -> bool {
        match self.locks_repository.acquire_update(old_version.clone(), &saving) {
            Ok(_) => {
                self.locks.lock().unwrap().insert(saving.key.clone(), saving.clone());
                return true;
            }
            Err(err) => {
                log::warn!("update_lock:: failed to update lock for {} due to {}", saving.key, err);
                false
            }
        }
    }

    // find regular locks or semaphore locks
    fn get_locks_by_key(&self, key: &String, uses_semaphore: bool) -> Vec<LockItem> {
        if uses_semaphore {
            match self.find_by_semaphore(key) {
                Ok(locks) => { locks }
                Err(_) => { vec![] }
            }
        } else {
            match self.get_lock(key) {
                Ok(lock) => { vec![lock] }
                Err(_) => { vec![] }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prometheus::default_registry;
    use rand::Rng;

    use crate::domain::models::AcquireLockOptionsBuilder;
    use crate::repository::data_source;
    use crate::repository::orm_lock_repository::OrmLockRepository;
    use crate::repository::orm_semaphore_repository::OrmSemaphoreRepository;

    use super::*;
    use std::env;
    use uuid::Uuid;

    #[test]
    fn test_should_acquire_lock_serially() {
        let lock_key = Uuid::new_v4().to_string();
        let lock_service = build_lock_service();
        let lock_options = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).build();
        let lock = lock_service.acquire_lock(&lock_options).expect("should acquire serial lock");
        assert_eq!(true, lock_service.release_lock(&lock.to_release_options()).unwrap());
    }

    #[test]
    fn test_should_delete_lock_with_release() {
        let lock_key = Uuid::new_v4().to_string();
        let lock_service = Arc::new(build_lock_service());
        let lock_options = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_acquire_only_if_already_exists(false)
            .with_data("0".to_string())
            .with_refresh_period_ms(10).build();
        let lock = lock_service.acquire_lock(&lock_options).unwrap();
        let mut release_opts = lock.to_release_options();
        release_opts.data = None;
        release_opts.delete_lock = Some(true);
        assert_eq!(true, lock_service.release_lock(&release_opts).unwrap());
        assert!(lock_service.get_lock(&lock.key).is_err());
    }


    #[tokio::test]
    async fn test_should_fail_to_acquire_lock_with_acquire_only_if_already_exists_true_for_nonexistant_lock() {
        let lock_service = build_lock_service();
        let lock_options = AcquireLockOptionsBuilder::new(String::from("non_existant_lock"), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_acquire_only_if_already_exists(true)
            .with_refresh_period_ms(10).build();
        // should have failed because we didn't inserted lock before and have with_acquire_only_if_already_exists true
        assert_eq!(true, lock_service.acquire_lock(&lock_options).is_err());
    }

    #[tokio::test]
    async fn test_should_acquire_lock_concurrently() {
        let lock_key = Uuid::new_v4().to_string();
        let lock_service = Arc::new(build_lock_service());
        let lock_options = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_acquire_only_if_already_exists(false)
            .with_data("0".to_string())
            .with_refresh_period_ms(10).build();
        let lock = lock_service.acquire_lock(&lock_options).unwrap();
        let mut release_opts = lock.to_release_options();
        release_opts.data = None;
        release_opts.delete_lock = Some(false);
        assert_eq!(true, lock_service.release_lock(&release_opts).unwrap());

        let repetition_count = 10;
        let thread_count = 5;
        let mut tasks = vec![];
        for _i in 0..thread_count {
            tasks.push(repeat_acquire_release_locks(&lock_service, repetition_count, lock_key.clone()));
        }
        let failed: i32 = join_all(tasks).await.iter().sum();
        // TODO max timeout 5 min
        log::debug!("{}", lock_service.metrics.dump());
        assert_eq!(0, failed);
        assert_eq!(Some("50".to_string()), lock_service.get_lock(&lock.key).unwrap().data);

        let lock_options = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_acquire_only_if_already_exists(false)
            .with_refresh_period_ms(10).build();
        let lock = lock_service.acquire_lock(&lock_options).unwrap();
        assert_eq!(Some("50".to_string()), lock.data);
        assert_eq!(true, lock_service.release_lock(&lock.to_release_options()).unwrap());
        assert_eq!(52, lock_service.metrics.get_request_total("acquire_lock"));
        assert_eq!(52, lock_service.metrics.get_request_total("release_lock"));
    }

    async fn repeat_acquire_release_locks(lock_service: &Arc<LockServiceImpl>, repetition_count: i32, lock_key: String) -> i32 {
        let max_done_ms = 100;
        let mut failed = 0;
        for _j in 0..repetition_count {
            let lock_options = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_acquire_only_if_already_exists(true)
                .with_refresh_period_ms(10).build();
            match lock_service.acquire_lock(&lock_options) {
                Ok(mut lock) => {
                    let count: u32 = lock.data.unwrap_or_else(|| "0".to_string()).trim().parse().expect("could not parse");
                    lock.data = Some(format!("{}", count + 1));
                    let mut rng = rand::thread_rng();
                    let sleep_ms = rng.gen_range(1..max_done_ms);
                    thread::sleep(Duration::from_millis(sleep_ms as u64));
                    if lock_service.release_lock(&lock.to_release_options()).is_err() {
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

    #[tokio::test]
    async fn test_should_acquire_semaphore_serially() {
        let sem_key = Uuid::new_v4().to_string();
        let lock_options = AcquireLockOptionsBuilder::new(sem_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3)
            .with_uses_semaphore(true)
            .with_override_time_to_wait_for_lock_ms(10)
            .with_refresh_period_ms(10).build();

        let lock_service = build_lock_service();
        assert_eq!(1, lock_service.create_semaphore(&lock_options.to_semaphore(10)).unwrap());
        let mut locks = vec![];
        for _i in 0..10 {
            locks.push(lock_service.acquire_lock(&lock_options).expect("should acquire serial semaphore"));
        }
        // next should fail
        assert_eq!(true, lock_service.acquire_lock(&lock_options).is_err());
        for lock in locks {
            assert_eq!(true, lock_service.release_lock(&lock.to_release_options()).unwrap());
        }
    }

    fn build_lock_service() -> LockServiceImpl {
        let _ = env_logger::builder().is_test(true).try_init();
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| String::from("test_db.sqlite"));

        let pool = build_pool!(database_url);
        let config = LocksConfig { heatbeat_period_ms: Some(100), owner: Some("test_owner".to_string()) };
        let lock_repository = OrmLockRepository::new(pool.clone());
        let semaphore_repository = OrmSemaphoreRepository::new(
            pool.clone(),
            OrmLockRepository::new(pool.clone()));
        LockServiceImpl::new(
            config,
            lock_repository,
            semaphore_repository,
            default_registry().clone(),
        ).expect("failed to initialize lock service")
    }
}
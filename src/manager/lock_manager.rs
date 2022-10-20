use std::fmt::Display;
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use prometheus::Registry;
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, LocksConfig, MutexLock, Semaphore};
use crate::domain::options::{AcquireLockOptions, ReleaseLockOptions, SendHeartbeatOptions};
use crate::manager::LockManager;
use crate::store::LockStore;
use crate::utils;
use crate::utils::lock_metrics::LockMetrics;

pub struct LockManagerImpl {
    max_semaphore_mutexes: i32,
    store: Box<dyn LockStore + Send + Sync>,
    metrics: LockMetrics,
    config: LocksConfig,
}

impl LockManagerImpl {
    pub fn new(
        config: &LocksConfig,
        store: Box<dyn LockStore + Send + Sync>,
        registry: &Registry) -> LockResult<Self> {
        Ok(LockManagerImpl {
            config: config.clone(),
            max_semaphore_mutexes: config.get_max_semaphore_size(),
            store,
            metrics: LockMetrics::new("locks_manager", registry)?,
        })
    }
}

impl Display for LockManagerImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LockManager fair-semaphore={} tenant_id={} metrics={:?}",
               self.config.is_fair_semaphore(), self.config.get_tenant_id(), self.metrics.summary())
    }
}


#[async_trait]
impl LockManager for LockManagerImpl {
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
    async fn acquire_lock(&self, opts: &AcquireLockOptions) -> LockResult<MutexLock> {
        let _metric = self.metrics.new_metric("acquire_lock");

        let wait_ms = opts.get_additional_time_to_wait_for_lock_ms() + opts.get_override_time_to_wait_for_lock_ms();
        let refresh_period_ms = opts.get_refresh_period_ms();

        let started = utils::current_time_ms();

        if opts.get_semaphore_max_size() > 1 {
            let _ = self.store.validate_semaphore_size(
                opts.key.as_str(), opts.get_semaphore_max_size()).await?;
        }

        let mut tries = 0;
        loop {
            tries += 1;
            let acquired = self.store.try_acquire_lock(opts).await?;
            if let Some(lock) = acquired {
                return Ok(lock);
            }

            // check for no wait
            if opts.get_override_time_to_wait_for_lock_ms() == 0 {
                return Err(LockError::not_granted("didn't acquire lock with no wait time",
                                                  Some("NO_WAIT".to_string())));
            }

            // timed out
            if utils::current_time_ms() > started + wait_ms {
                return Err(LockError::not_granted(
                    format!("didn't acquire lock after {} retries, sleeping for {} ms, override-timeout {}, refresh {}",
                            tries, wait_ms,
                            opts.get_override_time_to_wait_for_lock_ms(),
                            refresh_period_ms).as_str(),
                    Some("TIMED_OUT".to_string())));
            }

            self.metrics.inc_retry_wait();

            thread::sleep(Duration::from_millis(refresh_period_ms as u64));
        }
    }

    // Releases the given lock if the current user still has it, returning true if the lock was
    // successfully released, and false if someone else already stole the lock. Deletes the
    // lock item if it is released and delete_lock_item_on_close is set.
    async fn release_lock(&self, opts: &ReleaseLockOptions) -> LockResult<bool> {
        let _metric = self.metrics.new_metric("release_lock");

        self.store.try_release_lock(opts).await
    }

    // Sends a heartbeat to indicate that the given lock is still being worked on.
    // This method will also set the lease duration of the lock to the given value.
    // This will also either update or delete the data from the lock, as specified in the options
    async fn send_heartbeat(&self, opts: &SendHeartbeatOptions) -> LockResult<MutexLock> {
        let _metric = self.metrics.new_metric("send_heartbeat");

        if opts.is_delete_data() && opts.delete_data != None {
            return Err(LockError::not_granted(
                "data must not be present if delete_data is true", None));
        }

        self.store.try_send_heartbeat(opts).await
    }

    // Creates mutex if doesn't exist
    async fn create_mutex(&self, mutex: &MutexLock) -> LockResult<usize> {
        self.store.create_mutex(mutex).await
    }

    // Deletes lock if not locked
    async fn delete_mutex(&self,
                          other_key: &str,
                          other_version: &str,
                          other_semaphore_key: Option<String>) -> LockResult<usize> {
        self.store.delete_mutex(other_key, other_version, other_semaphore_key).await
    }

    // Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
    // given lock. If the client currently has the lock, it will return the lock, and operations such as releaseLock will work.
    // However, if the client does not have the lock, then operations like releaseLock will not work (after calling get_lock, the
    // caller should check lockItem.isExpired() to figure out if it currently has the lock.)
    async fn get_mutex(&self, mutex_key: &str) -> LockResult<MutexLock> {
        self.store.get_mutex(mutex_key).await
    }

    // Creates or updates semaphore with given max size
    async fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        if semaphore.max_size > self.max_semaphore_mutexes {
            return Err(LockError::validation(
                format!("cannot create semaphore for {}, max allowed size {}",
                        semaphore, self.max_semaphore_mutexes).as_str(), None));
        }

        self.store.create_semaphore(semaphore).await
    }

    // Returns semaphore for the key
    async fn get_semaphore(&self, semaphore_key: &str) -> LockResult<Semaphore> {
        self.store.get_semaphore(semaphore_key).await
    }

    // find locks by semaphore
    async fn get_semaphore_mutexes(&self,
                                   semaphore_key: &str,
    ) -> LockResult<Vec<MutexLock>> {
        self.store.get_semaphore_mutexes(semaphore_key).await
    }

    // Deletes semaphore if all associated locks are not locked
    async fn delete_semaphore(&self,
                              other_key: &str,
                              other_version: &str,
    ) -> LockResult<usize> {
        self.store.delete_semaphore(other_key, other_version).await
    }
}


#[cfg(test)]
mod tests {
    use std::time::Instant;
    use env_logger::Env;
    use futures::future::join_all;

    use prometheus::default_registry;
    use rand::Rng;
    use uuid::Uuid;
    use crate::domain::models;

    use crate::domain::models::RepositoryProvider;
    use crate::domain::options::AcquireLockOptionsBuilder;
    use crate::repository::{factory, MutexRepository, SemaphoreRepository};
    use crate::store::default_lock_store::DefaultLockStore;
    use crate::store::fair_lock_store::FairLockStore;

    use super::*;

    #[tokio::test]
    async fn test_should_auto_create_and_acquire_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test1_{}", Uuid::new_v4());
            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_semaphore_max_size(1)
                .with_refresh_period_ms(10).build();
            // WHEN acquiring a non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options).await.expect("should acquire lock");
            // THEN it should succeed
            assert_eq!(Some(true), lock.locked);
            // AND semaphore should show busy count
            if lock_manager.config.is_fair_semaphore() {
                let loaded = lock_manager.get_semaphore(mutex_key.as_str()).await.unwrap();
                assert_eq!(Some(1), loaded.busy_count);
            }

            // WHEN releasing the same lock
            // THEN it should succeed
            let mut release_options = lock.to_release_options();
            release_options.data = Some("new_data".to_string());
            assert!(lock_manager.release_lock(&release_options).await.unwrap());

            if lock_manager.config.is_fair_semaphore() {
                let loaded = lock_manager.get_semaphore(mutex_key.as_str()).await.unwrap();
                assert_eq!(Some(0), loaded.busy_count);
            } else {
                let loaded = lock_manager.get_mutex(mutex_key.as_str()).await.unwrap();
                assert_eq!(release_options.data, loaded.data);
                assert_eq!(Some(false), loaded.locked);
            }
        }
    }

    #[tokio::test]
    async fn test_should_not_acquire_already_acquired_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test1_{}", Uuid::new_v4());
            let mut lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_semaphore_max_size(1)
                .with_lease_duration_secs(1)
                .build();
            // WHEN acquiring a non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options).await.expect("should acquire lock");
            // THEN it should succeed
            assert_eq!(Some(true), lock.locked);

            // WHEN trying to lock it again
            lock_options.override_time_to_wait_for_lock_ms = Some(0);
            assert!(lock_manager.acquire_lock(&lock_options).await.is_err());

            // WHEN releasing the same lock
            // THEN it should succeed
            assert!(lock_manager.release_lock(&lock.to_release_options()).await.unwrap());

            // AND we should be able to lock it again
            if lock_manager.config.is_fair_semaphore() {
                assert_eq!(lock.semaphore_key, lock_manager.acquire_lock(
                    &lock_options).await.expect("should acquire lock again").semaphore_key);
            } else {
                assert_eq!(lock, lock_manager.acquire_lock(
                    &lock_options).await.expect("should acquire lock again"));
            }
        }
    }

    #[tokio::test]
    async fn test_should_acquire_reentrant_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test1_{}", Uuid::new_v4());
            let mut lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_semaphore_max_size(1)
                .with_lease_duration_secs(1)
                .build();

            // WHEN acquiring a non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options)
                .await.expect("should acquire lock");
            // THEN it should succeed
            assert_eq!(Some(true), lock.locked);

            // WHEN trying to lock it again with reentrant flag
            lock_options.reentrant = Some(true);
            if lock_manager.config.is_fair_semaphore() {
                assert_eq!(lock.semaphore_key, lock_manager.acquire_lock(&lock_options)
                    .await.expect("should acquire lock again").semaphore_key);

            } else {
                assert_eq!(lock, lock_manager.acquire_lock(&lock_options)
                    .await.expect("should acquire lock again"));
            }
        }
    }

    #[tokio::test]
    async fn test_should_delete_lock_after_release() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options
            let mutex_key = format!("test2_{}", Uuid::new_v4());
            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_acquire_only_if_already_exists(false)
                .with_semaphore_max_size(1)
                .with_data("0")
                .build();
            // WHEN acquiring a non-existing lock
            // THEN it should succeed
            let lock = lock_manager.acquire_lock(&lock_options).await.unwrap();
            let mut release_opts = lock.to_release_options();

            // WHEN releasing with delete lock
            release_opts.delete_lock = Some(true);
            // THEN it should delete
            assert!(lock_manager.release_lock(&release_opts).await.unwrap());

            // AND it should not find it
            assert!(lock_manager.get_mutex(&lock.mutex_key).await.is_err());
        }
    }


    #[tokio::test]
    async fn test_should_fail_to_acquire_lock_with_acquire_only_if_already_exists_true_for_nonexistant_lock() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // AND lock options with acquire_only_if_already_exists is true for_nonexistant_lock
            let mutex_key = format!("test2_{}", Uuid::new_v4());
            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_acquire_only_if_already_exists(true)
                .with_lease_duration_secs(1)
                .build();
            // should have failed because we didn't inserted lock before and have with_acquire_only_if_already_exists true
            assert!(lock_manager.acquire_lock(&lock_options).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_should_create_and_get_semaphore() {
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            // WHEN creating a semaphore
            let sem_key = format!("SEM_{}", Uuid::new_v4());
            let lock_options = AcquireLockOptionsBuilder::new(sem_key.as_str())
                .with_acquire_only_if_already_exists(false)
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3)
                .with_semaphore_max_size(100)
                .with_override_time_to_wait_for_lock_ms(10)
                .build();

            // THEN it should succeed
            let semaphore = lock_options.to_semaphore(lock_manager.config.get_tenant_id().as_str());
            assert_eq!(1, lock_manager.create_semaphore(&semaphore).await.unwrap());

            // WHEN finding semaphore by key
            // THEN it should succeed
            assert_eq!(semaphore, lock_manager.get_semaphore(sem_key.as_str()).await.unwrap());

            // WHEN finding mutexes by semaphore
            // THEN it should succeed
            assert_eq!(100, lock_manager.get_semaphore_mutexes(sem_key.as_str()).await.unwrap().len());
        }
    }

    #[tokio::test]
    async fn test_should_create_and_acquire_and_release_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        // GIVEN lock manager
        for lock_manager in build_test_lock_manager(true).await {
            let pre_create_semaphore = [true, false];
            for pre_create in pre_create_semaphore {
                let sem_key = format!("SEM_{}", Uuid::new_v4());
                let semaphore_max_size = 10;
                let lock_options = AcquireLockOptionsBuilder::new(sem_key.as_str())
                    .with_lease_duration_secs(5)
                    .with_semaphore_max_size(semaphore_max_size)
                    .with_override_time_to_wait_for_lock_ms(0)
                    .with_data("abc")
                    .build();

                if pre_create {
                    // WHEN creating a semaphore
                    // THEN it should succeed
                    assert_eq!(1, lock_manager.create_semaphore(
                        &lock_options.to_semaphore(tenant_id.as_str())).await.unwrap());
                } else {
                    // OTHERWISE it should create it on demand
                }

                // WHEN acquiring all locks for semaphore
                let mut acquired = vec![];
                let now = Instant::now();
                for i in 0..10 {
                    // THEN it should succeed
                    log::debug!("acquiring lock i {}, elapsed {:?}", i, now.elapsed());
                    let next = lock_manager.acquire_lock(
                        &lock_options).await.expect("should acquire semaphore lock");
                    acquired.push(next);
                }

                let semaphore = lock_manager.get_semaphore(sem_key.as_str())
                    .await.expect("failed to get semaphore");

                // BUT WHEN acquiring next lock
                // THEN it should fail
                assert!(lock_manager.acquire_lock(&lock_options).await.is_err());

                let mut renewed = vec![];
                // WHEN sending heartbeat for the lock
                for lock in acquired {
                    // THEN it should succeed
                    let updated = lock_manager.send_heartbeat(
                        &lock.to_heartbeat_options()).await.expect("should renew semaphore lock");
                    renewed.push(updated.clone());

                    // WHEN releasing with old versions
                    // THEN it should fail
                    if !semaphore.is_fair_semaphore() {
                        assert!(lock_manager.release_lock(&lock.to_release_options()).await.is_err());
                    }
                }

                // WHEN releasing the lock
                for lock in renewed {
                    // THEN it should succeed
                    assert!(lock_manager.release_lock(&lock.to_release_options()).await.unwrap());
                    // AND it should acquire it again
                    assert_eq!(lock.data, lock_manager.acquire_lock(
                        &lock_options).await.expect("should acquire semaphore lock").data);
                }
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
                .with_semaphore_max_size(1)
                .with_data("0")
                .with_refresh_period_ms(10).build();

            // WHEN acquiring non-existing lock
            let lock = lock_manager.acquire_lock(&lock_options).await.unwrap();
            // THEN it should succeed
            let mut release_opts = lock.to_release_options();
            release_opts.data = None;
            release_opts.delete_lock = Some(false);
            assert!(lock_manager.release_lock(&release_opts).await.unwrap());

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
            if lock_manager.config.is_fair_semaphore() {
                let semaphore = lock_manager.get_semaphore(
                    lock.get_semaphore_key().as_str()).await.unwrap();
                assert_eq!(Some(0), semaphore.busy_count);
            } else {
                assert_eq!(Some("20".to_string()), lock_manager.get_mutex(&lock.mutex_key).await.unwrap().data);
            }

            let lock_options = AcquireLockOptionsBuilder::new(mutex_key.as_str())
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_acquire_only_if_already_exists(false)
                .with_semaphore_max_size(1)
                .with_refresh_period_ms(10).build();
            let lock = lock_manager.acquire_lock(&lock_options).await.unwrap();
            if !lock_manager.config.is_fair_semaphore() {
                assert_eq!(Some("20".to_string()), lock.data);
            }
            assert!(lock_manager.release_lock(&lock.to_release_options()).await.unwrap());
            assert_eq!(22, lock_manager.metrics.get_request_total("acquire_lock"));
            assert_eq!(22, lock_manager.metrics.get_request_total("release_lock"));

            for (k, v) in lock_manager.metrics.summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    async fn repeat_acquire_release_locks(lock_manager: &LockManagerImpl, repetition_count: i32, mutex_key: &str) -> i32 {
        let max_done_ms = 100;
        let mut failed = 0;
        for _j in 0..repetition_count {
            let lock_options = AcquireLockOptionsBuilder::new(
                mutex_key)
                .with_lease_duration_secs(1)
                .with_additional_time_to_wait_for_lock_ms(3000)
                .with_acquire_only_if_already_exists(true)
                .with_semaphore_max_size(1)
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

    async fn build_test_lock_manager(read_consistency: bool) -> Vec<LockManagerImpl> {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or(
            "info,aws_config=warn,aws_smithy_http=warn,aws_config=warn,aws_sigv4=warn,aws_smithy_http_tower=warn")).is_test(true).try_init();
        let mut config = LocksConfig::new(models::get_default_tenant().as_str());
        config.ddb_read_consistency = Some(read_consistency);
        config.redis_url = Some(String::from("redis://192.168.1.102"));
        let fair_semaphores = [true, false];
        let mut managers = vec![];
        for fair_semaphore in fair_semaphores {
            let providers = vec![RepositoryProvider::Rdb, RepositoryProvider::Ddb, RepositoryProvider::Redis];
            for provider in providers {
                config.fair_semaphore = Some(fair_semaphore && provider == RepositoryProvider::Redis);
                let store: Box<dyn LockStore + Send + Sync> = if config.is_fair_semaphore() && provider == RepositoryProvider::Redis {
                    let fair_semaphore_repo = factory::build_fair_semaphore_repository(provider, &config)
                        .await.expect("failed to create fair semaphore");
                    Box::new(FairLockStore::new(
                        &config,
                        fair_semaphore_repo,
                    ))
                } else {
                    let mutex_repo = build_test_mutex_repo(provider, &config).await;
                    let semaphore_repo = build_test_semaphore_repo(provider, &config).await;
                    Box::new(DefaultLockStore::new(
                        &config,
                        mutex_repo,
                        semaphore_repo))
                };
                match store.ping().await {
                    Ok(_) => {
                        managers.push(LockManagerImpl::new(
                            &config,
                            store,
                            default_registry(),
                        ).expect("failed to build manager"))
                    }
                    Err(err) => {
                        log::error!("failed to validate repo {} due to err {}", provider, err);
                    }
                }
            }
        }
        managers
    }

    async fn build_test_mutex_repo(provider: RepositoryProvider, config: &LocksConfig) -> Box<dyn MutexRepository + Send + Sync> {
        factory::build_mutex_repository(
            provider,
            config)
            .await.expect("failed to build mutex repository")
    }

    async fn build_test_semaphore_repo(provider: RepositoryProvider, config: &LocksConfig) -> Box<dyn SemaphoreRepository + Send + Sync> {
        factory::build_semaphore_repository(
            provider,
            config)
            .await.expect("failed to build semaphore repository")
    }
}

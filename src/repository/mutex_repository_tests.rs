#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use async_recursion::async_recursion;
    use env_logger::Env;
    use futures::future::join_all;
    use tokio::time::Instant;
    use uuid::Uuid;
    use crate::domain::models;

    use crate::domain::models::{LocksConfig, MutexLock, RepositoryProvider, SemaphoreBuilder};
    use crate::domain::options::AcquireLockOptionsBuilder;
    use crate::repository;
    use crate::repository::MutexRepository;
    use crate::repository::factory;

    #[tokio::test]
    async fn test_should_delete_create_tables() {
        for mutex_repo in build_test_mutex_repos(true).await {
            // run manually
            mutex_repo.setup_database(false).await.expect("should delete and create tables");
        }
    }

    #[tokio::test]
    async fn test_should_get_lock_after_create() {
        let mut versions: HashMap<String, String> = HashMap::new();
        // GIVEN mutex repository
        for mutex_repo in build_test_mutex_repos(true).await {
            let key = Uuid::new_v4().to_string();
            for i in 0..3 {
                let mutex = build_mutex(key.as_str(), i);
                if i < 2 {
                    // WHEN creating a new lock with a new key and tenant-id
                    // THEN it should succeed
                    mutex_repo.create(&mutex).await
                        .expect("should create lock");
                } else {
                    // WHEN creating a lock with existing key and tenant-id
                    // THEN it should fail
                    assert!(mutex_repo.create(&mutex).await.is_err());

                    let old_version = versions.get(&mutex.full_key("test")).unwrap().clone();
                    // WHEN acquiring lock with existing key and tenant-id
                    // THEN it should succeed
                    mutex_repo.acquire_update(old_version.as_str(), &mutex).await
                        .expect("should acquire lock");
                }

                // WHEN finding lock with existing key and tenant-id
                // THEN it should succeed
                assert_eq!(mutex, mutex_repo.get(mutex.mutex_key.as_str(), mutex.tenant_id.as_str()).await.unwrap());

                versions.insert(mutex.full_key("test"), mutex.version);
            }

            // Note: Though we are writing 5 times but unique keys are only mutex_key and tenant_id so we will only
            // overwrite key1 with tenant_ids tenant_id_100 and tenant_id_200
            // WHEN finding lock with existing key and tenant-id
            // THEN it should succeed
            let loaded1 = mutex_repo.get(key.as_str(), "tenant_id_100").await.expect("should get lock");
            assert_eq!(key, loaded1.mutex_key);
            assert_eq!(Some(true), loaded1.locked);
            assert_eq!(Some("data_2".to_string()), loaded1.data);

            // WHEN finding lock with existing key and tenant-id
            // THEN it should succeed
            let loaded2 = mutex_repo.get(key.as_str(), "tenant_id_200").await.expect("should get lock");
            assert_eq!(key, loaded2.mutex_key);
            assert_eq!(Some(false), loaded2.locked);
            assert_eq!(Some("data_1".to_string()), loaded2.data);
        }
    }

    #[tokio::test]
    async fn test_should_not_get_after_delete_lock() {
        let mut versions: HashMap<String, String> = HashMap::new();
        let key = Uuid::new_v4().to_string();
        // GIVEN mutex repository
        for mutex_repo in build_test_mutex_repos(true).await {
            for i in 0..3 {
                let lock = build_mutex(&key, i);
                if i < 2 {
                    // WHEN creating a new lock with a new key and tenant-id
                    // THEN it should succeed
                    mutex_repo.create(&lock)
                        .await.expect("should create lock");
                } else {
                    // WHEN creating a lock with existing key and tenant-id
                    // THEN it should fail
                    assert!(mutex_repo.create(&lock).await.is_err());

                    let old_version = versions.get(&lock.full_key("test")).unwrap().clone();
                    // WHEN acquiring lock with existing key and tenant-id
                    // THEN it should succeed
                    mutex_repo.acquire_update(old_version.as_str(), &lock).await
                        .expect("should acquire lock");
                }
                versions.insert(lock.full_key("test"), lock.version);
            }

            // WHEN finding lock with existing key and tenant-id
            // THEN it should succeed
            let loaded1 = mutex_repo.get(key.as_str(), "tenant_id_100").await.expect("should get lock");
            assert_eq!(key, loaded1.mutex_key);
            assert_eq!(Some("data_2".to_string()), loaded1.data);
            assert_eq!(Some(true), loaded1.locked);

            // WHEN deleting lock with existing key and tenant-id
            // THEN it should succeed
            let _ = mutex_repo.delete(
                key.as_str(), loaded1.tenant_id.as_str(), loaded1.version.as_str()).await.expect("should delete lock");

            // WHEN finding lock with deleted key and tenant-id
            // THEN it should fail
            assert!(mutex_repo.get(key.as_str(), "tenant_id_100").await.is_err());

            // WHEN finding lock with existing key and tenant-id
            // THEN it should succeed
            let loaded2 = mutex_repo.get(key.as_str(), "tenant_id_200").await.expect("should get lock");
            assert_eq!(key, loaded2.mutex_key);
            assert_eq!(Some("data_1".to_string()), loaded2.data);
            assert_eq!(Some(false), loaded2.locked);

            // WHEN deleting lock with existing key and tenant-id
            // THEN it should succeed
            let _ = mutex_repo.delete(
                key.as_str(), loaded2.tenant_id.as_str(), loaded2.version.as_str()).await
                .expect("should delete lock");

            // WHEN finding lock with deleted key and tenant-id
            // THEN it should fail
            assert!(mutex_repo.get(key.as_str(), "tenant_id_200").await.is_err());
        }
    }

    #[tokio::test]
    async fn test_should_get_lock_after_update() {
        // GIVEN mutex repository
        for mutex_repo in build_test_mutex_repos(true).await {
            let key = Uuid::new_v4().to_string();
            for i in 0..2 {
                let lock = build_mutex(&key, i);
                // WHEN creating a new lock with a new key and tenant-id
                // THEN it should succeed
                assert_eq!(1, mutex_repo.create(&lock).await
                    .expect("should create lock"));

                // WHEN finding lock with existing key and tenant-id
                // THEN it should succeed
                let mut after_insert = mutex_repo.get(
                    key.as_str(), lock.tenant_id.as_str()).await.unwrap();
                assert_eq!(lock, after_insert);

                // making updates
                let old_version = lock.version.clone();
                after_insert.data = Some("data".to_string());
                after_insert.version = Uuid::new_v4().to_string();

                // WHEN acquiring lock with existing key and tenant-id with updated data and version
                // THEN it should succeed
                assert_eq!(1, mutex_repo.acquire_update(
                    old_version.as_str(), &after_insert).await.unwrap());

                // WHEN finding lock with existing key and tenant-id
                // THEN it should succeed and match the updated attributes
                let after_update = mutex_repo.get(
                    key.as_str(), lock.tenant_id.as_str()).await.unwrap();
                assert_eq!(after_insert, after_update);
                assert_eq!(Some(true), after_update.locked);

                // WHEN releasing lock with existing key and tenant-id with updated data
                // THEN it should succeed
                assert_eq!(1, mutex_repo.release_update(
                    key.as_str(), after_update.tenant_id.as_str(), after_update.version.as_str(),
                    Some("123")).await.unwrap());

                // WHEN finding lock with existing key and tenant-id
                // THEN it should succeed and match the updated attributes
                let after_release = mutex_repo.get(
                    key.as_str(), lock.tenant_id.as_str()).await.unwrap();
                assert_eq!(Some(false), after_release.locked);
                assert_eq!(Some("123".to_string()), after_release.data);
            }
        }
    }

    #[tokio::test]
    async fn test_should_not_delete_acquired_lock() {
        // GIVEN mutex repository
        for mutex_repo in build_test_mutex_repos(true).await {
            let key = Uuid::new_v4().to_string();
            let mut lock = build_mutex(&key, 1);
            // WHEN creating a new lock with a new key and tenant-id
            // THEN it should succeed
            assert_eq!(1, mutex_repo.create(&lock)
                .await.expect("should create lock"));

            // updating version
            let old_version = lock.version.clone();
            lock.version = Uuid::new_v4().to_string();
            // WHEN acquiring lock with existing key and tenant-id with updated version
            // THEN it should succeed
            assert_eq!(1, mutex_repo.acquire_update(old_version.as_str(), &lock).await.unwrap());
            // AND get should return it as locked
            assert_eq!(Some(true), mutex_repo.get(key.as_str(), lock.tenant_id.as_str()).await.unwrap().locked);

            // WHEN deleting mutex that is locked
            // THEN it should fail
            assert!(mutex_repo.delete_expired_lock(
                key.as_str(), lock.tenant_id.as_str()).await.is_err());

            // WHEN releasing lock with existing key and tenant-id
            // THEN it should succeed
            assert_eq!(1, mutex_repo.release_update(
                key.as_str(), lock.tenant_id.as_str(), lock.version.as_str(), None).await.unwrap());

            // WHEN deleting mutex that is released
            // THEN it should succeed
            assert_eq!(1, mutex_repo.delete_expired_lock(
                key.as_str(), lock.tenant_id.as_str()).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_should_paginate_query_by_semaphore_key() {
        // GIVEN mutex repository, tenant-id and semaphore-id
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        for mutex_repo in build_test_mutex_repos(true).await {
            // GIVEN initialize data with test data
            let sem_key = format!("SEMAPHORE_{}", Uuid::new_v4());
            let semaphore = SemaphoreBuilder::new(sem_key.as_str(), 500)
                .with_lease_duration_secs(5).build();
            let mutexes = semaphore.generate_mutexes(0);
            for (i, mutex) in mutexes.iter().enumerate() {
                let mut mutex = mutex.locked_clone();
                mutex.tenant_id = tenant_id.clone();
                mutex.semaphore_key = Some(sem_key.clone());
                mutex.data = Some(format!("{}", i));
                // WHEN creating a new lock with a new key and tenant-id
                // THEN it should succeed
                assert_eq!(1, mutex_repo.create(&mutex)
                    .await.expect("should create lock"));
            }
            let mut next_page: Option<String> = None;
            let mut data_map = HashMap::new();
            for i in 0..6 {
                if i == 5 && next_page == None {
                    continue;
                }
                // WHEN finding mutexes by tenant-id
                let res = mutex_repo.find_by_semaphore(
                    sem_key.as_str(), tenant_id.as_str(), next_page.as_deref(), 100)
                    .await.expect("failed to find by semaphore-key");
                for (_j, lock) in res.records.iter().enumerate() {
                    data_map.insert(lock.data.clone().unwrap(), true);
                }
                // THEN it should find saved data
                if i == 5 {
                    assert_eq!(0, res.records.len(), "unexpected result for page {} - {}", i, res.records.len());
                } else {
                    assert!(res.records.len() >= 90 && res.records.len() <= 105); // redis doesn't return exact count
                }
                next_page = res.next_page.clone();
            }
            assert!(next_page == None || next_page == Some("-1".to_string()));
            assert_eq!(500, data_map.len());
        }
    }

    #[tokio::test]
    async fn test_should_paginate_query_by_tenant() {
        // GIVEN mutex repository and tenant-id
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        for mutex_repo in build_test_mutex_repos(true).await {
            // GIVEN initialize data with test data
            for i in 0..500 {
                let key = Uuid::new_v4().to_string();
                let mut lock = build_mutex(&key, i);
                lock.tenant_id = tenant_id.clone();
                lock.data = Some(format!("{}", i));
                // WHEN creating a new lock with a new key and tenant-id
                // THEN it should succeed
                assert_eq!(1, mutex_repo.create(&lock)
                    .await.expect("should create lock"));
            }
            let mut next_page: Option<String> = None;
            let mut data_map = HashMap::new();
            for i in 0..6 {
                if i == 5 && next_page == None {
                    continue;
                }
                // WHEN finding mutexes by tenant-id
                let res = mutex_repo.find_by_tenant_id(
                    tenant_id.as_str(), next_page.as_deref(), 100).await.expect("failed to find by tenant-id");
                for (_j, lock) in res.records.iter().enumerate() {
                    data_map.insert(lock.data.clone().unwrap(), true);
                }
                // THEN it should find saved data
                if i == 5 {
                    assert_eq!(0, res.records.len(), "unexpected result for page {} - {}", i, res.records.len());
                } else {
                    assert!(res.records.len() >= 90 && res.records.len() <= 105); // redis doesn't return exact count
                }
                next_page = res.next_page.clone();
            }
            assert!(next_page == None || next_page == Some("-1".to_string()));
            assert_eq!(500, data_map.len());
        }
    }

    #[tokio::test]
    async fn test_should_send_heartbeat() {
        // GIVEN mutex repository
        for mutex_repo in build_test_mutex_repos(true).await {
            for i in 0..20 {
                let key = Uuid::new_v4().to_string();
                let mut lock = build_mutex(&key, i);
                // WHEN creating a new lock with a new key and tenant-id
                // THEN it should succeed
                assert_eq!(1, mutex_repo.create(&lock)
                    .await.expect("should create lock"));

                // updating data and version
                let old_version = lock.version.clone();
                lock.data = Some(format!("new_data {}", i));
                lock.version = Uuid::new_v4().to_string();
                // WHEN acquiring lock with existing key and tenant-id with updated version and data
                // THEN it should succeed
                assert_eq!(1, mutex_repo.acquire_update(old_version.as_str(), &lock).await.unwrap());

                // WHEN acquiring already locked mutex
                // THEN it should fail
                assert!(mutex_repo.acquire_update(old_version.as_str(), &lock).await.is_err());

                // WHEN fetching mutex with existing lock and tenant-id
                // THEN it should find it
                let after_lock = mutex_repo.get(
                    key.as_str(), lock.tenant_id.as_str()).await.unwrap();

                // WHEN renewing lease with heart for locked mutex
                // THEN it should succeed
                assert_eq!(1, mutex_repo.heartbeat_update(
                    after_lock.version.as_str(), &after_lock).await.unwrap());

                // WHEN fetching mutex with existing lock and tenant-id
                // THEN it should find it and show it locked
                let after_update = mutex_repo.get(
                    key.as_str(), after_lock.tenant_id.as_str()).await.unwrap();
                assert_eq!(lock, after_update);
                assert_eq!(Some(true), after_update.locked);

                // WHEN releasing locked mutex with existing lock and tenant-id
                // THEN it should succeed
                assert_eq!(1, mutex_repo.release_update(
                    key.as_str(), after_update.tenant_id.as_str(), after_update.version.as_str(), None).await.unwrap());
            }
            for (k, v) in mutex_repo.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    #[tokio::test]
    async fn test_should_update_locks_concurrently_without_read_consistency() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        let repetition_count = 10;
        let thread_count = 2;
        for mutex_repo in build_test_mutex_repos(false).await {
            let mutex_repo = Arc::new(mutex_repo);
            let key = Uuid::new_v4().to_string();
            let mut lock = build_mutex(&key, 0);
            lock.tenant_id = tenant_id.clone();
            // WHEN creating a new lock with a new key and tenant-id
            // THEN it should succeed
            assert_eq!(1, mutex_repo.create(&lock).await.expect("should create mutex"));

            let mut tasks = vec![];
            for _i in 0..thread_count {
                tasks.push(repeat_update_locks(&mutex_repo, repetition_count, &lock));
            }

            // AND background tasks should complete
            join_all(tasks).await;
            for (k, v) in mutex_repo.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    #[tokio::test]
    async fn test_should_update_locks_concurrently() {
        let repetition_count = 10;
        let thread_count = 2;
        for mutex_repo in build_test_mutex_repos(true).await {
            let mutex_repo = Arc::new(mutex_repo);
            let key = Uuid::new_v4().to_string();
            let lock = build_mutex(&key, 0);
            // WHEN creating a new lock with a new key and tenant-id
            // THEN it should succeed
            assert_eq!(1, mutex_repo.create(&lock).await.expect("should create mutex"));

            let mut tasks = vec![];
            for _i in 0..thread_count {
                tasks.push(repeat_update_locks(&mutex_repo, repetition_count, &lock));
            }
            join_all(tasks).await;
            for (k, v) in mutex_repo.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    async fn repeat_update_locks(mutex_repo: &Arc<repository::MeasurableMutexRepository>, repetition_count: i32, lock: &MutexLock) -> Duration {
        let now = Instant::now();
        for _j in 0..repetition_count {
            update_test_lock(mutex_repo, lock, 0).await;
        }
        now.elapsed()
    }

    #[async_recursion]
    async fn update_test_lock(
        mutex_repo: &Arc<repository::MeasurableMutexRepository>,
        lock: &MutexLock, retries: usize) {
        let loaded = mutex_repo.get(
            lock.mutex_key.as_str(), lock.tenant_id.as_str()).await.unwrap();
        assert_eq!(*lock, loaded);
        match mutex_repo.acquire_update(lock.version.as_str(), lock).await {
            Ok(_size) => {
                assert_eq!(1, mutex_repo.heartbeat_update(lock.version.as_str(), lock).await.unwrap());
                assert_eq!(1, mutex_repo.release_update(
                    lock.mutex_key.as_str(), lock.tenant_id.as_str(),
                    lock.version.as_str(), None).await.unwrap());
            }
            Err(err) => {
                if retries < 10 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    update_test_lock(mutex_repo, lock, retries + 1).await;
                } else {
                    log::warn!("failed to update mutex {} {}", lock, err)
                }
            }
        }
    }

    fn build_mutex(key: &str, i: i32) -> MutexLock {
        let lock = AcquireLockOptionsBuilder::new(key)
            .with_lease_duration_minutes(5)
            .with_data(format!("data_{}", i).as_str())
            .with_replace_data(true)
            .with_delete_on_release(false)
            .with_reentrant(true)
            .with_acquire_only_if_already_exists(true)
            .with_refresh_period_ms(10)
            .with_additional_time_to_wait_for_lock_ms(5)
            .build().to_unlocked_mutex(format!("tenant_id_{}", if i % 2 == 0 { 100 } else { 200 }).as_str());
        lock
    }

    async fn build_test_mutex_repos(read_consistency: bool) -> Vec<repository::MeasurableMutexRepository> {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or(
            "info,aws_config=warn,aws_smithy_http=warn,aws_config=warn,aws_sigv4=warn,aws_smithy_http_tower=warn")).is_test(true).try_init();
        let mut config = LocksConfig::new(models::get_default_tenant().as_str());
        config.ddb_read_consistency = Some(read_consistency);
        config.redis_url = Some(String::from("redis://192.168.1.102"));
        let mut repos = vec![];
        if let Ok(repo) = factory::build_measurable_mutex_repository(
            RepositoryProvider::Redis, &config).await {
            match repo.ping().await {
                Ok(_) => {
                    repos.push(repo);
                }
                Err(err) => {
                    log::error!("failed to validate Redis repo {}", err);
                }
            }
        }
        if let Ok(repo) = factory::build_measurable_mutex_repository(
            RepositoryProvider::Rdb, &config).await {
            match repo.ping().await {
                Ok(_) => {
                    repos.push(repo);
                }
                Err(err) => {
                    log::error!("failed to validate RDB repo {}", err);
                }
            }
        }
        if let Ok(repo) = factory::build_measurable_mutex_repository(
            RepositoryProvider::Ddb, &config).await {
            match repo.ping().await {
                Ok(_) => {
                    repos.push(repo);
                }
                Err(err) => {
                    log::error!("failed to validate DDB repo {}", err);
                }
            }
        }
        repos
    }
}

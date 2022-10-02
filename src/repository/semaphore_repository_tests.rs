#[cfg(test)]
mod tests {
    use std::cmp;
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_recursion::async_recursion;
    use env_logger::Env;
    use futures::future::join_all;
    use rand::Rng;
    use tokio::time::Duration;
    use uuid::Uuid;

    use crate::domain::models::{LocksConfig, MutexLock, PaginatedResult, RepositoryProvider, Semaphore, SemaphoreBuilder};
    use crate::repository::{factory, MeasurableMutexRepository, MeasurableSemaphoreRepository, MutexRepository, SemaphoreRepository};

    #[tokio::test]
    async fn test_should_delete_create_tables() {
        // GIVEN mutex repository
        for semaphore_repo in build_test_semaphore_repos(true).await {
            // run manually
            semaphore_repo.setup_database(false).await.expect("should delete and create tables");
        }
    }

    #[tokio::test]
    async fn test_should_create_and_load_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN mutex repository
        for semaphore_repo in build_test_semaphore_repos(true).await {
            let key = Uuid::new_v4().to_string();

            // GIVEN semaphore
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 10)
                .with_lease_duration_millis(20)
                .build();
            semaphore.tenant_id = tenant_id.clone();

            // WHEN creating a new semaphore
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());

            // THEN it should find the saved semaphore
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());

            // WHEN searching mutexes by semaphore-key
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 1000).await;
            // THEN it should find those records
            assert_eq!(10, locks.records.len());
            log::info!("metrics {}", semaphore_repo.dump_metrics());
        }
    }

    #[tokio::test]
    async fn test_should_not_delete_acquired_mutexes() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN mutex repository
        for semaphore_repo in build_test_semaphore_repos(true).await {
            let key = Uuid::new_v4().to_string();

            // GIVEN semaphore
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 50)
                .with_lease_duration_secs(30)
                .build();
            semaphore.tenant_id = tenant_id.clone();

            // WHEN creating a new semaphore
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());

            // THEN it should find the saved semaphore
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());

            // WHEN searching mutexes by semaphore-key
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 50).await;
            // THEN it should find those records
            assert_eq!(50, locks.records.len());

            // WHEN acquiring mutexes
            // THEN it should succeed
            let mut acquired = HashMap::new();

            for (i, lock) in locks.records.iter().enumerate() {
                if i < 20 {
                    assert_eq!(1, semaphore_repo.mutex_repository.acquire_update(
                        lock.version.as_str(), &lock).await.unwrap());
                    acquired.insert(lock.version.clone(), lock.mutex_key.clone());
                }
            }

            // WHEN deleting expired mutexes
            // THEN it should succeed
            for (_i, lock) in locks.records.iter().enumerate() {
                if acquired.get(lock.version.as_str()) == None {
                    assert_eq!(1, semaphore_repo.mutex_repository.delete_expired_lock(
                        lock.mutex_key.as_str(), lock.tenant_id.as_str()).await.unwrap());
                } else {
                    assert_eq!(true, semaphore_repo.mutex_repository.delete_expired_lock(
                        lock.mutex_key.as_str(), lock.tenant_id.as_str()).await.is_err());
                }
            }
            // AND we should not find deleted locks
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 20).await;
            assert_eq!(20, locks.records.len());
        }
    }

    #[tokio::test]
    async fn test_should_delete_expired_mutexes() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN mutex repository
        for semaphore_repo in build_test_semaphore_repos(true).await {
            let key = Uuid::new_v4().to_string();

            // GIVEN semaphore
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 30)
                .with_lease_duration_minutes(10)
                .build();
            semaphore.tenant_id = tenant_id.clone();

            // WHEN creating a new semaphore
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());

            // THEN it should find the saved semaphore
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());

            // WHEN searching mutexes by semaphore-key
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 30).await;
            // THEN it should find those records
            assert_eq!(30, locks.records.len());

            // WHEN deleting expired mutexes
            // THEN it should succeed
            for (i, lock) in locks.records.iter().enumerate() {
                if i < 10 {
                    assert_eq!(1, semaphore_repo.mutex_repository.delete_expired_lock(
                        lock.mutex_key.as_str(), lock.tenant_id.as_str()).await.unwrap());
                }
            }
            // AND we should find remaining locks
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 20).await;
            assert_eq!(20, locks.records.len());
        }
    }

    #[tokio::test]
    async fn test_should_not_resize_to_smaller_semaphore_after_acquire_lock() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN mutex repository
        for semaphore_repo in build_test_semaphore_repos(true).await {
            let key = Uuid::new_v4().to_string();

            // GIVEN semaphore
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 50)
                .with_lease_duration_minutes(5)
                .build();
            semaphore.tenant_id = tenant_id.clone();

            // WHEN creating a new semaphore
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());

            // THEN it should find the saved semaphore
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());

            // querying only 40 mutexes though we added 50
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 40).await;

            let mut acquired = HashMap::new();
            // Iterate over mutexes
            for lock in locks.records {
                // WHEN acquiring locks
                // THEN it should succeed
                assert_eq!(1, semaphore_repo.mutex_repository.acquire_update(
                    lock.version.as_str(), &lock).await.unwrap());
                acquired.insert(lock.version.clone(), lock.mutex_key.clone());
            }

            // WHEN resizing semaphore to smaller size
            semaphore.max_size = 30;
            // THEN it should fail
            assert_eq!(true, semaphore_repo.update(
                semaphore.version.as_str(), &semaphore).await.is_err());

            // Now release 40 locks
            for (version, key) in acquired {
                assert_eq!(1, semaphore_repo.mutex_repository.release_update(
                    key.as_str(), semaphore.tenant_id.as_str(), version.as_str(), None)
                    .await.expect("should release lock"));
            }

            // WHEN resizing semaphore to available size that are not locked
            // THEN it should succeed
            assert_eq!(1, semaphore_repo.update(
                semaphore.version.as_str(), &semaphore).await.expect("should update semaphore"));


            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 30).await;
            // we locked 40 locks so those should not be deleted
            assert_eq!(30, locks.records.len());
        }
    }

    #[tokio::test]
    async fn test_should_resize_semaphore_without_lock() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN mutex repository
        for semaphore_repo in build_test_semaphore_repos(true).await {
            let key = Uuid::new_v4().to_string();

            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 50)
                .with_lease_duration_millis(100)
                .build();
            semaphore.tenant_id = tenant_id.clone();
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 50).await;
            assert_eq!(50, locks.records.len());

            semaphore.max_size = 50;
            assert_eq!(1, semaphore_repo.update(
                semaphore.version.as_str(), &semaphore).await.unwrap());
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 50).await;
            assert_eq!(50, locks.records.len());

            semaphore.max_size = 100;
            assert_eq!(1, semaphore_repo.update(
                semaphore.version.as_str(), &semaphore).await.unwrap());
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 100).await;
            assert_eq!(100, locks.records.len());

            semaphore.max_size = 20;
            assert_eq!(1, semaphore_repo.update(
                semaphore.version.as_str(), &semaphore).await.unwrap());
            assert_eq!(semaphore, semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());
            let locks = get_mutexes_by_semaphore_key(
                &semaphore_repo.provider, &semaphore_repo.mutex_repository, &semaphore, 20).await;
            assert_eq!(20, locks.records.len());
        }
    }

    #[tokio::test]
    async fn test_should_not_get_semaphore_after_delete() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        // GIVEN mutex repository
        for semaphore_repo in build_test_semaphore_repos(true).await {
            let key = Uuid::new_v4().to_string();

            // WHEN creating semaphore
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 100)
                .with_lease_duration_millis(50)
                .build();
            semaphore.tenant_id = tenant_id.clone();
            // THEN it should succeed
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());
            let after_insert = semaphore_repo.get(
                key.as_str(), semaphore.tenant_id.as_str()).await.unwrap();
            // AND should find saved record
            assert_eq!(semaphore, after_insert);

            // WHEN deleting semaphore
            assert_eq!(1, semaphore_repo.delete(
                key.as_str(), after_insert.tenant_id.as_str(), after_insert.version.as_str()).await.unwrap());
            // THEN it should not find it
            assert_eq!(true, semaphore_repo.get(
                key.as_str(), semaphore.tenant_id.as_str()).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_should_paginate_query_by_semaphore_key() {
        // GIVEN mutex repository, tenant-id and semaphore-id
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        for semaphore_repo in build_test_semaphore_repos(true).await {
            // GIVEN initialize data with test data
            let key = format!("SEMAPHORE_{}", Uuid::new_v4().to_string());
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 1000)
                .with_lease_duration_millis(500)
                .build();
            semaphore.tenant_id = tenant_id.clone();
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());
            let mut next_page: Option<String> = None;
            for i in 0..6 {
                if i == 5 && next_page == None {
                    continue;
                }
                // WHEN finding mutexes by tenant-id
                let res = semaphore_repo.mutex_repository.find_by_semaphore(
                    key.as_str(), tenant_id.as_str(), next_page.as_deref(), 100)
                    .await.expect(format!("failed to find by semaphore-key i {} next {:?}, tenant {:?}, semaphore {:?}",
                                          i, next_page, tenant_id, key).as_str());
                // THEN it should find saved data
                if i == 5 {
                    assert_eq!(0, res.records.len(), "unexpected result for page {} - {}", i, res.records.len());
                } else {
                    assert_eq!(100, res.records.len());
                }
                next_page = res.next_page.clone();
            }
            assert_eq!(None, next_page);
            for (k, v) in semaphore_repo.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    #[tokio::test]
    async fn test_should_paginate_query_by_tenant() {
        // GIVEN mutex repository and tenant-id
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        for semaphore_repo in build_test_semaphore_repos(true).await {
            // GIVEN initialize data with test data
            for i in 0..100 {
                let key = format!("SEMAPHORE_{}", Uuid::new_v4().to_string());
                let mut semaphore = SemaphoreBuilder::new(key.as_str(), 5)
                    .with_lease_duration_millis(5)
                    .build();
                semaphore.tenant_id = tenant_id.clone();
                semaphore.data = Some(format!("{}", i));
                assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());
            }
            let mut next_page: Option<String> = None;
            let mut data_map = HashMap::new();
            for i in 0..6 {
                if i == 5 && next_page == None {
                    continue;
                }
                // WHEN finding mutexes by tenant-id
                let res = semaphore_repo.find_by_tenant_id(
                    tenant_id.as_str(), next_page.as_deref(), 20).await.expect("failed to find by tenant-id");
                for (_j, sem) in res.records.iter().enumerate() {
                    data_map.insert(sem.data.clone().unwrap(), true);
                }
                // THEN it should find saved data
                if i == 5 {
                    assert_eq!(0, res.records.len(), "unexpected result for page {} - {}", i, res.records.len());
                } else {
                    assert_eq!(20, res.records.len());
                }
                next_page = res.next_page.clone();
            }
            assert_eq!(None, next_page);
            assert_eq!(100, data_map.len());
            for (k, v) in semaphore_repo.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
            for (k, v) in semaphore_repo.mutex_repository.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    #[tokio::test]
    async fn test_should_update_locks_concurrently() {
        // GIVEN mutex repository
        let tenant_id = format!("TENANT_{}", Uuid::new_v4().to_string());
        let repetition_count = 10;
        let thread_count = 5;
        for semaphore_repo in build_test_semaphore_repos(true).await {
            let semaphore_repo = Arc::new(semaphore_repo);
            // WHEN creating semaphore
            let key = Uuid::new_v4().to_string();
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 50)
                .with_lease_duration_millis(500)
                .build();
            semaphore.tenant_id = tenant_id.clone();
            // THEN it should succeed
            assert_eq!(1, semaphore_repo.create(&semaphore).await.unwrap());

            let mut tasks = vec![];
            // WHEN updating semaphore concurrently
            for _i in 0..thread_count {
                tasks.push(repeat_update_semaphores(&semaphore_repo, repetition_count, &semaphore));
            }
            // THEN it should succeed in updating
            // AND background tasks should complete
            join_all(tasks).await;
            for (k, v) in semaphore_repo.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
            for (k, v) in semaphore_repo.mutex_repository.metrics_summary() {
                log::info!("metrics {} = {}", k, v);
            }
        }
    }

    async fn repeat_update_semaphores(
        semaphore_repo: &Arc<MeasurableSemaphoreRepository>,
        repetition_count: usize, semaphore: &Semaphore) {
        for _j in 0..repetition_count {
            let mut rng = rand::thread_rng();
            update_test_semaphore(semaphore_repo, semaphore, rng.gen_range(1..100), 0).await;
        }
    }

    #[async_recursion]
    async fn update_test_semaphore(
        semaphore_repo: &Arc<MeasurableSemaphoreRepository>,
        semaphore: &Semaphore,
        max_size: i32,
        retries: usize) {
        let mut loaded = semaphore_repo.get(semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap();
        assert_eq!(*semaphore, loaded);
        loaded.max_size = max_size;

        match semaphore_repo.update(loaded.version.as_str(), &loaded).await {
            Ok(_size) => {}
            Err(err) => {
                if retries < 10 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    update_test_semaphore(semaphore_repo, semaphore, max_size, retries + 1).await
                } else {
                    log::warn!("failed to update semaphore {} {}", semaphore, err)
                }
            }
        }
    }

    async fn get_mutexes_by_semaphore_key(
        _provider: &RepositoryProvider,
        mutex_repo: &MeasurableMutexRepository,
        semaphore: &Semaphore,
        expected_size: usize) -> PaginatedResult<MutexLock> {
        let mut locks: PaginatedResult<MutexLock> = PaginatedResult::new(None, None, expected_size, 0, vec![]);
        let max_attempts = 20;
        for i in 0..max_attempts {
            // WHEN finding mutexes by semaphore-key just created
            locks = mutex_repo.find_by_semaphore(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str(), None, cmp::max(200, expected_size)).await.unwrap();

            // DDB is eventually consistent so give it a short time
            if i < 5 || (i < max_attempts - 1 && locks.records.len() != expected_size) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            } else {
                break;
            }
        }
        locks
    }

    async fn build_test_semaphore_repos(read_consistency: bool) -> Vec<MeasurableSemaphoreRepository> {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or(
            "info,aws_config=warn,aws_smithy_http=warn,aws_config=warn,aws_sigv4=warn,aws_smithy_http_tower=warn")).is_test(true).try_init();
        let _ = env_logger::builder().is_test(true).try_init();
        let mut config = LocksConfig::new("default_tenant");
        config.ddb_read_consistency = Some(read_consistency);

        vec![
            factory::build_measurable_semaphore_repository(
                RepositoryProvider::Rdb,
                &config)
                .await.expect("failed to build RDB semaphore repository"),
            factory::build_measurable_semaphore_repository(
                RepositoryProvider::Ddb,
                &config)
                .await.expect("failed to build DDB semaphore repository"),
        ]
    }
}

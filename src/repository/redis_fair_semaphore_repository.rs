use async_trait::async_trait;
use redis::Client;
use uuid::Uuid;
use crate::domain::error::LockError;
use crate::domain::models::{LockResult, MutexLock, PaginatedResult, Semaphore};

use crate::repository::{FairSemaphoreRepository, redis_common, SemaphoreRepository};
use crate::repository::redis_semaphore_repository::RedisSemaphoreRepository;

pub(crate) struct RedisFairSemaphoreRepository {
    client: Client,
    semaphore_repository: RedisSemaphoreRepository,
}

impl RedisFairSemaphoreRepository {
    pub(crate) fn new(
        client: Client,
        semaphore_repository: RedisSemaphoreRepository) -> Self {
        RedisFairSemaphoreRepository {
            client,
            semaphore_repository,
        }
    }

    fn create_semaphore(&self,
                        semaphore: &Semaphore,
    ) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let mut semaphore = semaphore.clone_with_tenant_id(semaphore, semaphore.tenant_id.as_str());
        semaphore.fair_semaphore = Some(true);
        self.semaphore_repository.create_semaphore(&mut conn, &semaphore)
    }

    fn get_semaphore(&self,
                     other_key: &str,
                     other_tenant_id: &str,
    ) -> LockResult<Semaphore> {
        let mut conn = self.client.get_connection()?;
        self.semaphore_repository.get_semaphore(
            &mut conn, other_key, other_tenant_id).map(|mut semaphore| {
            if let Ok(ids) = self.busy_identifiers(other_key, other_tenant_id) {
                semaphore.busy_count = Some(ids.len() as i32);
            }
            semaphore
        })
    }

    fn busy_identifiers(&self,
                        other_key: &str,
                        other_tenant_id: &str,
    ) -> LockResult<Vec<String>> {
        let mut conn = self.client.get_connection()?;
        let lock_name = MutexLock::build_full_key(
            other_key, other_tenant_id);
        redis_common::semaphore_busy_identifiers(&mut conn, lock_name.as_str())
    }

    fn delete_semaphore(&self,
                        other_key: &str,
                        other_tenant_id: &str,
                        other_version: &str,
    ) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let lock_name = MutexLock::build_full_key(
            other_key, other_tenant_id);
        let _ = redis_common::delete_semaphore(&mut conn, lock_name.as_str())?;
        self.semaphore_repository.delete_semaphore(&mut conn, other_key, other_tenant_id, other_version)
    }
}

#[async_trait]
impl FairSemaphoreRepository for RedisFairSemaphoreRepository {
    async fn setup_database(&self, _recreate: bool) -> LockResult<()> {
        Ok(())
    }

    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        self.create_semaphore(semaphore)
    }

    async fn get(&self, other_key: &str, other_tenant_id: &str) -> LockResult<Semaphore> {
        self.get_semaphore(other_key, other_tenant_id)
    }

    async fn delete(&self, other_key: &str, other_tenant_id: &str, other_version: &str) -> LockResult<usize> {
        self.delete_semaphore(other_key, other_tenant_id, other_version)
    }

    async fn acquire_update(&self, semaphore: &Semaphore) -> LockResult<MutexLock> {
        let mut conn = self.client.get_connection()?;
        let lock_name = semaphore.full_key();
        let id = Uuid::new_v4().to_string();

        let size = redis_common::acquire_semaphore(
            &mut conn, lock_name.as_str(), semaphore.max_size as i64,
            id.as_str(), semaphore.lease_duration_ms)?;
        if size == 0 {
            return Err(LockError::database("failed to acquire lock", None, false));
        }
        let mut semaphore = semaphore.clone_with_tenant_id(semaphore, semaphore.tenant_id.as_str());
        semaphore.fair_semaphore = Some(true);
        let _ = self.semaphore_repository.save_semaphore(&mut conn, &semaphore)?;
        Ok(semaphore.to_mutex_with_key_version(semaphore.semaphore_key.as_str(), id.as_str(), true))
    }

    async fn heartbeat_update(&self,
                              other_key: &str,
                              other_tenant_id: &str,
                              other_version: &str,
                              lease_duration_ms: i64) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let lock_name = MutexLock::build_full_key(
            other_key, other_tenant_id);
        let size = redis_common::refresh_semaphore(
            &mut conn,
            lock_name.as_str(),
            other_version,
            lease_duration_ms)?;
        if size == 0 {
            return Err(LockError::database("failed to refresh semaphore", None, false));
        }
        Ok(1)
    }

    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            _other_data: Option<&str>) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let lock_name = MutexLock::build_full_key(
            other_key, other_tenant_id);
        let size = redis_common::release_semaphore(
            &mut conn,
            lock_name.as_str(),
            other_version)?;
        if size == 0 {
            return Err(LockError::database("failed to release semaphore", None, false));
        }
        Ok(1)
    }

    // find locks by semaphore
    async fn get_semaphore_mutexes(&self,
                                   other_key: &str,
                                   other_tenant_id: &str,
    ) -> LockResult<Vec<MutexLock>> {
        let semaphore = self.get_semaphore(other_key, other_tenant_id)?;
        let ids = self.busy_identifiers(
            semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).unwrap();
        let mut mutexes = vec![];
        for (i, id) in ids.iter().enumerate() {
            let key = Semaphore::build_key_rank(other_key, i as i32);
            let mutex = semaphore.to_mutex_with_key_version(key.as_str(), id, true);
            mutexes.push(mutex);
        }
        for i in mutexes.len()..semaphore.max_size as usize {
            let key = Semaphore::build_key_rank(other_key, i as i32);
            let mutex = semaphore.to_mutex_with_key_version(key.as_str(), "", false);
            mutexes.push(mutex);
        }
        Ok(mutexes)
    }

    async fn find_by_tenant_id(&self, other_tenant_id: &str, page: Option<&str>, page_size: usize) -> LockResult<PaginatedResult<Semaphore>> {
        self.semaphore_repository.find_by_tenant_id(other_tenant_id, page, page_size).await
    }

    async fn ping(&self) -> LockResult<()> {
        if let Err(err) = self.find_by_tenant_id("test", None, 1).await {
            match err {
                LockError::AccessDenied { .. } => {
                    return Err(err);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn eventually_consistent(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use env_logger::Env;
    use uuid::Uuid;
    use crate::domain::models;
    use crate::domain::models::{LocksConfig, RepositoryProvider, SemaphoreBuilder};
    use crate::repository::factory;
    use super::*;

    #[tokio::test]
    async fn test_semaphore_acquire() {
        let fair_semaphore_repo = build_fair_repository().await;
        let max = 10;
        let mut mutexes = vec![];
        let semaphore_id = Uuid::new_v4().to_string();
        let mut semaphore = SemaphoreBuilder::new(semaphore_id.as_str(), 10)
            .with_lease_duration_secs(1).build();
        semaphore.tenant_id = Uuid::new_v4().to_string();
        for _i in 0..max {
            let mutex = fair_semaphore_repo.acquire_update(&semaphore).await.expect("failed to acquire lock");
            mutexes.push(mutex);
        }
        assert!(fair_semaphore_repo.acquire_update(&semaphore).await.is_err());
        assert!(fair_semaphore_repo.acquire_update(&semaphore).await.is_err());
        for mutex in &mutexes {
            let size = fair_semaphore_repo.heartbeat_update(
                mutex.mutex_key.as_str(),
                mutex.tenant_id.as_str(),
                mutex.version.as_str(),
                mutex.lease_duration_ms)
                .await.expect("should refresh semaphore");
            assert!(size > 0);
        }
        // should fail
        assert!(fair_semaphore_repo.acquire_update(&semaphore).await.is_err());
        thread::sleep(Duration::from_secs(1));
        let mut mutexes = vec![];
        for _i in 0..max {
            let mutex = fair_semaphore_repo.acquire_update(&semaphore)
                .await.expect("failed to acquire lock");
            mutexes.push(mutex);
        }

        let ids = fair_semaphore_repo.busy_identifiers(
            semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).unwrap();
        assert_eq!(10, ids.len());

        for (i, mutex) in mutexes.iter().enumerate() {
            let size = fair_semaphore_repo.release_update(
                semaphore.semaphore_key.as_str(),
                semaphore.tenant_id.as_str(),
                mutex.version.as_str(),
                mutex.data.clone().as_deref(),
            )
                .await.expect("should release semaphore");
            assert!(size > 0);
            assert!(ids.contains(&mutex.version.clone()));
            let semaphore = fair_semaphore_repo.get(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap();
            assert_eq!(10 - i - 1, semaphore.busy_count.unwrap() as usize);
            let mutexes = fair_semaphore_repo.get_semaphore_mutexes(
                semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap();
            assert_eq!(semaphore.max_size, mutexes.len() as i32);
        }

        let semaphore = fair_semaphore_repo.get(
            semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap();
        assert_eq!(Some(0), semaphore.busy_count);
        let _ = fair_semaphore_repo.delete(
            semaphore.semaphore_key.as_str(),
            semaphore.tenant_id.as_str(),
            semaphore.version.as_str()).await.unwrap();
    }

    #[tokio::test]
    async fn test_should_delete_create_tables() {
        // GIVEN mutex repository
        let fair_semaphore_repo = build_fair_repository().await;
        // run manually
        fair_semaphore_repo.setup_database(false).await.expect("should delete and create tables");
    }

    #[tokio::test]
    async fn test_should_create_and_load_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        // GIVEN mutex repository
        let semaphore_repo = build_fair_repository().await;
        let key = Uuid::new_v4().to_string();

        // GIVEN semaphore
        let mut semaphore = SemaphoreBuilder::new(key.as_str(), 10)
            .with_lease_duration_millis(20)
            .build();
        semaphore.tenant_id = tenant_id.clone();

        // WHEN creating a new semaphore
        assert!(semaphore_repo.acquire_update(&semaphore).await.is_ok());

        // THEN it should find the saved semaphore
        assert_eq!(semaphore, semaphore_repo.get(
            semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap());

        // WHEN deleting semaphore by semaphore-key
        // THEN it should delete it
        assert_eq!(1, semaphore_repo.delete(
            semaphore.semaphore_key.as_str(),
            semaphore.tenant_id.as_str(),
            semaphore.version.as_str()).await.unwrap());
    }

    #[tokio::test]
    async fn test_should_resize_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        // GIVEN mutex repository
        let semaphore_repo = build_fair_repository().await;
        let key = Uuid::new_v4().to_string();

        let mut semaphore = SemaphoreBuilder::new(key.as_str(), 10)
            .with_lease_duration_secs(3)
            .build();
        semaphore.tenant_id = tenant_id.clone();

        // WHEN acquiring lock within range
        for _i in 0..10 {
            // THEN it should succeed
            assert!(semaphore_repo.acquire_update(&semaphore).await.is_ok());
        }
        // BUT not beyond range
        assert!(semaphore_repo.acquire_update(&semaphore).await.is_err());

        // UNLESS we change size
        semaphore.max_size = 11;
        assert!(semaphore_repo.acquire_update(&semaphore).await.is_ok());

        // AND new size should be reflected
        let loaded = semaphore_repo.get(
            semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await.unwrap();

        assert_eq!(11, loaded.max_size);
    }

    #[tokio::test]
    async fn test_should_not_get_semaphore_after_delete() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        // GIVEN mutex repository
        let semaphore_repo = build_fair_repository().await;
        let key = Uuid::new_v4().to_string();

        // WHEN creating semaphore
        let mut semaphore = SemaphoreBuilder::new(key.as_str(), 100)
            .with_lease_duration_millis(50)
            .build();
        semaphore.tenant_id = tenant_id.clone();
        // THEN it should succeed
        assert!(semaphore_repo.acquire_update(&semaphore).await.is_ok());
        let after_insert = semaphore_repo.get(
            key.as_str(), semaphore.tenant_id.as_str()).await.unwrap();
        // AND should find saved record
        assert_eq!(semaphore, after_insert);

        // WHEN deleting semaphore
        assert_eq!(1, semaphore_repo.delete(
            key.as_str(), after_insert.tenant_id.as_str(), after_insert.version.as_str()).await.unwrap());
        // THEN it should not find it
        assert!(semaphore_repo.get(
            key.as_str(), semaphore.tenant_id.as_str()).await.is_err());
    }

    #[tokio::test]
    async fn test_should_paginate_query_by_tenant() {
        // GIVEN mutex repository and tenant-id
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        let semaphore_repo = build_fair_repository().await;
        // GIVEN initialize data with test data
        for _i in 0..100 {
            let key = format!("SEMAPHORE_{}", Uuid::new_v4());
            let mut semaphore = SemaphoreBuilder::new(key.as_str(), 5)
                .with_lease_duration_millis(5)
                .build();
            semaphore.tenant_id = tenant_id.clone();
            assert!(semaphore_repo.acquire_update(&semaphore).await.is_ok());
        }
        let mut next_page: Option<String> = None;
        for i in 0..6 {
            if i == 5 && next_page == None {
                continue;
            }
            // WHEN finding mutexes by tenant-id
            let res = semaphore_repo.find_by_tenant_id(
                tenant_id.as_str(), next_page.as_deref(), 20).await.expect("failed to find by tenant-id");
            // THEN it should return paginated semaphores
            if i == 5 {
                assert_eq!(0, res.records.len(), "unexpected result for page {} - {}", i, res.records.len());
            } else {
                assert!(!res.records.is_empty());
            }
            next_page = res.next_page.clone();
        }
        assert!(None == next_page || Some("-1".to_string()) == next_page);
    }

    async fn build_fair_repository() -> RedisFairSemaphoreRepository {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or(
            "info")).is_test(true).try_init();
        let mut config = LocksConfig::new(models::get_default_tenant().as_str());
        config.redis_url = Some(String::from("redis://192.168.1.102"));
        let semaphore_repo = RedisSemaphoreRepository::new(
            &config,
            redis::Client::open(config.get_redis_url()).unwrap(),
            factory::build_mutex_repository(RepositoryProvider::Redis, &config).await.unwrap(),
        );
        RedisFairSemaphoreRepository::new(
            redis::Client::open(config.get_redis_url()).unwrap(),
            semaphore_repo,
        )
    }
}

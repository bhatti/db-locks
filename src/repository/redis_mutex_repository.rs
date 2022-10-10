use async_trait::async_trait;
use redis::{Client, Commands};
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, LocksConfig, MutexLock, PaginatedResult, SemaphoreBuilder};
use crate::repository::{MutexRepository, redis_common};

#[derive(Clone)]
pub(crate) struct RedisMutexRepository {
    client: Client,
    table_name: String,
}

impl RedisMutexRepository {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(config: &LocksConfig) -> LockResult<Box<dyn MutexRepository + Send + Sync>> {
        let client = redis::Client::open(config.get_redis_url())?;
        Ok(Box::new(RedisMutexRepository {
            client,
            table_name: config.get_mutexes_table_name(),
        }))
    }

    fn build_table_name(&self, tenant_id: &str) -> String {
        format!("{}_{}", tenant_id, self.table_name)
    }

    // update lock item
    fn update_with_lock_expiration(&self, mutex: &MutexLock) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let lock_name = mutex.full_key();
        let size: usize = redis_common::update_atomic_lock_expiration(
            &mut conn,
            lock_name.as_str(),
            mutex.get_lease_duration_secs())?;
        if size == 0 {
            return Err(LockError::database(
                format!("failed to update expiration lock {}",
                        lock_name).as_str(), None, false));
        }

        let value = serde_json::to_string(mutex)?;
        let _size: usize = conn.hset(
            self.build_table_name(mutex.tenant_id.as_str()).as_str(),
            mutex.mutex_key.as_str(),
            value.as_str())?;
        // Note: If size > 0, it means this is new key otherwise it's old key but it still updates it
        // so we can't use it for checking successfully insertion.
        Ok(1)
    }

    fn populate_records(&self,
                        next_token: String,
                        result: Vec<String>,
                        page: Option<&str>,
                        page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>> {
        let mut records = vec![];
        for (i, val) in result.iter().enumerate() {
            if i % 2 == 1 {
                let mutex: MutexLock = serde_json::from_str(val.as_str())?;
                records.push(mutex);
            }
        }
        Ok(PaginatedResult::from_redis(next_token.as_str(), page, page_size, records))
    }
}

#[async_trait]
impl MutexRepository for RedisMutexRepository {
    // run migrations or create table if necessary
    async fn setup_database(&self, _: bool) -> LockResult<()> {
        Ok(()) // no migrations needed for redis
    }

    // create lock item
    async fn create(&self, mutex: &MutexLock) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let value = serde_json::to_string(mutex)?;
        let size: usize = conn.hset_nx(
            self.build_table_name(mutex.tenant_id.as_str()).as_str(),
            mutex.mutex_key.as_str(),
            value.as_str())?;

        if size > 0 {
            log::debug!("created mutex lock {} {}", mutex, size);
            Ok(size)
        } else {
            Err(LockError::database(
                format!("failed to insert mutex {}, it already exists",
                        mutex).as_str(), None, false))
        }
    }

    // updates existing lock item for acquire
    async fn acquire_update(&self,
                            old_version: &str,
                            mutex: &MutexLock) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let lock_name = mutex.full_key();
        let size = redis_common::acquire_atomic_lock(
            &mut conn, lock_name.as_str(), old_version, mutex.version.as_str())?;
        if size == 0 {
            return Err(LockError::database("failed to acquire lock", None, false));
        }
        self.update_with_lock_expiration(&mutex.locked_clone())
    }

    // updates existing lock item for heartbeat
    async fn heartbeat_update(&self,
                              old_version: &str,
                              mutex: &MutexLock) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let lock_name = mutex.full_key();
        let size = redis_common::refresh_atomic_lock_expiration(
            &mut conn,
            lock_name.as_str(),
            old_version,
            mutex.version.as_str(),
            mutex.get_lease_duration_secs())?;
        if size == 0 {
            return Err(LockError::database("failed to acquire lock", None, false));
        }
        let locked = mutex.locked_clone();
        self.update_with_lock_expiration(&locked)
    }

    // updates existing lock item for release
    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            other_data: Option<&str>) -> LockResult<usize> {
        let mut existing = self.get(other_key, other_tenant_id).await?;
        if existing.version != other_version {
            return Err(LockError::database("version mismatched for releasing mutex", None, false));
        }

        let mut conn = self.client.get_connection()?;
        let lock_name = MutexLock::build_full_key(
            other_key,
            other_tenant_id);
        let size = redis_common::delete_atomic_lock(
            &mut conn, lock_name.as_str(), other_version)?;
        if size == 0 {
            log::warn!("failed to delete expired lock {}", lock_name.as_str());
        }

        existing.data = other_data.map(|s| s.to_string());
        existing.lease_duration_ms = 0;
        existing.locked = Some(false);
        existing.expires_at = None;
        self.update_with_lock_expiration(&existing)
    }

    // get lock by key
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<MutexLock> {
        let mut conn = self.client.get_connection()?;
        let val: Vec<u8> = conn.hget(
            self.build_table_name(other_tenant_id),
            other_key)?;
        if val.is_empty() {
            return Err(LockError::not_found(
                format!("lock not found for {}, tenant_id {}",
                        other_key, other_tenant_id).as_str()));
        }
        let mutex: MutexLock = serde_json::from_slice(&val)?;
        Ok(mutex)
    }

    // delete lock
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str,
    ) -> LockResult<usize> {
        let existing = self.get(other_key, other_tenant_id).await?;
        if existing.version != other_version {
            return Err(LockError::database("version mismatched for deleting mutex", None, false));
        }

        let lock_name = MutexLock::build_full_key(
            other_key,
            other_tenant_id);

        let mut conn = self.client.get_connection()?;
        let size = redis_common::delete_atomic_lock(
            &mut conn,
            lock_name.as_str(),
            other_version)?;
        if size == 0 {
            log::debug!("failed to delete mutex lock {} {}", other_key, size);
        }

        let size: usize = conn.hdel(
            self.build_table_name(other_tenant_id),
            other_key)?;
        Ok(size)
    }

    async fn delete_expired_lock(&self, other_key: &str, other_tenant_id: &str) -> LockResult<usize> {
        let lock_name = MutexLock::build_full_key(
            other_key,
            other_tenant_id);
        let mut conn = self.client.get_connection()?;
        let size: usize = redis_common::delete_atomic_lock(
            &mut conn,
            lock_name.as_str(),
            "")?;
        if size == 0 {
            log::debug!("failed to delete mutex lock {} {}", other_key, size);
        }

        let size: usize = conn.hdel(
            self.build_table_name(other_tenant_id),
            other_key)?;
        Ok(size)
    }

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>> {
        if let Some(next_token) = page {
            if next_token == "-1" {
                return Ok(PaginatedResult::new(page, Some("-1".to_string()), page_size, 0, vec![]));
            }
        }

        let mut conn = self.client.get_connection()?;
        let offset = page.unwrap_or("0");
        let result: (String, Vec<String>) = redis::cmd("HSCAN").arg(self.build_table_name(other_tenant_id))
            .arg(offset).arg("COUNT").arg(page_size).query(&mut conn)?;
        self.populate_records(result.0, result.1, page, page_size)
    }


    // find by semaphore
    async fn find_by_semaphore(&self,
                               other_semaphore_key: &str,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>> {
        let offset: i32 = page_size as i32 * page.unwrap_or("0").parse().unwrap_or(0);
        let max_size: i32 = offset + page_size as i32;
        // HSCAN is not reliable so using HMGET
        let keys: Vec<String> = SemaphoreBuilder::new(other_semaphore_key, max_size).build()
            .generate_mutexes(offset as i32).iter().map(|m| m.mutex_key.clone()).collect();
        let mut conn = self.client.get_connection()?;
        let mut mutexes = vec![];
        let values: Vec<Vec<u8>> = conn.hget(
            self.build_table_name(other_tenant_id),
            keys)?;
        if values.is_empty() {
            return Ok(PaginatedResult::from_rdb(page, page_size, mutexes));
        }
        for val in values {
            if !val.is_empty() {
                let mutex: MutexLock = serde_json::from_slice(&val)?;
                mutexes.push(mutex);
            }
        }
        Ok(PaginatedResult::from_rdb(page, page_size, mutexes))
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
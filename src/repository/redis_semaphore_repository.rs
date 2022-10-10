use std::cmp::Ordering;
use async_trait::async_trait;
use redis::{Client, Commands, Connection};
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, LocksConfig, PaginatedResult, Semaphore};
use crate::repository::MutexRepository;
use crate::repository::SemaphoreRepository;

pub(crate) struct RedisSemaphoreRepository {
    client: Client,
    table_name: String,
    mutex_repository: Box<dyn MutexRepository + Send + Sync>,
}

impl RedisSemaphoreRepository {
    pub(crate) fn new(
        config: &LocksConfig,
        client: Client,
        mutex_repository: Box<dyn MutexRepository + Send + Sync>) -> Self {
        RedisSemaphoreRepository {
            client,
            table_name: config.get_semaphores_table_name(),
            mutex_repository,
        }
    }

    fn build_table_name(&self, tenant_id: &str) -> String {
        format!("{}_{}", tenant_id, self.table_name)
    }

    pub(crate) fn save_semaphore(
        &self,
        conn: &mut Connection,
        semaphore: &Semaphore) -> LockResult<usize> {
        let value = serde_json::to_string(semaphore)?;
        let size: usize = conn.hset(
            self.build_table_name(semaphore.tenant_id.as_str()).as_str(),
            semaphore.semaphore_key.as_str(),
            value.as_str())?;
        Ok(size)
    }

    pub(crate) fn create_semaphore(
        &self,
        conn: &mut Connection,
        semaphore: &Semaphore) -> LockResult<usize> {
        if semaphore.max_size <= 0 {
            return Err(LockError::validation("semaphore max_size must be greater than 0", None));
        }
        let value = serde_json::to_string(semaphore)?;
        let size: usize = conn.hset_nx(
            self.build_table_name(semaphore.tenant_id.as_str()).as_str(),
            semaphore.semaphore_key.as_str(),
            value.as_str())?;

        if size > 0 {
            log::debug!("created semaphore {} {}", semaphore, size);
            Ok(size)
        } else {
            Err(LockError::database(
                format!("failed to insert semaphore {}, it already exists",
                        semaphore).as_str(), None, false))
        }
    }

    fn update_semaphore(&self,
                              conn: &mut Connection,
                              old_version: &str,
                              semaphore: &Semaphore) -> LockResult<usize> {
        let existing = self.get_semaphore(
            conn, semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str())?;
        if existing.version != old_version {
            return Err(LockError::database("version mismatched for semaphore", None, false));
        }
        let value = serde_json::to_string(semaphore)?;
        let _size: usize = conn.hset(
            self.build_table_name(semaphore.tenant_id.as_str()).as_str(),
            semaphore.semaphore_key.as_str(),
            value.as_str())?;
        // Note: If size > 0, it means this is new key otherwise it's old key but it still updates it
        // so we can't use it for checking successfully insertion.
        Ok(1)
    }

    pub(crate) fn get_semaphore(&self,
                     conn: &mut Connection,
                     other_key: &str,
                     other_tenant_id: &str) -> LockResult<Semaphore> {
        let val: Vec<u8> = conn.hget(
            self.build_table_name(other_tenant_id),
            other_key)?;
        if val.is_empty() {
            return Err(LockError::not_found(
                format!("semaphore not found for sem-key {}, tenant_id {}",
                        other_key, other_tenant_id).as_str()));
        }
        let semaphore: Semaphore = serde_json::from_slice(&val)?;
        Ok(semaphore)
    }


    pub(crate) fn delete_semaphore(&self,
                                   conn: &mut Connection,
                                   other_key: &str,
                                   other_tenant_id: &str,
                                   other_version: &str,
    ) -> LockResult<usize> {
        let existing = self.get_semaphore(conn, other_key, other_tenant_id)?;
        if existing.version.as_str() != other_version {
            return Err(LockError::database("version mismatched for semaphore", None, false));
        }
        let size: usize = conn.hdel(
            self.build_table_name(other_tenant_id),
            other_key)?;
        Ok(size)
    }
}

#[async_trait]
impl SemaphoreRepository for RedisSemaphoreRepository {
    // run migrations or create table if necessary
    async fn setup_database(&self, _: bool) -> LockResult<()> {
        // run migrations when acquiring data source pool
        Ok(())
    }

    // create semaphore
    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let mut conn = self.client.get_connection()?;
        let size = self.create_semaphore(&mut conn, semaphore)?;
        let locks = semaphore.generate_mutexes(0);
        log::debug!("creating semaphore {} locks for {}",
                   locks.len(), &semaphore);
        for lock in locks {
            self.mutex_repository.create(&lock).await?;
        }
        Ok(size)
    }

    // updates existing semaphore item
    async fn update(&self, other_version: &str, semaphore: &Semaphore) -> LockResult<usize> {
        let old = self.get(
            semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await?;
        match old.max_size.cmp(&semaphore.max_size) {
            Ordering::Less => {
                let new_locks = semaphore.generate_mutexes(old.max_size);
                log::debug!("will create new {} locks for {} semaphore after resize",
                       new_locks.len(), &semaphore);
                for lock in new_locks {
                    self.mutex_repository.create(&lock).await?;
                }
            }
            Ordering::Equal => {
                // nothing to do
            }
            Ordering::Greater => {
                let expired = old.generate_mutexes(semaphore.max_size);
                log::debug!("update will delete {} locks for {} semaphore after resize {}",
                       expired.len(), &semaphore, semaphore.max_size);
                for lock in expired {
                    self.mutex_repository.delete_expired_lock(
                        lock.mutex_key.as_str(), lock.tenant_id.as_str()).await?;
                }
            }
        }
        let mut conn = self.client.get_connection()?;
        self.update_semaphore(&mut conn, other_version, semaphore)
    }

    // find by key
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<Semaphore> {
        let mut conn = self.client.get_connection()?;
        self.get_semaphore(&mut conn, other_key, other_tenant_id)
    }

    // delete semaphore
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str,
    ) -> LockResult<usize> {
        let old = self.get(other_key, other_tenant_id).await?;
        let expired = old.generate_mutexes(0);
        // we will try to delete lock items before deleting semaphore
        for lock in expired {
            self.mutex_repository.delete_expired_lock(
                lock.mutex_key.as_str(), lock.tenant_id.as_str()).await?;
        }
        let mut conn = self.client.get_connection()?;
        self.delete_semaphore(&mut conn, other_key, other_tenant_id, other_version)
    }

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<Semaphore>> {
        if let Some(next_token) = page {
            if next_token == "-1" {
                return Ok(PaginatedResult::new(page, Some("-1".to_string()), page_size, 0, vec![]));
            }
        }

        let mut conn = self.client.get_connection()?;
        let offset = page.unwrap_or("0");
        let result: (String, Vec<String>) = redis::cmd("HSCAN").arg(self.build_table_name(other_tenant_id))
            .arg(offset).arg("COUNT").arg(page_size).query(&mut conn)?;
        let mut records = vec![];
        for (i, val) in result.1.iter().enumerate() {
            if i % 2 == 1 {
                let semaphore: Semaphore = serde_json::from_str(val.as_str())?;
                records.push(semaphore);
            }
        }
        Ok(PaginatedResult::from_redis(result.0.as_str(), page, page_size, records))
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

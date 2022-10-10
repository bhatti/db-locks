use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel_migrations::{FileBasedMigrations, MigrationHarness};
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, MutexLock, PaginatedResult};
use crate::domain::schema::mutexes;
use crate::domain::schema::mutexes::dsl::*;
use crate::repository::MutexRepository;
use crate::repository::pool_decl;

#[derive(Debug, Clone)]
pub(crate) struct OrmMutexRepository {
    pool: pool_decl!(),
}

impl OrmMutexRepository {
    pub(crate) fn new(pool: pool_decl!()) -> Self {
        OrmMutexRepository {
            pool,
        }
    }
}

#[async_trait]
impl MutexRepository for OrmMutexRepository {
    // run migrations or create table if necessary
    async fn setup_database(&self, _: bool) -> LockResult<()> {
        // run migrations when acquiring data source pool
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(
                format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        let migrations = FileBasedMigrations::find_migrations_directory()?;
        match conn.run_pending_migrations(migrations) {
            Ok(_) => {}
            Err(err) => {
                return Err(LockError::database(
                    format!("failed to run pending migrations {}", err).as_str(), None, false));
            }
        };
        conn.begin_test_transaction()?;
        Ok(())
    }

    // create lock item
    async fn create(&self, mutex: &MutexLock) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;

        match diesel::insert_into(mutexes::table)
            .values(mutex)
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    log::debug!("created mutex lock {} {}", mutex, size);
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to insert {}",
                                mutex.mutex_key).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    // updates existing lock item for acquire
    async fn acquire_update(&self,
                            old_version: &str,
                            lock: &MutexLock) -> LockResult<usize> {
        let now = Some(Utc::now().naive_utc());
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;

        match diesel::update(
            mutexes.filter(
                version.eq(&old_version)
                    .and(mutex_key.eq(&lock.mutex_key))
                    .and(semaphore_key.eq(&lock.semaphore_key).or(semaphore_key.is(&lock.semaphore_key)))
                    .and(tenant_id.eq(&lock.tenant_id).or(tenant_id.is(&lock.tenant_id)))
                    .and(locked.eq(Some(false)).or(expires_at.lt(now)))
            ))
            .set((
                version.eq(&lock.version),
                data.eq(&lock.data),
                tenant_id.eq(&lock.tenant_id),
                delete_on_release.eq(lock.delete_on_release),
                locked.eq(Some(true)),
                lease_duration_ms.eq(lock.lease_duration_ms),
                expires_at.eq(lock.expires_at),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq(&lock.updated_by),
            ))
            .execute(&mut conn)
        {
            Ok(size) => {
                if size > 0 {
                    log::debug!("acquired lock {} {}", lock, size);
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to find records for acquiring lock update {} version {:?}",
                                lock.mutex_key, lock.version).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    // updates existing lock item for heartbeat
    async fn heartbeat_update(&self,
                              old_version: &str,
                              mutex: &MutexLock) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        match diesel::update(
            mutexes.filter(mutex_key.eq(&mutex.mutex_key)
                .and(version.eq(&old_version))
                .and(tenant_id.eq(&mutex.tenant_id).or(tenant_id.is(&mutex.tenant_id)))
            ))
            .set((
                version.eq(&mutex.version),
                data.eq(&mutex.data),
                lease_duration_ms.eq(mutex.lease_duration_ms),
                expires_at.eq(mutex.expires_at),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq(&mutex.updated_by),
            ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    log::debug!("updated heartbeat for mutex lock {} {}", mutex, size);
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to find records for updating heartbeat {}",
                                mutex.mutex_key).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    // updates existing lock item for release
    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            other_data: Option<&str>) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        match diesel::update(
            mutexes.filter(mutex_key.eq(&other_key)
                .and(version.eq(&other_version))
                .and(tenant_id.eq(&other_tenant_id).or(tenant_id.is(&other_tenant_id)))
            ))
            .set((
                locked.eq(Some(false)),
                data.eq(other_data),
                expires_at.eq(Some(NaiveDateTime::from_timestamp(0, 0))),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq("".to_string()),
            ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    log::debug!("released mutex lock {} {}", other_key, size);
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to find records for updating release {}",
                                other_key).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    // get lock by key
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<MutexLock> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        match mutexes
            .filter(
                mutex_key.eq(&other_key)
                    .and(tenant_id.eq(&other_tenant_id).or(tenant_id.is(&other_tenant_id))))
            .limit(2)
            .load::<MutexLock>(&mut conn) {
            Ok(mut items) => {
                if items.len() > 1 {
                    return Err(LockError::database(
                        format!("too many lock items for {}, tenant_id {}",
                                other_key, other_tenant_id).as_str(), None, false));
                } else if !items.is_empty() {
                    if let Some(lock) = items.pop() {
                        return Ok(lock);
                    }
                }
                return Err(LockError::not_found(
                    format!("lock not found for {}, tenant_id {}",
                            other_key, other_tenant_id).as_str()));
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    // delete lock
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str,
    ) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        match diesel::delete(
            mutexes
                .filter(
                    mutex_key.eq(&other_key)
                        .and(version.eq(&other_version))
                        .and(tenant_id.eq(&other_tenant_id).or(tenant_id.is(&other_tenant_id)))
                ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    log::debug!("deleted mutex lock {} {}", other_key, size);
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to find records for deleting {} {:?}",
                                other_key, other_version).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    async fn delete_expired_lock(&self,
                                 other_key: &str,
                                 other_tenant_id: &str) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        let now = Some(Utc::now().naive_utc());
        match diesel::delete(
            mutexes
                .filter(mutex_key.eq(other_key)
                    .and(tenant_id.eq(&other_tenant_id).or(tenant_id.is(&other_tenant_id)))
                    .and(locked.eq(Some(false)).or(expires_at.lt(now)))
                ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    log::debug!("deleted expired lock {} {} - {}", other_key, other_tenant_id, size);
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to find records for deleting {}",
                                other_key).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        let offset: i64 = page_size as i64 * page.unwrap_or("0").parse().unwrap_or(0);
        match mutexes
            .filter(tenant_id.eq(&other_tenant_id.to_string()).or(tenant_id.is(&other_tenant_id.to_string())))
            .offset(offset)
            .limit(page_size as i64)
            .load::<MutexLock>(&mut conn) {
            Ok(items) => {
                Ok(PaginatedResult::from_rdb(page, page_size, items))
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }


    // find by semaphore
    async fn find_by_semaphore(&self,
                               other_semaphore_key: &str,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        let offset: i64 = page_size as i64 * page.unwrap_or("0").parse().unwrap_or(0);
        match mutexes
            .filter(
                semaphore_key.eq(&other_semaphore_key)
                    .and(tenant_id.eq(&other_tenant_id.to_string()).or(tenant_id.is(&other_tenant_id.to_string())))
            )
            .offset(offset)
            .limit(page_size as i64)
            .load::<MutexLock>(&mut conn)
        {
            Ok(items) => {
                Ok(PaginatedResult::from_rdb(page, page_size, items))
            }
            Err(err) => { Err(LockError::from(err)) }
        }
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


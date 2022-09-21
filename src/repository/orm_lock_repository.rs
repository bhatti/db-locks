use chrono::{NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};

use crate::domain::models::{LockError, LockItem, LockResult};
use crate::domain::schema::lock_items;
use crate::domain::schema::lock_items::dsl::*;
use crate::repository::LockRepository;
use crate::repository::pool_decl;
use crate::repository::build_pool;

#[derive(Debug, Clone)]
pub(crate) struct OrmLockRepository {
    pool: pool_decl!(),
}

impl OrmLockRepository {
    pub(crate) fn new(pool: pool_decl!()) -> Box<dyn LockRepository + Send + Sync> {
        Box::new(OrmLockRepository {
            pool,
        })
    }
}

impl LockRepository for OrmLockRepository {
    // create lock item
    fn create(&self, lock: &LockItem) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;

        match diesel::insert_into(lock_items::table)
            .values(lock)
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(format!("failed to insert {}", lock.key), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to insert {} due to {}", lock.key, err), None))
            }
        }
    }

    // updates existing lock item for acquire
    fn acquire_update(&self,
                      old_version: Option<String>,
                      lock: &LockItem) -> LockResult<usize> {
        if old_version == None || lock.version == None {
            return Err(LockError::validation(format!("no version specified for {}", lock.key), None));
        }
        let now = Some(Utc::now().naive_utc());
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;

        match diesel::update(
            lock_items.filter(
                version.eq(&old_version)
                    .and(key.eq(&lock.key).or(semaphore_key.eq(&lock.semaphore_key)))
                    .and(owner.eq(&lock.owner).or(owner.is(&lock.owner)))
                    .and(locked.eq(Some(false)).or(expires_at.lt(now)))
            ))
            .set((
                version.eq(&lock.version),
                data.eq(&lock.data),
                replace_data.eq(&lock.replace_data),
                owner.eq(&lock.owner),
                delete_on_release.eq(lock.delete_on_release),
                locked.eq(Some(true)),
                lease_duration_ms.eq(lock.lease_duration_ms),
                reentrant.eq(lock.reentrant),
                acquire_only_if_already_exists.eq(lock.acquire_only_if_already_exists),
                refresh_period_ms.eq(lock.refresh_period_ms),
                additional_time_to_wait_for_lock_ms.eq(lock.additional_time_to_wait_for_lock_ms),
                expires_at.eq(lock.expires_at),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq(&lock.updated_by),
            ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(format!("failed to find records for acquiring lock update {} version {:?}", lock.key, lock.version), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to acquire update {} version {:?} due to {}", lock.key, lock.version, err), None))
            }
        }
    }

    // updates existing lock item for heartbeat
    fn heartbeat_update(&self,
                        old_version: Option<String>,
                        lock: &LockItem) -> LockResult<usize> {
        if old_version == None || lock.version == None {
            return Err(LockError::validation(format!("no version specified for {}", lock.key), None));
        }
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match diesel::update(
            lock_items.filter(key.eq(&lock.key)
                .and(version.eq(&old_version))
                .and(owner.eq(&lock.owner).or(owner.is(&lock.owner)))
            ))
            .set((
                version.eq(&lock.version),
                data.eq(&lock.data),
                lease_duration_ms.eq(lock.lease_duration_ms),
                expires_at.eq(lock.expires_at),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq(&lock.updated_by),
            ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(format!("failed to find records for updating heartbeat {}", lock.key), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to update heartbeat {} due to {}", lock.key, err), None))
            }
        }
    }

    // updates existing lock item for release
    fn release_update(&self,
                      other_key: String,
                      other_version: Option<String>,
                      other_owner: Option<String>,
                      other_data: Option<String>) -> LockResult<usize> {
        if other_version == None {
            return Err(LockError::validation(format!("no version specified for {}", other_key), None));
        }
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match diesel::update(
            lock_items.filter(key.eq(&other_key)
                .and(version.eq(&other_version))
                .and(owner.eq(&other_owner).or(owner.is(&other_owner)))
            ))
            .set((
                owner.eq(&other_owner),
                locked.eq(Some(false)),
                data.eq(other_data),
                expires_at.eq(Some(NaiveDateTime::from_timestamp(0, 0))),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq("".to_string()),
            ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(format!("failed to find records for updating release {}", other_key), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to update release {} due to {}", other_key, err), None))
            }
        }
    }

    // get lock by key
    fn get(&self, other_key: String, other_owner: Option<String>) -> LockResult<LockItem> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match lock_items
            .filter(
                key.eq(&other_key)
                    .and(owner.eq(&other_owner).or(owner.is(&other_owner))))
            .limit(2)
            .load::<LockItem>(&mut conn) {
            Ok(mut items) => {
                if items.len() > 1 {
                    return Err(LockError::database(format!("too many lock items for {} owner {:?}", other_key, other_owner), None, false));
                } else if items.len() > 0 {
                    if let Some(lock) = items.pop() {
                        return Ok(lock);
                    }
                }
                return Err(LockError::not_found(format!("lock not found for {} owner {:?}", other_key, other_owner)));
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to query {} due to {}", other_key, err), None))
            }
        }
    }

    // delete lock
    fn delete(&self,
              other_key: String,
              other_version: Option<String>,
              other_owner: Option<String>) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match diesel::delete(
            lock_items
                .filter(
                    key.eq(&other_key)
                        .and(version.eq(&other_version))
                        .and(owner.eq(&other_owner).or(owner.is(&other_owner)))
                ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(format!("failed to find records for deleting {} {:?}", other_key, other_version), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to delete {} due to {}", other_key, err), None))
            }
        }
    }

    fn delete_expired_lock(&self, other_key: String,
                           other_owner: Option<String>) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        let now = Some(Utc::now().naive_utc());
        match diesel::delete(
            lock_items
                .filter(key.eq(other_key.clone())
                    .and(owner.eq(&other_owner).or(owner.is(&other_owner)))
                    .and(locked.eq(Some(false)).or(expires_at.lt(now)))
                ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    log::info!("deleted semaphore lock {}", other_key);
                    Ok(size)
                } else {
                    Err(LockError::database(format!("failed to find records for deleting {}", other_key), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to delete {} due to {}", other_key, err), None))
            }
        }
    }

    // find by owner
    fn find_by_owner(&self, other_owner: String) -> LockResult<Vec<LockItem>> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match lock_items
            .filter(owner.eq(&other_owner).or(owner.is(&other_owner)))
            .limit(5)
            .load::<LockItem>(&mut conn) {
            Ok(items) => {
                Ok(items)
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to find by owner {} due to {}", other_owner, err), None))
            }
        }
    }


    // find by semaphore
    fn find_by_semaphore(&self, other_semaphore_key: String, other_owner: Option<String>) -> LockResult<Vec<LockItem>> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match lock_items
            .filter(
                semaphore_key.eq(&other_semaphore_key)
                    .and(owner.eq(&other_owner).or(owner.is(&other_owner)))
            )
            .limit(1000)
            .load::<LockItem>(&mut conn)
        {
            Ok(items) => {
                Ok(items)
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to find by semaphore {} due to {}", other_semaphore_key, err), None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use futures::future::join_all;
    use crate::domain::models::AcquireLockOptionsBuilder;
    use crate::repository::data_source;
    use crate::repository::orm_lock_repository::OrmLockRepository;

    use super::*;
    use uuid::Uuid;
    use std::env;

    #[test]
    fn test_should_get_lock_after_insert_without_owner() {
        let lock_repo = Arc::new(build_lock_repo());
        let lock_key = Uuid::new_v4().to_string();

        let lock = AcquireLockOptionsBuilder::new(lock_key, 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).build().to_lock();
        assert_eq!(1, lock_repo.create(&lock).unwrap());
        assert_eq!(lock, lock_repo.get(lock.key.clone(), lock.owner.clone()).unwrap());
    }

    #[test]
    fn test_should_get_lock_after_insert_with_owner() {
        let lock_repo = Arc::new(build_lock_repo());
        let lock_key = Uuid::new_v4().to_string();

        let mut lock = AcquireLockOptionsBuilder::new(lock_key, 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).build().to_lock();
        lock.owner = Some("test".to_string());
        assert_eq!(1, lock_repo.create(&lock).unwrap());
        assert_eq!(lock, lock_repo.get(lock.key.clone(), lock.owner.clone()).unwrap());
    }

    #[test]
    fn test_should_get_lock_after_update() {
        let lock_repo = Arc::new(build_lock_repo());
        let lock_key = Uuid::new_v4().to_string();

        let lock = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).build().to_lock();
        assert_eq!(1, lock_repo.create(&lock).unwrap());
        let mut after_insert = lock_repo.get(lock_key.clone(), lock.owner.clone()).unwrap();
        assert_eq!(lock, after_insert);
        let old_version = lock.version.clone();
        after_insert.data = Some("data".to_string());
        after_insert.version = Some(Uuid::new_v4().to_string());
        assert_eq!(1, lock_repo.acquire_update(old_version, &after_insert).unwrap());
        let after_update = lock_repo.get(lock_key.clone(), lock.owner.clone()).unwrap();
        assert_eq!(after_insert, after_update);
        assert_eq!(Some(true), after_update.locked);
        assert_eq!(1, lock_repo.release_update(lock_key.clone(), after_update.version, after_update.owner, None).unwrap());
        let after_release = lock_repo.get(lock_key.clone(), lock.owner.clone()).unwrap();
        assert_eq!(Some(false), after_release.locked);
        assert_eq!(None, after_release.data);
    }

    #[test]
    fn test_should_not_get_lock_after_delete() {
        let lock_repo = Arc::new(build_lock_repo());
        let lock_key = Uuid::new_v4().to_string();

        let lock = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).build().to_lock();
        assert_eq!(1, lock_repo.create(&lock).unwrap());
        let after_insert = lock_repo.get(lock_key.clone(), lock.owner.clone()).unwrap();
        assert_eq!(lock, after_insert);
        assert_eq!(1, lock_repo.delete(lock_key.clone(), after_insert.version.clone(), after_insert.owner.clone()).unwrap());
        assert_eq!(true, lock_repo.get(lock_key.clone(), lock.owner.clone()).is_err());
    }

    #[test]
    fn test_should_not_delete_acquired_lock() {
        let lock_repo = Arc::new(build_lock_repo());
        let lock_key = Uuid::new_v4().to_string();

        let mut lock = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).build().to_lock();
        assert_eq!(1, lock_repo.create(&lock).unwrap());
        let old_version = lock.version.clone();
        lock.version = Some(Uuid::new_v4().to_string());
        assert_eq!(1, lock_repo.acquire_update(old_version, &lock).unwrap());
        assert_eq!(true, lock_repo.delete_expired_lock(lock_key.clone(), lock.owner.clone()).is_err());
        assert_eq!(1, lock_repo.release_update(lock_key.clone(), lock.version, lock.owner.clone(), None).unwrap());
        assert_eq!(1, lock_repo.delete_expired_lock(lock_key.clone(), lock.owner.clone()).unwrap());
    }

    #[test]
    fn test_should_send_heartbeat() {
        let lock_repo = Arc::new(build_lock_repo());
        let lock_key = Uuid::new_v4().to_string();

        let mut lock = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_reentrant(true)
            .with_refresh_period_ms(10).build().to_lock();
        assert_eq!(1, lock_repo.create(&lock).unwrap());
        let old_version = lock.version.clone();
        lock.data = Some("data".to_string());
        lock.version = Some(Uuid::new_v4().to_string());
        assert_eq!(1, lock_repo.acquire_update(old_version.clone(), &lock).unwrap());
        // lock is already expired
        assert_eq!(true, lock_repo.acquire_update(old_version.clone(), &lock).is_err());
        let after_lock = lock_repo.get(lock_key.clone(), lock.owner.clone()).unwrap();
        //
        assert_eq!(1, lock_repo.heartbeat_update(after_lock.version.clone(), &after_lock).unwrap());
        //
        let after_update = lock_repo.get(lock_key.clone(), after_lock.owner.clone()).unwrap();
        assert_eq!(lock, after_update);
        assert_eq!(Some(true), after_update.locked);
        assert_eq!(1, lock_repo.release_update(lock_key.clone(), after_update.version, after_update.owner, None).unwrap());
    }

    #[tokio::test]
    async fn test_should_update_locks_concurrently() {
        let lock_repo = Arc::new(build_lock_repo());
        let lock_key = Uuid::new_v4().to_string();
        let lock = AcquireLockOptionsBuilder::new(lock_key.clone(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).with_data("data1".to_string()).build().to_lock();
        assert_eq!(1, lock_repo.create(&lock).unwrap());

        let repetition_count = 10;
        let thread_count = 5;
        let mut tasks = vec![];
        for _i in 0..thread_count {
            tasks.push(repeat_update_locks(&lock_repo, repetition_count, &lock));
        }
        join_all(tasks).await;
    }

    async fn repeat_update_locks(lock_repo: &Arc<Box<dyn LockRepository + Send + Sync>>, repetition_count: i32, lock: &LockItem) {
        for _j in 0..repetition_count {
            update_test_lock(&lock_repo, lock);
        }
    }

    fn update_test_lock(lock_repo: &Arc<Box<dyn LockRepository + Send + Sync>>, lock: &LockItem) {
        assert_eq!(*lock, lock_repo.get(lock.key.clone(), lock.owner.clone()).unwrap());
        assert_eq!(1, lock_repo.acquire_update(lock.version.clone(), &lock).unwrap());
        assert_eq!(true, lock_repo.acquire_update(lock.version.clone(), &lock).is_err());
        assert_eq!(1, lock_repo.heartbeat_update(lock.version.clone(), &lock).unwrap());
        assert_eq!(1, lock_repo.release_update(lock.key.clone(), lock.version.clone(), lock.owner.clone(), None).unwrap());
    }

    fn build_lock_repo() -> Box<dyn LockRepository + Send + Sync> {
        let _ = env_logger::builder().is_test(true).try_init();
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| String::from("test_db.sqlite"));
        let pool = build_pool!(database_url );
        OrmLockRepository::new(pool.clone())
    }
}

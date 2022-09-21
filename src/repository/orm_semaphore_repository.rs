use chrono::Utc;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};

use crate::domain::models::{LockError, LockResult, Semaphore};
use crate::domain::schema::semaphores;
use crate::domain::schema::semaphores::dsl::*;
use crate::repository::SemaphoreRepository;
use crate::repository::LockRepository;
use crate::repository::pool_decl;
use crate::repository::build_pool;

pub(crate) struct OrmSemaphoreRepository {
    pool: pool_decl!(),
    lock_repository: Box<dyn LockRepository + Send + Sync>,
}

impl OrmSemaphoreRepository {
    pub(crate) fn new(pool: pool_decl!(),
                      lock_repository: Box<dyn LockRepository + Send + Sync>) -> Box<dyn SemaphoreRepository + Send + Sync> {
        Box::new(OrmSemaphoreRepository {
            pool,
            lock_repository,
        })
    }

    fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;

        match diesel::insert_into(semaphores::table)
            .values(semaphore)
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(format!("failed to insert semaphore {}", semaphore.key), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to insert semaphore {} due to {}", semaphore.key, err), None))
            }
        }
    }

    fn update_semaphore(&self, semaphore: &Semaphore) -> LockResult<(Semaphore, usize)> {
        let old = self.get(semaphore.key.clone(), semaphore.owner.clone())?;
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;

        match diesel::update(
            semaphores.filter(key.eq(&semaphore.key)
                .and(version.eq(&semaphore.version))
                .and(owner.eq(&semaphore.owner).or(owner.is(&semaphore.owner)))
            ))
            .set((
                version.eq(&semaphore.version),
                data.eq(&semaphore.data),
                replace_data.eq(&semaphore.replace_data),
                owner.eq(&semaphore.owner),
                delete_on_release.eq(semaphore.delete_on_release),
                lease_duration_ms.eq(semaphore.lease_duration_ms),
                max_size.eq(semaphore.max_size),
                reentrant.eq(semaphore.reentrant),
                refresh_period_ms.eq(semaphore.refresh_period_ms),
                additional_time_to_wait_for_lock_ms.eq(semaphore.additional_time_to_wait_for_lock_ms),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq(&semaphore.updated_by),
            ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok((old, size))
                } else {
                    Err(LockError::database(format!("failed to find records to update semaphore {}", semaphore), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to update semaphore {} due to {}", semaphore, err), None))
            }
        }
    }

    fn delete_semaphore(&self,
                        other_key: String,
                        other_version: Option<String>,
                        other_owner: Option<String>) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match diesel::delete(
            semaphores
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
                    Err(LockError::database(format!("failed to find records for deleting semaphore {} version {:?} owner {:?}", other_key, other_version, other_owner), None, false))
                }
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to delete semaphore {} version {:?} owner {:?} due to {}", other_key, other_version, other_owner, err), None))
            }
        }
    }
}


impl SemaphoreRepository for OrmSemaphoreRepository {
    // create semaphore
    fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let size = self.create_semaphore(semaphore)?;
        let locks = semaphore.generate_lock_items(0);
        log::info!("creating semaphore {} locks for {}", locks.len(), semaphore.key.clone());
        for lock in locks {
            self.lock_repository.create(&lock)?;
        }
        Ok(size)
    }

    // updates existing semaphore item
    fn update(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let (old, size) = self.update_semaphore(semaphore)?;
        if old.max_size == semaphore.max_size {
            // nothing to do
        } else if old.max_size > semaphore.max_size {
            let expired = old.generate_lock_items(semaphore.max_size);
            log::info!("will delete {} locks for {} semaphore after resize", expired.len(), semaphore.key.clone());
            for lock in expired {
                if let Err(err) = self.lock_repository.delete_expired_lock(lock.key.clone(), lock.owner) {
                    log::warn!("failed to delete semaphore lock {} because {}", lock.key.clone(), err);
                }
            }
        } else {
            let new_locks = semaphore.generate_lock_items(old.max_size);
            log::info!("will create new {} locks for {} semaphore after resize", new_locks.len(), semaphore.key.clone());
            for lock in new_locks {
                self.lock_repository.create(&lock)?;
            }
        }
        Ok(size)
    }

    // find by key
    fn get(&self, other_key: String, other_owner: Option<String>) -> LockResult<Semaphore> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match semaphores
            .filter(
                key.eq(&other_key)
                    .and(owner.eq(&other_owner).or(owner.is(&other_owner)))
            )
            .limit(2)
            .load::<Semaphore>(&mut conn) {
            Ok(mut items) => {
                if items.len() > 1 {
                    return Err(LockError::database(format!("too many semaphores for {} {:?}", other_key, other_owner), None, false));
                } else if items.len() > 0 {
                    if let Some(next) = items.pop() {
                        return Ok(next);
                    }
                }
                return Err(LockError::not_found(format!("semaphore not found for {} owner={:?}", other_key, other_owner)));
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to query {} owner {:?} due to {}", other_key, other_owner, err), None))
            }
        }
    }

    // delete semaphore
    fn delete(&self,
              other_key: String,
              other_version: Option<String>,
              other_owner: Option<String>) -> LockResult<usize> {
        let old = self.get(other_key.clone(), other_owner.clone())?;
        let expired = old.generate_lock_items(0);
        // we will try to delete lock items before deleting semaphore
        for lock in expired {
            self.lock_repository.delete_expired_lock(lock.key, lock.owner)?;
        }
        self.delete_semaphore(other_key, other_version, other_owner)
    }

    // find by owner
    fn find_by_owner(&self, other_owner: Option<String>) -> LockResult<Vec<Semaphore>> {
        let mut conn = self.pool.get().map_err(|err| LockError::database(format!("failed to get pool connection due to {}", err), None, true))?;
        match semaphores
            .filter(owner.eq(&other_owner))
            .limit(5)
            .load::<Semaphore>(&mut conn) {
            Ok(items) => {
                Ok(items)
            }
            Err(err) => {
                Err(LockError::unavailable(format!("failed to find by owner {:?} due to {}", other_owner, err), None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use futures::future::join_all;
    use crate::domain::models::{SemaphoreBuilder};
    use crate::repository::data_source;
    use crate::repository::orm_lock_repository::OrmLockRepository;

    use super::*;
    use uuid::Uuid;
    use rand::Rng;
    use std::env;

    #[test]
    fn test_should_create_and_load_semaphore() {
        let semaphore_repo = Arc::new(build_semaphore_repo());
        let semaphore_key = Uuid::new_v4().to_string();

        let semaphore = SemaphoreBuilder::new(semaphore_key.clone(), 1000, 30)
            .with_refresh_period_ms(10).build();
        assert_eq!(1, semaphore_repo.create(&semaphore).unwrap());
        assert_eq!(semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        assert_eq!(30, locks.len());
    }

    #[test]
    fn test_should_not_resize_semaphore_after_acquire_lock() {
        let semaphore_repo = Arc::new(build_semaphore_repo());
        let semaphore_key = Uuid::new_v4().to_string();

        let mut semaphore = SemaphoreBuilder::new(semaphore_key.clone(), 1000, 50)
            .with_refresh_period_ms(10)
            .build();
        assert_eq!(1, semaphore_repo.create(&semaphore).unwrap());
        assert_eq!(semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        assert_eq!(50, locks.len());
        for lock in locks {
            if let Some(rank) = lock.key_rank() {
                if rank < 40 {
                    assert_eq!(1, semaphore_repo.lock_repository.acquire_update(lock.version.clone(), &lock).unwrap());
                }
            }
        }
        semaphore.max_size = 30;
        assert_eq!(1, semaphore_repo.update(&semaphore).unwrap());
        assert_eq!(semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        // we locked 40 locks so those should not be deleted
        assert_eq!(40, locks.len());
    }

    #[test]
    fn test_should_resize_semaphore_without_lock() {
        let semaphore_repo = Arc::new(build_semaphore_repo());
        let semaphore_key = Uuid::new_v4().to_string();

        let mut semaphore = SemaphoreBuilder::new(semaphore_key.clone(), 1000, 50)
            .with_refresh_period_ms(10)
            .build();
        assert_eq!(1, semaphore_repo.create(&semaphore).unwrap());
        assert_eq!(semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        assert_eq!(50, locks.len());

        semaphore.max_size = 50;
        assert_eq!(1, semaphore_repo.update(&semaphore).unwrap());
        assert_eq!(semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        assert_eq!(50, locks.len());

        semaphore.max_size = 100;
        assert_eq!(1, semaphore_repo.update(&semaphore).unwrap());
        assert_eq!(semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        assert_eq!(100, locks.len());

        semaphore.max_size = 20;
        assert_eq!(1, semaphore_repo.update(&semaphore).unwrap());
        assert_eq!(semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        assert_eq!(20, locks.len());
    }

    #[test]
    fn test_should_not_get_semaphore_after_delete() {
        let semaphore_repo = Arc::new(build_semaphore_repo());
        let semaphore_key = Uuid::new_v4().to_string();

        let semaphore = SemaphoreBuilder::new(semaphore_key.clone(), 1000, 50)
            .with_refresh_period_ms(10).build();
        assert_eq!(1, semaphore_repo.create(&semaphore).unwrap());
        let after_insert = semaphore_repo.get(semaphore_key.clone(), semaphore.owner.clone()).unwrap();
        assert_eq!(semaphore, after_insert);
        assert_eq!(1, semaphore_repo.delete(semaphore_key.clone(), after_insert.version.clone(), after_insert.owner.clone()).unwrap());
        assert_eq!(true, semaphore_repo.get(semaphore_key.clone(), semaphore.owner.clone()).is_err());
    }

    #[test]
    fn test_should_not_delete_acquired_lock() {
        let semaphore_repo = Arc::new(build_semaphore_repo());
        let semaphore_key = Uuid::new_v4().to_string();

        let semaphore = SemaphoreBuilder::new(semaphore_key.clone(), 1000, 50)
            .with_refresh_period_ms(10).build();
        assert_eq!(1, semaphore_repo.create(&semaphore).unwrap());
        let locks = semaphore_repo.lock_repository.find_by_semaphore(semaphore_key.clone(), None).unwrap();
        assert_eq!(50, locks.len());
        for lock in locks {
            if let Some(rank) = lock.key_rank() {
                if rank < 40 {
                    assert_eq!(1, semaphore_repo.lock_repository.acquire_update(lock.version.clone(), &lock).unwrap());
                }
            }
        }
        assert_eq!(true, semaphore_repo.delete(semaphore_key.clone(), semaphore.version.clone(), semaphore.owner.clone()).is_err());
    }

    #[tokio::test]
    async fn test_should_update_locks_concurrently() {
        let semaphore_repo = Arc::new(build_semaphore_repo());
        let semaphore_key = Uuid::new_v4().to_string();
        let semaphore = SemaphoreBuilder::new(semaphore_key, 1000, 50)
            .with_refresh_period_ms(10).build();
        assert_eq!(1, semaphore_repo.create(&semaphore).unwrap());

        let repetition_count = 10;
        let thread_count = 5;
        let mut tasks = vec![];
        for _i in 0..thread_count {
            tasks.push(repeat_update_semaphores(&semaphore_repo, repetition_count, &semaphore));
        }
        join_all(tasks).await;
    }

    async fn repeat_update_semaphores(semaphore_repo: &Arc<OrmSemaphoreRepository>, repetition_count: i32, semaphore: &Semaphore) {
        for _j in 0..repetition_count {
            update_test_semaphore(&semaphore_repo, semaphore);
        }
    }

    fn update_test_semaphore(semaphore_repo: &Arc<OrmSemaphoreRepository>, semaphore: &Semaphore) {
        assert_eq!(*semaphore, semaphore_repo.get(semaphore.key.clone(), semaphore.owner.clone()).unwrap());
        let mut rng = rand::thread_rng();
        let mut copy = semaphore.clone();
        copy.max_size = rng.gen_range(1..100);
        assert_eq!(1, semaphore_repo.update(&copy).unwrap());
    }

    fn build_semaphore_repo() -> OrmSemaphoreRepository {
        let _ = env_logger::builder().is_test(true).try_init();
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| String::from("test_db.sqlite"));
        let pool = build_pool!(database_url );
        OrmSemaphoreRepository {
            pool: pool.clone(),
            lock_repository: OrmLockRepository::new(pool.clone()),
        }
    }
}

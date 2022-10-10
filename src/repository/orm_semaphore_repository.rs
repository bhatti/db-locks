use std::cmp::Ordering;
use async_trait::async_trait;
use chrono::Utc;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, PaginatedResult, Semaphore};
use crate::domain::schema::semaphores;
use crate::domain::schema::semaphores::dsl::*;
use crate::repository::MutexRepository;
use crate::repository::pool_decl;
use crate::repository::SemaphoreRepository;

pub(crate) struct OrmSemaphoreRepository {
    pool: pool_decl!(),
    mutex_repository: Box<dyn MutexRepository + Send + Sync>,
}

impl OrmSemaphoreRepository {
    pub(crate) fn new(pool: pool_decl!(),
                      mutex_repository: Box<dyn MutexRepository + Send + Sync>) -> Self {
        OrmSemaphoreRepository {
            pool,
            mutex_repository,
        }
    }

    async fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;

        match diesel::insert_into(semaphores::table)
            .values(semaphore)
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to insert semaphore {}", semaphore.semaphore_key).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    async fn update_semaphore(&self,
                              old_version: &str,
                              semaphore: &Semaphore) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}",
                                        err).as_str(), None, true))?;
        match diesel::update(
            semaphores.filter(semaphore_key.eq(&semaphore.semaphore_key)
                .and(version.eq(&old_version))
                .and(tenant_id.eq(&semaphore.tenant_id).or(tenant_id.is(&semaphore.tenant_id)))
            ))
            .set((
                version.eq(&semaphore.version),
                tenant_id.eq(&semaphore.tenant_id),
                lease_duration_ms.eq(semaphore.lease_duration_ms),
                max_size.eq(semaphore.max_size),
                busy_count.eq(semaphore.busy_count),
                updated_at.eq(Some(Utc::now().naive_utc())),
                updated_by.eq(&semaphore.updated_by),
            ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to find records to update semaphore {}",
                                semaphore).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }

    async fn delete_semaphore(&self,
                              other_key: &str,
                              other_tenant_id: &str,
                              other_version: &str,
                              ) -> LockResult<usize> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}",
                                        err).as_str(), None, true))?;
        match diesel::delete(
            semaphores
                .filter(
                    semaphore_key.eq(&other_key)
                        .and(version.eq(&other_version))
                        .and(tenant_id.eq(&other_tenant_id).or(tenant_id.is(&other_tenant_id)))
                ))
            .execute(&mut conn) {
            Ok(size) => {
                if size > 0 {
                    Ok(size)
                } else {
                    Err(LockError::database(
                        format!("failed to find records for deleting semaphore {} version {:?} tenant_id {:?}",
                                other_key, other_version, other_tenant_id).as_str(), None, false))
                }
            }
            Err(err) => { Err(LockError::from(err)) }
        }
    }
}

#[async_trait]
impl SemaphoreRepository for OrmSemaphoreRepository {
    // run migrations or create table if necessary
    async fn setup_database(&self, _: bool) -> LockResult<()> {
        // run migrations when acquiring data source pool
        Ok(())
    }

    // create semaphore
    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let size = self.create_semaphore(semaphore).await?;
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
        let old = self.get(semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await?;
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
        self.update_semaphore(other_version, semaphore).await
    }

    // find by key
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<Semaphore> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        match semaphores
            .filter(
                semaphore_key.eq(&other_key)
                    .and(tenant_id.eq(&other_tenant_id).or(tenant_id.is(&other_tenant_id)))
            )
            .limit(2)
            .load::<Semaphore>(&mut conn) {
            Ok(mut items) => {
                if items.len() > 1 {
                    return Err(LockError::database(
                        format!("too many semaphores for {} {:?}",
                                other_key, other_tenant_id).as_str(), None, false));
                } else if !items.is_empty() {
                    if let Some(next) = items.pop() {
                        return Ok(next);
                    }
                }
                return Err(LockError::not_found(
                    format!("semaphore not found for key {} tenant_id={:?}",
                            other_key, other_tenant_id).as_str()));
            }
            Err(err) => { Err(LockError::from(err)) }
        }
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
        self.delete_semaphore(other_key, other_tenant_id, other_version).await
    }

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<Semaphore>> {
        let mut conn = self.pool.get().map_err(|err|
            LockError::database(format!("failed to get pool connection due to {}", err).as_str(), None, true))?;
        let offset: i64 = page_size as i64 * page.unwrap_or("0").parse().unwrap_or(0);
        match semaphores
            .filter(tenant_id.eq(&other_tenant_id))
            .offset(offset)
            .limit(page_size as i64)
            .load::<Semaphore>(&mut conn) {
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

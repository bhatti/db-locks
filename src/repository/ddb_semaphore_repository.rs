use std::cmp;
use std::cmp::Ordering;
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::model::AttributeValue;
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, LocksConfig, PaginatedResult, Semaphore};
use crate::repository::{ddb_common, MutexRepository, SemaphoreRepository};

pub(crate) struct DdbSemaphoreRepository {
    client: Client,
    semaphores_table_name: String,
    mutex_repository: Box<dyn MutexRepository + Send + Sync>,
    ddb_read_consistency: bool,
}

impl DdbSemaphoreRepository {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        config: &LocksConfig,
        sdk_config: &SdkConfig,
        mutex_repository: Box<dyn MutexRepository + Send + Sync>) -> LockResult<Box<dyn SemaphoreRepository + Send + Sync>> {
        Ok(Box::new(DdbSemaphoreRepository {
            client: Client::new(sdk_config),
            semaphores_table_name: config.get_semaphores_table_name(),
            mutex_repository,
            ddb_read_consistency: config.has_ddb_read_consistency(),
        }))
    }
}

impl DdbSemaphoreRepository {
    async fn _create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let table_name: &str = self.semaphores_table_name.as_ref();
        let val = serde_json::to_value(&semaphore)?;

        self.client
            .put_item()
            .table_name(table_name)
            .condition_expression("attribute_not_exists(semaphore_key) AND attribute_not_exists(tenant_id)")
            .set_item(Some(ddb_common::parse_item(val)?))
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }

    async fn update_semaphore(&self, old_version: &str, semaphore: &Semaphore) -> LockResult<usize> {
        let table_name: &str = self.semaphores_table_name.as_ref();

        self.client
            .update_item()
            .table_name(table_name)
            .key("semaphore_key", AttributeValue::S(semaphore.semaphore_key.clone()))
            .key("tenant_id", AttributeValue::S(semaphore.tenant_id.clone()))
            .update_expression("SET version = :version, max_size = :max_size, \
            busy_count = :busy_count, lease_duration_ms = :lease_duration_ms, \
            updated_by = :updated_by, updated_at = :updated_at")
            .expression_attribute_values(":old_version", AttributeValue::S(old_version.to_string()))
            .expression_attribute_values(":version", AttributeValue::S(semaphore.version.clone()))
            .expression_attribute_values(":max_size", AttributeValue::N(semaphore.max_size.to_string()))
            .expression_attribute_values(":lease_duration_ms", AttributeValue::N(semaphore.lease_duration_ms.to_string()))
            .expression_attribute_values(":busy_count", AttributeValue::N(semaphore.busy_count.unwrap_or(0).to_string()))
            .expression_attribute_values(":updated_by", AttributeValue::S(semaphore.updated_by.clone().unwrap_or_else(|| "".to_string())))
            .expression_attribute_values(":updated_at", AttributeValue::S(semaphore.updated_at_string()))
            .condition_expression("attribute_exists(version) AND version = :old_version")
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }

    async fn _delete_semaphore(&self,
                               other_key: &str,
                               other_tenant_id: &str,
                               other_version: &str,
    ) -> LockResult<usize> {
        let table_name: &str = self.semaphores_table_name.as_ref();
        self.client.delete_item()
            .table_name(table_name)
            .key("semaphore_key", AttributeValue::S(other_key.to_string().clone()))
            .key("tenant_id", AttributeValue::S(other_tenant_id.to_string().clone()))
            .condition_expression(
                "version = :version",
            )
            .expression_attribute_values(
                ":version",
                AttributeValue::S(other_version.to_string()),
            )
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }
}

#[async_trait]
impl SemaphoreRepository for DdbSemaphoreRepository {
    // create DDB table if needed
    async fn setup_database(&self, recreate: bool) -> LockResult<()> {
        let table_name: &str = self.semaphores_table_name.as_ref();
        if recreate {
            ddb_common::delete_table(&self.client, table_name).await?;
        }
        ddb_common::create_table(&self.client, table_name, "semaphore_key", "version").await
    }


    // create semaphore item in DDB
    async fn create(&self, semaphore: &Semaphore) -> LockResult<usize> {
        let size = self._create_semaphore(semaphore).await?;

        let locks = semaphore.generate_mutexes(0);
        log::debug!("creating semaphore {} locks for {}",
                   locks.len(), semaphore.semaphore_key.clone());
        for lock in locks {
            self.mutex_repository.create(&lock).await?;
        }

        Ok(size)
    }

    // updates existing semaphore item
    async fn update(&self, old_version: &str, semaphore: &Semaphore) -> LockResult<usize> {
        let old = self.get(semaphore.semaphore_key.as_str(), semaphore.tenant_id.as_str()).await?;
        match old.max_size.cmp(&semaphore.max_size) {
            Ordering::Less => {
                let new_locks = semaphore.generate_mutexes(old.max_size);
                log::debug!("will create new {} locks for {} semaphore after resize",
                       new_locks.len(), semaphore.semaphore_key.clone());
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
        self.update_semaphore(old_version, semaphore).await
    }

    // find by key
    async fn get(&self, other_key: &str, other_tenant_id: &str) -> LockResult<Semaphore> {
        let table_name: &str = self.semaphores_table_name.as_ref();
        self.client
            .query()
            .table_name(table_name)
            .limit(2)
            .consistent_read(self.ddb_read_consistency)
            .key_condition_expression(
                "semaphore_key = :key AND tenant_id = :tenant_id",
            )
            .expression_attribute_values(
                ":key",
                AttributeValue::S(other_key.to_string()),
            )
            .expression_attribute_values(
                ":tenant_id",
                AttributeValue::S(other_tenant_id.to_string()),
            )
            .send()
            .await.map_err(LockError::from).and_then(|req| {
            if let Some(items) = req.items {
                if items.len() > 1 {
                    return Err(LockError::database(
                        format!("too many semaphores for {} tenant_id {:?}",
                                other_key, other_tenant_id).as_str(), None, false));
                } else if !items.is_empty() {
                    if let Some(map) = items.first() {
                        return ddb_common::map_to_semaphore(map);
                    }
                }
                Err(LockError::not_found(
                    format!("semaphore not found for key {} tenant_id {:?}",
                            other_key, other_tenant_id).as_str()))
            } else {
                Err(LockError::not_found(
                    format!("semaphore not found for key {} tenant_id {:?}",
                            other_key, other_tenant_id).as_str()))
            }
        })
    }


    // deletes a semaphore
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str,
    ) -> LockResult<usize> {
        let old = self.get(other_key, other_tenant_id).await?;
        let expired = old.generate_mutexes(0);
        // we will try to delete lock items before deleting semaphore
        for lock in expired {
            self.mutex_repository.delete_expired_lock(lock.mutex_key.as_str(), lock.tenant_id.as_str()).await?;
        }
        self._delete_semaphore(other_key, other_tenant_id, other_version).await
    }

    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<Semaphore>> {
        let table_name: &str = self.semaphores_table_name.as_ref();
        let exclusive_start_key = PaginatedResult::<Semaphore>::to_ddb_page(other_tenant_id, page);
        self.client
            .query()
            .table_name(table_name)
            .limit(cmp::max(page_size, 5) as i32)
            .consistent_read(self.ddb_read_consistency)
            .set_exclusive_start_key(exclusive_start_key)
            .key_condition_expression(
                "tenant_id = :tenant_id",
            )
            .expression_attribute_values(
                ":tenant_id",
                AttributeValue::S(other_tenant_id.to_string()),
            )
            .send()
            .await.map_err(LockError::from).and_then(|req| {
            if let Some(ref items) = req.items {
                let mut records = vec![];
                for item in items {
                    records.push(ddb_common::map_to_semaphore(item)?);
                }
                Ok(PaginatedResult::from_ddb(page, req.last_evaluated_key(), page_size, records))
            } else {
                Err(LockError::not_found(
                    format!("semaphores not found for tenant_id {:?}",
                            other_tenant_id).as_str()))
            }
        })
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
        true
    }
}


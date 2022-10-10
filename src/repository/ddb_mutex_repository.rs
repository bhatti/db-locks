use std::cmp;
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::model::AttributeValue;
use chrono::{NaiveDateTime, Utc};
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, LocksConfig, MutexLock, PaginatedResult};
use crate::repository::{ddb_common, MutexRepository};

#[derive(Debug)]
pub(crate) struct DdbMutexRepository {
    client: Client,
    locks_table_name: String,
    ddb_read_consistency: bool,
}

impl DdbMutexRepository {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(config: &LocksConfig, sdk_config: &SdkConfig) -> LockResult<Box<dyn MutexRepository + Send + Sync>> {
        Ok(Box::new(DdbMutexRepository {
            client: Client::new(sdk_config),
            locks_table_name: config.get_mutexes_table_name(),
            ddb_read_consistency: config.has_ddb_read_consistency(),
        }))
    }
}

#[async_trait]
impl MutexRepository for DdbMutexRepository {
    // create DDB table if needed
    async fn setup_database(&self, recreate: bool) -> LockResult<()> {
        let table_name: &str = self.locks_table_name.as_ref();
        if recreate {
            ddb_common::delete_table(&self.client, table_name).await?;
        }
        ddb_common::create_table(&self.client, table_name, "mutex_key", "semaphore_key").await
    }


    // create lock item in DDB
    async fn create(&self, mutex: &MutexLock) -> LockResult<usize> {
        let table_name: &str = self.locks_table_name.as_ref();
        let val = serde_json::to_value(&mutex)?;

        self.client
            .put_item()
            .table_name(table_name)
            .condition_expression("attribute_not_exists(mutex_key) AND attribute_not_exists(tenant_id)")
            .set_item(Some(ddb_common::parse_item(val)?))
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }

    // updates existing lock item for acquire
    async fn acquire_update(&self,
                            old_version: &str,
                            mutex: &MutexLock) -> LockResult<usize> {
        let now = Utc::now().naive_utc();
        let table_name: &str = self.locks_table_name.as_ref();

        self.client
            .update_item()
            .table_name(table_name)
            .key("mutex_key", AttributeValue::S(mutex.mutex_key.clone()))
            .key("tenant_id", AttributeValue::S(mutex.tenant_id.clone()))
            .update_expression("SET version = :version, locked = :locked, lease_duration_ms = :lease_duration_ms, \
           delete_on_release = :delete_on_release, #data = :data, expires_at = :expires_at, updated_by = :updated_by, updated_at = :updated_at")
            .expression_attribute_names("#data", "data")
            .expression_attribute_values(":old_version", AttributeValue::S(old_version.to_string()))
            .expression_attribute_values(":old_locked", AttributeValue::Bool(false))
            .expression_attribute_values(":old_expires_at", AttributeValue::S(ddb_common::date_time_to_string(&now)))
            .expression_attribute_values(":version", AttributeValue::S(mutex.version.clone()))
            .expression_attribute_values(":locked", AttributeValue::Bool(true))
            .expression_attribute_values(":lease_duration_ms", AttributeValue::N(mutex.lease_duration_ms.to_string()))
            .expression_attribute_values(":delete_on_release", AttributeValue::Bool(mutex.delete_on_release.unwrap_or(false)))
            .expression_attribute_values(":data", AttributeValue::S(mutex.data.clone().unwrap_or_else(|| "".to_string())))
            .expression_attribute_values(":expires_at", AttributeValue::S(mutex.expires_at_string()))
            .expression_attribute_values(":updated_by", AttributeValue::S(mutex.updated_by.clone().unwrap_or_else(|| "".to_string())))
            .expression_attribute_values(":updated_at", AttributeValue::S(mutex.updated_at_string()))
            .condition_expression("attribute_exists(version) AND version = :old_version AND \
        (attribute_not_exists(locked) OR attribute_not_exists(expires_at) OR locked = :old_locked OR expires_at < :old_expires_at)")
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }


    // updates existing lock item for heartbeat
    async fn heartbeat_update(&self,
                              old_version: &str,
                              mutex: &MutexLock) -> LockResult<usize> {
        let table_name: &str = self.locks_table_name.as_ref();

        self.client
            .update_item()
            .table_name(table_name)
            .key("mutex_key", AttributeValue::S(mutex.mutex_key.clone()))
            .key("tenant_id", AttributeValue::S(mutex.tenant_id.clone()))
            .update_expression("SET version = :version, lease_duration_ms = :lease_duration_ms, \
           #data = :data, expires_at = :expires_at, updated_by = :updated_by, updated_at = :updated_at")
            .expression_attribute_names("#data", "data")
            .expression_attribute_values(":old_version", AttributeValue::S(old_version.to_string()))
            .expression_attribute_values(":version", AttributeValue::S(mutex.version.clone()))
            .expression_attribute_values(":lease_duration_ms", AttributeValue::N(mutex.lease_duration_ms.to_string()))
            .expression_attribute_values(":data", AttributeValue::S(mutex.data.clone().unwrap_or_else(|| "".to_string())))
            .expression_attribute_values(":expires_at", AttributeValue::S(mutex.expires_at_string()))
            .expression_attribute_values(":updated_by", AttributeValue::S(mutex.updated_by.clone().unwrap_or_else(|| "".to_string())))
            .expression_attribute_values(":updated_at", AttributeValue::S(mutex.updated_at_string()))
            .condition_expression("attribute_exists(version) AND version = :old_version")
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }

    // updates existing lock item for release
    async fn release_update(&self,
                            other_key: &str,
                            other_tenant_id: &str,
                            other_version: &str,
                            other_data: Option<&str>) -> LockResult<usize> {
        let table_name: &str = self.locks_table_name.as_ref();
        let now = ddb_common::date_time_to_string(&Utc::now().naive_utc());
        let epoch = ddb_common::date_time_to_string(&NaiveDateTime::from_timestamp(0, 0));

        self.client
            .update_item()
            .table_name(table_name)
            .key("mutex_key", AttributeValue::S(other_key.to_string()))
            .key("tenant_id", AttributeValue::S(other_tenant_id.to_string()))
            .update_expression("SET locked = :locked, expires_at = :expires_at, \
           #data = :data, updated_by = :updated_by, updated_at = :updated_at")
            .expression_attribute_names("#data", "data")
            .expression_attribute_values(":old_version", AttributeValue::S(other_version.to_string()))
            .expression_attribute_values(":locked", AttributeValue::Bool(false))
            .expression_attribute_values(":expires_at", AttributeValue::S(epoch))
            .expression_attribute_values(":data", AttributeValue::S(other_data.unwrap_or("").to_string()))
            .expression_attribute_values(":updated_by", AttributeValue::S("".to_string()))
            .expression_attribute_values(":updated_at", AttributeValue::S(now))
            .condition_expression("attribute_exists(version) AND version = :old_version")
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }


    // get lock by key
    async fn get(&self,
                 other_key: &str,
                 other_tenant_id: &str) -> LockResult<MutexLock> {
        let table_name: &str = self.locks_table_name.as_ref();
        self.client
            .query()
            .table_name(table_name)
            .limit(2)
            .consistent_read(self.ddb_read_consistency)
            .key_condition_expression(
                "mutex_key = :key AND tenant_id = :tenant_id",
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
                        format!("too many lock items for {} tenant_id {:?}",
                                other_key, other_tenant_id).as_str(), None, false));
                } else if !items.is_empty() {
                    if let Some(map) = items.first() {
                        return ddb_common::map_to_lock(map);
                    }
                }
                Err(LockError::not_found(
                    format!("lock not found for {} tenant_id {}",
                            other_key, other_tenant_id).as_str()))
            } else {
                Err(LockError::not_found(
                    format!("lock not found for {} tenant_id {} without response",
                            other_key, other_tenant_id).as_str()))
            }
        })
    }


    // delete lock
    async fn delete(&self,
                    other_key: &str,
                    other_tenant_id: &str,
                    other_version: &str,
    ) -> LockResult<usize> {
        let table_name: &str = self.locks_table_name.as_ref();
        log::debug!("deleting lock {} {} {}", other_key, other_tenant_id, other_version);
        self.client.delete_item()
            .table_name(table_name)
            .key("mutex_key", AttributeValue::S(other_key.to_string()))
            .key("tenant_id", AttributeValue::S(other_tenant_id.to_string()))
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

    // delete expired lock
    async fn delete_expired_lock(&self, other_key: &str,
                                 other_tenant_id: &str) -> LockResult<usize> {
        let now = ddb_common::date_time_to_string(&Utc::now().naive_utc());
        log::debug!("deleting expired lock {} {} {}", other_key, other_tenant_id, now);
        let table_name: &str = self.locks_table_name.as_ref();
        self.client.delete_item()
            .table_name(table_name)
            .key("mutex_key", AttributeValue::S(other_key.to_string()))
            .key("tenant_id", AttributeValue::S(other_tenant_id.to_string()))
            .condition_expression(
                "attribute_not_exists(locked) OR attribute_not_exists(expires_at) OR \
                    locked = :locked OR expires_at < :expires_at",
            )
            .expression_attribute_values(
                ":locked",
                AttributeValue::Bool(false),
            )
            .expression_attribute_values(
                ":expires_at",
                AttributeValue::S(now),
            )
            .send()
            .await.map(|_| 1).map_err(LockError::from)
    }


    // find by tenant_id
    async fn find_by_tenant_id(&self,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>> {
        let table_name: &str = self.locks_table_name.as_ref();
        let exclusive_start_key = PaginatedResult::<MutexLock>::to_ddb_page(other_tenant_id, page);
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
                    records.push(ddb_common::map_to_lock(item)?);
                }
                Ok(PaginatedResult::from_ddb(page, req.last_evaluated_key(), page_size, records))
            } else {
                Err(LockError::not_found(
                    format!("locks not found for tenant_id {:?}",
                            other_tenant_id).as_str()))
            }
        })
    }


    // find by semaphore
    async fn find_by_semaphore(&self,
                               other_semaphore_key: &str,
                               other_tenant_id: &str,
                               page: Option<&str>,
                               page_size: usize,
    ) -> LockResult<PaginatedResult<MutexLock>> {
        let table_name: &str = self.locks_table_name.as_ref();
        let exclusive_start_key = PaginatedResult::<MutexLock>::to_ddb_page(other_tenant_id, page);
        self.client
            .query()
            .table_name(table_name)
            .index_name("semaphore_key_ndx")
            .limit(cmp::max(page_size, 5) as i32)
            .consistent_read(false) // Consistent reads are not supported on GSI
            .set_exclusive_start_key(exclusive_start_key)
            .key_condition_expression(
                "tenant_id = :tenant_id AND semaphore_key = :semaphore_key",
            )
            .expression_attribute_values(
                ":tenant_id",
                AttributeValue::S(other_tenant_id.to_string()),
            )
            .expression_attribute_values(
                ":semaphore_key",
                AttributeValue::S(other_semaphore_key.to_string()),
            )
            .send()
            .await.map_err(LockError::from).and_then(|req| {
            if let Some(ref items) = req.items {
                let mut records = vec![];
                for item in items {
                    records.push(ddb_common::map_to_lock(item)?);
                }
                log::debug!("find_by_semaphore {} {} - {}", other_semaphore_key, other_tenant_id, records.len());
                Ok(PaginatedResult::from_ddb(page, req.last_evaluated_key(), page_size, records))
            } else {
                Err(LockError::not_found(
                    format!("locks not found for semaphore {} {}",
                            other_semaphore_key, other_tenant_id).as_str()))
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

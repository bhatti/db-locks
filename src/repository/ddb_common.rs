use std::collections::HashMap;

use aws_sdk_dynamodb::{Client, error};
use aws_sdk_dynamodb::model::{AttributeDefinition, AttributeValue, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection, ProjectionType, ProvisionedThroughput, ScalarAttributeType, TableStatus};
use aws_sdk_dynamodb::types::SdkError;
use serde_json::Value;

use crate::domain::models::{LockError, LockResult, MutexLock, Semaphore};
use chrono::NaiveDateTime;
use uuid::Uuid;
use tokio::time::Duration;
use aws_sdk_dynamodb::error::{DeleteItemError, PutItemError, QueryError, UpdateItemError};

const DEFAULT_LEASE_DURATION: i64 = 20000;

pub(crate) fn map_to_lock(map: &HashMap<String, AttributeValue>) -> LockResult<MutexLock> {
    Ok(MutexLock {
        mutex_key: parse_string_attribute("mutex_key", map)?,
        version: parse_opt_string_attribute("version", map).unwrap_or_else(|| Uuid::new_v4().to_string()),
        lease_duration_ms: parse_opt_number_attribute("lease_duration_ms", map).unwrap_or_else(|| DEFAULT_LEASE_DURATION),
        tenant_id: parse_string_attribute("tenant_id", map)?,
        semaphore_key: parse_opt_string_attribute("semaphore_key", map),
        data: parse_opt_string_attribute("data", map),
        delete_on_release: parse_bool_attribute("delete_on_release", map),
        locked: parse_bool_attribute("locked", map),
        expires_at: parse_date_attribute("expires_at", map),
        created_at: parse_date_attribute("created_at", map),
        created_by: parse_opt_string_attribute("created_by", map),
        updated_at: parse_date_attribute("updated_at", map),
        updated_by: parse_opt_string_attribute("updated_by", map),
    })
}

pub(crate) fn map_to_semaphore(map: &HashMap<String, AttributeValue>) -> LockResult<Semaphore> {
    Ok(Semaphore {
        semaphore_key: parse_string_attribute("semaphore_key", map)?,
        version: parse_opt_string_attribute("version", map).unwrap_or_else(|| Uuid::new_v4().to_string()),
        data: parse_opt_string_attribute("data", map),
        tenant_id: parse_string_attribute("tenant_id", map)?,
        max_size: parse_number_attribute("max_size", map)? as i32,
        lease_duration_ms: parse_number_attribute("lease_duration_ms", map)?,
        created_at: parse_date_attribute("created_at", map),
        created_by: parse_opt_string_attribute("created_by", map),
        updated_at: parse_date_attribute("updated_at", map),
        updated_by: parse_opt_string_attribute("updated_by", map),
    })
}


pub(crate) async fn describe_table(client: &Client, table_name: &str) -> LockResult<TableStatus> {
    match client
        .describe_table()
        .table_name(table_name)
        .send()
        .await
    {
        Ok(out) => {
            if let Some(table) = out.table() {
                if let Some(status) = table.table_status() {
                    return Ok(status.clone());
                }
            }
            Ok(TableStatus::Unknown(format!("unknown status for {}", table_name)))
        }
        Err(err) => {
            if let SdkError::ServiceError { err, .. } = err {
                let retryable = if let error::DescribeTableErrorKind::InternalServerError(_) = err.kind { true } else { false };
                Err(LockError::database(
                    format!("failed to describe {} table due to {}",
                            table_name, err).as_str(), None, retryable))
            } else {
                Err(LockError::database(format!(
                    "failed to describe {} table due to {}",
                    table_name, err).as_str(), None, false))
            }
        }
    }
}

pub(crate) async fn create_table(client: &Client, table_name: &str, key: &str, gsi_key: &str) -> LockResult<()> {
    let gsi = GlobalSecondaryIndex::builder()
        .index_name(format!("{}_ndx", gsi_key))
        .key_schema(KeySchemaElement::builder()
            .attribute_name("tenant_id")
            .key_type(KeyType::Hash).build())
        .key_schema(KeySchemaElement::builder()
            .attribute_name(gsi_key)
            .key_type(KeyType::Range).build())
        .projection(Projection::builder().projection_type(ProjectionType::All).build())
        .provisioned_throughput(
            ProvisionedThroughput::builder().read_capacity_units(10).write_capacity_units(10).build())
        .build();

    match client
        .create_table()
        .table_name(table_name)
        .global_secondary_indexes(gsi)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("tenant_id")
                .key_type(KeyType::Hash)
                .build(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(key)
                .key_type(KeyType::Range)
                .build(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(key)
                .attribute_type(ScalarAttributeType::S)
                .build(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("tenant_id")
                .attribute_type(ScalarAttributeType::S)
                .build(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(gsi_key)
                .attribute_type(ScalarAttributeType::S)
                .build(),
        )
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(10)
                .write_capacity_units(10)
                .build(),
        )
        .send()
        .await
    {
        Ok(k) => {
            log::info!("created table for {}: {:?}", table_name, k);
            wait_until_table_status_is_not(client, table_name, TableStatus::Creating).await;
            Ok(())
        }
        Err(err) => {
            if let SdkError::ServiceError { err, .. } = err {
                if let error::CreateTableErrorKind::ResourceInUseException(_) = err.kind {
                    Ok(())
                } else {
                    let retryable = if let error::CreateTableErrorKind::InternalServerError(_) = err.kind { true } else { false };
                    Err(LockError::database(
                        format!("failed to create {} table due to {}",
                                table_name, err).as_str(), None, retryable))
                }
            } else {
                Err(LockError::database(
                    format!("failed to create {} table due to {}",
                            table_name, err).as_str(), None, false))
            }
        }
    }
}

pub(crate) async fn delete_table(client: &Client, table_name: &str) -> LockResult<()> {
    match client.delete_table().table_name(table_name).send().await {
        Ok(k) => {
            log::info!("deleted table for {}: {:?}", table_name, k);
            wait_until_table_status_is_not(client, table_name, TableStatus::Deleting).await;
            Ok(())
        }
        Err(err) => {
            if let SdkError::ServiceError { err, .. } = err {
                if let error::DeleteTableErrorKind::ResourceNotFoundException(_) = err.kind {
                    Ok(())
                } else {
                    let retryable = if let error::DeleteTableErrorKind::InternalServerError(_) = err.kind { true } else { false };
                    Err(LockError::database(
                        format!("failed to delete {} table due to {}",
                                table_name, err).as_str(), None, retryable))
                }
            } else {
                Err(LockError::database(
                    format!("failed to delete {} table due to {}",
                            table_name, err).as_str(), None, false))
            }
        }
    }
}

async fn wait_until_table_status_is_not(client: &Client, table_name: &str, other_status: TableStatus) {
    let mut last_status = TableStatus::Unknown("unknown".to_string());
    let mut last_error = None;
    for _i in 0..30 {
        match describe_table(client, table_name).await {
            Ok(status) => {
                last_status = status.clone();
                if status != other_status {
                    return;
                }
            }
            Err(err) => {
                last_error = Some(err);
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    log::warn!("failed to get table status to {:?}, last_status {:?}, error: {:?}", 
               other_status, last_status, last_error);
}

pub(crate) fn parse_item(value: Value) -> LockResult<HashMap<String, AttributeValue>> {
    match value_to_item(value) {
        AttributeValue::M(map) => Ok(map),
        other => Err(LockError::database(
            format!("failed to parse{:?}", other).as_str(), None, false))
    }
}

fn value_to_item(value: Value) -> AttributeValue {
    match value {
        Value::Null => AttributeValue::Null(true),
        Value::Bool(b) => AttributeValue::Bool(b),
        Value::Number(n) => AttributeValue::N(n.to_string()),
        Value::String(s) => AttributeValue::S(s),
        Value::Array(a) => AttributeValue::L(a.into_iter().map(value_to_item).collect()),
        Value::Object(o) => {
            AttributeValue::M(o.into_iter().map(|(k, v)| (k, value_to_item(v))).collect())
        }
    }
}

pub(crate) fn date_time_to_string(date: &NaiveDateTime) -> String {
    date.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
}

fn parse_string_attribute(name: &str, map: &HashMap<String, AttributeValue>) -> LockResult<String> {
    if let Some(attr) = map.get(name) {
        if let AttributeValue::S(str) = attr {
            return Ok(str.clone());
        }
    }
    Err(LockError::validation(
        format!("no string value for {} in {:?}", name, map).as_str(), None))
}

fn parse_bool_attribute(name: &str, map: &HashMap<String, AttributeValue>) -> Option<bool> {
    if let Some(attr) = map.get(name) {
        if let AttributeValue::Bool(b) = attr {
            return Some(*b);
        }
    }
    None
}

fn parse_opt_string_attribute(name: &str, map: &HashMap<String, AttributeValue>) -> Option<String> {
    if let Some(attr) = map.get(name) {
        if let AttributeValue::S(str) = attr {
            return Some(str.clone());
        }
    }
    None
}

fn parse_date_attribute(name: &str, map: &HashMap<String, AttributeValue>) -> Option<NaiveDateTime> {
    if let Some(attr) = map.get(name) {
        if let AttributeValue::S(str) = attr {
            // e.g. 2022-09-24T04:40:35.726029
            if let Ok(date) = NaiveDateTime::parse_from_str(str, "%Y-%m-%dT%H:%M:%S%.f") {
                return Some(date);
            }
        }
    }
    None
}

fn parse_number_attribute(name: &str, map: &HashMap<String, AttributeValue>) -> LockResult<i64> {
    if let Some(attr) = map.get(name) {
        if let AttributeValue::N(str) = attr {
            if let Ok(n) = str.parse::<i64>() {
                return Ok(n);
            }
        }
    }
    Err(LockError::validation(
        format!("no number value for {} in {:?}", name, map).as_str(), None))
}

fn parse_opt_number_attribute(name: &str, map: &HashMap<String, AttributeValue>) -> Option<i64> {
    if let Some(attr) = map.get(name) {
        if let AttributeValue::N(str) = attr {
            if let Ok(n) = str.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

fn retryable_sdk_error<T>(err: &SdkError<T>) -> (bool, Option<String>) {
    match err {
        SdkError::ConstructionFailure(_) => { (false, Some("ConstructionFailure".to_string())) }
        SdkError::TimeoutError(_) => { (true, Some("TimeoutError".to_string())) }
        SdkError::DispatchFailure(_) => { (true, Some("DispatchFailure".to_string())) }
        SdkError::ResponseError { .. } => { (true, Some("ResponseError".to_string())) }
        SdkError::ServiceError {
            err: _err, raw
        } => { (raw.http().status().is_server_error() || has_exceeded_limit(raw.http().body().bytes()), Some(raw.http().status().to_string())) }
    }
}

fn has_exceeded_limit(opts: Option<&[u8]>) -> bool {
    if let Some(b) = opts {
        for i in 0..(b.len() - 6) {
            if b[i] == b'c' && b[i + 1] == b'e' && b[i + 2] == b'e' && b[i + 3] == b'd' && b[i + 4] == b'e' && b[i + 5] == b'd' {
                return true; //"ceeded"
            }
        }
    }
    false
}

impl std::convert::From<SdkError<UpdateItemError>> for LockError {
    fn from(err: SdkError<UpdateItemError>) -> Self {
        let (retryable, reason) = retryable_sdk_error(&err);
        if retryable {
            LockError::unavailable(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, true)
        } else {
            LockError::database(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, false)
        }
    }
}

impl std::convert::From<SdkError<PutItemError>> for LockError {
    fn from(err: SdkError<PutItemError>) -> Self {
        let (retryable, reason) = retryable_sdk_error(&err);
        if retryable {
            LockError::unavailable(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, true)
        } else {
            LockError::database(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, false)
        }
    }
}

impl std::convert::From<SdkError<DeleteItemError>> for LockError {
    fn from(err: SdkError<DeleteItemError>) -> Self {
        let (retryable, reason) = retryable_sdk_error(&err);
        if retryable {
            LockError::unavailable(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, true)
        } else {
            LockError::database(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, false)
        }
    }
}

impl std::convert::From<SdkError<QueryError>> for LockError {
    fn from(err: SdkError<QueryError>) -> Self {
        let (retryable, reason) = retryable_sdk_error(&err);
        if retryable {
            LockError::unavailable(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, true)
        } else if Some("404".to_string()) == reason {
            LockError::not_found(
                format!("not found error {:?} {:?}", err, reason).as_str())
        } else {
            LockError::database(
                format!("database error {:?} {:?}", err, reason).as_str(), reason, false)
        }
    }
}


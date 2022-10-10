use std::str::Utf8Error;
use chrono::Utc;
use redis::{Commands, Connection, RedisError};
use crate::domain::error::LockError;
use crate::domain::models::LockResult;

// See following:
// https://redis.io/docs/reference/patterns/distributed-locks/
// https://redis.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-2-distributed-locking/6-2-5-locks-with-timeouts/
// https://redis.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-2-distributed-locking/6-2-3-building-a-lock-in-redis/
// https://redis.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-3-counting-semaphores/6-3-2-fair-semaphores/
//
impl From<RedisError> for LockError {
    fn from(err: RedisError) -> Self {
        if err.is_connection_refusal() {
            LockError::access_denied(
                format!("redis connection error {:?}", err).as_str(), None)
        } else {
            LockError::database(
                format!("redis database error {:?}", err).as_str(), None, false)
        }
    }
}

impl From<Utf8Error> for LockError {
    fn from(err: Utf8Error) -> Self {
        LockError::database(
            format!("redis database error {:?}", err).as_str(), None, false)
    }
}

pub(crate) fn delete_semaphore(
    conn: &mut Connection,
    lock_name: &str,
) -> LockResult<usize> {
    let cz_set = format!("{}:owner", lock_name);
    let ctr = format!("{}:counter", lock_name);
    let response: Option<(i64, i64, i64)> = redis::pipe()
        .atomic()
        .del(lock_name)
        .del(cz_set)
        .del(ctr)
        .query(conn)?;
    match response {
        None => {
            Err(LockError::database(
                format!("failed to delete semaphore semaphore {}",
                        lock_name).as_str(), None, false))
        }
        Some(response) => {
            log::info!("deleted semaphore {} - response {:?}", lock_name, response);
            Ok(1)
        }
    }
}

pub(crate) fn acquire_semaphore(
    conn: &mut Connection,
    lock_name: &str,
    limit: i64,
    identifier: &str,
    lease_duration_ms: i64,
) -> LockResult<usize> {
    let cz_set = format!("{}:owner", lock_name);
    let ctr = format!("{}:counter", lock_name);
    let now = Utc::now().naive_utc().timestamp_millis();
    let response: Option<(i64, i64, i64)> = redis::pipe()
        .atomic()
        // zremrangebyscore Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
        // O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation.
        .zrembyscore(lock_name, "-inf", now - lease_duration_ms)
        // Computes the intersection of numkeys sorted sets given by the specified keys, and stores the result in destination.
        // It is mandatory to provide the number of input keys (numkeys) before passing the input keys and the other (optional) arguments.
        // By default, the resulting score of an element is the sum of its scores in the sorted sets where it exists.
        // Because intersection requires an element to be a member of every given sorted set, this results in the score of every element
        // in the resulting sorted set to be equal to the number of input sorted sets.
        // O(NK)+O(Mlog(M)) worst case with N being the smallest input sorted set, K being the number of input sorted sets and M being
        // the number of elements in the resulting sorted set.
        //    pipeline.zinterstore(czset, {czset: 1, semname: 0})
        //.zinterstore(cz_set.as_str(), &[lock_name, cz_set.as_str()])
        .cmd("ZINTERSTORE").arg(cz_set.as_str()).arg(2).arg(&[lock_name, cz_set.as_str()]).arg("WEIGHTS").arg(&[0, 1])
        .incr(ctr.as_str(), 1) // Time out old entries.
        .query(conn)?;
    let counter = match response {
        None => {
            Err(LockError::database(
                format!("failed to run pipeline for zremrangebyscore, semaphore {}, lease {:?}",
                        lock_name, lease_duration_ms).as_str(), None, false))
        }
        Some(response) => {
            Ok(response.2)
        }
    }?;

    // Get the counter
    let response: Option<(i64, i64, i64)> = redis::pipe()
        .atomic()
        .zadd(lock_name, identifier, now)
        .zadd(cz_set.as_str(), identifier, counter)
        .zrank(cz_set.as_str(), identifier) // Try to acquire the semaphore.
        .query(conn)?;
    let rank = match response {
        None => {
            Err(LockError::database(
                format!("failed to run pipeline for zadd, semaphore {}, lease {:?}",
                        lock_name, lease_duration_ms).as_str(), None, false))
        }
        Some(response) => {
            Ok(response.2)
        }
    }?;

    // Check the rank to determine if we got the semaphore.
    if rank < limit {
        Ok(1)
    } else {
        let response: Option<(i64, i64)> = redis::pipe()
            .atomic()
            .zrem(lock_name, identifier)
            .zrem(cz_set.as_str(), identifier)
            .query(conn)?;
        let _ = match response {
            None => {
                Err(LockError::database(
                    format!("failed to run pipeline for zrem , semaphore {}, lease {:?}",
                            lock_name, lease_duration_ms).as_str(), None, false))
            }
            Some(_response) => {
                Ok(1)
            }
        }?;
        Err(LockError::database(
            format!("failed to acquire semaphore for {}, lease {:?}",
                    lock_name, lease_duration_ms).as_str(), None, false))
    }
}

pub(crate) fn semaphore_busy_identifiers(
    conn: &mut Connection,
    lock_name: &str,
) -> LockResult<Vec<String>> {
    let cz_set = format!("{}:owner", lock_name);
    let ids: Vec<String> = conn.zrangebyscore(cz_set, 0, "inf")?;
    Ok(ids)
}

pub(crate) fn refresh_semaphore(
    conn: &mut Connection,
    lock_name: &str,
    identifier: &str,
    lease_duration_ms: i64,
) -> LockResult<usize> {
    let now = Utc::now().naive_utc().timestamp_millis();

    let size: i64 = conn.zadd(lock_name, identifier, now)?;
    if size > 0 {
        release_semaphore(conn, lock_name, identifier)?;
        return Err(LockError::database(
            format!("didn't find identifier {} in {}, failed to refresh lease {}",
                    identifier, lock_name, lease_duration_ms).as_str(), None, false));
    }
    Ok(1)
}

pub(crate) fn release_semaphore(
    conn: &mut Connection,
    lock_name: &str,
    identifier: &str,
) -> LockResult<usize> {
    let cz_set = format!("{}:owner", lock_name);

    let response: Option<(i64, i64)> = redis::pipe()
        .atomic()
        .zrem(lock_name, identifier)
        .zrem(cz_set.as_str(), identifier)
        .query(conn)?;
    match response {
        None => {
            Err(LockError::database(
                format!("failed to run pipeline for zrem, semaphore {}",
                        lock_name).as_str(), None, false))
        }
        Some(response) => {
            if response.0 == 0 {
                log::warn!("release_semaphore failed to release identifier {}, lock {}, cz {}, response {:?}",
                    identifier, lock_name, cz_set, response);
            }
            Ok(response.0 as usize)
        }
    }
}

pub(crate) fn acquire_atomic_lock(
    conn: &mut Connection,
    lock_name: &str,
    old_version: &str,
    new_version: &str,
) -> LockResult<usize> {
    let _deleted = delete_atomic_lock(conn, lock_name, old_version)?;
    let size: usize = conn.set_nx(lock_name, new_version)?;
    //let size: usize = conn.ttl(lock_name.as_str())?;
    Ok(size)
}

pub(crate) fn update_atomic_lock_expiration(
    conn: &mut Connection,
    lock_name: &str,
    lease_secs: usize) -> LockResult<usize> {
    let size: usize = conn.expire(lock_name, lease_secs)?;
    if size == 0 && lease_secs > 0 {
        return Err(LockError::database(
            format!("failed to update expiration for lock {}, lease {:?}",
                    lock_name, lease_secs).as_str(), None, false));
    }

    let size: usize = conn.ttl(lock_name)?;
    if size == 0 {
        return Err(LockError::database(
            format!("failed to fetch expiration for lock {}, lease {:?}",
                    lock_name, lease_secs).as_str(), None, false));
    }
    Ok(size)
}

pub(crate) fn refresh_atomic_lock_expiration(
    conn: &mut Connection,
    lock_name: &str,
    old_version: &str,
    new_version: &str,
    lease_secs: usize) -> LockResult<usize> {
    loop {
        // start watching the key so that our exec fails if the key is different
        redis::cmd("WATCH").arg(lock_name).query(conn)?;

        // load the old value
        let version_val: Vec<u8> = conn.get(lock_name)?;
        if version_val.is_empty() {
            return Err(LockError::database(
                format!("failed to find lock to renew {}, version {:?}, lease {:?}",
                        lock_name, old_version, lease_secs).as_str(), None, false));
        }

        if std::str::from_utf8(&version_val) == Ok(old_version) {
            let response: Option<(usize, usize)> = redis::pipe()
                .atomic()
                .expire(lock_name, lease_secs)
                .ttl(lock_name)
                .query(conn)?;
            match response {
                None => {
                    continue;
                }
                Some(response) => {
                    redis::cmd("UNWATCH").query::<()>(conn)?;
                    if response.1 > 0 {
                        conn.set(lock_name, new_version)?;
                    }
                    log::debug!("refresh_atomic_lock_expirationOK  {} {:?}", lock_name, response);
                    return Ok(response.1); // return ttl
                }
            }
        } else {
            redis::cmd("UNWATCH").query::<()>(conn)?;
            return Err(LockError::database(
                format!("failed to find matching version to renew lock {}, version {:?}, lease {:?}",
                        lock_name, old_version, lease_secs).as_str(), None, false));
        }
    }
}

pub(crate) fn delete_atomic_lock(
    conn: &mut Connection,
    lock_name: &str,
    old_version: &str) -> LockResult<usize> {
    loop {
        // start watching the key so that our exec fails if the key is different
        redis::cmd("WATCH").arg(lock_name).query(conn)?;

        // load the old value
        let version_val: Vec<u8> = conn.get(lock_name)?;
        if version_val.is_empty() {
            redis::cmd("UNWATCH").query::<()>(conn)?;
            return Ok(0);
        }
        let stored_version = std::str::from_utf8(&version_val)?;
        let ttl: usize = conn.ttl(lock_name)?;
        if ttl == 0 || stored_version == old_version {
            let response: Option<(usize, )> = redis::pipe()
                .atomic()
                .del(lock_name)
                .query(conn)?;
            match response {
                None => {
                    continue;
                }
                Some(response) => {
                    redis::cmd("UNWATCH").query::<()>(conn)?;
                    log::debug!("delete_atomic_lock {} response {:?}", lock_name, response);
                    return Ok(response.0);
                }
            }
        } else {
            redis::cmd("UNWATCH").query::<()>(conn)?;
            return Err(LockError::database(
                format!("failed to find matching version to delete expired lock {} version {:?} against stored {:?}",
                        lock_name, old_version, stored_version).as_str(), None, false));
        }
    }
}
use async_trait::async_trait;

use crate::domain::models::{AcquireLockOptions, LockResult, MutexLock, ReleaseLockOptions, Semaphore, SendHeartbeatOptions};

pub mod locks_manager;

#[async_trait]
pub trait LocksManager {
    // Attempts to acquire a lock until it either acquires the lock, or a specified additional_time_to_wait_for_lock_ms is
    // reached. This method will poll database based on the refresh_period. If it does not see the lock in database, it
    // will immediately return the lock to the caller. If it does see the lock, it will note the lease expiration on the lock. If
    // the lock is deemed stale, (that is, there is no heartbeat on it for at least the length of its lease duration) then this
    // will acquire and return it. Otherwise, if it waits for as long as additional_time_to_wait_for_lock_ms without acquiring the
    // lock, then it will return LockError::NotGranted.
    //
    async fn acquire_lock(&self, options: &AcquireLockOptions) -> LockResult<MutexLock>;

    // Releases the given lock if the current user still has it, returning true if the lock was
    // successfully released, and false if someone else already stole the lock. Deletes the
    // lock item if it is released and delete_lock_item_on_close is set.
    async fn release_lock(&self, options: &ReleaseLockOptions) -> LockResult<bool>;

    // Sends a heartbeat to indicate that the given lock is still being worked on.
    // This method will also set the lease duration of the lock to the given value.
    // This will also either update or delete the data from the lock, as specified in the options
    async fn send_heartbeat(&self, options: &SendHeartbeatOptions) -> LockResult<MutexLock>;

    // Deletes mutex lock if not locked
    async fn delete_mutex(&self,
                          other_key: &str,
                          other_version: &str,
                          other_semaphore_key: Option<String>) -> LockResult<usize>;

    // Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
    // given lock. If the client currently has the lock, it will return the lock, and operations such as release_lock will work.
    // However, if the client does not have the lock, then operations like releaseLock will not work (after calling get_lock, the
    // caller should check mutex.expired() to figure out if it currently has the lock.)
    async fn get_mutex(&self, mutex_key: &str) -> LockResult<MutexLock>;

    // Creates or updates semaphore with given max size
    async fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize>;

    // Returns semaphore for the key
    async fn get_semaphore(&self, semaphore_key: &str) -> LockResult<Semaphore>;

    // find locks by semaphore
    async fn get_mutexes_for_semaphore(&self,
                                       other_semaphore_key: &str,
    ) -> LockResult<Vec<MutexLock>>;

    // Deletes semaphore if all associated locks are not locked
    async fn delete_semaphore(&self,
                              other_key: &str,
                              other_version: &str,
    ) -> LockResult<usize>;
}

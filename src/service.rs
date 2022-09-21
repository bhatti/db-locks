use crate::domain::models::{AcquireLockOptions, LockItem, LockResult, ReleaseLockOptions, SendHeartbeatOptions, DeleteLockOptions, Semaphore};

pub mod lock_service;
pub mod lock_metrics;

pub trait LockService {
    // Attempts to acquire a lock until it either acquires the lock, or a specified additional_time_to_wait_for_lock_ms is
    // reached. This method will poll database based on the refresh_period. If it does not see the lock in database, it
    // will immediately return the lock to the caller. If it does see the lock, it will note the lease expiration on the lock. If
    // the lock is deemed stale, (that is, there is no heartbeat on it for at least the length of its lease duration) then this
    // will acquire and return it. Otherwise, if it waits for as long as additional_time_to_wait_for_lock_ms without acquiring the
    // lock, then it will return LockError::NotGranted.
    //
    fn acquire_lock(&self, options: &AcquireLockOptions) -> LockResult<LockItem>;

    // Releases the given lock if the current user still has it, returning true if the lock was
    // successfully released, and false if someone else already stole the lock. Deletes the
    // lock item if it is released and delete_lock_item_on_close is set.
    fn release_lock(&self, options: &ReleaseLockOptions) -> LockResult<bool>;

    // Sends a heartbeat to indicate that the given lock is still being worked on.
    // This method will also set the lease duration of the lock to the given value.
    // This will also either update or delete the data from the lock, as specified in the options
    fn send_heartbeat(&self, options: &SendHeartbeatOptions) -> LockResult<()>;

    // Creates or updates semaphore with given max size
    fn create_semaphore(&self, semaphore: &Semaphore) -> LockResult<usize>;

    // Deletes semaphore if all associated locks are not locked
    fn delete_semaphore(&self, options: &DeleteLockOptions) -> LockResult<usize>;

    // Deletes lock if not locked
    fn delete_lock(&self, options: &DeleteLockOptions) -> LockResult<usize>;

    // Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
    // given lock. If the client currently has the lock, it will return the lock, and operations such as releaseLock will work.
    // However, if the client does not have the lock, then operations like releaseLock will not work (after calling get_lock, the
    // caller should check lockItem.isExpired() to figure out if it currently has the lock.)
    fn get_lock(&self, key: &String) -> LockResult<LockItem>;

    // Returns semaphore for the key
    fn get_semaphore(&self, key: &String) -> LockResult<Semaphore>;

    // find locks by semaphore
    fn find_by_semaphore(&self, other_semaphore_key: &String) -> LockResult<Vec<LockItem>>;
}



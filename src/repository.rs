use crate::domain::models::{LockItem, LockResult, Semaphore};

pub mod data_source;
pub mod orm_lock_repository;
pub mod orm_semaphore_repository;
pub mod ddb_repository;
pub mod redis_repository;

macro_rules! pool_decl {
    () => {
        Pool<ConnectionManager<SqliteConnection>>
        //Pool<ConnectionManager<PgConnection>>
    };
}

macro_rules! build_pool {
    ($database_url:expr) => {
        data_source::build_sqlite_pool($database_url).expect("failed to create db")
        //data_source::build_pg_pool($database_url).expect("failed to create db")
    };
}

pub(crate) use pool_decl;
pub(crate) use build_pool;

pub(crate) trait LockRepository {
    // create lock item
    fn create(&self, lock: &LockItem) -> LockResult<usize>;

    // updates existing lock item for acquire
    fn acquire_update(&self,
                      old_version: Option<String>,
                      lock: &LockItem) -> LockResult<usize>;

    // updates existing lock item for heartbeat
    fn heartbeat_update(&self,
                        old_version: Option<String>,
                        lock: &LockItem) -> LockResult<usize>;

    // updates existing lock item for release
    fn release_update(&self,
                      other_key: String,
                      other_version: Option<String>,
                      other_owner: Option<String>,
                      other_data: Option<String>) -> LockResult<usize>;

    // get lock by key
    fn get(&self, other_key: String, other_owner: Option<String>) -> LockResult<LockItem>;

    // delete lock
    fn delete(&self,
              other_key: String,
              other_version: Option<String>,
              other_owner: Option<String>) -> LockResult<usize>;

    // delete expired lock
    fn delete_expired_lock(&self, other_key: String,
                           other_owner: Option<String>) -> LockResult<usize>;

    // find by owner
    fn find_by_owner(&self, other_owner: String) -> LockResult<Vec<LockItem>>;

    // find by semaphore
    fn find_by_semaphore(&self, key: String, other_owner: Option<String>) -> LockResult<Vec<LockItem>>;
}

pub(crate) trait SemaphoreRepository {
    // create semaphore
    fn create(&self, semaphore: &Semaphore) -> LockResult<usize>;

    // updates existing semaphore item
    fn update(&self, semaphore: &Semaphore) -> LockResult<usize>;

    // find by key and owner
    fn get(&self, other_key: String, other_owner: Option<String>) -> LockResult<Semaphore>;

    // delete semaphore
    fn delete(&self, other_key: String, other_version: Option<String>, other_owner: Option<String>) -> LockResult<usize>;

    // find by owner
    fn find_by_owner(&self, other_owner: Option<String>) -> LockResult<Vec<Semaphore>>;
}

# db_locks
Locks, Mutex and Semaphores using Database

## Locks and Mutex

### Create Lock Service
The LockService abstracts operations for acquiring, releasing, renewing leases, etc.
```rust
let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| String::from("test_db.sqlite"));

let pool = build_pool!(database_url);

let config = LocksConfig { heatbeat_period_ms: Some(100), owner: Some("test_owner".to_string()) };

let lock_repository = OrmLockRepository::new(pool.clone());

let semaphore_repository = OrmSemaphoreRepository::new(pool.clone(), OrmLockRepository::new(pool.clone()));

let lock_service = LockServiceImpl::new(config, lock_repository, semaphore_repository, default_registry().clone()).expect("failed to initialize lock service");
```

### Acquiring Lock
You can build options for acquiring with key name and lease period in milliseconds and then acquire it:

```rust
let acquire_opts = AcquireLockOptionsBuilder::new("mylock".to_string(), 1000)
            .with_additional_time_to_wait_for_lock_ms(3000)
            .with_refresh_period_ms(10).build();
let lock = lock_service.acquire_lock(&acquire_opts).expect("should acquire serial lock");
```
The acquire_lock will automatically create lock if it doesn't exist and wait for the period of lease-time if lock is not available.

### Renew Lease of Lock
The lock is only available for the lease duration but you can renew it periodically if needed:

```rust
let renew_opts = SendHeartbeatOptions::new(lock.key.clone(), lock.version.clone(), None, 2000, None);
lock_service.send_heartbeat(&renew_opts).expect("should renew lock");
```
Above method will renew lease for another 2 seconds.

### Releasing Lock
You can build options for releasing from the lock returned by above API and then release it:

```rust
let release_opts = ReleaseLockOptions::new(lock.key.clone(), lock.version.clone(), None, None);
lock_service.release_lock(&release_opts).expect("should release lock");
```
The release request may optionally delete the lock otherwise it will reuse same lock object in the database.

### Creating Semaphores
The semaphores allows you to define a set of locks for a resource with a maximum size. You will first
need to create semaphores before using it unlike locks that can be created on demand, e.g.:

```rust
let sem_opts = AcquireLockOptionsBuilder::new("my_pool".to_string(), 1000).build();
lock_service.create_semaphore(&lock_options.to_semaphore(10)).expect("should create semaphore");
```
Above operation will create a semaphore of size 10 with default lease of 1 second. You can also use this API to change size of semaphore locks.

### Acquiring Semaphores
The operation for acquiring semaphore is similar to acquiring regular lock except you specify that it uses semaphore, e.g.:

```rust
let acquire_opts = AcquireLockOptionsBuilder::new("my_pool".to_string(), 1000)
            .with_uses_semaphore(true).build();
let lock = lock_service.acquire_lock(&acquire_opts).expect("should acquire serial semaphore");
```

### Renewing Lease for Semaphores
The operation for renewing lease for semaphore is similar to acquiring regular lock, e.g.:

```rust
let renew_opts = SendHeartbeatOptions::new(lock.key.clone(), lock.version.clone(), None, 2000, None);
lock_service.send_heartbeat(&renew_opts).expect("should renew lock");
```

### Releases Semaphores
The operation for releasing semaphore is similar to acquiring regular lock, e.g.:

```rust
let release_opts = ReleaseLockOptions::new(lock.key.clone(), lock.version.clone(), None, None);
lock_service.release_lock(&release_opts).expect("should release lock");
```

## Setup

Install diesel cli:
```shell
brew install libpq
brew link --force libpq
PQ_LIB_DIR="$(brew --prefix libpq)/lib"
cargo install diesel_cli --no-default-features --features sqlite
cargo install diesel_cli --no-default-features --features postgres
```

Set URL to database
```shell
export DATABASE_URL=postgres://<postgres_username>:<postgres_password>@<postgres_host>:<postgres_port>/school
```

Perform migrations. At this step Rust schema is also automatically generated and printed to the file defined in `diesel.toml`
```shell
diesel setup
diesel migration run
```


## Execute

Put database in vanilla state
```shell
diesel migration redo
```

Run lock server
```shell
cargo run
```

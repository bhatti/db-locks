# db_locks

## Mutex and Semaphores
This is a Rust library for providing database based Mutex and Semaphores with support of relationa databases and AWS Dynamo DB.

### Create Lock Manager
The LockManager abstracts operations for acquiring, releasing, renewing leases, etc.
```rust
let config = LocksConfig::new("test_tenant");
let mutex_repo = build_test_mutex_repo(&config, RepositoryProvider::RDB).await;
let semaphore_repo = build_test_semaphore_repo(&config, RepositoryProvider::RDB).await;
let lock_manager = LocksManagerImpl::new(&config, mutex_repo, semaphore_repo, &default_registry()).expect("failed to initialize lock manager");
```

The db_locks uses https://diesel.rs/ library for ORM and you can customize it to any suported database. You can switch to 
AWS Dynamo DB using:
```rust
let mutex_repo = build_test_mutex_repo(&config, RepositoryProvider::DDB).await;
let semaphore_repo = build_test_semaphore_repo(&config, RepositoryProvider::DDB).await;
```

### Acquiring Mutex Lock
You can build options for acquiring with key name and lease period in milliseconds and then acquire it:

```rust
let acquire_opts = AcquireLockOptionsBuilder::new("mylock")
            .with_lease_duration_secs(10).build();
let lock = lock_manager.acquire_lock(&acquire_opts).expect("should acquire lock");
```
The acquire_lock will automatically create lock if it doesn't exist and wait for the period of lease-time if lock is not available.

### Renew Lease of Lock
The lock is only available for the lease duration but you can renew it periodically if needed:

```rust
let renew_opts = lock.to_heartbeat_options();
let updated_lock = lock_manager.send_heartbeat(&renew_opts).expect("should renew lock");
```
Above method will renew lease for another 10 seconds.

### Releasing Lock
You can build options for releasing from the lock returned by above API and then release it:

```rust
let release_opts = updated_lock.to_release_options();
lock_manager.release_lock(&release_opts).expect("should release lock");
```
The release request may optionally delete the lock otherwise it will reuse same lock object in the database.

### Creating Semaphores
The semaphores allow you to define a set of locks for a resource with a maximum size. You will first
need to create semaphores before using it unlike locks that can be created on demand, e.g.:

```rust
let semaphore = SemaphoreBuilder::new("my_pool", 20)
                .with_lease_duration_secs(10)
                .build();
lock_manager.create_semaphore(&semaphore).expect("should create semaphore");
```
Above operation will create a semaphore of size 20 with default lease of 10 second. You can also use this API to change size of semaphore locks.

### Acquiring Semaphores
The operation for acquiring semaphore is similar to acquiring regular lock except you specify that it uses semaphore, e.g.:

```rust
let acquire_opts = AcquireLockOptionsBuilder::new("my_pool".to_string())
            .with_requires_semaphore(true)
            .with_lease_duration_secs(10).build();
let lock = lock_manager.acquire_lock(&acquire_opts).expect("should acquire semaphore lock");
```

### Renewing Lease for Semaphores
The operation for renewing lease for semaphore is similar to acquiring regular lock, e.g.:

```rust
let renew_opts = lock.to_heartbeat_options();
let updated_lock = lock_manager.send_heartbeat(&renew_opts).expect("should renew lock");
```

### Releases Semaphores
The operation for releasing semaphore is similar to acquiring regular lock, e.g.:

```rust
let release_opts = updated_lock.to_release_options();
lock_manager.release_lock(&release_opts).expect("should release lock");
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
cargo run --
```
This will show following command line options
```shell
Database based Mutexes and Semaphores

Usage: db-locks [PROVIDER] <COMMAND>

Commands:
  acquire

  heartbeat

  release

  get-mutex

  delete-mutex

  create-semaphore

  get-semaphore

  delete-semaphore

  get-semaphore-mutexes

  help
          Print this message or the help of the given subcommand(s)

Arguments:
  [PROVIDER]
          Database provider [default: rdb] [possible values: rdb, ddb, redis]

Options:
  -t, --tenant <TENANT>
          tentant-id for the database [default: default]
  -c, --config <FILE>
          Sets a custom config file
  -h, --help
          Print help information
  -V, --version
          Print version information
```

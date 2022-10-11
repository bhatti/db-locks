# db_locks
Distributed Locks using Databases

## Mutex and Semaphores
This is a Rust library for providing distributed Mutex and Semaphore based locks using databases.
The project goals include:
- Allow creating lease based locks that can either a single shared resource with a Mutex or a fixed set of related resources with a Semaphore.
- Allow renewing leases based on periodic intervals.
- Allow releasing mutex and semaphore locks explicitly after the user action.
- CRUD APIs to manage Mutex and Semaphore entities in the database.
- Multi-tenancy support if the database is shared by multiple users.
- Fair locks to support first-come and first serve based access grant when acquiring same lock concurrently.
- Scalable solution for supporting tens of thousands concurrent locks and semaphores.
- Support multiple data stores such as relational databases such as MySQL, PostgreSQL, Sqlite and as well as NoSQL/Cache data stores such as AWS Dynamo DB and Redis.

### Create Lock Manager
The LockManager abstracts operations for acquiring, releasing, renewing leases, etc for distributed locks. You can instantiate it as follows:
```rust
let config = LocksConfig::new("test_tenant");
let mutex_repo = factory::build_mutex_repository(RepositoryProvider::Rdb, &config).await.expect("failed to build mutex repository");
let semaphore_repo = factory::build_semaphore_repository(RepositoryProvider::Rdb, &config).await.expect("failed to build semaphore repository");
let store = Box::new(DefaultLockStore::new(&config, mutex_repo, semaphore_repo));

let locks_manager = LockManagerImpl::new(&config, store, &default_registry()).expect("failed to initialize lock manager");
```

The db_locks uses https://diesel.rs/ library for ORM when using relational databases and you can 
customize it to any suported database. However, you can switch to AWS Dynamo DB or Redis using:
```rust
let mutex_repo = factory::build_mutex_repository(RepositoryProvider::Ddb, &config).await.expect("failed to build mutex repository");
let semaphore_repo = factory::build_semaphore_repository(RepositoryProvider::Ddb, &config).await.expect("failed to build semaphore repository");
let store = Box::new(DefaultLockStore::new(&config, mutex_repo, semaphore_repo));
```
or
```rust
let mutex_repo = factory::build_mutex_repository(RepositoryProvider::Redis, &config).await.expect("failed to build mutex repository");
let semaphore_repo = factory::build_semaphore_repository(RepositoryProvider::Redis, &config).await.expect("failed to build semaphore repository");
let store = Box::new(DefaultLockStore::new(&config, mutex_repo, semaphore_repo));
```

### Acquiring Mutex Lock
You will need to build options for acquiring with key name and lease period in milliseconds and then acquire it:

```rust
let acquire_opts = AcquireLockOptionsBuilder::new("mylock").with_lease_duration_secs(10).build();
let lock = lock_manager.acquire_lock(&acquire_opts).expect("should acquire lock");
```
The acquire_lock operation will automatically create mutex lock if it doesn't exist otherwise it will wait for the period of lease-time if the lock is not available.

### Renewing lease of Lock
The lock is only available for the duration specified in lease_duration period, but you can renew it periodically if needed:

```rust
let renew_opts = lock.to_heartbeat_options();
let updated_lock = lock_manager.send_heartbeat(&renew_opts).expect("should renew lock");
```
Above method will renew lease for another 10 seconds.

### Releasing Lock
You can build options for releasing from the lock returned by above API as follows and then release it:

```rust
let release_opts = updated_lock.to_release_options();
lock_manager.release_lock(&release_opts).expect("should release lock");
```
The release request may optionally delete the lock otherwise it will reuse same lock object in the database.

### Acquiring Semaphore based Locks
The semaphores allow you to define a set of locks for a resource with a maximum size. The operation 
for acquiring semaphore is similar to acquiring regular lock except you specify semaphore size, e.g.:

```rust
let acquire_opts = AcquireLockOptionsBuilder::new("my_pool")
                    .with_lease_duration_secs(1)
                    .with_semaphore_max_size(10)
                    .build();
let lock = lock_manager.acquire_lock(&acquire_opts).expect("should acquire semaphore lock");
```
The acquire_lock operation will automatically create semaphore if it doesn't exist and it will 
then check for available locks and wait if all the locks are busy.

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

### Acquiring Fair Semaphores
The fair semaphores is only available for Redis, and it requires enabling it via fair_semaphore configuration
option, otherwise it's similar to above operations, e.g.:
```rust
let mut config = LocksConfig::new("test_tenant");
config.fair_semaphore = Some(fair_semaphore);
let fair_semaphore_repo = factory::build_fair_semaphore_repository(RepositoryProvider::Redis, &config).await.expect("failed to create fair semaphore");
let store = Box::new(FairLockStore::new(&config, fair_semaphore_repo));
let locks_manager = LockManagerImpl::new(&config, store, &default_registry()).expect("failed to initialize lock manager");
```

```rust
let acquire_opts = AcquireLockOptionsBuilder::new("my_pool")
                    .with_lease_duration_secs(1)
                    .with_semaphore_max_size(10)
                    .build();
let lock = lock_manager.acquire_lock(&acquire_opts).expect("should acquire semaphore lock");
```

## Setup

### Relational Database
Install diesel cli:
```shell
brew install libpq
brew link --force libpq
PQ_LIB_DIR="$(brew --prefix libpq)/lib"
cargo install diesel_cli --no-default-features --features sqlite
cargo install diesel_cli --no-default-features --features postgres
```

Set URL to relational database
```shell
export DATABASE_URL=postgres://<postgres_username>:<postgres_password>@<postgres_host>:<postgres_port>/school
```

Perform migrations. At this step Rust schema is also automatically generated and printed to the file defined in `diesel.toml`
```shell
diesel setup
diesel migration run
```

Put database in vanilla state
```shell
diesel migration redo
```

### Redis
Set URL to Redis database
```shell
export REDIS_URL=redis://192.168.1.102
```

### AWS Dynamo DB
You can specify environment variables for AWS access based on https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html, e.g.
```shell
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

## Execute

Run lock server
```shell
cargo run --
```
This will show following command line options
```shell
Database based Mutexes and Semaphores

Usage: db-locks [OPTIONS] [PROVIDER] <COMMAND>

Commands:
  acquire

  heartbeat

  release

  get-mutex

  delete-mutex

  create-mutex

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
          tentant-id for the database [default: local-host-name]
  -f, --fair-semaphore <FAIR_SEMAPHORE>
          fair semaphore lock [default: false] [possible values: true, false]
  -c, --config <FILE>
          Sets a custom config file
  -h, --help
          Print help information
  -V, --version
          Print version information
```

### Examples

#### Acquiring Fair lock
Following snippet acquires lock for key `one` with default 15 second lease:
```bash
REDIS_URL=redis://192.168.1.102 cargo run -- --fair-semaphore true redis acquire --key one
```
or, you can specify lease period as:
```bash
REDIS_URL=redis://192.168.1.102 cargo run -- --fair-semaphore true redis acquire --lease 20 --key one
```

#### Refresh Fair lock
```bash
REDIS_URL=redis://192.168.1.102 cargo run -- --fair-semaphore true redis heartbeat --key one --version 4bc177f3-383b-4f88-9c63-034f7d2fdcd6
```

#### Release Fair lock
```bash
REDIS_URL=redis://192.168.1.102 cargo run -- --fair-semaphore true redis release --key one --version 4bc177f3-383b-4f88-9c63-034f7d2fdcd6
```
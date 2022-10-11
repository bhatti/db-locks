extern crate diesel;
extern crate diesel_migrations;
extern crate dotenv;

use std::fs;

use clap::Parser;
use env_logger::Env;
use prometheus::default_registry;
use domain::options::{AcquireLockOptionsBuilder, ReleaseLockOptionsBuilder, SendHeartbeatOptionsBuilder};

use domain::args::{Args, CommandActions};

use crate::domain::models::{LocksConfig, RepositoryProvider, SemaphoreBuilder};
use crate::manager::lock_manager::LockManagerImpl;
use crate::manager::LockManager;
use crate::repository::factory;
use crate::store::default_lock_store::DefaultLockStore;
use crate::store::fair_lock_store::FairLockStore;
use crate::store::LockStore;

mod domain;
mod repository;
mod manager;
mod store;
mod utils;

#[tokio::main(worker_threads = 2)]
async fn main() {
    let args: Args = Args::parse();

    let _ = env_logger::Builder::from_env(Env::default().default_filter_or(
        "info,aws_config=warn,aws_smithy_http=warn,aws_config=warn,aws_sigv4=warn,aws_smithy_http_tower=warn")).init();

    let mut config = LocksConfig::new(&args.tenant.as_str());
    if let Some(custom) = &args.config {
        let body = fs::read_to_string(custom).expect("failed to parse config file");
        config = serde_yaml::from_str(body.as_str()).expect("failed to deserialize config file");
    }

    if let Some(fair_semaphore) = args.fair_semaphore {
        config.fair_semaphore = Some(fair_semaphore);
        if fair_semaphore && args.provider != RepositoryProvider::Redis {
            panic!("fair semaphore is only supported for Redis store");
        }
    }

    log::info!("using config: {:?}", config);

    let store: Box<dyn LockStore + Send + Sync> = if config.is_fair_semaphore() {
        let fair_semaphore_repo = factory::build_fair_semaphore_repository(
            args.provider, &config)
            .await.expect("failed to create fair semaphore");
        Box::new(FairLockStore::new(
            &config,
            fair_semaphore_repo,
        ))
    } else {
        let mutex_repo = factory::build_mutex_repository(
            args.provider, &config)
            .await.expect("failed to build mutex repository");
        let semaphore_repo = factory::build_semaphore_repository(
            args.provider, &config)
            .await.expect("failed to build semaphore repository");

        Box::new(DefaultLockStore::new(
            &config,
            mutex_repo,
            semaphore_repo))
    };

    let locks_manager = LockManagerImpl::new(
        &config,
        store,
        &default_registry())
        .expect("failed to initialize lock manager");

    match &args.action {
        CommandActions::Acquire { key, lease, semaphore_max_size, data } => {
            let opts = AcquireLockOptionsBuilder::new(key.as_str())
                .with_lease_duration_secs(*lease)
                .with_semaphore_max_size(semaphore_max_size.unwrap_or(1))
                .with_opt_data(data)
                .build();

            let mutex = locks_manager.acquire_lock(&opts).await
                .expect("failed to acquire lock");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&mutex).unwrap());
            } else {
                log::info!("acquired lock {}", mutex);
            }
        }
        CommandActions::Heartbeat { key, version, lease, semaphore_key, data } => {
            let opts = SendHeartbeatOptionsBuilder::new(key, version)
                .with_lease_duration_secs(*lease)
                .with_opt_semaphore_key(semaphore_key)
                .with_opt_data(data)
                .build();

            let mutex = locks_manager.send_heartbeat(&opts).await
                .expect("failed to renew lock");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&mutex).unwrap());
            } else {
                log::info!("renewed lock {}", mutex);
            }
        }
        CommandActions::Release { key, version, semaphore_key, data } => {
            let opts = ReleaseLockOptionsBuilder::new(key, version)
                .with_opt_semaphore_key(semaphore_key)
                .with_opt_data(data)
                .build();

            let done = locks_manager.release_lock(&opts).await
                .expect("failed to release lock");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&done).unwrap());
            } else {
                log::info!("released lock {}", done);
            }
        }
        CommandActions::GetMutex { key } => {
            let mutex = locks_manager.get_mutex(key.as_str()).await
                .expect("failed to find lock");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&mutex).unwrap());
            } else {
                log::info!("found lock {}", mutex);
            }
        }
        CommandActions::CreateMutex{ key, lease, data } => {
            let mutex = AcquireLockOptionsBuilder::new(key.as_str())
                .with_lease_duration_secs(*lease)
                .with_opt_data(data)
                .build().to_unlocked_mutex(config.get_tenant_id().as_str());
            let size = locks_manager.create_mutex(&mutex).await
                .expect("failed to create mutex");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&mutex).unwrap());
            } else {
                log::info!("created mutex {}", size);
            }
        }
        CommandActions::DeleteMutex { key, version, semaphore_key } => {
            let size = locks_manager.delete_mutex(
                key.as_str(), version.as_str(), semaphore_key.as_ref().map(|s| s.clone())).await
                .expect("failed to delete lock");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&size).unwrap());
            } else {
                log::info!("deleted lock {}", size);
            }
        }
        CommandActions::CreateSemaphore { key, max_size, lease} => {
            let semaphore = SemaphoreBuilder::new(key.as_str(), *max_size as i32)
                .with_lease_duration_secs(*lease)
                .build();
            let size = locks_manager.create_semaphore(&semaphore).await
                .expect("failed to create semaphore");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&semaphore).unwrap());
            } else {
                log::info!("created semaphore {}", size);
            }
        }
        CommandActions::GetSemaphore { key } => {
            let semaphore = locks_manager.get_semaphore(key.as_str()).await
                .expect("failed to find semaphore");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&semaphore).unwrap());
            } else {
                log::info!("found semaphore {}", semaphore);
            }
        }
        CommandActions::DeleteSemaphore { key, version } => {
            let size = locks_manager.delete_semaphore(key.as_str(), version.as_str()).await
                .expect("failed to delete semaphore");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&size).unwrap());
            } else {
                log::info!("deleted semaphore {}", size);
            }
        }
        CommandActions::GetSemaphoreMutexes { key } => {
            let mutexes = locks_manager.get_semaphore_mutexes(key.as_str()).await
                .expect("failed to find semaphore mutexes");
            if args.json_output.unwrap_or(false) {
                println!("{}", serde_json::to_string(&mutexes).unwrap());
            } else {
                log::info!("found {} semaphore mutexes:", mutexes.len());
                for mutex in mutexes {
                    log::info!("\t{}", mutex);
                }
            }
        }
    }
}

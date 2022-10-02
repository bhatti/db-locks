extern crate diesel;
extern crate diesel_migrations;
extern crate dotenv;

use std::fs;

use clap::Parser;
use env_logger::Env;
use prometheus::default_registry;

use domain::models::Args;

use crate::domain::models::{AcquireLockOptionsBuilder, CommandActions, LocksConfig, ReleaseLockOptionsBuilder, SemaphoreBuilder, SendHeartbeatOptionsBuilder};
use crate::manager::locks_manager::LocksManagerImpl;
use crate::manager::LocksManager;
use crate::repository::factory;

mod domain;
mod repository;
mod manager;
mod utils;

#[tokio::main(worker_threads = 2)]
async fn main() {
    let args = Args::parse();

    let _ = env_logger::Builder::from_env(Env::default().default_filter_or(
        "info,aws_config=warn,aws_smithy_http=warn,aws_config=warn,aws_sigv4=warn,aws_smithy_http_tower=warn")).init();

    let mut config = LocksConfig::new(&args.tenant.as_str());
    if let Some(custom) = &args.config {
        let body = fs::read_to_string(custom).expect("failed to parse config file");
        config = serde_json::from_str(body.as_str()).expect("failed to deserialize config file");
    }
    println!("{}", serde_json::to_string(&config).unwrap());

    let mutex_repo = factory::build_mutex_repository(
        args.provider, &config).await
        .expect("failed to build mutex repository");

    let semaphore_repo = factory::build_semaphore_repository(
        args.provider, &config).await
        .expect("failed to build semaphore repository");

    let locks_manager = LocksManagerImpl::new(
        &config, mutex_repo, semaphore_repo, &default_registry())
        .expect("failed to initialize lock manager");

    match &args.action {
        CommandActions::Acquire { key, lease, semaphore, data } => {
            let opts = AcquireLockOptionsBuilder::new(key.as_str())
                .with_lease_duration_secs(*lease)
                .with_requires_semaphore(semaphore.unwrap_or_else(|| false))
                .with_opt_data(data)
                .build();

            let mutex = locks_manager.acquire_lock(&opts).await
                .expect("failed to acquire lock");
            log::info!("acquired lock {}", mutex);
        }
        CommandActions::Heartbeat { key, version, lease, semaphore_key, data } => {
            let opts = SendHeartbeatOptionsBuilder::new(key, version)
                .with_lease_duration_secs(*lease)
                .with_opt_semaphore_key(semaphore_key)
                .with_opt_data(data)
                .build();

            let mutex = locks_manager.send_heartbeat(&opts).await
                .expect("failed to renew lock");
            log::info!("renewed lock {}", mutex);
        }
        CommandActions::Release { key, version, semaphore_key, data } => {
            let opts = ReleaseLockOptionsBuilder::new(key, version)
                .with_opt_semaphore_key(semaphore_key)
                .with_opt_data(data)
                .build();

            let done = locks_manager.release_lock(&opts).await
                .expect("failed to release lock");
            log::info!("released lock {}", done);

        }
        CommandActions::GetMutex { key } => {
           let mutex = locks_manager.get_mutex(key.as_str()).await
               .expect("failed to find lock");
            log::info!("found lock {}", mutex);
        }
        CommandActions::DeleteMutex { key, version, semaphore_key } => {
            let size = locks_manager.delete_mutex(
                key.as_str(), version.as_str(), semaphore_key.as_ref().map(|s|s.clone())).await
                .expect("failed to delete lock");
            log::info!("deleted lock {}", size);
        }
        CommandActions::CreateSemaphore { key, max_size, lease, data } => {
            let semaphore = SemaphoreBuilder::new(key.as_str(), *max_size as i32)
                .with_lease_duration_secs(*lease)
                .with_opt_data(data)
                .build();
            let size = locks_manager.create_semaphore(&semaphore).await
                .expect("failed to create semaphore");
            log::info!("created semaphore {}", size);

        }
        CommandActions::GetSemaphore { key } => {
            let semaphore = locks_manager.get_semaphore(key.as_str()).await
                .expect("failed to find semaphore");
            log::info!("found semaphore {}", semaphore);
        }
        CommandActions::DeleteSemaphore { key, version } => {
            let size = locks_manager.delete_semaphore(key.as_str(), version.as_str()).await
                .expect("failed to delete semaphore");
            log::info!("deleted semaphore {}", size);
        }
        CommandActions::GetSemaphoreMutexes { key } => {
            let mutexes = locks_manager.get_mutexes_for_semaphore(key.as_str()).await
                .expect("failed to find semaphore mutexes");
            log::info!("found {} semaphore mutexes:", mutexes.len());
            for mutex in mutexes {
                log::info!("\t{}", mutex);
            }
        }
    }
}
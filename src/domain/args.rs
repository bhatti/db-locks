use clap::{Parser, Subcommand};
use std::path::PathBuf;
use crate::domain::models::RepositoryProvider;

#[derive(Subcommand, Debug, Clone)]
pub enum CommandActions {
    Acquire {
        /// key of mutex or semaphore to acquire
        #[arg(short, long)]
        key: String,

        // How long the lease for the lock is (in seconds)
        #[arg(short, long, default_value_t = 15)]
        lease: i64,

        // If this requires a semaphore, then specify semaphore size
        #[arg(short, long)]
        semaphore_max_size: Option<i32>,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    Heartbeat {
        /// key of mutex to renew lease
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,

        // How long the lease for the lock is (in seconds)
        #[arg(short, long, default_value_t = 15)]
        lease: i64,

        // If this requires a semaphore previously created
        #[arg(short, long)]
        semaphore_key: Option<String>,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    Release {
        /// key of mutex to release
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,

        // If this requires a semaphore previously created
        #[arg(short, long)]
        semaphore_key: Option<String>,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    GetMutex {
        /// key of mutex to retrieve
        #[arg(short, long)]
        key: String,
    },
    DeleteMutex {
        /// key of mutex to delete
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,

        // If this requires a semaphore previously created
        #[arg(short, long)]
        semaphore_key: Option<String>,
    },
    CreateMutex{
        /// key of semaphore to create
        #[arg(short, long)]
        key: String,

        // How long the lease for the lock is (in seconds)
        #[arg(short, long, default_value_t = 15)]
        lease: i64,

        // The data to be stored alongside the lock (can be empty)
        #[arg(short, long)]
        data: Option<String>,
    },
    CreateSemaphore {
        /// key of semaphore to create
        #[arg(short, long)]
        key: String,

        // The number of locks in semaphores
        #[arg(short, long)]
        max_size: i64,

        // How long the lease for the lock is (in seconds)
        #[arg(short, long, default_value_t = 15)]
        lease: i64,
    },
    GetSemaphore {
        /// key of semaphore to retrieve
        #[arg(short, long)]
        key: String,
    },
    DeleteSemaphore {
        /// key of semaphore to delete
        #[arg(short, long)]
        key: String,

        // record version of the lock in database. This is what tells the lock client when the lock is stale.
        #[arg(short, long)]
        version: String,
    },
    GetSemaphoreMutexes {
        /// key of semaphore for retrieving mutexes
        #[arg(short, long)]
        key: String,
    },
}

/// Mutexes and Semaphores based Distributed Locks with databases.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(next_line_help = true)]
pub struct Args {
    /// Action to perform
    #[command(subcommand)]
    pub action: CommandActions,

    /// Database provider
    #[arg(value_enum, default_value = "rdb")]
    pub provider: RepositoryProvider,

    /// tentant-id for the database
    #[arg(short, long, default_value = "local-host-name")]
    pub tenant: String,

    /// fair semaphore lock
    #[arg(short, long, default_value = "false")]
    pub fair_semaphore: Option<bool>,

    /// json output of result from action
    #[arg(short, long, default_value = "false")]
    pub json_output: Option<bool>,

    /// Sets a custom config file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,
}

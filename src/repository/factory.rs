use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::Region;
use crate::domain::error::LockError;

use crate::domain::models::{LockResult, LocksConfig, RepositoryProvider};
use crate::repository::{FairSemaphoreRepository, MeasurableMutexRepository, MeasurableSemaphoreRepository, MutexRepository, RetryableFairSemaphoreRepository, RetryableMutexRepository, RetryableSemaphoreRepository, SemaphoreRepository};
use crate::repository::build_pool;
use crate::repository::ddb_mutex_repository::DdbMutexRepository;
use crate::repository::ddb_semaphore_repository::DdbSemaphoreRepository;
use crate::repository::orm_mutex_repository::OrmMutexRepository;
use crate::repository::orm_semaphore_repository::OrmSemaphoreRepository;
use crate::repository::redis_mutex_repository::RedisMutexRepository;
use crate::repository::redis_semaphore_repository::RedisSemaphoreRepository;
use crate::repository::rdb_common;
use crate::repository::redis_fair_semaphore_repository::RedisFairSemaphoreRepository;

pub async fn build_mutex_repository(
    provider: RepositoryProvider,
    config: &LocksConfig,
) -> LockResult<Box<dyn MutexRepository + Send + Sync>> {
    match provider {
        // Must update pool macros for this
        RepositoryProvider::Rdb => {
            let pool = build_pool!(config);
            Ok(Box::new(RetryableMutexRepository::new(config,
                                                      Box::new(OrmMutexRepository::new(pool)))))
        }
        RepositoryProvider::Ddb => {
            let region_provider = RegionProviderChain::default_provider()
                .or_else(Region::new(config.get_aws_region()));

            let shared_config = aws_config::from_env().region(region_provider).load().await;
            Ok(Box::new(RetryableMutexRepository::new(config, DdbMutexRepository::new(config, &shared_config)?)))
        }
        RepositoryProvider::Redis => {
            Ok(Box::new(RetryableMutexRepository::new(config, RedisMutexRepository::new(config)?)))
        }
    }
}

pub async fn build_semaphore_repository(
    provider: RepositoryProvider,
    config: &LocksConfig,
) -> LockResult<Box<dyn SemaphoreRepository + Send + Sync>> {
    let mutex_repository = build_mutex_repository(provider, config).await?;
    match provider {
        // Must update pool macros for this
        RepositoryProvider::Rdb => {
            let pool = build_pool!(config);
            Ok(Box::new(RetryableSemaphoreRepository::new(config,
                                                          Box::new(OrmSemaphoreRepository::new(pool, mutex_repository)))))
        }
        RepositoryProvider::Ddb => {
            let region_provider = RegionProviderChain::default_provider()
                .or_else(Region::new(config.get_aws_region()));

            let shared_config = aws_config::from_env().region(region_provider).load().await;
            Ok(Box::new(RetryableSemaphoreRepository::new(config,
                                                          DdbSemaphoreRepository::new(config, &shared_config, mutex_repository)?)))
        }
        RepositoryProvider::Redis => {
            Ok(Box::new(RetryableSemaphoreRepository::new(config,
                                                          Box::new(RedisSemaphoreRepository::new(
                                                              config,
                                                              redis::Client::open(config.get_redis_url())?,
                                                              mutex_repository)))))
        }
    }
}

pub async fn build_fair_semaphore_repository(
    provider: RepositoryProvider,
    config: &LocksConfig,
) -> LockResult<Box<dyn FairSemaphoreRepository + Send + Sync>> {
    match provider {
        RepositoryProvider::Redis => {
            let mutex_repository = build_mutex_repository(provider, config).await?;
            let semaphore_repository = RedisSemaphoreRepository::new(
                config,
                redis::Client::open(config.get_redis_url())?,
                mutex_repository);
            Ok(Box::new(RetryableFairSemaphoreRepository::new(config,
                                                          Box::new(RedisFairSemaphoreRepository::new(
                                                              redis::Client::open(config.get_redis_url())?,
                                                              semaphore_repository)))))
        }
        _ => Err(LockError::runtime("fair semaphore is only supported for Redis store", None))
    }
}

pub(crate) async fn build_measurable_mutex_repository(
    provider: RepositoryProvider,
    config: &LocksConfig,
) -> LockResult<MeasurableMutexRepository> {
    Ok(MeasurableMutexRepository::new(provider,
                                      build_mutex_repository(provider, config).await?))
}

pub(crate) async fn build_measurable_semaphore_repository(
    provider: RepositoryProvider,
    config: &LocksConfig,
) -> LockResult<MeasurableSemaphoreRepository> {
    let mutex_repo = MeasurableMutexRepository::new(provider,
                                                    build_mutex_repository(provider, config).await?);
    Ok(MeasurableSemaphoreRepository::new(provider,
                                          build_semaphore_repository(provider, config).await?,
                                          mutex_repo,
    ))
}

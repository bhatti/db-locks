use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::Region;

use crate::domain::models::{LockError, LockResult, LocksConfig, RepositoryProvider};
use crate::repository::{MeasurableMutexRepository, MeasurableSemaphoreRepository, MutexRepository, RetryableMutexRepository, RetryableSemaphoreRepository, SemaphoreRepository};
use crate::repository::build_pool;
use crate::repository::ddb_mutex_repository::DdbMutexRepository;
use crate::repository::ddb_semaphore_repository::DdbSemaphoreRepository;
use crate::repository::orm_mutex_repository::OrmMutexRepository;
use crate::repository::orm_semaphore_repository::OrmSemaphoreRepository;
use crate::repository::rdb_common;

pub async fn build_mutex_repository(
    provider: RepositoryProvider,
    config: &LocksConfig,
) -> LockResult<Box<dyn MutexRepository + Send + Sync>> {
    match provider {
        // Must update pool macros for this
        RepositoryProvider::Rdb => {
            let pool = build_pool!(config);
            Ok(RetryableMutexRepository::new(config, OrmMutexRepository::new(pool.clone())))
        }
        RepositoryProvider::Ddb => {
            let region_provider = RegionProviderChain::default_provider()
                .or_else(Region::new(config.get_aws_region()));

            let shared_config = aws_config::from_env().region(region_provider).load().await;
            Ok(RetryableMutexRepository::new(config, DdbMutexRepository::new(config, &shared_config)?))
        }
        RepositoryProvider::Redis => {
            Err(LockError::runtime("Redis not supported for Mutex Repository yet", None))
        }
    }
}

pub async fn build_semaphore_repository(
    provider: RepositoryProvider,
    config: &LocksConfig,
) -> LockResult<Box<dyn SemaphoreRepository + Send + Sync>> {
    match provider {
        // Must update pool macros for this
        RepositoryProvider::Rdb => {
            let pool = build_pool!(config);
            let mutex_repository = OrmMutexRepository::new(pool.clone());
            Ok(RetryableSemaphoreRepository::new(config, OrmSemaphoreRepository::new(pool.clone(), mutex_repository)))
        }
        RepositoryProvider::Ddb => {
            let region_provider = RegionProviderChain::default_provider()
                .or_else(Region::new(config.get_aws_region()));

            let shared_config = aws_config::from_env().region(region_provider).load().await;
            let mutex_repository = DdbMutexRepository::new(config, &shared_config)?;
            Ok(RetryableSemaphoreRepository::new(config, DdbSemaphoreRepository::new(config, &shared_config, mutex_repository)?))
        }
        RepositoryProvider::Redis => {
            Err(LockError::runtime("Redis not for Semaphore Repository supported yet", None))
        }
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

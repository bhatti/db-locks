use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::domain::models::{MutexLock, Semaphore};

pub struct LockCache {
    cache_enabled: bool,
    semaphore_cache: Arc<RwLock<HashMap<String, Semaphore>>>,
    locks_by_semaphore_key: Arc<RwLock<HashMap<String, HashMap<String, MutexLock>>>>,
}

impl LockCache {
    pub fn new(cache_enabled: bool) -> Self {
        LockCache {
            cache_enabled,
            semaphore_cache: Arc::new(RwLock::new(HashMap::new())),
            locks_by_semaphore_key: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn put_cached_mutex(&self, mutex: &MutexLock) {
        if !self.cache_enabled {
            return;
        }
        if let Ok(mut outer) = self.locks_by_semaphore_key.write() {
            if let Some(inner) = outer.get_mut(mutex.get_semaphore_key().as_str()) {
                inner.insert(mutex.mutex_key.clone(), mutex.clone());
            } else {
                // if let Err(err) =
                // self.locks_by_semaphore_key.lock().unwrap().try_insert(
                //     lock.get_semaphore_key().clone(),
                //     Mutex::new(HashMap::from([(lock.mutex_key.clone(), lock.clone())]))) {
                //     err.value.lock().unwrap().insert(lock.mutex_key.clone(), lock.clone());
                // }
                outer.insert(
                    mutex.get_semaphore_key(),
                    HashMap::from([(mutex.mutex_key.clone(), mutex.clone())]));
            }
        }
    }

    pub(crate) fn get_cached_mutex(&self, semaphore_key: &str, mutex_key: &str) -> Option<MutexLock> {
        if !self.cache_enabled {
            return None;
        }
        if let Ok(outer) = self.locks_by_semaphore_key.read() {
            if let Some(inner) = outer.get(semaphore_key) {
                return inner.get(mutex_key).cloned();
            }
        }
        None
    }

    pub(crate) fn remove_cached_mutex(&self, semaphore_key: &str, mutex_key: &str) -> Option<MutexLock> {
        if !self.cache_enabled {
            return None;
        }
        if let Ok(mut outer) = self.locks_by_semaphore_key.write() {
            if let Some(inner) = outer.get_mut(semaphore_key) {
                return inner.remove(mutex_key);
            }
        }
        None
    }

    fn count_cached_mutexes_semaphore_keys(&self) -> usize {
        if let Ok(outer) = self.locks_by_semaphore_key.write() {
            return outer.len();
        }
        0
    }

    fn count_all_cached_mutexes_by_semaphore_key(&self, semaphore_key: &str) -> usize {
        if let Ok(outer) = self.locks_by_semaphore_key.write() {
            if let Some(inner) = outer.get(semaphore_key) {
                return inner.len();
            }
        }
        0
    }

    pub(crate) fn remove_all_cached_mutexes_by_semaphore_key(&self, semaphore_key: &str) {
        if !self.cache_enabled {
            return;
        }
        if let Ok(mut outer) = self.locks_by_semaphore_key.write() {
            let _ = outer.remove(semaphore_key);
        }
        if let Ok(mut cache) = self.semaphore_cache.write() {
            let _ = cache.remove(semaphore_key);
        }
    }

    pub(crate) fn get_all_cached_mutexes(&self, semaphore_key: &str) -> Option<Vec<MutexLock>> {
        if !self.cache_enabled {
            return None;
        }
        if let Ok(outer) = self.locks_by_semaphore_key.read() {
            if let Some(inner) = outer.get(semaphore_key) {
                let mut result = vec![];
                for (_, v) in inner.iter() {
                    result.push(v.clone());
                }
                return Some(result);
            }
        }
        None
    }
    pub(crate) fn get_cached_semaphore(&self, semaphore_key: &str) -> Option<Semaphore> {
        if let Ok(cache) = self.semaphore_cache.read() {
            if let Some(existing) = cache.get(semaphore_key) {
                return Some(existing.clone());
            }
        }
        None
    }

    pub(crate) fn put_cached_semaphore(&self, semaphore: Semaphore) {
        if let Ok(mut cache) = self.semaphore_cache.write() {
            cache.insert(semaphore.semaphore_key.clone(), semaphore);
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use crate::domain::options::AcquireLockOptionsBuilder;
    use crate::store::lock_cache::LockCache;

    #[tokio::test]
    async fn test_should_cache_mutexes_and_semaphore() {
        let tenant_id = format!("TENANT_{}", Uuid::new_v4());
        // GIVEN lock cache
        let cache = LockCache::new(true);
            // WHEN creating a semaphore
            for i in 0..10 {
                let sem_key = format!("SEM_{}_{}", i, Uuid::new_v4());

                let semaphore_max_size = 100;
                let lock_options = AcquireLockOptionsBuilder::new(sem_key.as_str())
                    .with_acquire_only_if_already_exists(false)
                    .with_lease_duration_secs(1)
                    .with_additional_time_to_wait_for_lock_ms(3)
                    .with_semaphore_max_size(semaphore_max_size)
                    .with_override_time_to_wait_for_lock_ms(10)
                    .with_data("abc")
                    .build();

                // THEN it should succeed
                let semaphore = lock_options.to_semaphore(tenant_id.as_str());

                let mutexes = semaphore.generate_mutexes(0);
                for (j, mutex) in mutexes.iter().enumerate() {
                    cache.put_cached_mutex(mutex);
                    assert_eq!(i + 1, cache.count_cached_mutexes_semaphore_keys());
                    assert_eq!(j + 1, cache.count_all_cached_mutexes_by_semaphore_key(mutex.get_semaphore_key().as_str()));
                    let mut loaded1 = cache.get_cached_mutex(
                        mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str()).unwrap();
                    assert_eq!(*mutex, loaded1);

                    // WHEN updating mutex
                    // THEN it should update it
                    loaded1.data = Some(j.to_string());
                    cache.put_cached_mutex(&loaded1);

                    let loaded2 = cache.get_cached_mutex(
                        mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str()).unwrap();
                    assert_eq!(loaded1, loaded2);
                    assert_eq!(loaded1.data, loaded2.data);

                    // WHEN removing mutex
                    // THEN it should remove it
                    cache.remove_cached_mutex(mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str());

                    assert_eq!(None, cache.get_cached_mutex(
                        mutex.get_semaphore_key().as_str(), mutex.mutex_key.as_str()));

                    loaded1.data = Some("123".to_string());
                    cache.put_cached_mutex(&loaded1);
                }
                let mutexes = cache.get_all_cached_mutexes(semaphore.semaphore_key.as_str()).unwrap();
                assert_eq!(100, mutexes.len());
        }
    }
}
use futures03::Future;
use mini_moka::sync::Cache;
use std::{error::Error, fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, instrument, Level};

pub struct RpcCache<R, V> {
    name: String,
    cache: Arc<RwLock<Cache<R, V>>>,
}

impl<R, V> RpcCache<R, V>
where
    R: Clone + Hash + Eq + Send + Sync + Debug + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(name: &str, capacity: u64, ttl: u64) -> Self {
        let cache = Arc::new(RwLock::new(
            Cache::builder()
                .max_capacity(capacity)
                .time_to_live(std::time::Duration::from_secs(ttl))
                .build(),
        ));
        Self { name: name.to_string(), cache }
    }

    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn get<
        'a,
        E: Error,
        Fut: Future<Output = Result<(V, bool), E>> + Send + 'a,
        F: Fn(R) -> Fut + Send + Sync,
    >(
        &'a self,
        request: R,
        fallback: F,
    ) -> Result<V, E> {
        #[allow(unused_assignments)]
        let mut cache_entry_count: u64 = 0;

        // Check the cache for a cached response
        {
            let read_lock = self.cache.read().await;
            cache_entry_count = read_lock.entry_count();
            if let Some(cached_response) = read_lock.get(&request) {
                debug!(cache = self.name, ?request, "CacheHit");
                return Ok(cached_response);
            }
        }

        debug!(?request, cache_size = cache_entry_count, cache = self.name, "CacheMiss");
        // Acquire a write lock before querying the database (prevents concurrent db queries)
        let write_lock = self.cache.write().await;

        // Double-check if another thread has already fetched and cached the data
        if let Some(cached_response) = write_lock.get(&request) {
            return Ok(cached_response);
        }

        let (response, should_cache) = (fallback)(request.clone()).await?;

        if should_cache {
            debug!(?request, name = self.name, "IncompleteCacheValue");
            write_lock.insert(request, response.clone());
        }

        Ok(response)
    }
}

#[cfg(test)]
mod test {
    use crate::services::{cache::RpcCache, rpc::RpcError};
    use futures03::future::try_join_all;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_sequential_access() {
        let access_counter = Arc::new(Mutex::new(0));
        let cache = RpcCache::<String, i32>::new("test", 100, 3600);

        cache
            .get("k0".to_string(), |_| async { increment_counter(access_counter.clone()).await })
            .await
            .unwrap();

        cache
            .get("k0".to_string(), |_| async { increment_counter(access_counter.clone()).await })
            .await
            .unwrap();

        let v = *access_counter.lock().await;
        assert_eq!(v, 1);
    }

    async fn increment_counter(access_counter: Arc<Mutex<i32>>) -> Result<(i32, bool), RpcError> {
        let mut guard = access_counter.lock().await;
        *guard += 1;
        Ok((1, true))
    }

    #[tokio::test]
    async fn test_parallel_access() {
        let access_counter = Arc::new(Mutex::new(0));
        let cache = RpcCache::<String, i32>::new("test", 100, 3600);
        let tasks: Vec<_> = (0..10)
            .map(|_| {
                cache.get("k0".to_string(), |_| async {
                    increment_counter(access_counter.clone()).await
                })
            })
            .collect();

        try_join_all(tasks)
            .await
            .expect("a task failed");

        let v = *access_counter.lock().await;
        assert_eq!(v, 1);
    }
}

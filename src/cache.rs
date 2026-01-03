use std::path::PathBuf;

use quick_cache::{Lifecycle, Weighter, sync::Cache as QuickCache};
use tracing::warn;

const MAX_CLEANUP_TASKS_IN_QUEUE: usize = 1024;

pub type CacheKey = String;
pub type CleanupReceiver = flume::Receiver<PathBuf>;

#[derive(Clone)]
struct SizeWeighter;

impl Weighter<CacheKey, u64> for SizeWeighter {
    fn weight(&self, _key: &CacheKey, size: &u64) -> u64 {
        *size
    }
}

#[derive(Clone)]
struct EvictionLifecycle {
    dir: PathBuf,
    cleanup_tx: flume::Sender<PathBuf>,
}

impl Lifecycle<CacheKey, u64> for EvictionLifecycle {
    type RequestState = ();

    fn begin_request(&self) -> Self::RequestState {}

    fn on_evict(&self, _state: &mut Self::RequestState, key: CacheKey, _size: u64) {
        let path = self.dir.join(&key);
        if self.cleanup_tx.try_send(path.clone()).is_err() {
            warn!(
                ?path,
                "cleanup channel full, blocking to remove evicted file"
            );
            let _ = std::fs::remove_file(path);
        }
    }
}

pub async fn run_cleanup_loop(cleanup_rx: CleanupReceiver) {
    while let Ok(path) = cleanup_rx.recv_async().await {
        let _ = compio::fs::remove_file(path).await;
    }
}

type InnerCache =
    QuickCache<CacheKey, u64, SizeWeighter, quick_cache::DefaultHashBuilder, EvictionLifecycle>;

pub struct Cache {
    dir: PathBuf,
    inner: InnerCache,
}

impl Cache {
    pub fn new(dir: PathBuf, capacity_bytes: u64) -> (Self, CleanupReceiver) {
        let (cleanup_tx, cleanup_rx) = flume::bounded(MAX_CLEANUP_TASKS_IN_QUEUE);
        let lifecycle = EvictionLifecycle {
            dir: dir.clone(),
            cleanup_tx,
        };
        let inner = QuickCache::with(
            1000,
            capacity_bytes,
            SizeWeighter,
            quick_cache::DefaultHashBuilder::default(),
            lifecycle,
        );
        (Self { dir, inner }, cleanup_rx)
    }

    pub fn generate_path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }

    pub fn get(&self, key: &CacheKey) -> Option<PathBuf> {
        self.inner.get(key)?;
        Some(self.generate_path(key))
    }

    pub fn insert(&self, key: CacheKey, size: u64) {
        self.inner.insert(key, size);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn weight(&self) -> u64 {
        self.inner.weight()
    }

    pub fn capacity(&self) -> u64 {
        self.inner.capacity()
    }

    pub fn hits(&self) -> u64 {
        self.inner.hits()
    }

    pub fn misses(&self) -> u64 {
        self.inner.misses()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eviction_sends_to_cleanup_channel() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, rx) = Cache::new(dir.path().to_path_buf(), 100);

        cache.insert("a".into(), 60);
        cache.insert("b".into(), 60);

        let evicted = rx.try_recv().unwrap();
        assert!(evicted.ends_with("a"));
    }

    #[test]
    fn tracks_hits_and_misses() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, _rx) = Cache::new(dir.path().to_path_buf(), 1000);

        cache.get(&"x".into());
        cache.get(&"y".into());
        assert_eq!(cache.misses(), 2);

        cache.insert("x".into(), 10);
        cache.get(&"x".into());
        cache.get(&"x".into());
        assert_eq!(cache.hits(), 2);
    }
}

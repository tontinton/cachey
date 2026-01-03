use std::path::PathBuf;
use std::sync::Arc;

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

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct ChunkKey {
    pub id: String,
    pub start: u64,
    pub end: u64,
}

impl ChunkKey {
    pub fn new(id: String, start: u64, end: u64) -> Self {
        Self { id, start, end }
    }
}

#[derive(Clone)]
struct ChunkWeighter;

impl Weighter<ChunkKey, Arc<[u8]>> for ChunkWeighter {
    fn weight(&self, _key: &ChunkKey, data: &Arc<[u8]>) -> u64 {
        data.len() as u64
    }
}

type InnerMemoryCache = QuickCache<ChunkKey, Arc<[u8]>, ChunkWeighter>;

pub struct MemoryCache {
    inner: InnerMemoryCache,
}

impl MemoryCache {
    pub fn new(capacity_bytes: u64) -> Self {
        let estimated_items = (capacity_bytes / 65536).max(16) as usize;
        let inner = QuickCache::with_weighter(estimated_items, capacity_bytes, ChunkWeighter);
        Self { inner }
    }

    pub fn insert(&self, id: String, start: u64, end: u64, data: Vec<u8>) {
        let key = ChunkKey::new(id, start, end);
        self.inner.insert(key, data.into());
    }

    pub fn get(&self, id: &str, start: u64, end: u64) -> Option<Arc<[u8]>> {
        let key = ChunkKey::new(id.to_owned(), start, end);
        self.inner.get(&key)
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

type InnerDiskCache =
    QuickCache<CacheKey, u64, SizeWeighter, quick_cache::DefaultHashBuilder, EvictionLifecycle>;

pub struct DiskCache {
    dir: PathBuf,
    inner: InnerDiskCache,
}

impl DiskCache {
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

    mod disk_cache {
        use super::*;

        #[test]
        fn eviction_sends_to_cleanup_channel() {
            let dir = tempfile::tempdir().unwrap();
            let (disk_cache, rx) = DiskCache::new(dir.path().to_path_buf(), 100);

            disk_cache.insert("a".into(), 60);
            disk_cache.insert("b".into(), 60);

            let evicted = rx.try_recv().unwrap();
            assert!(evicted.ends_with("a"));
        }

        #[test]
        fn tracks_hits_and_misses() {
            let dir = tempfile::tempdir().unwrap();
            let (disk_cache, _rx) = DiskCache::new(dir.path().to_path_buf(), 1000);

            disk_cache.get(&"x".into());
            disk_cache.get(&"y".into());
            assert_eq!(disk_cache.misses(), 2);

            disk_cache.insert("x".into(), 10);
            disk_cache.get(&"x".into());
            disk_cache.get(&"x".into());
            assert_eq!(disk_cache.hits(), 2);
        }
    }

    mod memory_cache {
        use super::*;

        #[test]
        fn exact_match_required() {
            let cache = MemoryCache::new(10_000_000);
            let data = vec![0u8; 100];

            cache.insert("file1".into(), 0, 100, data.clone());

            assert!(cache.get("file1", 0, 100).is_some());
            assert!(cache.get("file1", 0, 50).is_none());
            assert!(cache.get("file1", 50, 100).is_none());
            assert!(cache.get("file1", 0, 101).is_none());
            assert!(cache.get("file2", 0, 100).is_none());

            assert_eq!(cache.hits(), 1);
            assert_eq!(cache.misses(), 4);
        }

        #[test]
        fn same_file_different_ranges_coexist() {
            let cache = MemoryCache::new(10_000_000);

            cache.insert("file1".into(), 0, 100, vec![1u8; 100]);
            cache.insert("file1".into(), 100, 200, vec![2u8; 100]);
            cache.insert("file1".into(), 200, 300, vec![3u8; 100]);

            assert_eq!(cache.len(), 3);

            let chunk1 = cache.get("file1", 0, 100).unwrap();
            let chunk2 = cache.get("file1", 100, 200).unwrap();
            let chunk3 = cache.get("file1", 200, 300).unwrap();

            assert!(chunk1.iter().all(|&b| b == 1));
            assert!(chunk2.iter().all(|&b| b == 2));
            assert!(chunk3.iter().all(|&b| b == 3));
        }

        #[test]
        fn eviction_by_weight() {
            let chunk_size = 100_000usize;
            let cache = MemoryCache::new((chunk_size * 2) as u64);

            cache.insert("a".into(), 0, chunk_size as u64, vec![1u8; chunk_size]);
            assert_eq!(cache.len(), 1);

            cache.insert("b".into(), 0, chunk_size as u64, vec![2u8; chunk_size]);
            assert_eq!(cache.len(), 2);

            cache.insert("c".into(), 0, chunk_size as u64, vec![3u8; chunk_size]);

            assert!(cache.weight() <= (chunk_size * 2) as u64);

            let c_data = cache.get("c", 0, chunk_size as u64).unwrap();
            assert!(c_data.iter().all(|&x| x == 3));
        }

        #[test]
        fn weight_reflects_data_size() {
            let cache = MemoryCache::new(10_000_000);

            cache.insert("small".into(), 0, 1000, vec![0u8; 1000]);
            cache.insert("medium".into(), 0, 10000, vec![0u8; 10000]);
            cache.insert("large".into(), 0, 100000, vec![0u8; 100000]);

            assert_eq!(cache.len(), 3);
            assert_eq!(cache.weight(), 111000);
        }
    }
}

use std::fs;

use tokio::sync::Semaphore;

const DEFAULT_MEMORY_LIMIT: u64 = 100 * 1024 * 1024 * 1024;
const MEMORY_HEADROOM_FACTOR: f64 = 0.85;
const MEMORY_PERMIT_UNIT: u64 = 4096;

fn get_cgroup_memory_limit() -> Option<u64> {
    if let Ok(limit_str) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = limit_str.trim();
        if trimmed != "max"
            && let Ok(limit) = trimmed.parse::<u64>()
        {
            return Some(limit);
        }
    }

    if let Ok(limit_str) = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        && let Ok(limit) = limit_str.trim().parse::<u64>()
    {
        return Some(limit);
    }

    None
}

fn bytes_to_permits(bytes: u64) -> u32 {
    bytes.div_ceil(MEMORY_PERMIT_UNIT) as u32
}

fn compute_semaphore_capacity(memory_cache_size: u64, fallback_limit: Option<u64>) -> usize {
    let total_memory =
        get_cgroup_memory_limit().unwrap_or(fallback_limit.unwrap_or(DEFAULT_MEMORY_LIMIT));
    let available = total_memory.saturating_sub(memory_cache_size);
    let capacity_bytes = (available as f64 * MEMORY_HEADROOM_FACTOR) as u64;
    bytes_to_permits(capacity_bytes) as usize
}

pub struct MemorySemaphore {
    semaphore: Semaphore,
    capacity: usize,
}

impl MemorySemaphore {
    #[must_use]
    pub fn new(memory_cache_size: u64, fallback_limit: Option<u64>) -> Self {
        let capacity = compute_semaphore_capacity(memory_cache_size, fallback_limit);
        Self {
            semaphore: Semaphore::new(capacity),
            capacity,
        }
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub async fn acquire(&self, bytes: u64) {
        let permits = bytes_to_permits(bytes);
        self.semaphore
            .acquire_many(permits)
            .await
            .expect("semaphore closed")
            .forget();
    }

    pub fn release(&self, bytes: u64) {
        let permits = bytes_to_permits(bytes);
        self.semaphore.add_permits(permits as usize);
    }

    #[must_use]
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    #[must_use]
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity as u64 * MEMORY_PERMIT_UNIT
    }

    #[must_use]
    pub fn allocated_bytes(&self) -> u64 {
        (self.capacity - self.semaphore.available_permits()) as u64 * MEMORY_PERMIT_UNIT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn create_test_semaphore(capacity_bytes: u64) -> MemorySemaphore {
        let capacity = bytes_to_permits(capacity_bytes) as usize;
        MemorySemaphore {
            semaphore: Semaphore::new(capacity),
            capacity,
        }
    }

    #[test]
    fn bytes_to_permits_rounds_up() {
        assert_eq!(bytes_to_permits(0), 0);
        assert_eq!(bytes_to_permits(1), 1);
        assert_eq!(bytes_to_permits(4096), 1);
        assert_eq!(bytes_to_permits(4097), 2);
    }

    #[tokio::test]
    async fn acquire_release_restores_permits() {
        let sem = create_test_semaphore(1024 * 1024);
        let initial = sem.available_permits();

        sem.acquire(64 * 1024).await;
        sem.acquire(32 * 1024).await;
        let expected_used = bytes_to_permits(64 * 1024 + 32 * 1024) as usize;
        assert_eq!(sem.available_permits(), initial - expected_used);

        sem.release(64 * 1024);
        sem.release(32 * 1024);
        assert_eq!(sem.available_permits(), initial);
    }

    #[tokio::test]
    async fn acquire_blocks_when_insufficient_permits() {
        let sem = Arc::new(create_test_semaphore(8192));
        sem.acquire(8192).await;

        let sem_clone = Arc::clone(&sem);
        let handle = tokio::spawn(async move {
            sem_clone.acquire(4096).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        sem.release(4096);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.is_finished());
    }

    #[tokio::test]
    async fn concurrent_acquire_release_maintains_invariants() {
        let sem = Arc::new(create_test_semaphore(256 * 1024));
        let initial = sem.available_permits();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let sem_clone = Arc::clone(&sem);
                tokio::spawn(async move {
                    for _ in 0..100 {
                        sem_clone.acquire(4096).await;
                        tokio::task::yield_now().await;
                        sem_clone.release(4096);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(sem.available_permits(), initial);
    }
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use compio::buf::IoBuf;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::runtime::spawn;
use prometheus::{
    GaugeVec, Histogram, IntCounterVec, IntGauge, IntGaugeVec, TextEncoder, register_gauge_vec,
    register_histogram, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
};
use tracing::{debug, error};

use crate::cache::{DiskCache, MemoryCache};
use crate::memory_semaphore::MemorySemaphore;
use crate::{BufResultExt, create_listener};

pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);

const METRICS_SERVER_NUM_LISTENERS: i32 = 128;

pub const LABEL_DISK: &str = "disk";
pub const LABEL_MEMORY: &str = "memory";
pub const LABEL_SUCCESS: &str = "success";
pub const LABEL_ERROR: &str = "error";

pub struct Metrics {
    // Server metrics
    pub requests_total: IntCounterVec,
    pub bytes_read_total: IntCounterVec,
    pub bytes_written_total: IntCounterVec,
    pub active_connections: IntGaugeVec,
    pub request_duration: Histogram,

    // Cache metrics (labeled by "type": "disk" or "memory")
    pub cache_items: IntGaugeVec,
    pub cache_weight_bytes: IntGaugeVec,
    pub cache_capacity_bytes: IntGaugeVec,
    pub cache_hits_total: IntCounterVec,
    pub cache_misses_total: IntCounterVec,
    pub cache_hit_rate: GaugeVec,
    pub cache_evictions_total: IntGaugeVec,
    pub cache_evicted_bytes_total: IntGaugeVec,

    // Disk cleanup metrics
    pub disk_cleanup_pending: IntGauge,

    // Memory semaphore metrics
    pub memory_semaphore_capacity_bytes: IntGauge,
    pub memory_semaphore_allocated_bytes: IntGauge,

    // Internal tracking (to calculate metrics)
    last_disk_hits: AtomicU64,
    last_disk_misses: AtomicU64,
    last_memory_hits: AtomicU64,
    last_memory_misses: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            requests_total: register_int_counter_vec!(
                "cachey_requests_total",
                "Total number of requests",
                &["cmd", "status"]
            )
            .expect("create requests_total"),

            bytes_read_total: register_int_counter_vec!(
                "cachey_bytes_read_total",
                "Total bytes read from cache",
                &["cmd"]
            )
            .expect("create bytes_read_total"),

            bytes_written_total: register_int_counter_vec!(
                "cachey_bytes_written_total",
                "Total bytes written to cache",
                &["cmd"]
            )
            .expect("create bytes_written_total"),

            active_connections: register_int_gauge_vec!(
                "cachey_active_connections",
                "Number of active connections",
                &["shard"]
            )
            .expect("create active_connections"),

            request_duration: register_histogram!(
                "cachey_request_duration_seconds",
                "Request duration in seconds"
            )
            .expect("create request_duration"),

            cache_items: register_int_gauge_vec!(
                "cachey_cache_items",
                "Number of items in cache",
                &["type"]
            )
            .expect("create cache_items"),

            cache_weight_bytes: register_int_gauge_vec!(
                "cachey_cache_weight_bytes",
                "Total weight of cached items in bytes",
                &["type"]
            )
            .expect("create cache_weight_bytes"),

            cache_capacity_bytes: register_int_gauge_vec!(
                "cachey_cache_capacity_bytes",
                "Maximum cache capacity in bytes",
                &["type"]
            )
            .expect("create cache_capacity_bytes"),

            cache_hits_total: register_int_counter_vec!(
                "cachey_cache_hits_total",
                "Total number of cache hits",
                &["type"]
            )
            .expect("create cache_hits_total"),

            cache_misses_total: register_int_counter_vec!(
                "cachey_cache_misses_total",
                "Total number of cache misses",
                &["type"]
            )
            .expect("create cache_misses_total"),

            cache_hit_rate: register_gauge_vec!(
                "cachey_cache_hit_rate",
                "Cache hit rate ratio (hits / total requests)",
                &["type"]
            )
            .expect("create cache_hit_rate"),

            cache_evictions_total: register_int_gauge_vec!(
                "cachey_cache_evictions_total",
                "Total number of cache evictions",
                &["type"]
            )
            .expect("create cache_evictions_total"),

            cache_evicted_bytes_total: register_int_gauge_vec!(
                "cachey_cache_evicted_bytes_total",
                "Total bytes evicted from cache",
                &["type"]
            )
            .expect("create cache_evicted_bytes_total"),

            disk_cleanup_pending: register_int_gauge!(
                "cachey_disk_cleanup_pending",
                "Number of files pending cleanup in the disk cleanup queue"
            )
            .expect("create disk_cleanup_pending"),

            memory_semaphore_capacity_bytes: register_int_gauge!(
                "cachey_memory_semaphore_capacity_bytes",
                "Total capacity of the memory semaphore in bytes"
            )
            .expect("create memory_semaphore_capacity_bytes"),

            memory_semaphore_allocated_bytes: register_int_gauge!(
                "cachey_memory_semaphore_allocated_bytes",
                "Currently allocated bytes in the memory semaphore"
            )
            .expect("create memory_semaphore_allocated_bytes"),

            last_disk_hits: AtomicU64::new(0),
            last_disk_misses: AtomicU64::new(0),
            last_memory_hits: AtomicU64::new(0),
            last_memory_misses: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    fn update_stats(
        &self,
        disk_cache: &DiskCache,
        memory_cache: &MemoryCache,
        memory_semaphore: &MemorySemaphore,
    ) {
        self.cache_items
            .with_label_values(&[LABEL_DISK])
            .set(disk_cache.len() as i64);
        self.cache_weight_bytes
            .with_label_values(&[LABEL_DISK])
            .set(disk_cache.weight() as i64);
        self.cache_capacity_bytes
            .with_label_values(&[LABEL_DISK])
            .set(disk_cache.capacity() as i64);
        self.cache_evictions_total
            .with_label_values(&[LABEL_DISK])
            .set(disk_cache.evictions() as i64);
        self.cache_evicted_bytes_total
            .with_label_values(&[LABEL_DISK])
            .set(disk_cache.evicted_bytes() as i64);
        self.disk_cleanup_pending
            .set(disk_cache.cleanup_pending() as i64);

        self.cache_items
            .with_label_values(&[LABEL_MEMORY])
            .set(memory_cache.len() as i64);
        self.cache_weight_bytes
            .with_label_values(&[LABEL_MEMORY])
            .set(memory_cache.weight() as i64);
        self.cache_capacity_bytes
            .with_label_values(&[LABEL_MEMORY])
            .set(memory_cache.capacity() as i64);
        self.cache_evictions_total
            .with_label_values(&[LABEL_MEMORY])
            .set(memory_cache.evictions() as i64);
        self.cache_evicted_bytes_total
            .with_label_values(&[LABEL_MEMORY])
            .set(memory_cache.evicted_bytes() as i64);

        self.update_hit_miss_counters(
            LABEL_DISK,
            disk_cache.hits(),
            disk_cache.misses(),
            &self.last_disk_hits,
            &self.last_disk_misses,
        );
        self.update_hit_miss_counters(
            LABEL_MEMORY,
            memory_cache.hits(),
            memory_cache.misses(),
            &self.last_memory_hits,
            &self.last_memory_misses,
        );

        self.memory_semaphore_capacity_bytes
            .set(memory_semaphore.capacity_bytes() as i64);
        self.memory_semaphore_allocated_bytes
            .set(memory_semaphore.allocated_bytes() as i64);
    }

    fn update_hit_miss_counters(
        &self,
        label: &str,
        hits: u64,
        misses: u64,
        last_hits: &AtomicU64,
        last_misses: &AtomicU64,
    ) {
        let prev_hits = last_hits.swap(hits, Ordering::Relaxed);
        let prev_misses = last_misses.swap(misses, Ordering::Relaxed);

        let hit_delta = hits.saturating_sub(prev_hits);
        let miss_delta = misses.saturating_sub(prev_misses);

        if hit_delta > 0 {
            self.cache_hits_total
                .with_label_values(&[label])
                .inc_by(hit_delta);
        }
        if miss_delta > 0 {
            self.cache_misses_total
                .with_label_values(&[label])
                .inc_by(miss_delta);
        }

        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        self.cache_hit_rate
            .with_label_values(&[label])
            .set(hit_rate);
    }
}

fn render(
    disk_cache: &DiskCache,
    memory_cache: &MemoryCache,
    memory_semaphore: &MemorySemaphore,
) -> String {
    METRICS.update_stats(disk_cache, memory_cache, memory_semaphore);

    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder
        .encode_to_string(&metric_families)
        .unwrap_or_default()
}

async fn handle_metrics_request(
    mut stream: TcpStream,
    disk_cache: Arc<DiskCache>,
    memory_cache: Arc<MemoryCache>,
    memory_semaphore: Arc<MemorySemaphore>,
) {
    let buf = vec![0u8; 1024];
    let Ok((n, buf)) = stream.read(buf).await.result() else {
        return;
    };

    if n == 0 {
        return;
    }

    let request = String::from_utf8_lossy(&buf[..n]);

    let response = if request.starts_with("GET /health") {
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: 2\r\n\
         Connection: close\r\n\
         \r\n\
         OK"
    } else if request.starts_with("GET /metrics") || request.starts_with("GET / ") {
        let body = render(&disk_cache, &memory_cache, &memory_semaphore);
        return write_response(
            stream,
            format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                body.len(),
                body
            ),
        )
        .await;
    } else {
        "HTTP/1.1 404 Not Found\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\
         \r\n"
    };

    write_response(stream, response).await;
}

async fn write_response(mut stream: TcpStream, response: impl Into<Vec<u8>>) {
    let _ = stream.write_all(response.into().slice(..)).await;
}

pub async fn serve_metrics(
    shard_id: usize,
    listen: String,
    disk_cache: Arc<DiskCache>,
    memory_cache: Arc<MemoryCache>,
    memory_semaphore: Arc<MemorySemaphore>,
) {
    let listener = match create_listener(&listen, METRICS_SERVER_NUM_LISTENERS) {
        Ok(l) => l,
        Err(e) => {
            error!(shard_id, "Failed to create metrics listener: {e}");
            return;
        }
    };

    debug!(shard_id, listen, "Metrics server listening");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let disk_cache = Arc::clone(&disk_cache);
                let memory_cache = Arc::clone(&memory_cache);
                let memory_semaphore = Arc::clone(&memory_semaphore);
                spawn(handle_metrics_request(
                    stream,
                    disk_cache,
                    memory_cache,
                    memory_semaphore,
                ))
                .detach();
            }
            Err(e) => {
                error!(shard_id, "Metrics accept error: {e}");
            }
        }
    }
}

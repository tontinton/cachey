use std::sync::{Arc, LazyLock};

use compio::buf::IoBuf;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;
use compio::runtime::spawn;
use prometheus::{
    Histogram, IntCounterVec, IntGaugeVec, TextEncoder, register_histogram,
    register_int_counter_vec, register_int_gauge_vec,
};
use tracing::{debug, error};

use crate::cache::{DiskCache, MemoryCache};
use crate::{BufResultExt, create_listener};

pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);

const METRICS_SERVER_NUM_LISTENERS: i32 = 128;

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
    pub cache_hits_total: IntGaugeVec,
    pub cache_misses_total: IntGaugeVec,
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

            cache_hits_total: register_int_gauge_vec!(
                "cachey_cache_hits_total",
                "Total number of cache hits",
                &["type"]
            )
            .expect("create cache_hits_total"),

            cache_misses_total: register_int_gauge_vec!(
                "cachey_cache_misses_total",
                "Total number of cache misses",
                &["type"]
            )
            .expect("create cache_misses_total"),
        }
    }
}

impl Metrics {
    fn update_cache_stats(&self, disk_cache: &DiskCache, memory_cache: &MemoryCache) {
        self.cache_items
            .with_label_values(&["disk"])
            .set(disk_cache.len() as i64);
        self.cache_weight_bytes
            .with_label_values(&["disk"])
            .set(disk_cache.weight() as i64);
        self.cache_capacity_bytes
            .with_label_values(&["disk"])
            .set(disk_cache.capacity() as i64);
        self.cache_hits_total
            .with_label_values(&["disk"])
            .set(disk_cache.hits() as i64);
        self.cache_misses_total
            .with_label_values(&["disk"])
            .set(disk_cache.misses() as i64);

        self.cache_items
            .with_label_values(&["memory"])
            .set(memory_cache.len() as i64);
        self.cache_weight_bytes
            .with_label_values(&["memory"])
            .set(memory_cache.weight() as i64);
        self.cache_capacity_bytes
            .with_label_values(&["memory"])
            .set(memory_cache.capacity() as i64);
        self.cache_hits_total
            .with_label_values(&["memory"])
            .set(memory_cache.hits() as i64);
        self.cache_misses_total
            .with_label_values(&["memory"])
            .set(memory_cache.misses() as i64);
    }
}

fn render(disk_cache: &DiskCache, memory_cache: &MemoryCache) -> String {
    METRICS.update_cache_stats(disk_cache, memory_cache);

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
) {
    let buf = vec![0u8; 1024];
    let Ok((n, buf)) = stream.read(buf).await.result() else {
        return;
    };

    if n == 0 {
        return;
    }

    let request = String::from_utf8_lossy(&buf[..n]);
    let is_metrics_request = request.starts_with("GET /metrics") || request.starts_with("GET / ");

    let response = if is_metrics_request {
        let body = render(&disk_cache, &memory_cache);
        format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {}",
            body.len(),
            body
        )
    } else {
        "HTTP/1.1 404 Not Found\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\
         \r\n"
            .to_string()
    };

    let _ = stream.write_all(response.into_bytes().slice(..)).await;
}

pub async fn serve_metrics(
    shard_id: usize,
    listen: String,
    disk_cache: Arc<DiskCache>,
    memory_cache: Arc<MemoryCache>,
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
                spawn(handle_metrics_request(stream, disk_cache, memory_cache)).detach();
            }
            Err(e) => {
                error!(shard_id, "Metrics accept error: {e}");
            }
        }
    }
}

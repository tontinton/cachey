use std::path::PathBuf;
use std::time::Duration;

use bytesize::ByteSize;
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Output logs formatted as JSON.
    #[clap(long, default_value = "false")]
    pub log_json: bool,

    /// Listen address (host:port).
    #[clap(short, long, default_value = "0.0.0.0:80")]
    pub listen: String,

    /// The provided value to listen() on the TCP server (man 2 listen).
    #[clap(long, default_value = "1024")]
    pub num_listeners: u32,

    /// Path to the disk cache dir.
    #[clap(long, default_value = "/cache")]
    pub disk_path: PathBuf,

    /// Disk cache size.
    #[clap(long, value_parser = parse_bytes, default_value = "1GiB")]
    pub disk_cache_size: ByteSize,

    /// Memory cache size for hot chunks.
    #[clap(long, value_parser = parse_bytes, default_value = "128MiB")]
    pub memory_cache_size: ByteSize,

    /// Metrics listen address (host:port).
    #[clap(long, default_value = "0.0.0.0:9090")]
    pub metrics_listen: String,

    /// Memory limit for semaphore (fallback if cgroup detection fails).
    /// Defaults to 100GiB if not specified.
    #[clap(long, value_parser = parse_bytes)]
    pub memory_limit: Option<ByteSize>,

    /// Enable io_uring submission queue polling with specified idle timeout.
    /// Trades CPU for lower latency. Example: --sqpoll-idle 10ms
    #[clap(long, value_parser = parse_duration)]
    pub sqpoll_idle: Option<Duration>,
}

fn parse_bytes(s: &str) -> Result<ByteSize, String> {
    s.parse::<ByteSize>().map_err(|e| {
        format!("Invalid memory size: {e}. Use formats like '512MiB', '2GB', '1.5GiB'",)
    })
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s)
        .map_err(|e| format!("Invalid duration: {e}. Use formats like '10ms', '1s', '500us'"))
}

#[must_use]
pub fn parse_args() -> Args {
    Args::parse()
}

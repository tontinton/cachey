use std::pin::pin;
use std::sync::Arc;
use std::thread;

use async_broadcast::broadcast;
use cachey::cache::{Cache, CleanupReceiver, run_cleanup_loop};
use cachey::metrics;
use compio::runtime::spawn;
use compio::signal::ctrl_c;
use futures_util::future::select;
use mimalloc::MiMalloc;
use tracing::info;

use cachey::{
    args::{Args, parse_args},
    server,
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(target_os = "linux")]
fn pin_to_core(core_id: usize) {
    use nix::sched::{CpuSet, sched_setaffinity};
    use nix::unistd::Pid;

    let mut cpuset = CpuSet::new();
    if cpuset.set(core_id).is_ok() {
        let _ = sched_setaffinity(Pid::from_raw(0), &cpuset);
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_to_core(_core_id: usize) {}

fn run_shard(
    shard_id: usize,
    args: Args,
    cache: Arc<Cache>,
    shutdown_rx: async_broadcast::Receiver<()>,
    cleanup_rx: CleanupReceiver,
) {
    pin_to_core(shard_id);
    let rt = compio::runtime::RuntimeBuilder::new()
        .build()
        .expect("failed to build runtime");

    rt.block_on(async move {
        spawn(run_cleanup_loop(cleanup_rx)).detach();
        spawn(metrics::serve_metrics(
            shard_id,
            args.metrics_listen.clone(),
            Arc::clone(&cache),
        ))
        .detach();

        if let Err(e) = server::serve_shard(shard_id, args, cache, shutdown_rx).await {
            tracing::error!(shard_id, "Shard error: {e}");
        }
    });
}

fn run_shard_with_signals(
    shard_id: usize,
    args: Args,
    cache: Arc<Cache>,
    shutdown_tx: async_broadcast::Sender<()>,
    shutdown_rx: async_broadcast::Receiver<()>,
    cleanup_rx: CleanupReceiver,
) {
    pin_to_core(shard_id);
    let rt = compio::runtime::RuntimeBuilder::new()
        .build()
        .expect("failed to build runtime");

    rt.block_on(async move {
        spawn(run_cleanup_loop(cleanup_rx)).detach();
        spawn(metrics::serve_metrics(
            shard_id,
            args.metrics_listen.clone(),
            Arc::clone(&cache),
        ))
        .detach();

        let serve = pin!(server::serve_shard(shard_id, args, cache, shutdown_rx));

        #[cfg(unix)]
        let signal = pin!(async {
            use compio::signal::unix::signal;

            let ctrl_c = pin!(ctrl_c());
            let sigterm = pin!(signal(libc::SIGTERM));

            select(ctrl_c, sigterm).await;
            info!("Shutdown signal received");
        });

        #[cfg(not(unix))]
        let signal = pin!(async {
            ctrl_c().await.ok();
            info!("Shutdown signal received");
        });

        select(serve, signal).await;
        drop(shutdown_tx);
    });
}

fn main() -> eyre::Result<()> {
    let args = parse_args();

    if args.log_json {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        tracing_subscriber::registry().with(filter).init();
    } else {
        tracing_subscriber::fmt().init();
    }

    #[cfg(unix)]
    let num_shards = num_cpus::get();
    #[cfg(not(unix))]
    let num_shards = 1; // SO_REUSEPORT not available

    info!(?args, num_shards, "Init");

    let (shutdown_tx, shutdown_rx) = broadcast::<()>(1);
    let (cache, cleanup_rx) = Cache::new(args.disk_path.clone(), args.disk_cache_size.as_u64());
    let cache = Arc::new(cache);

    let workers: Vec<_> = (1..num_shards)
        .map(|shard_id| {
            let args = args.clone();
            let cache = Arc::clone(&cache);
            let shutdown_rx = shutdown_rx.clone();
            let cleanup_rx = cleanup_rx.clone();
            thread::Builder::new()
                .name(format!("shard-{shard_id}"))
                .spawn(move || run_shard(shard_id, args, cache, shutdown_rx, cleanup_rx))
                .expect("failed to spawn worker thread")
        })
        .collect();

    run_shard_with_signals(0, args, cache, shutdown_tx, shutdown_rx, cleanup_rx);

    for worker in workers {
        let _ = worker.join();
    }

    info!("All shards stopped, bye bye!");
    Ok(())
}

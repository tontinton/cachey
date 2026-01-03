use std::pin::pin;
use std::sync::Arc;
use std::thread;

use async_broadcast::broadcast;
use cachey::MemorySemaphore;
use cachey::cache::{CleanupReceiver, DiskCache, MemoryCache, run_cleanup_loop};
use cachey::metrics;
use cachey::{
    args::{Args, parse_args},
    server,
};
use compio::runtime::spawn;
use compio::signal::ctrl_c;
use futures_util::future::select;
use mimalloc::MiMalloc;
use tracing::info;

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

#[allow(clippy::too_many_arguments)]
fn run_shard(
    shard_id: usize,
    args: Args,
    disk_cache: Arc<DiskCache>,
    memory_cache: Arc<MemoryCache>,
    memory_semaphore: Arc<MemorySemaphore>,
    shutdown_tx: Option<async_broadcast::Sender<()>>,
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
            Arc::clone(&disk_cache),
            Arc::clone(&memory_cache),
            Arc::clone(&memory_semaphore),
        ))
        .detach();

        let serve = pin!(server::serve_shard(
            shard_id,
            args,
            disk_cache,
            memory_cache,
            memory_semaphore,
            shutdown_rx
        ));

        if let Some(shutdown_tx) = shutdown_tx {
            #[cfg(unix)]
            let signal = pin!(async {
                use compio::signal::unix::signal;
                select(pin!(ctrl_c()), pin!(signal(libc::SIGTERM))).await;
                info!("Shutdown signal received");
            });

            #[cfg(not(unix))]
            let signal = pin!(async {
                ctrl_c().await.ok();
                info!("Shutdown signal received");
            });

            select(serve, signal).await;
            drop(shutdown_tx);
        } else if let Err(e) = serve.await {
            tracing::error!(shard_id, "Shard error: {e}");
        }
    });
}

fn main() -> eyre::Result<()> {
    let args = parse_args();

    if args.log_json {
        tracing_subscriber::fmt().json().init();
    } else {
        tracing_subscriber::fmt().init();
    }

    #[cfg(unix)]
    let num_shards = num_cpus::get();
    #[cfg(not(unix))]
    let num_shards = 1;

    info!(?args, num_shards, "Init");

    let (shutdown_tx, shutdown_rx) = broadcast::<()>(1);
    let (disk_cache, cleanup_rx) =
        DiskCache::new(args.disk_path.clone(), args.disk_cache_size.as_u64());
    let disk_cache = Arc::new(disk_cache);
    let memory_cache = Arc::new(MemoryCache::new(args.memory_cache_size.as_u64()));

    let memory_semaphore = Arc::new(MemorySemaphore::new(
        args.memory_cache_size.as_u64(),
        args.memory_limit.map(|b| b.as_u64()),
    ));
    info!(
        capacity = memory_semaphore.capacity(),
        "Memory semaphore initialized"
    );

    let workers: Vec<_> = (1..num_shards)
        .map(|shard_id| {
            let args = args.clone();
            let disk_cache = Arc::clone(&disk_cache);
            let memory_cache = Arc::clone(&memory_cache);
            let memory_semaphore = Arc::clone(&memory_semaphore);
            let shutdown_rx = shutdown_rx.clone();
            let cleanup_rx = cleanup_rx.clone();
            thread::Builder::new()
                .name(format!("shard-{shard_id}"))
                .spawn(move || {
                    run_shard(
                        shard_id,
                        args,
                        disk_cache,
                        memory_cache,
                        memory_semaphore,
                        None,
                        shutdown_rx,
                        cleanup_rx,
                    )
                })
                .expect("failed to spawn worker thread")
        })
        .collect();

    run_shard(
        0,
        args,
        disk_cache,
        memory_cache,
        memory_semaphore,
        Some(shutdown_tx),
        shutdown_rx,
        cleanup_rx,
    );

    for worker in workers {
        let _ = worker.join();
    }

    info!("All shards stopped");
    Ok(())
}

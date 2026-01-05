use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_broadcast::broadcast;
use bytesize::ByteSize;
use cachey::MemorySemaphore;
use cachey::args::Args as ServerArgs;
use cachey::cache::{DiskCache, MemoryCache};
use cachey::proto::{CacheServiceClient, MemoryCacheRanges};
use futures_util::AsyncReadExt;
use rsmp::Stream;
use tokio_util::compat::TokioAsyncReadCompatExt;

const TEST_DATA: &[u8] = b"Hello, rsmp protocol!";

fn get_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct TestServer {
    addr: String,
    metrics_addr: String,
    disk_cache: Arc<DiskCache>,
    memory_cache: Arc<MemoryCache>,
    shutdown_tx: async_broadcast::Sender<()>,
    handle: Option<thread::JoinHandle<()>>,
    _temp_dir: tempfile::TempDir,
}

impl TestServer {
    fn start() -> Self {
        Self::start_with_memory_cache_size(ByteSize::mib(1))
    }

    fn start_with_memory_cache_size(memory_cache_size: ByteSize) -> Self {
        Self::start_with_config(memory_cache_size, None)
    }

    fn start_with_config(memory_cache_size: ByteSize, memory_limit: Option<ByteSize>) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let port = get_free_port();
        let addr = format!("127.0.0.1:{}", port);
        let metrics_addr = format!("127.0.0.1:{}", get_free_port());

        let args = ServerArgs {
            log_json: false,
            listen: addr.clone(),
            num_listeners: 1,
            disk_path: temp_dir.path().to_path_buf(),
            disk_cache_size: ByteSize::mib(10),
            memory_cache_size,
            metrics_listen: metrics_addr.clone(),
            memory_limit,
        };

        let (shutdown_tx, shutdown_rx) = broadcast::<()>(1);
        let (disk_cache, cleanup_rx) =
            DiskCache::new(args.disk_path.clone(), args.disk_cache_size.as_u64());
        let disk_cache = Arc::new(disk_cache);
        let memory_cache = Arc::new(MemoryCache::new(args.memory_cache_size.as_u64()));

        let disk_cache_clone = Arc::clone(&disk_cache);
        let memory_cache_clone = Arc::clone(&memory_cache);
        let memory_semaphore = Arc::new(MemorySemaphore::new(
            args.memory_cache_size.as_u64(),
            args.memory_limit.map(|b| b.as_u64()),
        ));
        let memory_semaphore_clone = Arc::clone(&memory_semaphore);

        let handle = thread::spawn(move || {
            let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
            rt.block_on(async {
                compio::runtime::spawn(cachey::cache::run_cleanup_loop(cleanup_rx)).detach();
                compio::runtime::spawn(cachey::metrics::serve_metrics(
                    0,
                    args.metrics_listen.clone(),
                    Arc::clone(&disk_cache_clone),
                    Arc::clone(&memory_cache_clone),
                    Arc::clone(&memory_semaphore_clone),
                ))
                .detach();
                let _ = cachey::server::serve_shard(
                    0,
                    args,
                    disk_cache_clone,
                    memory_cache_clone,
                    memory_semaphore_clone,
                    shutdown_rx,
                )
                .await;
            });
        });

        Self {
            addr,
            metrics_addr,
            disk_cache,
            memory_cache,
            shutdown_tx,
            handle: Some(handle),
            _temp_dir: temp_dir,
        }
    }

    fn addr(&self) -> &str {
        &self.addr
    }

    fn metrics_addr(&self) -> &str {
        &self.metrics_addr
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.try_broadcast(());
        self.shutdown_tx.close();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

async fn run_put_test<T: rsmp::Transport>(client: &mut CacheServiceClient<T>) {
    client
        .put(
            "test-file",
            None,
            Stream::new(TEST_DATA, TEST_DATA.len() as u64),
        )
        .await
        .unwrap();
}

async fn run_get_test<T: rsmp::Transport>(client: &mut CacheServiceClient<T>) {
    let mut response = client
        .get("test-file", 0, TEST_DATA.len() as u64)
        .await
        .unwrap();

    let mut data = vec![0u8; TEST_DATA.len()];
    response.read_exact(&mut data).await.unwrap();
    assert_eq!(data, TEST_DATA);
}

async fn run_not_found_test<T: rsmp::Transport>(client: &mut CacheServiceClient<T>) {
    match client.get("nonexistent", 0, 100).await {
        Err(rsmp::ClientError::Server(cachey::proto::CacheError::NotFound(err))) => {
            assert_eq!(err.id, "nonexistent");
        }
        other => panic!("expected NotFound error, got {:?}", other.map(|_| ())),
    }
}

async fn connect_tokio(addr: &str) -> tokio::net::TcpStream {
    loop {
        if let Ok(s) = tokio::net::TcpStream::connect(addr).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test]
fn put_and_get_tokio() {
    let server = TestServer::start();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());
            run_not_found_test(&mut client).await;

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());
            run_put_test(&mut client).await;

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());
            run_get_test(&mut client).await;
            run_not_found_test(&mut client).await;
        });
}

#[test]
fn put_and_get_compio() {
    let server = TestServer::start();

    let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
    rt.block_on(async {
        let stream = connect_compio(server.addr()).await;
        let compat_stream = compio::io::compat::AsyncStream::new(stream);
        let mut client = CacheServiceClient::from_stream_local(compat_stream);
        run_not_found_test(&mut client).await;

        let stream = connect_compio(server.addr()).await;
        let compat_stream = compio::io::compat::AsyncStream::new(stream);
        let mut client = CacheServiceClient::from_stream_local(compat_stream);
        run_put_test(&mut client).await;

        let stream = connect_compio(server.addr()).await;
        let compat_stream = compio::io::compat::AsyncStream::new(stream);
        let mut client = CacheServiceClient::from_stream_local(compat_stream);
        run_get_test(&mut client).await;
        run_not_found_test(&mut client).await;
    });
}

async fn connect_compio(addr: &str) -> compio::net::TcpStream {
    loop {
        if let Ok(s) = compio::net::TcpStream::connect(addr).await {
            return s;
        }
        compio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test]
fn put_and_get_verifies_disk_cache_metrics() {
    let server = TestServer::start();

    assert_eq!(server.disk_cache.len(), 0);
    assert_eq!(server.disk_cache.hits(), 0);
    assert_eq!(server.disk_cache.misses(), 0);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            let result = client.get("missing", 0, 100).await;
            assert!(result.is_err());

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            client
                .put(
                    "file1",
                    None,
                    Stream::new(TEST_DATA, TEST_DATA.len() as u64),
                )
                .await
                .unwrap();

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            let mut body = client
                .get("file1", 0, TEST_DATA.len() as u64)
                .await
                .unwrap();
            let mut data = vec![0u8; TEST_DATA.len()];
            body.read_exact(&mut data).await.unwrap();
        });

    assert_eq!(server.disk_cache.len(), 1);
    assert_eq!(server.disk_cache.misses(), 1);
    assert_eq!(server.disk_cache.hits(), 1);
    assert_eq!(server.disk_cache.weight(), TEST_DATA.len() as u64);
}

#[test]
fn put_with_cache_ranges_populates_memory_cache() {
    let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    let server = TestServer::start_with_memory_cache_size(ByteSize::mib(10));

    assert_eq!(server.memory_cache.len(), 0);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            let memory_cache_ranges: MemoryCacheRanges = vec![0u64..100, 500..600].into();
            client
                .put(
                    "file1",
                    Some(memory_cache_ranges),
                    Stream::from_vec(data.clone()),
                )
                .await
                .unwrap();
        });

    assert_eq!(server.memory_cache.len(), 2);

    let chunk1 = server.memory_cache.get("file1", 0, 100).unwrap();
    let expected1: Vec<u8> = (0..100).map(|i| (i % 256) as u8).collect();
    assert_eq!(&chunk1[..], &expected1[..]);

    let chunk2 = server.memory_cache.get("file1", 500, 600).unwrap();
    let expected2: Vec<u8> = (500..600).map(|i| (i % 256) as u8).collect();
    assert_eq!(&chunk2[..], &expected2[..]);
}

#[test]
fn put_existing_file_skips_write() {
    let server = TestServer::start();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            client
                .put(
                    "file1",
                    None,
                    Stream::new(TEST_DATA, TEST_DATA.len() as u64),
                )
                .await
                .unwrap();

            assert_eq!(server.disk_cache.len(), 1);
            assert_eq!(server.disk_cache.weight(), TEST_DATA.len() as u64);

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            let different_data: &[u8] = b"different content!";
            let result = client
                .put(
                    "file1",
                    None,
                    Stream::new(different_data, different_data.len() as u64),
                )
                .await;

            match result {
                Err(rsmp::ClientError::Server(cachey::proto::CacheError::AlreadyExists(id))) => {
                    assert_eq!(id, "file1");
                }
                other => panic!("expected AlreadyExists error, got {:?}", other.map(|_| ())),
            }

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            assert_eq!(server.disk_cache.len(), 1);
            assert_eq!(server.disk_cache.weight(), TEST_DATA.len() as u64);

            let mut body = client
                .get("file1", 0, TEST_DATA.len() as u64)
                .await
                .unwrap();
            let mut data = vec![0u8; TEST_DATA.len()];
            body.read_exact(&mut data).await.unwrap();
            assert_eq!(data, TEST_DATA);
        });
}

#[test]
fn get_hits_memory_cache_before_disk() {
    let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    let server = TestServer::start_with_memory_cache_size(ByteSize::mib(10));

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            #[allow(clippy::single_range_in_vec_init)]
            let memory_cache_ranges: MemoryCacheRanges = vec![0u64..100].into();
            client
                .put(
                    "file1",
                    Some(memory_cache_ranges),
                    Stream::from_vec(data.clone()),
                )
                .await
                .unwrap();

            assert_eq!(server.memory_cache.hits(), 0);
            assert_eq!(server.disk_cache.hits(), 0);

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            let mut body = client.get("file1", 0, 100).await.unwrap();
            let mut received = vec![0u8; 100];
            body.read_exact(&mut received).await.unwrap();

            let expected: Vec<u8> = (0..100).map(|i| (i % 256) as u8).collect();
            assert_eq!(received, expected);
        });

    assert_eq!(server.memory_cache.hits(), 1);
    assert_eq!(server.disk_cache.hits(), 0);
}

#[test]
fn get_falls_back_to_disk_on_memory_miss() {
    let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    let server = TestServer::start_with_memory_cache_size(ByteSize::mib(10));

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            #[allow(clippy::single_range_in_vec_init)]
            let memory_cache_ranges: MemoryCacheRanges = vec![0u64..100].into();
            client
                .put(
                    "file1",
                    Some(memory_cache_ranges),
                    Stream::from_vec(data.clone()),
                )
                .await
                .unwrap();

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            let mut body = client.get("file1", 200, 100).await.unwrap();
            let mut received = vec![0u8; 100];
            body.read_exact(&mut received).await.unwrap();

            let expected: Vec<u8> = (200..300).map(|i| (i % 256) as u8).collect();
            assert_eq!(received, expected);
        });

    assert_eq!(server.memory_cache.misses(), 1);
    assert_eq!(server.disk_cache.hits(), 1);
}

#[test]
fn get_partial_file_from_disk() {
    let data: Vec<u8> = (0u8..=255).cycle().take(10000).collect();
    let server = TestServer::start();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            client
                .put("bigfile", None, Stream::from_vec(data.clone()))
                .await
                .unwrap();

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            {
                let mut body = client.get("bigfile", 0, 256).await.unwrap();
                let mut received = vec![0u8; 256];
                body.read_exact(&mut received).await.unwrap();
                assert_eq!(received, &data[0..256]);
            }

            {
                let mut body = client.get("bigfile", 5000, 1000).await.unwrap();
                let mut received = vec![0u8; 1000];
                body.read_exact(&mut received).await.unwrap();
                assert_eq!(received, &data[5000..6000]);
            }
        });
}

#[test]
fn get_range_past_eof_returns_partial_data() {
    let data: Vec<u8> = (0..100).collect();
    let server = TestServer::start();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            client
                .put("small", None, Stream::from_vec(data.clone()))
                .await
                .unwrap();

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            let mut body = client.get("small", 50, 200).await.unwrap();

            let mut received = Vec::new();
            body.read_to_end(&mut received).await.unwrap();

            assert_eq!(
                received.len(),
                50,
                "should only return 50 bytes (100 - 50 offset)"
            );
            assert_eq!(received, &data[50..100]);
        });
}

#[test]
fn sequential_requests_complete_under_memory_pressure() {
    let data: Vec<u8> = (0u8..=255).cycle().take(256 * 1024).collect();
    let server = TestServer::start_with_config(ByteSize::kib(64), Some(ByteSize::mib(1)));

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            client
                .put("pressure-test", None, Stream::from_vec(data.clone()))
                .await
                .unwrap();

            let stream = connect_tokio(server.addr()).await;
            let mut client = CacheServiceClient::from_stream(stream.compat());

            for _ in 0..3 {
                let mut body = client.get("pressure-test", 0, 256 * 1024).await.unwrap();

                let mut received = Vec::new();
                body.read_to_end(&mut received).await.unwrap();
                assert_eq!(received.len(), 256 * 1024);
            }
        });
}

#[test]
fn health_endpoint_returns_ok() {
    let server = TestServer::start();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut stream = connect_tokio(server.metrics_addr()).await;
            tokio::io::AsyncWriteExt::write_all(&mut stream, b"GET /health HTTP/1.1\r\n\r\n")
                .await
                .unwrap();

            let mut response = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut response)
                .await
                .unwrap();

            let response = String::from_utf8_lossy(&response);
            assert!(response.starts_with("HTTP/1.1 200 OK"));
            assert!(response.contains("OK"));
        });
}

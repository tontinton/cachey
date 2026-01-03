use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_broadcast::broadcast;
use bytesize::ByteSize;
use cachey::args::Args as ServerArgs;
use cachey::cache::Cache;
use cachey::proto::CacheServiceClient;
use futures_util::AsyncReadExt;
use rsmp::{BodyStream, StreamTransport, Transport};
use tokio_util::compat::TokioAsyncReadCompatExt;

const TEST_DATA: &[u8] = b"Hello, rsmp protocol!";

fn get_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct TestServer {
    addr: String,
    shutdown_tx: async_broadcast::Sender<()>,
    handle: Option<thread::JoinHandle<()>>,
    _temp_dir: tempfile::TempDir,
}

impl TestServer {
    fn start() -> Self {
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
            metrics_listen: metrics_addr,
        };

        let (shutdown_tx, shutdown_rx) = broadcast::<()>(1);
        let (cache, cleanup_rx) = Cache::new(args.disk_path.clone(), args.disk_cache_size.as_u64());
        let cache = Arc::new(cache);

        let handle = thread::spawn(move || {
            let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
            rt.block_on(async {
                compio::runtime::spawn(cachey::cache::run_cleanup_loop(cleanup_rx)).detach();
                let _ = cachey::server::serve_shard(0, args, cache, shutdown_rx).await;
            });
        });

        Self {
            addr,
            shutdown_tx,
            handle: Some(handle),
            _temp_dir: temp_dir,
        }
    }

    fn addr(&self) -> &str {
        &self.addr
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

async fn run_client_test<T: Transport>(client: &mut CacheServiceClient<T>) {
    client
        .put(
            &"test-file".to_string(),
            BodyStream::new(TEST_DATA, TEST_DATA.len() as u64),
        )
        .await
        .unwrap();

    let (_header, mut body) = client
        .get(&"test-file".to_string(), &0u64, &(TEST_DATA.len() as u64))
        .await
        .unwrap();

    let mut data = vec![0u8; TEST_DATA.len()];
    body.read_exact(&mut data).await.unwrap();
    assert_eq!(data, TEST_DATA);
}

async fn run_not_found_test<T: Transport>(client: &mut CacheServiceClient<T>) {
    match client.get(&"nonexistent".to_string(), &0u64, &100u64).await {
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
            let mut client = CacheServiceClient::new(StreamTransport::new(stream.compat()));
            run_not_found_test(&mut client).await;
            run_client_test(&mut client).await;
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
        let mut client = CacheServiceClient::new(StreamTransport::new(compat_stream));
        run_not_found_test(&mut client).await;
        run_client_test(&mut client).await;
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

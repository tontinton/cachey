use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_broadcast::broadcast;
use bytesize::ByteSize;
use cachey::args::Args as ServerArgs;
use cachey::cache::Cache;
use cachey::proto::{Command, GetArgs, PutArgs, Response, StreamHeaderResponse};
use rsmp::Args;
use rsmp::Stream;

fn send_request(stream: &mut TcpStream, cmd: &Command, body: Option<&[u8]>) -> Response {
    let encoded = cmd.encode_args();
    let len = encoded.len() as u32;

    stream.write_all(&len.to_be_bytes()).unwrap();
    stream.write_all(&encoded).unwrap();

    if let Some(data) = body {
        stream.write_all(data).unwrap();
    }
    stream.flush().unwrap();

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .expect("Failed to read response length");
    let resp_len = u32::from_be_bytes(len_buf) as usize;

    let mut resp_buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp_buf)
        .expect("Failed to read response body");

    Response::decode_args(&resp_buf).expect("Failed to decode response")
}

fn read_stream_data(stream: &mut TcpStream, size: u64) -> Vec<u8> {
    let mut data = vec![0u8; size as usize];
    stream.read_exact(&mut data).unwrap();
    data
}

#[test]
fn put_and_get_roundtrip() {
    let temp_dir = tempfile::tempdir().unwrap();
    let addr = "127.0.0.1:18080";
    let metrics_addr = "127.0.0.1:19090";

    let args = ServerArgs {
        log_json: false,
        listen: addr.to_string(),
        num_listeners: 1,
        disk_path: temp_dir.path().to_path_buf(),
        disk_cache_size: ByteSize::mib(10),
        metrics_listen: metrics_addr.to_string(),
    };

    let (shutdown_tx, shutdown_rx) = broadcast::<()>(1);

    let (cache, cleanup_rx) = Cache::new(args.disk_path.clone(), args.disk_cache_size.as_u64());
    let cache = Arc::new(cache);
    let cache_clone = Arc::clone(&cache);

    let server_handle = thread::spawn(move || {
        let rt = compio::runtime::RuntimeBuilder::new().build().unwrap();
        rt.block_on(async {
            compio::runtime::spawn(cachey::cache::run_cleanup_loop(cleanup_rx)).detach();
            let _ = cachey::server::serve_shard(0, args, cache_clone, shutdown_rx).await;
        });
    });

    // Wait for server to start with retry
    let mut stream = None;
    for _ in 0..50 {
        thread::sleep(Duration::from_millis(50));
        match TcpStream::connect(addr) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(_) => continue,
        }
    }
    let mut stream = stream.expect("Failed to connect to server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // PUT a file
    let test_data = b"Hello, rsmp protocol!";
    let put_cmd = Command::Put(PutArgs {
        id: "test-file".to_string(),
        stream: Stream(test_data.len() as u64),
    });

    let resp = send_request(&mut stream, &put_cmd, Some(test_data));
    assert!(
        matches!(resp, Response::Success(_)),
        "PUT should succeed: {resp:?}"
    );

    // GET the file back
    let get_cmd = Command::Get(GetArgs {
        id: "test-file".to_string(),
        offset: 0,
        size: test_data.len() as u64,
    });

    let resp = send_request(&mut stream, &get_cmd, None);
    match resp {
        Response::StreamHeader(StreamHeaderResponse {
            stream: Stream(size),
        }) => {
            assert_eq!(size, test_data.len() as u64);
            let data = read_stream_data(&mut stream, size);
            assert_eq!(data, test_data);
        }
        other => panic!("Expected StreamHeader, got {other:?}"),
    }

    // GET non-existent file should error
    let get_missing = Command::Get(GetArgs {
        id: "does-not-exist".to_string(),
        offset: 0,
        size: 100,
    });

    let resp = send_request(&mut stream, &get_missing, None);
    assert!(
        matches!(resp, Response::Error { .. }),
        "GET missing should error: {resp:?}"
    );

    drop(stream);
    drop(shutdown_tx); // Signal shutdown
    let _ = server_handle.join();
}

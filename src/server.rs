use std::io;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;

use async_broadcast::{Receiver, broadcast};
use compio::buf::{IntoInner, IoBuf};
use compio::fs::File;
use compio::io::{AsyncRead, AsyncReadAt, AsyncWriteAtExt, AsyncWriteExt};
use compio::net::TcpStream;
use compio::runtime::spawn;
use futures_util::FutureExt;
use futures_util::future::select;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tracing::{debug, error, info};

use rsmp::Args;

use crate::cache::Cache;
use crate::metrics::METRICS;
use crate::proto::{
    Command, ErrorResponse, GetArgs, PutArgs, RequestError, RequestErrorDiscriminants, Response,
    StreamHeaderResponse, SuccessResponse,
};
use crate::{BufResultExt, create_listener};

const STREAM_FILE_BUF_SIZE: usize = 64 * 1024;

struct Shard {
    id: usize,
    cache: Arc<Cache>,
}

struct RequestOutcome {
    response: ResponseFrame,
    bytes_read: u64,
    bytes_written: u64,
}

struct ResponseFrame {
    header: Vec<u8>,
    stream: Option<FileStream>,
}

struct FileStream {
    file: File,
    offset: u64,
    size: u64,
}

impl ResponseFrame {
    fn data(resp: Response) -> Self {
        Self {
            header: resp.encode_args(),
            stream: None,
        }
    }

    fn stream(resp: Response, file: File, offset: u64, size: u64) -> Self {
        Self {
            header: resp.encode_args(),
            stream: Some(FileStream { file, offset, size }),
        }
    }
}

async fn run_command(
    shard: &Shard,
    cmd: Command,
    stream: &mut TcpStream,
) -> Result<RequestOutcome, RequestError> {
    match cmd {
        Command::Get(GetArgs { id, offset, size }) => {
            let Some(path) = shard.cache.get(&id) else {
                debug!(shard_id = shard.id, id, "not found");
                return Err(RequestError::NotFound);
            };

            debug!(shard_id = shard.id, id, ?path, "found");

            let file = File::open(&path).await?;
            let resp = Response::StreamHeader(StreamHeaderResponse {
                stream: rsmp::Stream(size),
            });
            Ok(RequestOutcome {
                response: ResponseFrame::stream(resp, file, offset, size),
                bytes_read: size,
                bytes_written: 0,
            })
        }
        Command::Put(PutArgs {
            id,
            stream: rsmp::Stream(size),
        }) => {
            let path = shard.cache.generate_path(&id);

            debug!(shard_id = shard.id, id, ?path, "writing");

            let mut file = File::create(&path).await?;
            stream_to_file(stream, &mut file, size).await?;
            shard.cache.insert(id.clone(), id, size);

            debug!(shard_id = shard.id, ?path, "written");

            let resp = Response::Success(SuccessResponse { data: vec![] });
            Ok(RequestOutcome {
                response: ResponseFrame::data(resp),
                bytes_read: 0,
                bytes_written: size,
            })
        }
    }
}

fn error_response(err: &RequestError) -> Response {
    Response::Error(ErrorResponse {
        code: RequestErrorDiscriminants::from(err) as u16,
        message: err.to_string(),
    })
}

async fn handle_request(shard: &Shard, buf: Vec<u8>, stream: &mut TcpStream) -> ResponseFrame {
    let cmd = match Command::decode_args(&buf) {
        Ok(cmd) => cmd,
        Err(e) => {
            let e = RequestError::Decode(e);
            METRICS
                .requests_total
                .with_label_values(&["unknown", e.as_ref()])
                .inc();
            return ResponseFrame::data(error_response(&e));
        }
    };

    let cmd_label: String = cmd.as_ref().to_string();
    let timer = METRICS.request_duration.start_timer();

    match run_command(shard, cmd, stream).await {
        Ok(outcome) => {
            METRICS
                .requests_total
                .with_label_values(&[cmd_label.as_str(), "success"])
                .inc();
            if outcome.bytes_read > 0 {
                METRICS
                    .bytes_read_total
                    .with_label_values(&[cmd_label.as_str()])
                    .inc_by(outcome.bytes_read);
            }
            if outcome.bytes_written > 0 {
                METRICS
                    .bytes_written_total
                    .with_label_values(&[cmd_label.as_str()])
                    .inc_by(outcome.bytes_written);
            }
            timer.observe_duration();
            outcome.response
        }
        Err(e) => {
            METRICS
                .requests_total
                .with_label_values(&[cmd_label.as_str(), e.as_ref()])
                .inc();
            timer.observe_duration();
            ResponseFrame::data(error_response(&e))
        }
    }
}

async fn stream_to_file(socket: &mut TcpStream, file: &mut File, size: u64) -> io::Result<()> {
    let mut offset = 0u64;
    let mut buf = Vec::with_capacity(STREAM_FILE_BUF_SIZE);

    while offset < size {
        let (n, read_buf) = socket.read(buf).await.result()?;
        if n == 0 {
            break;
        }
        buf = read_buf;

        let slice = buf.slice(..n);
        let ((), slice) = file.write_all_at(slice, offset).await.result()?;

        buf = slice.into_inner();
        buf.clear();
        offset += n as u64;
    }

    Ok(())
}

async fn stream_file(
    socket: &mut TcpStream,
    file: &File,
    mut offset: u64,
    size: u64,
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(STREAM_FILE_BUF_SIZE);

    while offset < size {
        let (n, read_buf) = file.read_at(buf, offset).await.result()?;
        if n == 0 {
            break;
        }
        buf = read_buf;
        let slice = buf.slice(..n);
        let ((), slice) = socket.write_all(slice).await.result()?;
        buf = slice.into_inner();
        buf.clear();
        offset += n as u64;
    }

    Ok(())
}

async fn read_exact(stream: &mut TcpStream, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let mut filled = 0;

    while filled < len {
        let slice = buf.slice(filled..);
        let (n, returned_slice) = stream.read(slice).await.result()?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            ));
        }
        buf = returned_slice.into_inner();
        filled += n;
    }
    Ok(buf)
}

async fn handle_connection(shard: &Shard, mut stream: TcpStream, mut shutdown_rx: Receiver<()>) {
    loop {
        match shutdown_rx.try_recv() {
            Ok(_) | Err(async_broadcast::TryRecvError::Closed) => break,
            Err(async_broadcast::TryRecvError::Empty) => {}
            Err(async_broadcast::TryRecvError::Overflowed(_)) => {}
        }

        let len_buf = {
            let read_fut = read_exact(&mut stream, std::mem::size_of::<u32>());
            let shutdown_fut = shutdown_rx.recv();

            let read_fut = pin!(read_fut);
            let shutdown_fut = pin!(shutdown_fut);

            match select(read_fut, shutdown_fut).await {
                futures_util::future::Either::Left((result, _)) => match result {
                    Ok(buf) => buf,
                    Err(_) => break,
                },
                futures_util::future::Either::Right(_) => break,
            }
        };

        let len = u32::from_be_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;

        let buf = match read_exact(&mut stream, len).await {
            Ok(buf) => buf,
            Err(_) => break,
        };

        let resp = handle_request(shard, buf, &mut stream).await;
        let resp_len = (resp.header.len() as u32).to_be_bytes();

        if stream.write_all(resp_len.to_vec()).await.is_err()
            || stream.write_all(resp.header).await.is_err()
        {
            break;
        }

        if let Some(fs) = resp.stream
            && stream_file(&mut stream, &fs.file, fs.offset, fs.size)
                .await
                .is_err()
        {
            break;
        }
    }
}

pub async fn serve_shard(
    shard_id: usize,
    config: crate::args::Args,
    cache: Arc<Cache>,
    mut shutdown_rx: Receiver<()>,
) -> eyre::Result<()> {
    let listener = create_listener(&config.listen, config.num_listeners as i32)?;
    info!(shard_id, "Shard listening on {}", config.listen);

    let (conn_shutdown_tx, conn_shutdown_rx) = broadcast::<()>(1);
    let mut connections: FuturesUnordered<compio::runtime::JoinHandle<()>> =
        FuturesUnordered::new();

    let shard = Rc::new(Shard {
        id: shard_id,
        cache,
    });

    loop {
        let accept_fut = pin!(listener.accept());
        let mut next_conn = pin!(connections.next());
        let shutdown_fut = pin!(shutdown_rx.recv());

        futures_util::select_biased! {
            _ = shutdown_fut.fuse() => {
                break;
            }
            result = accept_fut.fuse() => {
                match result {
                    Ok((stream, addr)) => {
                        debug!(shard_id, %addr, "Connection accepted");
                        let shard = shard.clone();
                        let conn_shutdown_rx = conn_shutdown_rx.clone();
                        connections.push(spawn(async move {
                            handle_connection(&shard, stream, conn_shutdown_rx).await;
                            debug!(shard_id, %addr, "Connection closed");
                        }));
                        METRICS.active_connections.with_label_values(&[&shard_id.to_string()]).set(connections.len() as i64);
                    }
                    Err(e) => {
                        error!(shard_id, "Accept error: {}", e);
                    }
                }
            }
            _ = next_conn => {
                METRICS.active_connections.with_label_values(&[&shard_id.to_string()]).set(connections.len() as i64);
            }
        }
    }

    info!(
        shard_id,
        "Signaling shutdown to {} active connections",
        connections.len()
    );
    drop(conn_shutdown_tx);

    while connections.next().await.is_some() {}
    info!(shard_id, "Shard stopped");

    Ok(())
}

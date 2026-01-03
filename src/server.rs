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
use futures_util::stream::{FuturesUnordered, StreamExt};
use prometheus::HistogramTimer;
use rsmp::{AsyncStreamCompat, CallContext, Interceptor};
use tracing::{debug, error, info};

use crate::cache::Cache;
use crate::metrics::METRICS;
use crate::proto::{CacheError, CacheServiceDispatcher, CacheServiceHandlerLocal, NotFoundError};
use crate::{BufResultExt, create_listener};

const STREAM_FILE_BUF_SIZE: usize = 64 * 1024;

struct MetricsInterceptor;

impl Interceptor for MetricsInterceptor {
    type State = HistogramTimer;

    fn before(&self, _ctx: &CallContext) -> Self::State {
        METRICS.request_duration.start_timer()
    }

    fn after(&self, ctx: &CallContext, timer: Self::State, success: bool) {
        let status = if success { "success" } else { "error" };
        METRICS
            .requests_total
            .with_label_values(&[ctx.method_name, status])
            .inc();
        timer.observe_duration();
    }
}

struct TcpStreamCompat(TcpStream);

#[rsmp::local_stream_compat]
impl AsyncStreamCompat for TcpStreamCompat {
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let tmp = vec![0u8; buf.len() - filled];
            let (n, returned) = self.0.read(tmp.slice(..)).await.result()?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected eof",
                ));
            }
            buf[filled..filled + n].copy_from_slice(&returned.into_inner()[..n]);
            filled += n;
        }
        Ok(())
    }

    async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.0.write_all(data.to_vec()).await.result()?;
        Ok(())
    }
}

pub struct CacheHandler {
    pub shard_id: usize,
    pub cache: Arc<Cache>,
}

#[rsmp::local_handler]
impl CacheServiceHandlerLocal<TcpStreamCompat> for CacheHandler {
    async fn get(
        &self,
        id: String,
        offset: u64,
        size: u64,
        stream: &mut TcpStreamCompat,
    ) -> Result<u64, CacheError> {
        let path = match self.cache.get(&id) {
            Some(p) => p,
            None => {
                debug!(shard_id = self.shard_id, id, "not found");
                return Err(CacheError::NotFound(NotFoundError { id }));
            }
        };

        debug!(shard_id = self.shard_id, id, ?path, "found");

        let file = File::open(&path).await?;
        stream
            .0
            .write_all(size.to_be_bytes().to_vec())
            .await
            .result()?;

        let mut current_offset = offset;
        let end_offset = offset + size;
        let mut buf = Vec::with_capacity(STREAM_FILE_BUF_SIZE);

        while current_offset < end_offset {
            let (n, read_buf) = file.read_at(buf, current_offset).await.result()?;
            if n == 0 {
                break;
            }
            buf = read_buf;
            let slice = buf.slice(..n);
            let ((), slice) = stream.0.write_all(slice).await.result()?;
            buf = slice.into_inner();
            buf.clear();
            current_offset += n as u64;
        }

        METRICS
            .bytes_read_total
            .with_label_values(&["get"])
            .inc_by(size);

        Ok(size)
    }

    async fn put(
        &self,
        id: String,
        stream: &mut TcpStreamCompat,
        size: u64,
    ) -> Result<(), CacheError> {
        let path = self.cache.generate_path(&id);

        debug!(shard_id = self.shard_id, id, ?path, "writing");

        let mut file = File::create(&path).await?;
        let mut file_offset = 0u64;
        let mut buf = Vec::with_capacity(STREAM_FILE_BUF_SIZE);

        while file_offset < size {
            let (n, read_buf) = stream.0.read(buf).await.result()?;
            if n == 0 {
                break;
            }
            buf = read_buf;
            let slice = buf.slice(..n);
            let ((), slice) = file.write_all_at(slice, file_offset).await.result()?;
            buf = slice.into_inner();
            buf.clear();
            file_offset += n as u64;
        }

        self.cache.insert(id, size);

        debug!(shard_id = self.shard_id, ?path, "written");

        METRICS
            .bytes_written_total
            .with_label_values(&["put"])
            .inc_by(size);

        Ok(())
    }
}

async fn handle_connection<H>(handler: &H, stream: TcpStream, mut shutdown_rx: Receiver<()>)
where
    H: CacheServiceHandlerLocal<TcpStreamCompat>,
{
    let mut compat = TcpStreamCompat(stream);
    CacheServiceDispatcher::new(handler, &mut compat)
        .interceptor(MetricsInterceptor)
        .local()
        .run_until(shutdown_rx.recv())
        .await;
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
    let handler = Rc::new(CacheHandler { shard_id, cache });
    let shard_label = shard_id.to_string();

    macro_rules! handle_accept {
        ($result:expr) => {
            match $result {
                Ok((stream, addr)) => {
                    debug!(shard_id, %addr, "Connection accepted");
                    let handler = handler.clone();
                    let conn_shutdown_rx = conn_shutdown_rx.clone();
                    connections.push(spawn(async move {
                        handle_connection(handler.as_ref(), stream, conn_shutdown_rx).await;
                        debug!(shard_id, %addr, "Connection closed");
                    }));
                    METRICS.active_connections.with_label_values(&[&shard_label]).set(connections.len() as i64);
                }
                Err(e) => error!(shard_id, "Accept error: {}", e),
            }
        };
    }

    loop {
        let accept_fut = listener.accept();
        let shutdown_fut = shutdown_rx.recv();

        if connections.is_empty() {
            futures_util::select_biased! {
                _ = shutdown_fut.fuse() => break,
                result = accept_fut.fuse() => handle_accept!(result),
            }
        } else {
            let mut next_conn = pin!(connections.next());
            futures_util::select_biased! {
                _ = shutdown_fut.fuse() => break,
                result = accept_fut.fuse() => handle_accept!(result),
                _ = next_conn => {
                    METRICS.active_connections.with_label_values(&[&shard_label]).set(connections.len() as i64);
                }
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

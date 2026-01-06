use std::io;
use std::ops::Range;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;

use async_broadcast::{Receiver, broadcast};
use compio::buf::{IntoInner, IoBuf};
use compio::fs::File;
use compio::io::{AsyncRead, AsyncReadAt, AsyncWrite, AsyncWriteAtExt, AsyncWriteExt};
use compio::net::TcpStream;
use compio::runtime::spawn;
use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use prometheus::HistogramTimer;
use rsmp::{AsyncStreamCompat, CallContext, Interceptor, ReqBody};
use tracing::{debug, error, info};

use crate::cache::{DiskCache, MemoryCache};
use crate::metrics::{LABEL_ERROR, LABEL_SUCCESS, METRICS};
use crate::proto::{
    CacheError, CacheServiceDispatcher, CacheServiceHandlerLocal, IoError, MemoryCacheRanges,
    NotFoundError,
};
use crate::{BufResultExt, MemorySemaphore, create_listener};

const STREAM_FILE_BUF_SIZE: usize = 256 * 1024;

fn capture_ranges_from_chunk(
    chunk: &[u8],
    chunk_start: u64,
    captures: &mut [(Range<u64>, Vec<u8>)],
) {
    let chunk_end = chunk_start + chunk.len() as u64;
    for (range, capture_buf) in captures {
        if chunk_end > range.start && chunk_start < range.end {
            let copy_start = range.start.max(chunk_start);
            let copy_end = range.end.min(chunk_end);
            let buf_start = (copy_start - chunk_start) as usize;
            let buf_end = (copy_end - chunk_start) as usize;
            capture_buf.extend_from_slice(&chunk[buf_start..buf_end]);
        }
    }
}

impl From<std::io::Error> for CacheError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(IoError {
            message: e.to_string(),
        })
    }
}

struct MetricsInterceptor;

impl Interceptor for MetricsInterceptor {
    type State = HistogramTimer;

    fn before(&self, _ctx: &CallContext) -> Self::State {
        METRICS.request_duration.start_timer()
    }

    fn after(&self, ctx: &CallContext, timer: Self::State, success: bool) {
        let status = if success { LABEL_SUCCESS } else { LABEL_ERROR };
        METRICS
            .requests_total
            .with_label_values(&[ctx.method_name, status])
            .inc();
        timer.observe_duration();
    }
}

struct TcpStreamCompat(TcpStream);

#[rsmp::stream_compat_local]
impl AsyncStreamCompat for TcpStreamCompat {
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let len = buf.len();
        if len == 0 {
            return Ok(());
        }
        let mut tmp = Vec::with_capacity(len);
        // SAFETY: self.0.read writes into buffer before we read from it
        #[allow(clippy::uninit_vec)]
        unsafe {
            tmp.set_len(len);
        }
        let mut filled = 0;
        while filled < len {
            let (n, returned) = self.0.read(tmp.slice(filled..)).await.result()?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected eof",
                ));
            }
            tmp = returned.into_inner();
            filled += n;
        }
        buf.copy_from_slice(&tmp[..len]);
        Ok(())
    }

    async fn write_all(&mut self, data: Vec<u8>) -> io::Result<()> {
        self.0.write_all(data).await.result()?;
        Ok(())
    }

    async fn close(&mut self) -> io::Result<()> {
        AsyncWrite::shutdown(&mut &self.0).await
    }
}

pub struct CacheHandler {
    pub shard_id: usize,
    pub disk_cache: Arc<DiskCache>,
    pub memory_cache: Arc<MemoryCache>,
    pub memory_semaphore: Arc<MemorySemaphore>,
}

impl CacheHandler {
    async fn stream_buffer(data: Arc<[u8]>, stream: &mut TcpStreamCompat) -> io::Result<()> {
        let len = data.len() as u64;
        stream.0.write_all(len.to_be_bytes()).await.result()?;
        stream.0.write_all(data).await.result()?;
        Ok(())
    }

    async fn stream_from_file(
        file: &File,
        stream: &mut TcpStreamCompat,
        offset: u64,
        size: u64,
    ) -> io::Result<u64> {
        let file_size = file.metadata().await?.len();
        let actual_size = size.min(file_size.saturating_sub(offset));

        stream
            .0
            .write_all(actual_size.to_be_bytes())
            .await
            .result()?;

        let mut current_offset = offset;
        let end_offset = offset + actual_size;
        let mut buf = Vec::with_capacity(STREAM_FILE_BUF_SIZE);

        while current_offset < end_offset {
            let to_read = ((end_offset - current_offset) as usize).min(STREAM_FILE_BUF_SIZE);
            buf.reserve(to_read);
            // SAFETY: file.read_at writes into buffer before we read from it
            #[allow(clippy::uninit_vec)]
            unsafe {
                buf.set_len(to_read);
            }
            let (n, returned) = file.read_at(buf, current_offset).await.result()?;
            if n == 0 {
                break;
            }
            buf = returned;
            let to_write = n.min((end_offset - current_offset) as usize);
            let ((), returned) = stream.0.write_all(buf.slice(..to_write)).await.result()?;
            buf = returned.into_inner();
            current_offset += to_write as u64;
        }

        Ok(current_offset - offset)
    }

    async fn stream_to_file(
        stream: &mut TcpStreamCompat,
        file: &mut File,
        size: u64,
        cache_ranges: Vec<Range<u64>>,
    ) -> io::Result<Vec<(Range<u64>, Vec<u8>)>> {
        let mut buf = Vec::with_capacity(STREAM_FILE_BUF_SIZE);
        let mut captures: Vec<(Range<u64>, Vec<u8>)> = cache_ranges
            .into_iter()
            .map(|r| {
                let cap = (r.end - r.start).min(size) as usize;
                (r, Vec::with_capacity(cap))
            })
            .collect();

        let mut file_offset = 0u64;
        while file_offset < size {
            let to_read = ((size - file_offset) as usize).min(STREAM_FILE_BUF_SIZE);
            buf.reserve(to_read);
            // SAFETY: stream.0.read writes into buffer before we read from it
            #[allow(clippy::uninit_vec)]
            unsafe {
                buf.set_len(to_read);
            }
            let (n, read_buf) = stream.0.read(buf).await.result()?;
            if n == 0 {
                break;
            }
            buf = read_buf;
            if !captures.is_empty() {
                capture_ranges_from_chunk(&buf[..n], file_offset, &mut captures);
            }
            let ((), slice) = file
                .write_all_at(buf.slice(..n), file_offset)
                .await
                .result()?;
            buf = slice.into_inner();
            file_offset += n as u64;
        }

        Ok(captures)
    }

    async fn put_inner(
        &self,
        id: &str,
        memory_cache_ranges: Option<MemoryCacheRanges>,
        stream: &mut TcpStreamCompat,
        size: u64,
    ) -> Result<(), CacheError> {
        let ranges = memory_cache_ranges
            .map(|r| r.into_inner())
            .unwrap_or_default();

        let total_capture_size: u64 = ranges.iter().map(|r| (r.end - r.start).min(size)).sum();
        let permit_size = STREAM_FILE_BUF_SIZE as u64 + total_capture_size;
        let _permit = self.memory_semaphore.acquire(permit_size).await;

        let path = self.disk_cache.generate_path(id);

        debug!(shard_id = self.shard_id, id, ?path, "writing");

        let mut file = File::create(&path).await?;

        let captures = Self::stream_to_file(stream, &mut file, size, ranges).await?;

        for (range, data) in captures {
            if data.len() as u64 == range.end - range.start {
                debug!(
                    shard_id = self.shard_id,
                    id,
                    start = range.start,
                    end = range.end,
                    "caching chunk in memory"
                );
                self.memory_cache
                    .insert(id.to_string(), range.start, range.end, data);
            }
        }

        self.disk_cache.insert(id.to_string(), size);

        debug!(shard_id = self.shard_id, ?path, "written");

        METRICS
            .bytes_written_total
            .with_label_values(&[Self::PUT_NAME])
            .inc_by(size);

        Ok(())
    }
}

#[rsmp::handler_local]
impl CacheServiceHandlerLocal<TcpStreamCompat> for CacheHandler {
    async fn get(
        &self,
        id: &str,
        offset: u64,
        size: u64,
        stream: &mut TcpStreamCompat,
    ) -> Result<(), CacheError> {
        let end = offset + size;
        if let Some(data) = self.memory_cache.get(id, offset, end) {
            debug!(
                shard_id = self.shard_id,
                id, offset, size, "memory cache hit"
            );
            let bytes_sent = data.len() as u64;
            Self::stream_buffer(data, stream).await?;
            METRICS
                .bytes_read_total
                .with_label_values(&[Self::GET_NAME])
                .inc_by(bytes_sent);
            return Ok(());
        }

        let path = match self.disk_cache.get(id) {
            Some(p) => p,
            None => {
                debug!(shard_id = self.shard_id, id, "not found");
                return Err(CacheError::NotFound(NotFoundError { id: id.to_string() }));
            }
        };

        debug!(shard_id = self.shard_id, id, ?path, "disk cache hit");

        let _permit = self
            .memory_semaphore
            .acquire(STREAM_FILE_BUF_SIZE as u64)
            .await;

        let file = File::open(&path).await?;
        let bytes_sent = Self::stream_from_file(&file, stream, offset, size).await?;

        METRICS
            .bytes_read_total
            .with_label_values(&[Self::GET_NAME])
            .inc_by(bytes_sent);

        Ok(())
    }

    async fn put(
        &self,
        id: &str,
        memory_cache_ranges: Option<MemoryCacheRanges>,
        body: &mut ReqBody<'_, TcpStreamCompat>,
        size: u64,
    ) -> Result<(), CacheError> {
        if self.disk_cache.contains(id) || !self.disk_cache.start_writing(id) {
            debug!(
                shard_id = self.shard_id,
                id, "already cached or being written"
            );
            return Err(CacheError::AlreadyExists(id.to_string()));
        }

        let stream = body.start().await?;
        let result = self.put_inner(id, memory_cache_ranges, stream, size).await;
        self.disk_cache.finish_writing(id);
        result
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
    disk_cache: Arc<DiskCache>,
    memory_cache: Arc<MemoryCache>,
    memory_semaphore: Arc<MemorySemaphore>,
    mut shutdown_rx: Receiver<()>,
) -> eyre::Result<()> {
    let listener = create_listener(&config.listen, config.num_listeners as i32)?;
    info!(shard_id, "Shard listening on {}", config.listen);

    let (conn_shutdown_tx, conn_shutdown_rx) = broadcast::<()>(1);
    let mut connections: FuturesUnordered<compio::runtime::JoinHandle<()>> =
        FuturesUnordered::new();
    let handler = Rc::new(CacheHandler {
        shard_id,
        disk_cache,
        memory_cache,
        memory_semaphore,
    });
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

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;

    fn make_captures(ranges: &[Range<u64>]) -> Vec<(Range<u64>, Vec<u8>)> {
        ranges
            .iter()
            .map(|r| (r.clone(), Vec::with_capacity((r.end - r.start) as usize)))
            .collect()
    }

    fn simulate_stream_file(file_data: &[u8], offset: u64, size: u64) -> Vec<u8> {
        let mut output = Vec::new();
        let mut current_offset = offset;
        let end_offset = offset + size;

        while current_offset < end_offset {
            let to_read = ((end_offset - current_offset) as usize).min(STREAM_FILE_BUF_SIZE);
            let file_start = current_offset as usize;
            let file_end = (file_start + to_read).min(file_data.len());

            if file_start >= file_data.len() {
                break;
            }

            let bytes_from_file = file_end - file_start;
            let to_write = bytes_from_file.min((end_offset - current_offset) as usize);
            output.extend_from_slice(&file_data[file_start..file_start + to_write]);
            current_offset += to_write as u64;
        }

        output
    }

    #[test]
    fn stream_file_returns_exact_requested_range() {
        let file_data: Vec<u8> = (0..300_000).map(|i| (i % 256) as u8).collect();

        let data = simulate_stream_file(&file_data, 0, 100);
        assert_eq!(data, &file_data[0..100]);

        let data = simulate_stream_file(&file_data, 500, 200);
        assert_eq!(data, &file_data[500..700]);

        let data = simulate_stream_file(&file_data, 50_000, 100);
        assert_eq!(data.len(), 100, "must return exactly requested size");
        assert_eq!(data, &file_data[50_000..50_100]);

        let offset = STREAM_FILE_BUF_SIZE as u64 - 50;
        let data = simulate_stream_file(&file_data, offset, 100);
        assert_eq!(data, &file_data[offset as usize..(offset + 100) as usize]);
    }

    #[test]
    fn stream_file_truncates_at_eof() {
        let file_data: Vec<u8> = (0..100).collect();
        let data = simulate_stream_file(&file_data, 50, 1000);
        assert_eq!(data.len(), 50);
        assert_eq!(data, &file_data[50..100]);
    }

    #[test]
    fn capture_ranges_single_chunk() {
        let chunk: Vec<u8> = (0u8..=255).collect();
        let mut captures = make_captures(&[10..20, 100..150]);

        capture_ranges_from_chunk(&chunk, 0, &mut captures);

        assert_eq!(captures[0].1, (10..20).collect::<Vec<u8>>());
        assert_eq!(captures[1].1, (100..150).collect::<Vec<u8>>());
    }

    #[test]
    fn capture_ranges_across_chunks() {
        let mut captures = make_captures(&[50..150, 200..250]);

        let chunk1: Vec<u8> = (0..100).collect();
        capture_ranges_from_chunk(&chunk1, 0, &mut captures);

        let chunk2: Vec<u8> = (100..300).map(|x| x as u8).collect();
        capture_ranges_from_chunk(&chunk2, 100, &mut captures);

        assert_eq!(
            captures[0].1,
            (50..150).map(|x| x as u8).collect::<Vec<u8>>()
        );
        assert_eq!(
            captures[1].1,
            (200..250).map(|x| x as u8).collect::<Vec<u8>>()
        );
    }

    #[test]
    fn capture_ranges_no_overlap_produces_empty() {
        let chunk: Vec<u8> = (0..100).collect();
        let mut captures = make_captures(&[200..300]);

        capture_ranges_from_chunk(&chunk, 0, &mut captures);

        assert!(captures[0].1.is_empty());
    }

    #[test]
    fn read_exact_with_reused_buffer_respects_remaining() {
        let source: Vec<u8> = (0..200).collect();
        let mut buf = vec![0u8; 100];
        let mut filled = 0;
        let mut tmp = Vec::with_capacity(buf.len());
        let mut source_offset = 0;

        while filled < buf.len() {
            let remaining = buf.len() - filled;
            tmp.reserve(remaining);
            tmp.resize(remaining.max(tmp.len()), 0);

            let read_limit = remaining;
            let n = read_limit.min(source.len() - source_offset);
            tmp[..n].copy_from_slice(&source[source_offset..source_offset + n]);
            source_offset += n;

            assert!(filled + n <= buf.len());
            buf[filled..filled + n].copy_from_slice(&tmp[..n]);
            filled += n;
        }

        assert_eq!(buf, &source[..100]);
    }
}

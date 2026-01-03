use std::io;
use std::ops::Range;
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

use crate::cache::{DiskCache, MemoryCache};
use crate::metrics::METRICS;
use crate::proto::{
    CacheError, CacheServiceDispatcher, CacheServiceHandlerLocal, MemoryCacheRanges, NotFoundError,
};
use crate::{BufResultExt, create_listener};

const STREAM_FILE_BUF_SIZE: usize = 64 * 1024;

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
    pub disk_cache: Arc<DiskCache>,
    pub memory_cache: Arc<MemoryCache>,
}

impl CacheHandler {
    async fn stream_file(
        &self,
        file: &File,
        stream: &mut TcpStreamCompat,
        offset: u64,
        size: u64,
    ) -> io::Result<()> {
        stream
            .0
            .write_all(size.to_be_bytes().to_vec())
            .await
            .result()?;

        let mut current_offset = offset;
        let end_offset = offset + size;

        while current_offset < end_offset {
            let to_read = ((end_offset - current_offset) as usize).min(STREAM_FILE_BUF_SIZE);
            let buf = vec![0u8; to_read];
            let (n, buf) = file.read_at(buf, current_offset).await.result()?;
            if n == 0 {
                break;
            }
            let to_write = n.min((end_offset - current_offset) as usize);
            stream
                .0
                .write_all(buf[..to_write].to_vec())
                .await
                .result()?;
            current_offset += to_write as u64;
        }

        Ok(())
    }

    async fn stream_buffer(&self, data: &[u8], stream: &mut TcpStreamCompat) -> io::Result<()> {
        let size = data.len() as u64;
        stream
            .0
            .write_all(size.to_be_bytes().to_vec())
            .await
            .result()?;
        stream.0.write_all(data.to_vec()).await.result()?;
        Ok(())
    }

    async fn stream_to_file(
        &self,
        stream: &mut TcpStreamCompat,
        file: &mut File,
        size: u64,
        cache_ranges: Vec<Range<u64>>,
    ) -> io::Result<Vec<(Range<u64>, Vec<u8>)>> {
        let mut captures: Vec<(Range<u64>, Vec<u8>)> = cache_ranges
            .into_iter()
            .map(|r| {
                let chunk = Vec::with_capacity((r.end - r.start) as usize);
                (r, chunk)
            })
            .collect();

        let mut file_offset = 0u64;
        let mut buf = Vec::with_capacity(STREAM_FILE_BUF_SIZE);

        while file_offset < size {
            let to_read = ((size - file_offset) as usize).min(STREAM_FILE_BUF_SIZE);
            buf.resize(to_read, 0);
            let (n, read_buf) = stream.0.read(buf).await.result()?;
            if n == 0 {
                break;
            }
            buf = read_buf;

            if !captures.is_empty() {
                capture_ranges_from_chunk(&buf[..n], file_offset, &mut captures);
            }

            let slice = buf.slice(..n);
            let ((), slice) = file.write_all_at(slice, file_offset).await.result()?;
            buf = slice.into_inner();
            buf.clear();
            file_offset += n as u64;
        }

        Ok(captures)
    }
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
        let end = offset + size;
        if let Some(data) = self.memory_cache.get(&id, offset, end) {
            debug!(
                shard_id = self.shard_id,
                id, offset, size, "memory cache hit"
            );
            self.stream_buffer(&data, stream).await?;
            METRICS
                .bytes_read_total
                .with_label_values(&["get"])
                .inc_by(size);
            return Ok(size);
        }

        let path = match self.disk_cache.get(&id) {
            Some(p) => p,
            None => {
                debug!(shard_id = self.shard_id, id, "not found");
                return Err(CacheError::NotFound(NotFoundError { id }));
            }
        };

        debug!(shard_id = self.shard_id, id, ?path, "disk cache hit");

        let file = File::open(&path).await?;
        self.stream_file(&file, stream, offset, size).await?;

        METRICS
            .bytes_read_total
            .with_label_values(&["get"])
            .inc_by(size);

        Ok(size)
    }

    async fn put(
        &self,
        id: String,
        memory_cache_ranges: Option<MemoryCacheRanges>,
        stream: &mut TcpStreamCompat,
        size: u64,
    ) -> Result<(), CacheError> {
        let path = self.disk_cache.generate_path(&id);

        debug!(shard_id = self.shard_id, id, ?path, "writing");

        let mut file = File::create(&path).await?;
        let ranges = memory_cache_ranges
            .map(|r| r.into_inner())
            .unwrap_or_default();

        let captures = self.stream_to_file(stream, &mut file, size, ranges).await?;

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
                    .insert(id.clone(), range.start, range.end, data);
            }
        }

        self.disk_cache.insert(id, size);

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
    disk_cache: Arc<DiskCache>,
    memory_cache: Arc<MemoryCache>,
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

    /// Simulates stream_file logic: given file data, offset, and size,
    /// returns (size_header, data) that would be written to the stream.
    fn simulate_stream_file(file_data: &[u8], offset: u64, size: u64) -> (u64, Vec<u8>) {
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

        (size, output)
    }

    #[test]
    fn stream_file_exact_range_from_start() {
        let file_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (header_size, data) = simulate_stream_file(&file_data, 0, 100);

        assert_eq!(header_size, 100);
        assert_eq!(data.len(), 100);
        assert_eq!(data, &file_data[0..100]);
    }

    #[test]
    fn stream_file_exact_range_from_middle() {
        let file_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (header_size, data) = simulate_stream_file(&file_data, 500, 200);

        assert_eq!(header_size, 200);
        assert_eq!(data.len(), 200);
        assert_eq!(data, &file_data[500..700]);
    }

    #[test]
    fn stream_file_small_range_from_large_file() {
        let file_data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let (header_size, data) = simulate_stream_file(&file_data, 50_000, 100);

        assert_eq!(header_size, 100);
        assert_eq!(
            data.len(),
            100,
            "must write exactly requested size, not more"
        );
        assert_eq!(data, &file_data[50_000..50_100]);
    }

    #[test]
    fn stream_file_range_spanning_buffer_boundary() {
        let file_data: Vec<u8> = (0..200_000).map(|i| (i % 256) as u8).collect();
        let offset = STREAM_FILE_BUF_SIZE as u64 - 50;
        let size = 100u64;
        let (header_size, data) = simulate_stream_file(&file_data, offset, size);

        assert_eq!(header_size, size);
        assert_eq!(data.len(), size as usize);
        assert_eq!(data, &file_data[offset as usize..(offset + size) as usize]);
    }

    #[test]
    fn stream_file_entire_file() {
        let file_data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let (header_size, data) = simulate_stream_file(&file_data, 0, 10_000);

        assert_eq!(header_size, 10_000);
        assert_eq!(data.len(), 10_000);
        assert_eq!(data, file_data);
    }

    #[test]
    fn stream_file_last_byte() {
        let file_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (header_size, data) = simulate_stream_file(&file_data, 999, 1);

        assert_eq!(header_size, 1);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0], file_data[999]);
    }

    #[test]
    fn stream_file_request_beyond_eof_returns_available() {
        let file_data: Vec<u8> = (0..100).collect();
        let (header_size, data) = simulate_stream_file(&file_data, 50, 1000);

        assert_eq!(header_size, 1000, "header reflects requested size");
        assert_eq!(data.len(), 50, "data is truncated to available bytes");
        assert_eq!(data, &file_data[50..100]);
    }

    #[test]
    fn stream_file_sequential_ranges_no_overlap() {
        let file_data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();

        let (_, data1) = simulate_stream_file(&file_data, 0, 256);
        let (_, data2) = simulate_stream_file(&file_data, 5000, 1000);

        assert_eq!(data1.len(), 256);
        assert_eq!(data2.len(), 1000);
        assert_eq!(data1, &file_data[0..256]);
        assert_eq!(data2, &file_data[5000..6000]);

        // Verify no data overlap (the bug that was fixed)
        let last_byte_range1 = data1.last().copied();
        let first_byte_range2 = data2.first().copied();
        assert_ne!(
            last_byte_range1.map(|b| b.wrapping_add(1)),
            first_byte_range2,
            "ranges should not be adjacent in output"
        );
    }

    #[test]
    fn capture_range_entirely_within_chunk() {
        let chunk: Vec<u8> = (0..100).collect();
        let mut captures = make_captures(&[10..20]);

        capture_ranges_from_chunk(&chunk, 0, &mut captures);

        assert_eq!(captures[0].1, (10..20).collect::<Vec<u8>>());
    }

    #[test]
    fn capture_range_spans_multiple_chunks() {
        let mut captures = make_captures(&[50..150]);

        let chunk1: Vec<u8> = (0..100).collect();
        capture_ranges_from_chunk(&chunk1, 0, &mut captures);

        let chunk2: Vec<u8> = (100..200).map(|x| x as u8).collect();
        capture_ranges_from_chunk(&chunk2, 100, &mut captures);

        let expected: Vec<u8> = (50..150).map(|x| x as u8).collect();
        assert_eq!(captures[0].1, expected);
    }

    #[test]
    fn capture_range_starts_mid_chunk() {
        let chunk: Vec<u8> = (0..100).collect();
        let mut captures = make_captures(&[50..150]);

        capture_ranges_from_chunk(&chunk, 0, &mut captures);

        assert_eq!(captures[0].1, (50..100).collect::<Vec<u8>>());
    }

    #[test]
    fn capture_range_ends_mid_chunk() {
        let chunk: Vec<u8> = (100..200).map(|x| x as u8).collect();
        let mut captures = make_captures(&[50..150]);

        capture_ranges_from_chunk(&chunk, 100, &mut captures);

        let expected: Vec<u8> = (100..150).map(|x| x as u8).collect();
        assert_eq!(captures[0].1, expected);
    }

    #[test]
    fn capture_chunk_fully_contains_range() {
        let chunk: Vec<u8> = (0u8..=255).collect();
        let mut captures = make_captures(&[100..150]);

        capture_ranges_from_chunk(&chunk, 0, &mut captures);

        assert_eq!(captures[0].1, (100..150).collect::<Vec<u8>>());
    }

    #[test]
    fn capture_range_fully_contains_chunk() {
        let chunk: Vec<u8> = (50..100).collect();
        let mut captures = make_captures(&[0..200]);

        capture_ranges_from_chunk(&chunk, 50, &mut captures);

        assert_eq!(captures[0].1, (50..100).collect::<Vec<u8>>());
    }

    #[test]
    fn capture_no_overlap() {
        let chunk: Vec<u8> = (0..100).collect();
        let mut captures = make_captures(&[200..300]);

        capture_ranges_from_chunk(&chunk, 0, &mut captures);

        assert!(captures[0].1.is_empty());
    }

    #[test]
    fn capture_multiple_ranges() {
        let chunk: Vec<u8> = (0..100).collect();
        let mut captures = make_captures(&[0..10, 50..60, 90..100]);

        capture_ranges_from_chunk(&chunk, 0, &mut captures);

        assert_eq!(captures[0].1, (0..10).collect::<Vec<u8>>());
        assert_eq!(captures[1].1, (50..60).collect::<Vec<u8>>());
        assert_eq!(captures[2].1, (90..100).collect::<Vec<u8>>());
    }

    #[test]
    fn capture_accumulates_across_chunks() {
        let mut captures = make_captures(&[0..100, 150..250]);

        for chunk_start in (0..300).step_by(50) {
            let chunk: Vec<u8> = (chunk_start..chunk_start + 50).map(|x| x as u8).collect();
            capture_ranges_from_chunk(&chunk, chunk_start as u64, &mut captures);
        }

        let expected0: Vec<u8> = (0..100).map(|x| x as u8).collect();
        let expected1: Vec<u8> = (150..250).map(|x| x as u8).collect();
        assert_eq!(captures[0].1, expected0);
        assert_eq!(captures[1].1, expected1);
    }
}

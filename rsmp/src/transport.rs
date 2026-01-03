use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};

use crate::{BoxAsyncRead, ERROR_MARKER, Transport, TransportError};

pub enum Response {
    Ok(Vec<u8>),
    Err(Vec<u8>),
}

pub struct StreamTransport<S> {
    stream: S,
}

impl<S> StreamTransport<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> StreamTransport<S> {
    async fn write_frame(&mut self, method_id: u16, args_data: &[u8]) -> io::Result<()> {
        let total_len = 2 + args_data.len();
        let len_bytes = (total_len as u32).to_be_bytes();

        write_all(&mut self.stream, &len_bytes).await?;
        write_all(&mut self.stream, &method_id.to_be_bytes()).await?;
        write_all(&mut self.stream, args_data).await?;

        Ok(())
    }

    async fn read_response(&mut self) -> Result<Response, TransportError> {
        let mut len_buf = [0u8; 4];
        read_exact(&mut self.stream, &mut len_buf).await?;
        let len_raw = u32::from_be_bytes(len_buf);

        let is_error = (len_raw & 0x8000_0000) != 0;
        let len = (len_raw & 0x7FFF_FFFF) as usize;

        let mut data = vec![0u8; len];
        read_exact(&mut self.stream, &mut data).await?;

        if is_error {
            return Ok(Response::Err(data));
        }

        Ok(Response::Ok(data))
    }

    async fn read_error(&mut self) -> Result<Vec<u8>, TransportError> {
        let mut len_buf = [0u8; 4];
        read_exact(&mut self.stream, &mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut data = vec![0u8; len];
        read_exact(&mut self.stream, &mut data).await?;

        Ok(data)
    }

    async fn write_stream(
        &mut self,
        body: &mut (dyn AsyncRead + Unpin),
        size: u64,
    ) -> io::Result<()> {
        let mut remaining = size;
        let mut buf = [0u8; 8192];

        while remaining > 0 {
            let to_read = (remaining as usize).min(buf.len());
            let n = read_some(body, &mut buf[..to_read]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "body stream ended early",
                ));
            }
            write_all(&mut self.stream, &buf[..n]).await?;
            remaining -= n as u64;
        }

        Ok(())
    }
}

#[crate::async_trait(?Send)]
impl<S: AsyncRead + AsyncWrite + Unpin> Transport for StreamTransport<S> {
    async fn call_raw(
        &mut self,
        method_id: u16,
        args_data: &[u8],
    ) -> Result<Response, TransportError> {
        self.write_frame(method_id, args_data).await?;
        self.read_response().await
    }

    async fn call_with_body_raw(
        &mut self,
        method_id: u16,
        args_data: &[u8],
        body: &mut (dyn AsyncRead + Unpin),
        body_size: u64,
    ) -> Result<Response, TransportError> {
        self.write_frame(method_id, args_data).await?;
        write_all(&mut self.stream, &body_size.to_be_bytes()).await?;
        self.write_stream(body, body_size).await?;
        self.read_response().await
    }

    async fn call_with_response_stream_raw<'a>(
        &'a mut self,
        method_id: u16,
        args_data: &[u8],
    ) -> Result<Result<BoxAsyncRead<'a>, Vec<u8>>, TransportError> {
        self.write_frame(method_id, args_data).await?;

        let mut size_buf = [0u8; 8];
        read_exact(&mut self.stream, &mut size_buf).await?;
        let response_size = u64::from_be_bytes(size_buf);

        if response_size == ERROR_MARKER {
            return Ok(Err(self.read_error().await?));
        }

        let stream_reader: BoxAsyncRead<'a> =
            Box::pin(TakeReader::new(&mut self.stream, response_size));

        Ok(Ok(stream_reader))
    }

    async fn call_with_body_and_response_stream_raw<'a>(
        &'a mut self,
        method_id: u16,
        args_data: &[u8],
        body: &mut (dyn AsyncRead + Unpin),
        body_size: u64,
    ) -> Result<Result<(Vec<u8>, BoxAsyncRead<'a>), Vec<u8>>, TransportError> {
        self.write_frame(method_id, args_data).await?;
        write_all(&mut self.stream, &body_size.to_be_bytes()).await?;
        self.write_stream(body, body_size).await?;
        let response = match self.read_response().await? {
            Response::Ok(data) => data,
            Response::Err(err) => return Ok(Err(err)),
        };

        let mut size_buf = [0u8; 8];
        read_exact(&mut self.stream, &mut size_buf).await?;
        let response_size = u64::from_be_bytes(size_buf);

        if response_size == ERROR_MARKER {
            return Ok(Err(self.read_error().await?));
        }

        let stream_reader: BoxAsyncRead<'a> =
            Box::pin(TakeReader::new(&mut self.stream, response_size));
        Ok(Ok((response, stream_reader)))
    }
}

struct TakeReader<'a, S> {
    stream: &'a mut S,
    remaining: u64,
}

impl<'a, S> TakeReader<'a, S> {
    fn new(stream: &'a mut S, limit: u64) -> Self {
        Self {
            stream,
            remaining: limit,
        }
    }
}

impl<'a, S: AsyncRead + Unpin> AsyncRead for TakeReader<'a, S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(0));
        }
        let max = (self.remaining as usize).min(buf.len());
        match Pin::new(&mut *self.stream).poll_read(cx, &mut buf[..max]) {
            Poll::Ready(Ok(n)) => {
                self.remaining -= n as u64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }
}

async fn write_all<W: AsyncWrite + Unpin>(writer: &mut W, mut buf: &[u8]) -> io::Result<()> {
    use std::future::poll_fn;

    while !buf.is_empty() {
        let n = poll_fn(|cx| Pin::new(&mut *writer).poll_write(cx, buf)).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write data",
            ));
        }
        buf = &buf[n..];
    }
    poll_fn(|cx| Pin::new(&mut *writer).poll_flush(cx)).await?;
    Ok(())
}

async fn read_exact<R: AsyncRead + Unpin>(reader: &mut R, mut buf: &mut [u8]) -> io::Result<()> {
    use std::future::poll_fn;

    while !buf.is_empty() {
        let n = poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, buf)).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            ));
        }
        buf = &mut buf[n..];
    }
    Ok(())
}

async fn read_some<R: AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    buf: &mut [u8],
) -> io::Result<usize> {
    use std::future::poll_fn;
    poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, buf)).await
}

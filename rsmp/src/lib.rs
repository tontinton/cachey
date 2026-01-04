extern crate self as rsmp;

use std::io;
use std::pin::Pin;

use strum::FromRepr;
use thiserror::Error;

pub use async_trait::async_trait;
pub use futures_io::{AsyncRead, AsyncWrite};
pub use futures_util;
pub use rsmp_derive::{Args, handler, local_handler, local_stream_compat, service, stream_compat};

pub mod transport;

pub use transport::{StreamTransport, StreamTransportLocal};

pub type FieldIndex = u16;
const FIELD_INDEX_SIZE: usize = std::mem::size_of::<FieldIndex>();

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unexpected end of data")]
    UnexpectedEof,
    #[error("invalid wire type: {0}")]
    InvalidWireType(u8),
    #[error("invalid UTF-8")]
    InvalidUtf8,
    #[error("missing required field: {0}")]
    MissingField(FieldIndex),
    #[error("unknown variant: {0}")]
    UnknownVariant(FieldIndex),
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, FromRepr)]
pub enum WireType {
    None = 0,
    I8 = 1,
    U8 = 2,
    I16 = 3,
    U16 = 4,
    I32 = 5,
    U32 = 6,
    I64 = 7,
    U64 = 8,
    F32 = 9,
    F64 = 10,
    Bool = 11,
    String = 12,
    Bytes = 13,
    Array = 14,
}

pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
    fn wire_type(&self) -> WireType;
}

pub trait Decode<'a>: Sized {
    fn decode(wire_type: WireType, data: &'a [u8]) -> Result<Self, ProtocolError>;
}

pub trait Args: Sized {
    fn encode_args(&self) -> Vec<u8>;
    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError>;
}

impl Args for std::convert::Infallible {
    fn encode_args(&self) -> Vec<u8> {
        match *self {}
    }

    fn decode_args(_data: &[u8]) -> Result<Self, ProtocolError> {
        Err(ProtocolError::UnknownVariant(0))
    }
}

pub trait StreamLike {
    type Body: AsyncRead + Unpin + ?Sized;
    fn into_parts(self) -> (Box<Self::Body>, u64);
}

pub struct Stream {
    pub reader: Box<dyn AsyncRead + Unpin + Send>,
    pub size: u64,
}

impl Stream {
    pub fn new<R: AsyncRead + Unpin + Send + 'static>(reader: R, size: u64) -> Self {
        Self {
            reader: Box::new(reader),
            size,
        }
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        let size = data.len() as u64;
        Self {
            reader: Box::new(futures_util::io::Cursor::new(data)),
            size,
        }
    }

    pub fn empty() -> Self {
        Self {
            reader: Box::new(futures_util::io::empty()),
            size: 0,
        }
    }
}

impl StreamLike for Stream {
    type Body = dyn AsyncRead + Unpin + Send;
    fn into_parts(self) -> (Box<Self::Body>, u64) {
        (self.reader, self.size)
    }
}

pub struct StreamLocal {
    pub reader: Box<dyn AsyncRead + Unpin>,
    pub size: u64,
}

impl StreamLocal {
    pub fn new<R: AsyncRead + Unpin + 'static>(reader: R, size: u64) -> Self {
        Self {
            reader: Box::new(reader),
            size,
        }
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        let size = data.len() as u64;
        Self {
            reader: Box::new(futures_util::io::Cursor::new(data)),
            size,
        }
    }

    pub fn empty() -> Self {
        Self {
            reader: Box::new(futures_util::io::empty()),
            size: 0,
        }
    }
}

impl StreamLike for StreamLocal {
    type Body = dyn AsyncRead + Unpin;
    fn into_parts(self) -> (Box<Self::Body>, u64) {
        (self.reader, self.size)
    }
}

pub struct RequestFrame {
    pub method_id: u16,
    pub args_data: Vec<u8>,
    pub body_size: u64,
}

impl RequestFrame {
    pub async fn read<C: AsyncStreamCompat>(
        conn: &mut C,
        has_request_stream: impl Fn(u16) -> bool,
    ) -> io::Result<Self> {
        let mut header = [0u8; 6];
        conn.read_exact(&mut header[..4]).await?;
        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if len < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame too short",
            ));
        }

        conn.read_exact(&mut header[4..6]).await?;
        let method_id = u16::from_be_bytes([header[4], header[5]]);

        let args_len = len - 2;
        let args_data = if args_len > 0 {
            let mut buf = vec![0u8; args_len];
            conn.read_exact(&mut buf).await?;
            buf
        } else {
            Vec::new()
        };

        let body_size = if has_request_stream(method_id) {
            let mut size_buf = [0u8; 8];
            conn.read_exact(&mut size_buf).await?;
            u64::from_be_bytes(size_buf)
        } else {
            0
        };

        Ok(Self {
            method_id,
            args_data,
            body_size,
        })
    }
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("method not found: {0}")]
    MethodNotFound(u16),
    #[error("decode error: {0}")]
    Decode(#[from] ProtocolError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

pub const ERROR_MARKER: u64 = u64::MAX;

pub struct CallContext<'a> {
    pub method_id: FieldIndex,
    pub method_name: &'a str,
}

pub trait Interceptor {
    type State;

    fn before(&self, ctx: &CallContext) -> Self::State;
    fn after(&self, ctx: &CallContext, state: Self::State, success: bool);
}

impl Interceptor for () {
    type State = ();
    fn before(&self, _ctx: &CallContext) {}
    fn after(&self, _ctx: &CallContext, _state: (), _success: bool) {}
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("decode error: {0}")]
    Decode(#[from] ProtocolError),
}

#[derive(Debug, Error)]
pub enum ClientError<E: std::fmt::Debug> {
    #[error("transport error: {0}")]
    Transport(TransportError),
    #[error("server error: {0:?}")]
    Server(E),
}

impl<E: std::fmt::Debug> From<TransportError> for ClientError<E> {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}

impl<E: std::fmt::Debug> From<ProtocolError> for ClientError<E> {
    fn from(e: ProtocolError) -> Self {
        Self::Transport(TransportError::Decode(e))
    }
}

impl<E: std::fmt::Debug> From<io::Error> for ClientError<E> {
    fn from(e: io::Error) -> Self {
        Self::Transport(TransportError::Io(e))
    }
}

pub type BoxAsyncRead<'a> = Pin<Box<dyn AsyncRead + Send + 'a>>;
pub type BoxAsyncReadLocal<'a> = Pin<Box<dyn AsyncRead + 'a>>;

#[async_trait(?Send)]
pub trait AsyncStreamCompat {
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()>;
    async fn write_all(&mut self, data: &[u8]) -> io::Result<()>;
}

pub use transport::Response;

pub type StreamResult<T> = Result<Result<T, Vec<u8>>, TransportError>;

pub trait Transport {
    type BoxRead<'a>: AsyncRead + Unpin
    where
        Self: 'a;

    fn call_raw(
        &mut self,
        method_id: u16,
        args_data: &[u8],
    ) -> impl Future<Output = Result<Response, TransportError>>;

    fn call_with_body_raw<B: AsyncRead + Unpin + ?Sized>(
        &mut self,
        method_id: u16,
        args_data: &[u8],
        body: &mut B,
        body_size: u64,
    ) -> impl Future<Output = Result<Response, TransportError>>;

    fn call_with_response_stream_raw(
        &mut self,
        method_id: u16,
        args_data: &[u8],
    ) -> impl Future<Output = StreamResult<Self::BoxRead<'_>>>;

    fn call_with_response_and_stream_raw(
        &mut self,
        method_id: u16,
        args_data: &[u8],
    ) -> impl Future<Output = StreamResult<(Vec<u8>, Self::BoxRead<'_>)>>;

    fn call_with_body_and_response_stream_raw<B: AsyncRead + Unpin + ?Sized>(
        &mut self,
        method_id: u16,
        args_data: &[u8],
        body: &mut B,
        body_size: u64,
    ) -> impl Future<Output = StreamResult<(Vec<u8>, Self::BoxRead<'_>)>>;
}

pub fn write_field(buf: &mut Vec<u8>, field_id: FieldIndex, wire_type: WireType, data: &[u8]) {
    buf.extend_from_slice(&field_id.to_be_bytes());
    buf.push(wire_type as u8);
    buf.extend_from_slice(&(data.len() as u16).to_be_bytes());
    buf.extend_from_slice(data);
}

const FIELD_HEADER_SIZE: usize = FIELD_INDEX_SIZE + 1 + 2;

pub fn read_field(data: &[u8]) -> Result<(FieldIndex, WireType, &[u8], usize), ProtocolError> {
    if data.len() < FIELD_HEADER_SIZE {
        return Err(ProtocolError::UnexpectedEof);
    }
    let field_idx = FieldIndex::from_be_bytes(data[..FIELD_INDEX_SIZE].try_into().unwrap());
    let wire_type = WireType::from_repr(data[FIELD_INDEX_SIZE])
        .ok_or(ProtocolError::InvalidWireType(data[FIELD_INDEX_SIZE]))?;
    let len = u16::from_be_bytes([data[FIELD_INDEX_SIZE + 1], data[FIELD_INDEX_SIZE + 2]]) as usize;
    if data.len() < FIELD_HEADER_SIZE + len {
        return Err(ProtocolError::UnexpectedEof);
    }
    Ok((
        field_idx,
        wire_type,
        &data[FIELD_HEADER_SIZE..FIELD_HEADER_SIZE + len],
        FIELD_HEADER_SIZE + len,
    ))
}

macro_rules! impl_int {
    ($t:ty, $wire:ident) => {
        impl Encode for $t {
            fn encode(&self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(&self.to_be_bytes());
            }

            fn wire_type(&self) -> WireType {
                WireType::$wire
            }
        }

        impl Decode<'_> for $t {
            fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
                if wire_type != WireType::$wire {
                    return Err(ProtocolError::InvalidWireType(wire_type as u8));
                }
                const SIZE: usize = std::mem::size_of::<$t>();
                if data.len() < SIZE {
                    return Err(ProtocolError::UnexpectedEof);
                }
                Ok(<$t>::from_be_bytes(data[..SIZE].try_into().unwrap()))
            }
        }

        impl Args for $t {
            fn encode_args(&self) -> Vec<u8> {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(1 as FieldIndex).to_be_bytes());
                let mut field_data = Vec::new();
                self.encode(&mut field_data);
                write_field(&mut buf, 0, self.wire_type(), &field_data);
                buf
            }

            fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
                if data.len() < FIELD_INDEX_SIZE {
                    return Err(ProtocolError::UnexpectedEof);
                }
                let field_count =
                    FieldIndex::from_be_bytes(data[..FIELD_INDEX_SIZE].try_into().unwrap());
                if field_count == 0 {
                    return Err(ProtocolError::MissingField(0));
                }
                let (_, wire_type, field_data, _) = read_field(&data[FIELD_INDEX_SIZE..])?;
                Self::decode(wire_type, field_data)
            }
        }
    };
}

impl_int!(i8, I8);
impl_int!(u8, U8);
impl_int!(i16, I16);
impl_int!(u16, U16);
impl_int!(i32, I32);
impl_int!(u32, U32);
impl_int!(i64, I64);
impl_int!(u64, U64);
impl_int!(f32, F32);
impl_int!(f64, F64);

impl Encode for bool {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.push(if *self { 1 } else { 0 });
    }

    fn wire_type(&self) -> WireType {
        WireType::Bool
    }
}

impl Decode<'_> for bool {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::Bool {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        if data.is_empty() {
            return Err(ProtocolError::UnexpectedEof);
        }
        Ok(data[0] != 0)
    }
}

impl Args for bool {
    fn encode_args(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(1 as FieldIndex).to_be_bytes());
        let mut field_data = Vec::new();
        self.encode(&mut field_data);
        write_field(&mut buf, 0, self.wire_type(), &field_data);
        buf
    }

    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < FIELD_INDEX_SIZE {
            return Err(ProtocolError::UnexpectedEof);
        }
        let field_count = FieldIndex::from_be_bytes(data[..FIELD_INDEX_SIZE].try_into().unwrap());
        if field_count == 0 {
            return Err(ProtocolError::MissingField(0));
        }
        let (_, wire_type, field_data, _) = read_field(&data[FIELD_INDEX_SIZE..])?;
        Self::decode(wire_type, field_data)
    }
}

impl Encode for String {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    fn wire_type(&self) -> WireType {
        WireType::String
    }
}

impl Decode<'_> for String {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::String {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        String::from_utf8(data.to_vec()).map_err(|_| ProtocolError::InvalidUtf8)
    }
}

impl Args for String {
    fn encode_args(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(1 as FieldIndex).to_be_bytes());
        let mut field_data = Vec::new();
        self.encode(&mut field_data);
        write_field(&mut buf, 0, self.wire_type(), &field_data);
        buf
    }

    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < FIELD_INDEX_SIZE {
            return Err(ProtocolError::UnexpectedEof);
        }
        let field_count = FieldIndex::from_be_bytes(data[..FIELD_INDEX_SIZE].try_into().unwrap());
        if field_count == 0 {
            return Err(ProtocolError::MissingField(0));
        }
        let (_, wire_type, field_data, _) = read_field(&data[FIELD_INDEX_SIZE..])?;
        Self::decode(wire_type, field_data)
    }
}

impl Encode for Vec<u8> {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn wire_type(&self) -> WireType {
        WireType::Bytes
    }
}

impl Decode<'_> for Vec<u8> {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::Bytes {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        Ok(data.to_vec())
    }
}

impl Args for Vec<u8> {
    fn encode_args(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(1 as FieldIndex).to_be_bytes());
        let mut field_data = Vec::new();
        self.encode(&mut field_data);
        write_field(&mut buf, 0, self.wire_type(), &field_data);
        buf
    }

    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < FIELD_INDEX_SIZE {
            return Err(ProtocolError::UnexpectedEof);
        }
        let field_count = FieldIndex::from_be_bytes(data[..FIELD_INDEX_SIZE].try_into().unwrap());
        if field_count == 0 {
            return Err(ProtocolError::MissingField(0));
        }
        let (_, wire_type, field_data, _) = read_field(&data[FIELD_INDEX_SIZE..])?;
        Self::decode(wire_type, field_data)
    }
}

impl Encode for &[u8] {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn wire_type(&self) -> WireType {
        WireType::Bytes
    }
}

impl<'a> Decode<'a> for &'a [u8] {
    fn decode(wire_type: WireType, data: &'a [u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::Bytes {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        Ok(data)
    }
}

impl Encode for &str {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    fn wire_type(&self) -> WireType {
        WireType::String
    }
}

impl<'a> Decode<'a> for &'a str {
    fn decode(wire_type: WireType, data: &'a [u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::String {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        std::str::from_utf8(data).map_err(|_| ProtocolError::InvalidUtf8)
    }
}

use std::ops::Range;

impl<T: Encode> Encode for Range<T> {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.start.encode(buf);
        self.end.encode(buf);
    }

    fn wire_type(&self) -> WireType {
        WireType::Bytes
    }
}

macro_rules! impl_range {
    ($t:ty, $size:expr) => {
        impl Decode<'_> for Range<$t> {
            fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
                if wire_type != WireType::Bytes {
                    return Err(ProtocolError::InvalidWireType(wire_type as u8));
                }
                if data.len() < $size * 2 {
                    return Err(ProtocolError::UnexpectedEof);
                }
                let start = <$t>::from_be_bytes(data[..$size].try_into().unwrap());
                let end = <$t>::from_be_bytes(data[$size..$size * 2].try_into().unwrap());
                Ok(start..end)
            }
        }

        impl Args for Range<$t> {
            fn encode_args(&self) -> Vec<u8> {
                let mut buf = Vec::new();
                buf.extend_from_slice(&(1 as FieldIndex).to_be_bytes());
                let mut field_data = Vec::new();
                self.encode(&mut field_data);
                write_field(&mut buf, 0, self.wire_type(), &field_data);
                buf
            }

            fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
                if data.len() < FIELD_INDEX_SIZE {
                    return Err(ProtocolError::UnexpectedEof);
                }
                let field_count =
                    FieldIndex::from_be_bytes(data[..FIELD_INDEX_SIZE].try_into().unwrap());
                if field_count == 0 {
                    return Err(ProtocolError::MissingField(0));
                }
                let (_, wire_type, field_data, _) = read_field(&data[FIELD_INDEX_SIZE..])?;
                Self::decode(wire_type, field_data)
            }
        }
    };
}

impl_range!(i8, 1);
impl_range!(u8, 1);
impl_range!(i16, 2);
impl_range!(u16, 2);
impl_range!(i32, 4);
impl_range!(u32, 4);
impl_range!(i64, 8);
impl_range!(u64, 8);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Array<T>(pub Vec<T>);

impl<T> Array<T> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T> From<Vec<T>> for Array<T> {
    fn from(v: Vec<T>) -> Self {
        Self(v)
    }
}

impl<T> std::ops::Deref for Array<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Array<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Args> Encode for Array<T> {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.0.len() as u32).to_be_bytes());
        for item in &self.0 {
            let encoded = item.encode_args();
            buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            buf.extend_from_slice(&encoded);
        }
    }

    fn wire_type(&self) -> WireType {
        WireType::Array
    }
}

impl<T: Args> Decode<'_> for Array<T> {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::Array {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        if data.len() < 4 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let count = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        let mut result = Vec::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            if data.len() < offset + 4 {
                return Err(ProtocolError::UnexpectedEof);
            }
            let item_len =
                u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if data.len() < offset + item_len {
                return Err(ProtocolError::UnexpectedEof);
            }
            let item = T::decode_args(&data[offset..offset + item_len])?;
            result.push(item);
            offset += item_len;
        }
        Ok(Self(result))
    }
}

pub mod prelude {
    pub use crate::{Args, Stream};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Args, Debug, PartialEq)]
    struct TwoFields {
        #[field(idx = 0)]
        a: i64,
        #[field(idx = 1)]
        b: i32,
    }

    #[derive(Args, Debug, PartialEq)]
    struct WithOptional {
        #[field(idx = 0)]
        required: i64,
        #[field(idx = 1)]
        optional: Option<String>,
    }

    #[derive(Args, Debug, PartialEq)]
    struct AllTypes {
        #[field(idx = 0)]
        f_i8: i8,
        #[field(idx = 1)]
        f_u8: u8,
        #[field(idx = 2)]
        f_i16: i16,
        #[field(idx = 3)]
        f_u16: u16,
        #[field(idx = 4)]
        f_i32: i32,
        #[field(idx = 5)]
        f_u32: u32,
        #[field(idx = 6)]
        f_i64: i64,
        #[field(idx = 7)]
        f_u64: u64,
        #[field(idx = 8)]
        f_bool: bool,
        #[field(idx = 9)]
        f_string: String,
        #[field(idx = 10)]
        f_bytes: Vec<u8>,
    }

    #[test]
    fn args_all_types_roundtrip() {
        let orig = AllTypes {
            f_i8: -128,
            f_u8: 255,
            f_i16: -32768,
            f_u16: 65535,
            f_i32: i32::MIN,
            f_u32: u32::MAX,
            f_i64: i64::MIN,
            f_u64: u64::MAX,
            f_bool: true,
            f_string: "hello".into(),
            f_bytes: vec![0, 1, 255],
        };
        let encoded = orig.encode_args();
        let decoded = AllTypes::decode_args(&encoded).unwrap();
        assert_eq!(orig, decoded);
    }

    #[test]
    fn args_optional_present() {
        let orig = WithOptional {
            required: 42,
            optional: Some("test".into()),
        };
        let encoded = orig.encode_args();
        let decoded = WithOptional::decode_args(&encoded).unwrap();
        assert_eq!(orig, decoded);
    }

    #[test]
    fn args_optional_none() {
        let orig = WithOptional {
            required: 42,
            optional: None,
        };
        let encoded = orig.encode_args();
        let decoded = WithOptional::decode_args(&encoded).unwrap();
        assert_eq!(orig, decoded);
    }

    #[test]
    fn forwards_compat_unknown_fields_ignored() {
        let orig = TwoFields { a: 10, b: 20 };
        let mut encoded = orig.encode_args();

        // Inject an extra unknown field
        let field_count =
            FieldIndex::from_be_bytes(encoded[..FIELD_INDEX_SIZE].try_into().unwrap());
        encoded[..FIELD_INDEX_SIZE].copy_from_slice(&(field_count + 1).to_be_bytes());
        write_field(&mut encoded, 99, WireType::I64, &12345i64.to_be_bytes());

        let decoded = TwoFields::decode_args(&encoded).unwrap();
        assert_eq!(orig, decoded);
    }

    #[test]
    fn backwards_compat_missing_optional_defaults_none() {
        // Encode only the required field, omitting optional
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(1 as FieldIndex).to_be_bytes());
        write_field(&mut encoded, 0, WireType::I64, &42i64.to_be_bytes());

        let decoded = WithOptional::decode_args(&encoded).unwrap();
        assert_eq!(decoded.required, 42);
        assert_eq!(decoded.optional, None);
    }

    #[test]
    fn missing_required_field_errors() {
        let encoded = (0 as FieldIndex).to_be_bytes().to_vec();
        let result = TwoFields::decode_args(&encoded);
        assert!(matches!(result, Err(ProtocolError::MissingField(0))));
    }

    #[test]
    fn invalid_wire_type_errors() {
        let result = i64::decode(WireType::String, &[0; 8]);
        assert!(matches!(result, Err(ProtocolError::InvalidWireType(_))));
    }

    #[test]
    fn truncated_field_header_errors() {
        let result = read_field(&[0, 1, 2]);
        assert!(matches!(result, Err(ProtocolError::UnexpectedEof)));
    }

    #[test]
    fn truncated_field_data_errors() {
        let mut buf = Vec::new();
        write_field(&mut buf, 0, WireType::I64, &[0; 8]);
        let result = read_field(&buf[..buf.len() - 1]);
        assert!(matches!(result, Err(ProtocolError::UnexpectedEof)));
    }

    #[test]
    fn invalid_utf8_string_errors() {
        let result = String::decode(WireType::String, &[0xFF, 0xFE]);
        assert!(matches!(result, Err(ProtocolError::InvalidUtf8)));
    }

    #[test]
    fn big_endian_byte_order() {
        let val: u32 = 0x01020304;
        let mut buf = Vec::new();
        val.encode(&mut buf);
        assert_eq!(buf, vec![0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn f32_roundtrip() {
        let values = [
            0.0f32,
            1.0,
            -1.0,
            std::f32::consts::PI,
            f32::MIN,
            f32::MAX,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ];
        for val in values {
            let mut buf = Vec::new();
            val.encode(&mut buf);
            let decoded = f32::decode(WireType::F32, &buf).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn f32_nan_roundtrip() {
        let val = f32::NAN;
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let decoded = f32::decode(WireType::F32, &buf).unwrap();
        assert!(decoded.is_nan());
    }

    #[test]
    fn f64_roundtrip() {
        let values = [
            0.0f64,
            1.0,
            -1.0,
            std::f64::consts::PI,
            f64::MIN,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for val in values {
            let mut buf = Vec::new();
            val.encode(&mut buf);
            let decoded = f64::decode(WireType::F64, &buf).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn f64_nan_roundtrip() {
        let val = f64::NAN;
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let decoded = f64::decode(WireType::F64, &buf).unwrap();
        assert!(decoded.is_nan());
    }

    #[test]
    fn f32_args_roundtrip() {
        let val = std::f32::consts::PI;
        let encoded = val.encode_args();
        let decoded = f32::decode_args(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn f64_args_roundtrip() {
        let val = std::f64::consts::E;
        let encoded = val.encode_args();
        let decoded = f64::decode_args(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn borrowed_slice_zero_copy() {
        let bytes_input = vec![10, 20, 30];
        let decoded_bytes: &[u8] = Decode::decode(WireType::Bytes, &bytes_input).unwrap();
        assert!(std::ptr::eq(decoded_bytes.as_ptr(), bytes_input.as_ptr()));

        let str_input = b"test string".to_vec();
        let decoded_str: &str = Decode::decode(WireType::String, &str_input).unwrap();
        assert!(std::ptr::eq(decoded_str.as_ptr(), str_input.as_ptr()));
    }

    #[test]
    fn bool_decode() {
        assert!(bool::decode(WireType::Bool, &[1]).unwrap());
        assert!(bool::decode(WireType::Bool, &[42]).unwrap());
        assert!(bool::decode(WireType::Bool, &[255]).unwrap());
        assert!(!bool::decode(WireType::Bool, &[0]).unwrap());
    }

    #[test]
    fn large_field_id() {
        let mut buf = Vec::new();
        write_field(&mut buf, FieldIndex::MAX, WireType::U8, &[42]);
        let (field_id, wire_type, data, _) = read_field(&buf).unwrap();
        assert_eq!(field_id, FieldIndex::MAX);
        assert_eq!(wire_type, WireType::U8);
        assert_eq!(data, &[42]);
    }

    mod service_tests {
        use super::*;

        #[allow(dead_code)]
        #[derive(Args, Debug, PartialEq, Clone)]
        struct EchoRequest {
            #[field(idx = 0)]
            message: String,
        }

        #[allow(dead_code)]
        #[derive(Args, Debug, PartialEq, Clone)]
        struct EchoResponse {
            #[field(idx = 0)]
            message: String,
        }

        #[allow(dead_code)]
        #[derive(Args, Debug, PartialEq, Clone)]
        struct AddRequest {
            #[field(idx = 0)]
            a: i64,
            #[field(idx = 1)]
            b: i64,
        }

        #[allow(dead_code)]
        #[derive(Args, Debug, PartialEq, Clone)]
        struct AddResponse {
            #[field(idx = 0)]
            result: i64,
        }

        #[service]
        pub trait TestService {
            async fn echo(&self, req: EchoRequest) -> EchoResponse;
            async fn add(&self, req: AddRequest) -> AddResponse;
        }

        #[test]
        fn service_generates_method_ids() {
            assert_eq!(test_service::ECHO, 0);
            assert_eq!(test_service::ADD, 1);
        }

        #[test]
        fn service_generates_method_names() {
            assert_eq!(test_service::method_name(0), Some("echo"));
            assert_eq!(test_service::method_name(1), Some("add"));
            assert_eq!(test_service::method_name(99), None);
        }
    }

    #[derive(Args, Debug, PartialEq)]
    struct UnitStruct;

    #[test]
    fn unit_struct_roundtrip() {
        let encoded = UnitStruct.encode_args();
        assert_eq!(encoded, (0 as FieldIndex).to_be_bytes().to_vec());
        let decoded = UnitStruct::decode_args(&encoded).unwrap();
        assert_eq!(decoded, UnitStruct);
    }

    #[derive(Args, Debug, PartialEq)]
    enum TestEnum {
        #[field(idx = 0)]
        First(TwoFields),
        #[field(idx = 1)]
        Second(WithOptional),
    }

    #[test]
    fn enum_variant_roundtrip() {
        let first = TestEnum::First(TwoFields { a: 1, b: 2 });
        let encoded = first.encode_args();
        let decoded = TestEnum::decode_args(&encoded).unwrap();
        assert_eq!(decoded, first);

        let second = TestEnum::Second(WithOptional {
            required: 99,
            optional: Some("hello".into()),
        });
        let encoded = second.encode_args();
        let decoded = TestEnum::decode_args(&encoded).unwrap();
        assert_eq!(decoded, second);
    }

    #[test]
    fn enum_unknown_variant_errors() {
        let mut encoded = (99 as FieldIndex).to_be_bytes().to_vec();
        encoded.extend_from_slice(&TwoFields { a: 1, b: 2 }.encode_args());
        let result = TestEnum::decode_args(&encoded);
        assert!(matches!(result, Err(ProtocolError::UnknownVariant(99))));
    }

    #[test]
    fn decode_truncated_data_errors() {
        let result = TwoFields::decode_args(&[0]);
        assert!(matches!(result, Err(ProtocolError::UnexpectedEof)));
    }

    #[test]
    fn wire_type_from_invalid_byte_returns_none() {
        assert!(WireType::from_repr(200).is_none());
        assert!(WireType::from_repr(0).is_some());
        assert_eq!(WireType::from_repr(0), Some(WireType::None));
    }

    #[test]
    fn infallible_args_decode_always_errors() {
        let result = std::convert::Infallible::decode_args(&[]);
        assert!(matches!(result, Err(ProtocolError::UnknownVariant(0))));
    }

    #[test]
    fn primitive_args_roundtrip() {
        let val: u64 = 0xDEADBEEF;
        let encoded = val.encode_args();
        let decoded = u64::decode_args(&encoded).unwrap();
        assert_eq!(val, decoded);

        let val: i32 = -12345;
        let encoded = val.encode_args();
        let decoded = i32::decode_args(&encoded).unwrap();
        assert_eq!(val, decoded);

        let val = "hello world".to_string();
        let encoded = val.encode_args();
        let decoded = String::decode_args(&encoded).unwrap();
        assert_eq!(val, decoded);

        let val = true;
        let encoded = val.encode_args();
        let decoded = bool::decode_args(&encoded).unwrap();
        assert_eq!(val, decoded);

        let val: Vec<u8> = vec![1, 2, 3, 255];
        let encoded = val.encode_args();
        let decoded = Vec::<u8>::decode_args(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn primitive_args_boundary_values() {
        for val in [i64::MIN, i64::MAX, 0i64] {
            let encoded = val.encode_args();
            let decoded = i64::decode_args(&encoded).unwrap();
            assert_eq!(val, decoded);
        }

        for val in [u64::MIN, u64::MAX] {
            let encoded = val.encode_args();
            let decoded = u64::decode_args(&encoded).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn args_derived_types_implement_encode_decode() {
        let val = TwoFields { a: 100, b: 200 };
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let decoded = TwoFields::decode(WireType::Bytes, &buf).unwrap();
        assert_eq!(val, decoded);
    }

    mod multi_arg_service_tests {
        use super::*;

        #[derive(Args, Debug, PartialEq, Clone)]
        struct Response {
            #[field(idx = 0)]
            result: String,
        }

        #[service]
        pub trait MultiArgService {
            async fn single_primitive(&self, x: u64) -> Response;
            async fn two_primitives(&self, a: String, b: i32) -> Response;
            async fn three_mixed(&self, name: String, count: u64, data: TwoFields) -> Response;
        }

        #[test]
        fn multi_arg_method_ids_generated() {
            assert_eq!(multi_arg_service::SINGLE_PRIMITIVE, 0);
            assert_eq!(multi_arg_service::TWO_PRIMITIVES, 1);
            assert_eq!(multi_arg_service::THREE_MIXED, 2);
        }

        #[test]
        fn response_roundtrip() {
            let resp = Response {
                result: "success".to_string(),
            };
            let mut buf = Vec::new();
            resp.encode(&mut buf);
            let decoded = Response::decode(WireType::Bytes, &buf).unwrap();
            assert_eq!(resp, decoded);
        }
    }

    mod multi_arg_compat_tests {
        use super::*;

        #[derive(Args, Debug, PartialEq, Clone)]
        struct CompatResponse {
            #[field(idx = 0)]
            value: i64,
        }

        #[service]
        pub trait CompatService {
            async fn with_optional(
                &self,
                required: String,
                optional: Option<i64>,
            ) -> CompatResponse;
            async fn all_optional(&self, a: Option<String>, b: Option<i64>) -> CompatResponse;
        }

        #[test]
        fn multi_arg_forwards_compat_unknown_args_ignored() {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(3 as FieldIndex).to_be_bytes());

            let mut field0 = Vec::new();
            Encode::encode(&"hello".to_string(), &mut field0);
            write_field(&mut buf, 0, WireType::String, &field0);

            let mut field1 = Vec::new();
            Encode::encode(&42i64, &mut field1);
            write_field(&mut buf, 1, WireType::I64, &field1);

            let mut field99 = Vec::new();
            Encode::encode(&999i64, &mut field99);
            write_field(&mut buf, 99, WireType::I64, &field99);

            let arg_map = compat_service::parse_args(&buf).unwrap();

            let arg0: String = arg_map
                .get(&0)
                .map(|(wt, data)| String::decode(*wt, data))
                .unwrap()
                .unwrap();
            assert_eq!(arg0, "hello");

            let arg1: i64 = arg_map
                .get(&1)
                .map(|(wt, data)| i64::decode(*wt, data))
                .unwrap()
                .unwrap();
            assert_eq!(arg1, 42);

            assert!(!arg_map.contains_key(&2));
        }

        #[test]
        fn multi_arg_backwards_compat_optional_defaults_none() {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(1 as FieldIndex).to_be_bytes());

            let mut field0 = Vec::new();
            Encode::encode(&"hello".to_string(), &mut field0);
            write_field(&mut buf, 0, WireType::String, &field0);

            let arg_map = compat_service::parse_args(&buf).unwrap();

            let arg0: String = arg_map
                .get(&0)
                .map(|(wt, data)| String::decode(*wt, data))
                .unwrap()
                .unwrap();
            assert_eq!(arg0, "hello");

            let arg1: Option<i64> = arg_map
                .get(&1)
                .map(|(wt, data)| {
                    if *wt == WireType::None {
                        Ok(None)
                    } else {
                        i64::decode(*wt, data).map(Some)
                    }
                })
                .transpose()
                .unwrap()
                .flatten();
            assert_eq!(arg1, None);
        }

        #[test]
        fn multi_arg_optional_with_value_decodes() {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(2 as FieldIndex).to_be_bytes());

            let mut field0 = Vec::new();
            Encode::encode(&"hello".to_string(), &mut field0);
            write_field(&mut buf, 0, WireType::String, &field0);

            let mut field1 = Vec::new();
            Encode::encode(&42i64, &mut field1);
            write_field(&mut buf, 1, WireType::I64, &field1);

            let arg_map = compat_service::parse_args(&buf).unwrap();

            let arg1: Option<i64> = arg_map
                .get(&1)
                .map(|(wt, data)| {
                    if *wt == WireType::None {
                        Ok(None)
                    } else {
                        i64::decode(*wt, data).map(Some)
                    }
                })
                .transpose()
                .unwrap()
                .flatten();
            assert_eq!(arg1, Some(42));
        }

        #[test]
        fn multi_arg_optional_explicit_none_decodes() {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(2 as FieldIndex).to_be_bytes());

            let mut field0 = Vec::new();
            Encode::encode(&"hello".to_string(), &mut field0);
            write_field(&mut buf, 0, WireType::String, &field0);

            write_field(&mut buf, 1, WireType::None, &[]);

            let arg_map = compat_service::parse_args(&buf).unwrap();

            let arg1: Option<i64> = arg_map
                .get(&1)
                .map(|(wt, data)| {
                    if *wt == WireType::None {
                        Ok(None)
                    } else {
                        i64::decode(*wt, data).map(Some)
                    }
                })
                .transpose()
                .unwrap()
                .flatten();
            assert_eq!(arg1, None);
        }

        #[test]
        fn multi_arg_all_optional_empty_request() {
            let buf = (0 as FieldIndex).to_be_bytes().to_vec();

            let arg_map = compat_service::parse_args(&buf).unwrap();

            let arg0: Option<String> = arg_map
                .get(&0)
                .map(|(wt, data)| {
                    if *wt == WireType::None {
                        Ok(None)
                    } else {
                        String::decode(*wt, data).map(Some)
                    }
                })
                .transpose()
                .unwrap()
                .flatten();
            assert_eq!(arg0, None);

            let arg1: Option<i64> = arg_map
                .get(&1)
                .map(|(wt, data)| {
                    if *wt == WireType::None {
                        Ok(None)
                    } else {
                        i64::decode(*wt, data).map(Some)
                    }
                })
                .transpose()
                .unwrap()
                .flatten();
            assert_eq!(arg1, None);
        }

        #[test]
        fn compat_response_roundtrip() {
            let resp = CompatResponse { value: 42 };
            let mut buf = Vec::new();
            resp.encode(&mut buf);
            let decoded = CompatResponse::decode(WireType::Bytes, &buf).unwrap();
            assert_eq!(resp, decoded);
        }
    }

    mod empty_response_tests {
        use super::*;

        #[derive(Args, Debug, PartialEq, Clone)]
        struct TestResponse {
            #[field(idx = 0)]
            value: i64,
        }

        #[derive(Args, Debug, PartialEq, Clone)]
        struct StreamMeta;

        #[service]
        pub trait EmptyResponseService {
            async fn no_response(&self, x: u64);
            async fn with_response(&self, x: u64) -> TestResponse;
            async fn stream_only(&self, x: u64) -> Stream;
            async fn stream_with_response(&self, x: u64) -> (StreamMeta, Stream);
            async fn request_stream_no_response(&self, x: u64, body: Stream);
            async fn request_stream_with_response(&self, x: u64, body: Stream) -> TestResponse;
        }

        #[test]
        fn empty_response_method_ids_generated() {
            assert_eq!(empty_response_service::NO_RESPONSE, 0);
            assert_eq!(empty_response_service::WITH_RESPONSE, 1);
            assert_eq!(empty_response_service::STREAM_ONLY, 2);
            assert_eq!(empty_response_service::STREAM_WITH_RESPONSE, 3);
            assert_eq!(empty_response_service::REQUEST_STREAM_NO_RESPONSE, 4);
            assert_eq!(empty_response_service::REQUEST_STREAM_WITH_RESPONSE, 5);
        }

        #[test]
        fn has_request_stream_correct() {
            assert!(!empty_response_service::has_request_stream(0));
            assert!(!empty_response_service::has_request_stream(1));
            assert!(!empty_response_service::has_request_stream(2));
            assert!(!empty_response_service::has_request_stream(3));
            assert!(empty_response_service::has_request_stream(4));
            assert!(empty_response_service::has_request_stream(5));
        }

        #[test]
        fn test_response_roundtrip() {
            let resp = TestResponse { value: 42 };
            let mut buf = Vec::new();
            resp.encode(&mut buf);
            let decoded = TestResponse::decode(WireType::Bytes, &buf).unwrap();
            assert_eq!(resp, decoded);
        }

        #[test]
        fn unit_struct_default_impl() {
            let meta: StreamMeta = Default::default();
            assert_eq!(meta, StreamMeta);
        }

        #[test]
        fn unit_struct_args_roundtrip() {
            let meta = StreamMeta;
            let encoded = meta.encode_args();
            let decoded = StreamMeta::decode_args(&encoded).unwrap();
            assert_eq!(meta, decoded);
        }

        #[test]
        fn unit_struct_encode_is_empty_field_count() {
            let meta = StreamMeta;
            let encoded = meta.encode_args();
            assert_eq!(encoded, (0 as FieldIndex).to_be_bytes().to_vec());
        }
    }

    mod array_tests {
        use super::*;

        #[test]
        fn array_of_ranges_roundtrip() {
            let range = 0u64..100;
            let range_encoded = range.encode_args();
            let range_decoded = Range::<u64>::decode_args(&range_encoded).unwrap();
            assert_eq!(range, range_decoded);

            let arr: Array<Range<u64>> = vec![0u64..100, 500..600, 1000..2000].into();
            let mut buf = Vec::new();
            arr.encode(&mut buf);
            let decoded = Array::<Range<u64>>::decode(WireType::Array, &buf).unwrap();
            assert_eq!(arr, decoded);
        }

        #[test]
        fn empty_array_roundtrip() {
            let arr: Array<Range<u64>> = vec![].into();
            let mut buf = Vec::new();
            arr.encode(&mut buf);
            let decoded = Array::<Range<u64>>::decode(WireType::Array, &buf).unwrap();
            assert_eq!(arr, decoded);
        }

        #[test]
        fn array_of_u64_roundtrip() {
            let arr: Array<u64> = vec![1, 2, 3, u64::MAX].into();
            let mut buf = Vec::new();
            arr.encode(&mut buf);
            let decoded = Array::<u64>::decode(WireType::Array, &buf).unwrap();
            assert_eq!(arr, decoded);
        }
    }
}

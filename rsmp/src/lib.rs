extern crate self as rsmp;

use std::io;
use std::pin::Pin;

use thiserror::Error;

pub use async_trait::async_trait;
pub use futures_io::AsyncRead;
pub use rsmp_derive::{Args, handler, local_handler, local_stream_compat, service, stream_compat};

pub mod transport;

pub use transport::StreamTransport;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unexpected end of data")]
    UnexpectedEof,
    #[error("invalid wire type: {0}")]
    InvalidWireType(u8),
    #[error("invalid UTF-8")]
    InvalidUtf8,
    #[error("missing required field: {0}")]
    MissingField(u16),
    #[error("unknown variant: {0}")]
    UnknownVariant(u16),
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WireType {
    I8 = 0,
    U8 = 1,
    I16 = 2,
    U16 = 3,
    I32 = 4,
    U32 = 5,
    I64 = 6,
    U64 = 7,
    Bool = 8,
    String = 9,
    Bytes = 10,
    None = 11,
}

impl WireType {
    pub fn from_u8(v: u8) -> Result<Self, ProtocolError> {
        match v {
            0 => Ok(Self::I8),
            1 => Ok(Self::U8),
            2 => Ok(Self::I16),
            3 => Ok(Self::U16),
            4 => Ok(Self::I32),
            5 => Ok(Self::U32),
            6 => Ok(Self::I64),
            7 => Ok(Self::U64),
            8 => Ok(Self::Bool),
            9 => Ok(Self::String),
            10 => Ok(Self::Bytes),
            11 => Ok(Self::None),
            _ => Err(ProtocolError::InvalidWireType(v)),
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stream;

pub struct BodyStream<R> {
    pub reader: R,
    pub size: u64,
}

impl<R> BodyStream<R> {
    pub fn new(reader: R, size: u64) -> Self {
        Self { reader, size }
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

pub type BoxAsyncRead<'a> = Pin<Box<dyn AsyncRead + 'a>>;

#[async_trait(?Send)]
pub trait AsyncStreamCompat {
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()>;
    async fn write_all(&mut self, data: &[u8]) -> io::Result<()>;
}

pub use transport::Response;

#[async_trait(?Send)]
pub trait Transport {
    async fn call_raw(
        &mut self,
        method_id: u16,
        args_data: &[u8],
    ) -> Result<Response, TransportError>;

    async fn call_with_body_raw(
        &mut self,
        method_id: u16,
        args_data: &[u8],
        body: &mut (dyn AsyncRead + Unpin),
        body_size: u64,
    ) -> Result<Response, TransportError>;

    async fn call_with_response_stream_raw<'a>(
        &'a mut self,
        method_id: u16,
        args_data: &[u8],
    ) -> Result<Result<BoxAsyncRead<'a>, Vec<u8>>, TransportError>;

    async fn call_with_body_and_response_stream_raw<'a>(
        &'a mut self,
        method_id: u16,
        args_data: &[u8],
        body: &mut (dyn AsyncRead + Unpin),
        body_size: u64,
    ) -> Result<Result<(Vec<u8>, BoxAsyncRead<'a>), Vec<u8>>, TransportError>;
}

pub fn write_field(buf: &mut Vec<u8>, field_id: u16, wire_type: WireType, data: &[u8]) {
    buf.extend_from_slice(&field_id.to_be_bytes());
    buf.push(wire_type as u8);
    buf.extend_from_slice(&(data.len() as u16).to_be_bytes());
    buf.extend_from_slice(data);
}

pub fn read_field(data: &[u8]) -> Result<(u16, WireType, &[u8], usize), ProtocolError> {
    if data.len() < 5 {
        return Err(ProtocolError::UnexpectedEof);
    }
    let field_idx = u16::from_be_bytes([data[0], data[1]]);
    let wire_type = WireType::from_u8(data[2])?;
    let len = u16::from_be_bytes([data[3], data[4]]) as usize;
    if data.len() < 5 + len {
        return Err(ProtocolError::UnexpectedEof);
    }
    Ok((field_idx, wire_type, &data[5..5 + len], 5 + len))
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
                buf.extend_from_slice(&1u16.to_be_bytes());
                let mut field_data = Vec::new();
                self.encode(&mut field_data);
                write_field(&mut buf, 0, self.wire_type(), &field_data);
                buf
            }

            fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
                if data.len() < 2 {
                    return Err(ProtocolError::UnexpectedEof);
                }
                let field_count = u16::from_be_bytes([data[0], data[1]]);
                if field_count == 0 {
                    return Err(ProtocolError::MissingField(0));
                }
                let (_, wire_type, field_data, _) = read_field(&data[2..])?;
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
        buf.extend_from_slice(&1u16.to_be_bytes());
        let mut field_data = Vec::new();
        self.encode(&mut field_data);
        write_field(&mut buf, 0, self.wire_type(), &field_data);
        buf
    }

    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < 2 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let field_count = u16::from_be_bytes([data[0], data[1]]);
        if field_count == 0 {
            return Err(ProtocolError::MissingField(0));
        }
        let (_, wire_type, field_data, _) = read_field(&data[2..])?;
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
        buf.extend_from_slice(&1u16.to_be_bytes());
        let mut field_data = Vec::new();
        self.encode(&mut field_data);
        write_field(&mut buf, 0, self.wire_type(), &field_data);
        buf
    }

    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < 2 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let field_count = u16::from_be_bytes([data[0], data[1]]);
        if field_count == 0 {
            return Err(ProtocolError::MissingField(0));
        }
        let (_, wire_type, field_data, _) = read_field(&data[2..])?;
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
        buf.extend_from_slice(&1u16.to_be_bytes());
        let mut field_data = Vec::new();
        self.encode(&mut field_data);
        write_field(&mut buf, 0, self.wire_type(), &field_data);
        buf
    }

    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < 2 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let field_count = u16::from_be_bytes([data[0], data[1]]);
        if field_count == 0 {
            return Err(ProtocolError::MissingField(0));
        }
        let (_, wire_type, field_data, _) = read_field(&data[2..])?;
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
    fn args_roundtrip() {
        let orig = TwoFields { a: -999, b: 123 };
        let encoded = orig.encode_args();
        let decoded = TwoFields::decode_args(&encoded).unwrap();
        assert_eq!(orig, decoded);
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
        let field_count = u16::from_be_bytes([encoded[0], encoded[1]]);
        encoded[0..2].copy_from_slice(&(field_count + 1).to_be_bytes());
        write_field(&mut encoded, 99, WireType::I64, &12345i64.to_be_bytes());

        let decoded = TwoFields::decode_args(&encoded).unwrap();
        assert_eq!(orig, decoded);
    }

    #[test]
    fn backwards_compat_missing_optional_defaults_none() {
        // Encode only the required field, omitting optional
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&1u16.to_be_bytes());
        write_field(&mut encoded, 0, WireType::I64, &42i64.to_be_bytes());

        let decoded = WithOptional::decode_args(&encoded).unwrap();
        assert_eq!(decoded.required, 42);
        assert_eq!(decoded.optional, None);
    }

    #[test]
    fn missing_required_field_errors() {
        let encoded = 0u16.to_be_bytes().to_vec();
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
    fn empty_string_roundtrip() {
        let val = String::new();
        let mut buf = Vec::new();
        val.encode(&mut buf);
        assert!(buf.is_empty());
        let decoded = String::decode(WireType::String, &buf).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn empty_bytes_roundtrip() {
        let val: Vec<u8> = vec![];
        let mut buf = Vec::new();
        val.encode(&mut buf);
        let decoded = Vec::<u8>::decode(WireType::Bytes, &buf).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn byte_slice_encode_decode() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut buf = Vec::new();
        data.encode(&mut buf);
        assert_eq!(buf, vec![1, 2, 3, 4, 5]);

        let decoded: &[u8] = Decode::decode(WireType::Bytes, &buf).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn byte_slice_borrows_from_input() {
        let input = vec![10, 20, 30];
        let decoded: &[u8] = Decode::decode(WireType::Bytes, &input).unwrap();
        assert!(std::ptr::eq(decoded.as_ptr(), input.as_ptr()));
    }

    #[test]
    fn str_slice_encode_decode() {
        let data: &str = "hello world";
        let mut buf = Vec::new();
        data.encode(&mut buf);
        assert_eq!(buf, b"hello world");

        let decoded: &str = Decode::decode(WireType::String, &buf).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn str_slice_borrows_from_input() {
        let input = b"test string".to_vec();
        let decoded: &str = Decode::decode(WireType::String, &input).unwrap();
        assert!(std::ptr::eq(decoded.as_ptr(), input.as_ptr()));
    }

    #[test]
    fn str_slice_invalid_utf8_errors() {
        let invalid = vec![0xFF, 0xFE];
        let result: Result<&str, _> = Decode::decode(WireType::String, &invalid);
        assert!(matches!(result, Err(ProtocolError::InvalidUtf8)));
    }

    #[test]
    fn bool_nonzero_is_true() {
        assert!(bool::decode(WireType::Bool, &[1]).unwrap());
        assert!(bool::decode(WireType::Bool, &[42]).unwrap());
        assert!(bool::decode(WireType::Bool, &[255]).unwrap());
        assert!(!bool::decode(WireType::Bool, &[0]).unwrap());
    }

    #[test]
    fn field_id_preserved() {
        let mut buf = Vec::new();
        write_field(&mut buf, 0xABCD, WireType::U8, &[42]);
        let (field_id, _, _, _) = read_field(&buf).unwrap();
        assert_eq!(field_id, 0xABCD);
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
    }

    #[derive(Args, Debug, PartialEq)]
    struct UnitStruct;

    #[test]
    fn unit_struct_roundtrip() {
        let encoded = UnitStruct.encode_args();
        assert_eq!(encoded, vec![0, 0]);
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
        let mut encoded = vec![0, 99];
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
    fn wire_type_from_invalid_byte_errors() {
        let result = WireType::from_u8(200);
        assert!(matches!(result, Err(ProtocolError::InvalidWireType(200))));
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
            buf.extend_from_slice(&3u16.to_be_bytes());

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
            buf.extend_from_slice(&1u16.to_be_bytes());

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
            buf.extend_from_slice(&2u16.to_be_bytes());

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
            buf.extend_from_slice(&2u16.to_be_bytes());

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
            let buf = 0u16.to_be_bytes().to_vec();

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
}

extern crate self as rsmp;

use thiserror::Error;

pub use rsmp_derive::Args;

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

pub trait Decode: Sized {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError>;
}

pub trait Args: Sized {
    fn encode_args(&self) -> Vec<u8>;
    fn decode_args(data: &[u8]) -> Result<Self, ProtocolError>;
    fn stream_size(&self) -> Option<u64> {
        None
    }
}

/// Marker type indicating a streaming body follows this message.
/// The inner value is the size in bytes of the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stream(pub u64);

impl Encode for Stream {
    fn encode(&self, buf: &mut Vec<u8>) {
        self.0.encode(buf);
    }
    fn wire_type(&self) -> WireType {
        WireType::U64
    }
}

impl Decode for Stream {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
        Ok(Stream(u64::decode(wire_type, data)?))
    }
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

        impl Decode for $t {
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

impl Decode for bool {
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

impl Encode for String {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }
    fn wire_type(&self) -> WireType {
        WireType::String
    }
}

impl Decode for String {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::String {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        String::from_utf8(data.to_vec()).map_err(|_| ProtocolError::InvalidUtf8)
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

impl Decode for Vec<u8> {
    fn decode(wire_type: WireType, data: &[u8]) -> Result<Self, ProtocolError> {
        if wire_type != WireType::Bytes {
            return Err(ProtocolError::InvalidWireType(wire_type as u8));
        }
        Ok(data.to_vec())
    }
}

pub mod prelude {
    pub use crate::{
        Args, Decode, Encode, ProtocolError, Stream, WireType, read_field, write_field,
    };
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
}

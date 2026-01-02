use std::io;

use rsmp::prelude::*;
use thiserror::Error;

#[derive(Error, Debug, strum::AsRefStr, strum::EnumDiscriminants)]
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(repr(u16))]
pub enum RequestError {
    #[error("not found")]
    NotFound,
    #[error("decode error: {0}")]
    Decode(#[from] ProtocolError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Args, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum Command {
    #[field(idx = 0)]
    Get(GetArgs),
    #[field(idx = 1)]
    Put(PutArgs),
}

#[derive(Args)]
pub struct GetArgs {
    #[field(idx = 0)]
    pub id: String,
    #[field(idx = 1)]
    pub offset: u64,
    #[field(idx = 2)]
    pub size: u64,
}

#[derive(Args)]
pub struct PutArgs {
    #[field(idx = 0)]
    pub id: String,
    #[field(idx = 1)]
    pub stream: Stream,
}

#[derive(Args, Debug)]
pub struct SuccessResponse {
    #[field(idx = 0)]
    pub data: Vec<u8>,
}

#[derive(Args, Debug)]
pub struct StreamHeaderResponse {
    #[field(idx = 0)]
    pub stream: Stream,
}

#[derive(Args, Debug)]
pub struct ErrorResponse {
    #[field(idx = 0)]
    pub code: u16,
    #[field(idx = 1)]
    pub message: String,
}

#[derive(Args, Debug)]
pub enum Response {
    #[field(idx = 0)]
    Success(SuccessResponse),
    #[field(idx = 1)]
    StreamHeader(StreamHeaderResponse),
    #[field(idx = 2)]
    Error(ErrorResponse),
}

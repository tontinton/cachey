use rsmp::prelude::*;

#[derive(Args, Debug)]
pub struct SuccessResponse;

#[derive(Args, Debug)]
pub struct GetResponse;

#[derive(Args, Debug)]
pub struct NotFoundError {
    #[field(idx = 0)]
    pub id: String,
}

#[derive(Args, Debug)]
pub struct IoError {
    #[field(idx = 0)]
    pub message: String,
}

#[derive(Args, Debug)]
pub enum CacheError {
    #[field(idx = 1)]
    NotFound(NotFoundError),
    #[field(idx = 2)]
    Io(IoError),
}

impl From<std::io::Error> for CacheError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(IoError {
            message: e.to_string(),
        })
    }
}

#[rsmp::service(error = CacheError)]
pub trait CacheService {
    async fn get(&self, id: String, offset: u64, size: u64) -> (GetResponse, Stream);
    async fn put(&self, id: String, body: Stream) -> SuccessResponse;
}

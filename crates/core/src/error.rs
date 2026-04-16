use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unsupported exchange: {0}")]
    UnsupportedExchange(&'static str),
    #[error("unsupported channel: {0}")]
    UnsupportedChannel(&'static str),
    #[error("unsupported symbol: {0}")]
    UnsupportedSymbol(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

use thiserror::Error;

#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("unsupported exchange: {0}")]
    UnsupportedExchange(String),
    #[error("unsupported channel: {0}")]
    UnsupportedChannel(String),
    #[error("unsupported symbol: {0}")]
    UnsupportedSymbol(String),
    #[error("ambiguous symbol: {0}")]
    AmbiguousSymbol(String),
    #[error("unsupported capability: {0}")]
    UnsupportedCapability(String),
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("subscription error: {0}")]
    Subscription(String),
    #[error("malformed market data: {0}")]
    MalformedData(String),
    #[error("HTTP status {status} (Retry-After: {retry_after:?})")]
    HttpStatus {
        status: u16,
        retry_after: Option<std::time::Duration>,
    },
    #[error("transport error: {0}")]
    Transport(String),
    #[error("parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

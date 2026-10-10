#![cfg_attr(
    not(any(
        feature = "ticker",
        feature = "trade",
        feature = "orderbook",
        feature = "candles",
        feature = "funding",
        feature = "liquidations",
        feature = "markprice",
        feature = "openinterest",
        feature = "index"
    )),
    allow(
        dead_code,
        unreachable_code,
        unused_imports,
        unused_mut,
        unused_variables
    )
)]

#[cfg(feature = "orderbook")]
pub mod books;
pub mod catalog;
pub mod discovery;
pub mod exchange;
pub mod feed;
pub mod handler;
#[doc(hidden)]
pub mod markets;
pub mod options;
pub mod prelude;
pub mod runtime;
pub mod transport;

pub use catalog::MarketCatalog;
pub use exchange::*;
pub use feed::{EventCounters, FeedHandler, FeedStatus};

pub use crate::feed::control::{
    ConnectionInfo, FeedEnvelope, FeedId, FeedIdentity, FeedInfo, FeedSnapshot, FeedState,
    RuntimeControl,
};

pub use discovery::{DiscoveryFeed, DiscoveryHandle, DiscoverySnapshot, DiscoveryState};

#[cfg(feature = "orderbook")]
pub use books::{BookAnchor, BookRecovery, BookSnapshot, BookUpdate, BookUpdates, L2BookHandle};

pub use options::{IdlePolicy, RuntimeOptions};

pub use transport::TransportConfig;

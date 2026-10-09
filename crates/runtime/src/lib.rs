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

pub mod catalog;
pub mod exchange;
pub mod feed;
pub mod handler;
#[doc(hidden)]
pub mod markets;
pub mod prelude;
pub mod runtime;

pub use catalog::MarketCatalog;
pub use exchange::*;
pub use feed::{EventCounters, FeedHandler, FeedStatus};

pub use crate::feed::control::{
    FeedEnvelope, FeedId, FeedIdentity, FeedInfo, FeedState, RuntimeControl,
};

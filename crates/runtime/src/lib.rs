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
pub mod market_info;
#[doc(hidden)]
pub mod markets;
pub mod options;
pub mod prelude;
#[cfg(feature = "recording")]
pub mod recording;
pub mod rest;
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

pub use market_info::MarketInfo;

pub use rest::{PublicRestClient, RestSnapshot};

#[cfg(feature = "funding")]
pub use crate::rest::{FundingHistory, FundingHistoryCursor, FundingHistoryQuery, HistoryStop};

#[cfg(feature = "candles")]
pub use crate::rest::{CandleHistory, CandleHistoryCursor, CandleHistoryQuery, CandleHistoryStop};

#[cfg(feature = "trade")]
pub use crate::rest::{
    TradeHistory, TradeHistoryCursor, TradeHistoryKind, TradeHistoryQuery, TradeHistoryStop,
};

#[cfg(feature = "recording")]
pub use crate::recording::{
    RecordedEvent, RecordingEnd, RecordingLimits, RecordingReader, RecordingSummary,
    RecordingWriter, ReplayOptions, ReplayTiming, record_stream,
};

#[cfg(feature = "recording")]
pub use crate::recording::raw::{
    RawCaptureHandle, RawCaptureReceiver, RawFeedInfo, RawObservation, RawObservationKind,
    RawPayload, RawSessionInfo, raw_capture_channel,
};

#[cfg(feature = "recording")]
pub use crate::recording::raw::{
    RawRecordingLimits, RawRecordingReader, RawRecordingWriter, record_raw_stream,
};

#[cfg(feature = "recording")]
pub use crate::recording::raw::{
    RawReplayEvent, RawReplayItem, RawReplayOptions, RawReplaySummary,
};

#[cfg(feature = "recording")]
pub use crate::recording::raw::RawSnapshotFailure;

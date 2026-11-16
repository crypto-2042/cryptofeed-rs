//! Per-venue feed support for the runtime.
//!
//! Each submodule owns the feed/session plumbing, wire-message processing and
//! normalized event dispatch for exactly one exchange. Cross-venue machinery
//! stays in `runtime.rs` (run loop and supervisor glue) and
//! `runtime/venue_support.rs` (generic channel dispatch, book-state and
//! connection helpers).

pub(crate) mod binance;
pub(crate) mod bitget;
pub(crate) mod bithumb;
pub(crate) mod bybit;
pub(crate) mod coinbase;
pub(crate) mod deribit;
pub(crate) mod gateio;
pub(crate) mod htx;
pub(crate) mod hyperliquid;
pub(crate) mod kraken;
pub(crate) mod kucoin;
pub(crate) mod okx;
pub(crate) mod upbit;

/// Imports shared by every venue module.
pub(crate) mod prelude {
    #[cfg(all(feature = "orderbook", test))]
    pub(crate) use crate::exchange::binance::book_sync::BinanceDepthDelta;
    #[cfg(feature = "orderbook")]
    pub(crate) use crate::exchange::bithumb::book_sync::BithumbBookSync;
    #[cfg(feature = "orderbook")]
    pub(crate) use crate::exchange::gateio::adapter::GateioInstrument;
    pub(crate) use crate::exchange::{
        ExchangeFeed,
        binance::adapter::{
            BinanceAdapter, BinanceConnectionPlan, BinanceEvent, BinanceInstrument,
        },
        bitget::adapter::{BitgetAdapter, BitgetEvent},
        bithumb::adapter::{BithumbAdapter, BithumbEvent},
        bybit::adapter::{BybitAdapter, BybitEvent, BybitProduct},
        coinbase::adapter::{CoinbaseAdapter, CoinbaseEvent},
        deribit::adapter::{DeribitAdapter, DeribitControl, DeribitEvent},
        gateio::adapter::{GateioAdapter, GateioConnectionPlan, GateioEvent},
        htx::adapter::{HtxAdapter, HtxControl, HtxEvent},
        hyperliquid::adapter::{HyperliquidAdapter, HyperliquidControl, HyperliquidEvent},
        kraken::adapter::{KrakenAdapter, KrakenEvent},
        kucoin::adapter::{KucoinAdapter, KucoinEvent},
        okx::adapter::{OkxAdapter, OkxEvent},
        upbit::adapter::{UpbitAdapter, UpbitEvent},
    };
    #[cfg(feature = "orderbook")]
    pub(crate) use crate::exchange::{
        binance::{
            book_sync::{BinanceBookSync, BinanceSequencedDepthDelta},
            parser as binance_parser,
        },
        bitget::book_sync::{BitgetBookAction, BitgetBookSync, BitgetDepthUpdate},
        bybit::book_sync::BybitBookSync,
        coinbase::book_sync::{CoinbaseBookOutcome, CoinbaseBookSync},
        deribit::book_sync::{DeribitBookOutcome, DeribitBookSync},
        gateio::{
            book_sync::{GateioBookSync, GateioBookUpdate, GateioDepthDelta},
            parser as gateio_parser,
        },
        htx::book_sync::{HtxBookMode, HtxBookOutcome, HtxBookSync},
        hyperliquid::book_sync::{HyperliquidBookOutcome, HyperliquidBookSync},
        kraken::{
            book_sync::{KrakenBookOutcome, KrakenBookSync},
            parser as kraken_parser,
        },
        kucoin::{
            book_sync::{KucoinBookMode, KucoinBookOutcome, KucoinBookSync},
            parser as kucoin_parser,
        },
        okx::{book_sync::OkxBookSync, parser as okx_parser},
        upbit::book_sync::UpbitBookSync,
    };
    // The wire-frame helpers used by the always-built text-message entry points;
    // the book-sync types stay orderbook-gated.
    #[cfg(feature = "orderbook")]
    pub(crate) use crate::exchange::hyperliquid::parser as hyperliquid_parser;
    pub(crate) use crate::exchange::{
        bithumb::parser as bithumb_parser, coinbase::parser as coinbase_parser,
        deribit::parser as deribit_parser, htx::parser as htx_parser,
        upbit::parser as upbit_parser,
    };
    pub(crate) use crate::runtime::connection;
    pub(crate) use crate::runtime::connection::Session;
    #[cfg(feature = "orderbook")]
    pub(crate) use crate::runtime::current_timestamp;
    pub(crate) use crate::runtime::planned_url;
    pub(crate) use crate::runtime::quota;
    pub(crate) use crate::runtime::readiness;
    #[cfg(feature = "orderbook")]
    pub(crate) use crate::runtime::snapshot;
    pub(crate) use crate::runtime::supervisor;
    pub(crate) use cryptofeed_core::{
        error::{Error, Result},
        exchange::{Channel, ExchangeId},
        symbol::InstrumentKind,
    };
    #[cfg(feature = "orderbook")]
    pub(crate) use cryptofeed_orderbook::L2Book;
    pub(crate) use futures::{Sink, Stream};
    pub(crate) use serde_json::Value;
    pub(crate) use std::fmt::Display;
    pub(crate) use tokio::sync::watch;
    pub(crate) use tokio::task::JoinSet;
    pub(crate) use tokio_tungstenite::tungstenite::Message;
    pub(crate) use url::Url;
}

// Re-export each venue's public-to-the-crate items so `runtime.rs` and its
// integration tests can keep calling them without module-qualified paths.
pub(crate) use binance::*;
pub(crate) use bitget::*;
pub(crate) use bithumb::*;
pub(crate) use bybit::*;
pub(crate) use coinbase::*;
pub(crate) use deribit::*;
pub(crate) use gateio::*;
pub(crate) use htx::*;
pub(crate) use hyperliquid::*;
pub(crate) use kraken::*;
pub(crate) use kucoin::*;
pub(crate) use okx::*;
pub(crate) use upbit::*;

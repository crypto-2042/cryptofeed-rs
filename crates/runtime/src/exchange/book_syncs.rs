//! Venue-scoped L2 book-sync state for a physical exchange feed.
//!
//! An [`ExchangeFeed`](super::ExchangeFeed) always targets exactly one
//! exchange, so it only needs the sync map for that exchange. Keeping the
//! thirteen maps in one venue-tagged enum makes that invariant explicit,
//! keeps the type of each map, and lets recovery paths lock only the map
//! that belongs to the feed instead of every venue's map.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cryptofeed_core::exchange::ExchangeId;

use crate::exchange::{
    binance::book_sync::BinanceBookSync, bitget::book_sync::BitgetBookSync,
    bithumb::book_sync::BithumbBookSync, bybit::book_sync::BybitBookSync,
    coinbase::book_sync::CoinbaseBookSync, deribit::book_sync::DeribitBookSync,
    gateio::book_sync::GateioBookSync, htx::book_sync::HtxBookSync,
    hyperliquid::book_sync::HyperliquidBookSync, kraken::book_sync::KrakenBookSync,
    kucoin::book_sync::KucoinBookSync, okx::book_sync::OkxBookSync,
    upbit::book_sync::UpbitBookSync,
};

/// Shared per-symbol book-sync map for one venue.
pub(crate) type BookSyncMap<T> = Arc<Mutex<HashMap<String, T>>>;

/// The book-sync maps owned by a feed, for its exchange only.
#[derive(Clone)]
pub(crate) enum VenueBookSyncs {
    Binance(BookSyncMap<BinanceBookSync>),
    Bitget(BookSyncMap<BitgetBookSync>),
    Bithumb(BookSyncMap<BithumbBookSync>),
    Bybit(BookSyncMap<BybitBookSync>),
    Coinbase(BookSyncMap<CoinbaseBookSync>),
    Deribit(BookSyncMap<DeribitBookSync>),
    Gateio(BookSyncMap<GateioBookSync>),
    Htx(BookSyncMap<HtxBookSync>),
    Hyperliquid(BookSyncMap<HyperliquidBookSync>),
    Kraken(BookSyncMap<KrakenBookSync>),
    Kucoin(BookSyncMap<KucoinBookSync>),
    Okx(BookSyncMap<OkxBookSync>),
    Upbit(BookSyncMap<UpbitBookSync>),
}

impl VenueBookSyncs {
    /// Creates the empty sync map for `exchange`; `None` for any exchange
    /// without book-sync support (all current exchanges are supported, but
    /// `ExchangeId` is `#[non_exhaustive]`).
    pub(crate) fn for_exchange(exchange: ExchangeId) -> Option<Self> {
        Some(match exchange {
            ExchangeId::Binance => Self::Binance(Default::default()),
            ExchangeId::Bitget => Self::Bitget(Default::default()),
            ExchangeId::Bithumb => Self::Bithumb(Default::default()),
            ExchangeId::Bybit => Self::Bybit(Default::default()),
            ExchangeId::Coinbase => Self::Coinbase(Default::default()),
            ExchangeId::Deribit => Self::Deribit(Default::default()),
            ExchangeId::Gateio => Self::Gateio(Default::default()),
            ExchangeId::Htx => Self::Htx(Default::default()),
            ExchangeId::Hyperliquid => Self::Hyperliquid(Default::default()),
            ExchangeId::Kraken => Self::Kraken(Default::default()),
            ExchangeId::Kucoin => Self::Kucoin(Default::default()),
            ExchangeId::Okx => Self::Okx(Default::default()),
            ExchangeId::Upbit => Self::Upbit(Default::default()),
            _ => return None,
        })
    }

    /// Drops one symbol from the feed's own venue map.
    pub(crate) fn clear_symbol(&self, symbol: &str) {
        match self {
            Self::Binance(syncs) => {
                syncs.lock().expect("binance sync lock").remove(symbol);
            }
            Self::Bitget(syncs) => {
                syncs.lock().expect("bitget sync lock").remove(symbol);
            }
            Self::Bithumb(syncs) => {
                syncs.lock().expect("bithumb sync lock").remove(symbol);
            }
            Self::Bybit(syncs) => {
                syncs.lock().expect("bybit sync lock").remove(symbol);
            }
            Self::Coinbase(syncs) => {
                syncs.lock().expect("coinbase sync lock").remove(symbol);
            }
            Self::Deribit(syncs) => {
                syncs.lock().expect("deribit sync lock").remove(symbol);
            }
            Self::Gateio(syncs) => {
                syncs.lock().expect("gateio sync lock").remove(symbol);
            }
            Self::Htx(syncs) => {
                syncs.lock().expect("htx sync lock").remove(symbol);
            }
            Self::Hyperliquid(syncs) => {
                syncs.lock().expect("hyperliquid sync lock").remove(symbol);
            }
            Self::Kraken(syncs) => {
                syncs.lock().expect("kraken sync lock").remove(symbol);
            }
            Self::Kucoin(syncs) => {
                syncs.lock().expect("kucoin sync lock").remove(symbol);
            }
            Self::Okx(syncs) => {
                syncs.lock().expect("okx sync lock").remove(symbol);
            }
            Self::Upbit(syncs) => {
                syncs.lock().expect("upbit sync lock").remove(symbol);
            }
        }
    }
}

macro_rules! venue_accessor {
    ($method:ident, $variant:ident, $sync:ty) => {
        /// Typed book-sync map for this venue, if the feed targets it.
        pub(crate) fn $method(&self) -> Option<&BookSyncMap<$sync>> {
            match self {
                Self::$variant(syncs) => Some(syncs),
                _ => None,
            }
        }
    };
}

impl VenueBookSyncs {
    venue_accessor!(binance, Binance, BinanceBookSync);
    venue_accessor!(bitget, Bitget, BitgetBookSync);
    venue_accessor!(bithumb, Bithumb, BithumbBookSync);
    venue_accessor!(bybit, Bybit, BybitBookSync);
    venue_accessor!(coinbase, Coinbase, CoinbaseBookSync);
    venue_accessor!(deribit, Deribit, DeribitBookSync);
    venue_accessor!(gateio, Gateio, GateioBookSync);
    venue_accessor!(htx, Htx, HtxBookSync);
    venue_accessor!(hyperliquid, Hyperliquid, HyperliquidBookSync);
    venue_accessor!(kraken, Kraken, KrakenBookSync);
    venue_accessor!(kucoin, Kucoin, KucoinBookSync);
    venue_accessor!(okx, Okx, OkxBookSync);
    venue_accessor!(upbit, Upbit, UpbitBookSync);
}

impl super::ExchangeFeed {
    /// Book-sync map for this feed's exchange, if it is a Binance feed.
    pub(crate) fn binance_book_syncs(&self) -> Option<&BookSyncMap<BinanceBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::binance)
    }

    /// Book-sync map for this feed's exchange, if it is a Bitget feed.
    pub(crate) fn bitget_book_syncs(&self) -> Option<&BookSyncMap<BitgetBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::bitget)
    }

    /// Book-sync map for this feed's exchange, if it is a Bithumb feed.
    pub(crate) fn bithumb_book_syncs(&self) -> Option<&BookSyncMap<BithumbBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::bithumb)
    }

    /// Book-sync map for this feed's exchange, if it is a Bybit feed.
    pub(crate) fn bybit_book_syncs(&self) -> Option<&BookSyncMap<BybitBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::bybit)
    }

    /// Book-sync map for this feed's exchange, if it is a Coinbase feed.
    pub(crate) fn coinbase_book_syncs(&self) -> Option<&BookSyncMap<CoinbaseBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::coinbase)
    }

    /// Book-sync map for this feed's exchange, if it is a Deribit feed.
    pub(crate) fn deribit_book_syncs(&self) -> Option<&BookSyncMap<DeribitBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::deribit)
    }

    /// Book-sync map for this feed's exchange, if it is a Gate.io feed.
    pub(crate) fn gateio_book_syncs(&self) -> Option<&BookSyncMap<GateioBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::gateio)
    }

    /// Book-sync map for this feed's exchange, if it is an HTX feed.
    pub(crate) fn htx_book_syncs(&self) -> Option<&BookSyncMap<HtxBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::htx)
    }

    /// Book-sync map for this feed's exchange, if it is a Hyperliquid feed.
    pub(crate) fn hyperliquid_book_syncs(&self) -> Option<&BookSyncMap<HyperliquidBookSync>> {
        self.book_syncs
            .as_ref()
            .and_then(VenueBookSyncs::hyperliquid)
    }

    /// Book-sync map for this feed's exchange, if it is a Kraken feed.
    pub(crate) fn kraken_book_syncs(&self) -> Option<&BookSyncMap<KrakenBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::kraken)
    }

    /// Book-sync map for this feed's exchange, if it is a KuCoin feed.
    pub(crate) fn kucoin_book_syncs(&self) -> Option<&BookSyncMap<KucoinBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::kucoin)
    }

    /// Book-sync map for this feed's exchange, if it is an OKX feed.
    pub(crate) fn okx_book_syncs(&self) -> Option<&BookSyncMap<OkxBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::okx)
    }

    /// Book-sync map for this feed's exchange, if it is an Upbit feed.
    pub(crate) fn upbit_book_syncs(&self) -> Option<&BookSyncMap<UpbitBookSync>> {
        self.book_syncs.as_ref().and_then(VenueBookSyncs::upbit)
    }
}

#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// Bithumb public WebSocket builder.
///
/// Spot-only public market data over `wss://ws-api.bithumb.com/websocket/v1`:
/// `trade` and snapshot `orderbook` (at most 15 units). The venue `ticker` frame
/// carries no best bid/ask and is therefore not a normalized `Ticker`
/// capability; there is no public WebSocket candle stream, so `Candles` is
/// rejected by capability preflight.
pub struct Bithumb;

#[allow(clippy::new_ret_no_self)]
impl Bithumb {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Bithumb)
    }
}

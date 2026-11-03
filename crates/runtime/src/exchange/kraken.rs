#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// Kraken Spot WebSocket v2 builder.
///
/// Spot-only public market data over `wss://ws.kraken.com/v2`; funding, open
/// interest, mark price and liquidations live on the separate Kraken
/// Derivatives platform and stay rejected by capability preflight.
pub struct Kraken;

#[allow(clippy::new_ret_no_self)]
impl Kraken {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Kraken)
    }
}

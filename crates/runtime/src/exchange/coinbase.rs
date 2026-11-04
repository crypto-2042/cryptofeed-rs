#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// Coinbase Advanced Trade public market-data builder.
///
/// Public, no-JWT spot data over `wss://advanced-trade-ws.coinbase.com`. The
/// public `candles` channel only transmits five-minute buckets; `L1Book` is
/// projected from the documented ticker BBO sizes. Options, futures, margin and
/// authenticated channels stay rejected by capability preflight.
pub struct Coinbase;

#[allow(clippy::new_ret_no_self)]
impl Coinbase {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Coinbase)
    }
}

#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// Upbit Korean quotation WebSocket builder.
///
/// Spot-only public market data over `wss://api.upbit.com/websocket/v1`:
/// `trade`, snapshot `orderbook` and `candle.<unit>`. The venue `ticker` frame
/// carries no best bid/ask and is therefore not a normalized `Ticker`
/// capability; derivative channels do not exist on this public surface and stay
/// rejected by capability preflight.
pub struct Upbit;

#[allow(clippy::new_ret_no_self)]
impl Upbit {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Upbit)
    }
}

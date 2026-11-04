#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// HTX (Huobi) public spot and USDT-M perpetual builder.
///
/// Spot market topics use `wss://api.huobi.pro/ws`; USDT-M market topics use
/// `wss://api.hbdm.com/linear-swap-ws` and funding/liquidation notification
/// topics use `wss://api.hbdm.com/linear-swap-notification`. Every frame is
/// GZIP-compressed. Options, delivery contracts and margin products stay
/// rejected by capability preflight.
pub struct Htx;

#[allow(clippy::new_ret_no_self)]
impl Htx {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Htx)
    }
}

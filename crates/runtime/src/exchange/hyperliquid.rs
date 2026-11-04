#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// Hyperliquid public perpetual and spot builder.
///
/// The public WebSocket is `wss://api.hyperliquid.xyz/ws`; the public catalog
/// is `POST https://api.hyperliquid.xyz/info` with `{"type":"meta"}` for
/// perpetuals and `{"type":"spotMeta"}` for spot. `l2Book` pushes are complete
/// replacement snapshots (20 levels, or 5 with the documented `fast` flag);
/// there is no public liquidation channel. Options, dated futures, margin
/// products and user/private channels stay rejected by capability preflight.
pub struct Hyperliquid;

#[allow(clippy::new_ret_no_self)]
impl Hyperliquid {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Hyperliquid)
    }
}

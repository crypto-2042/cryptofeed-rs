#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;
/// KuCoin futures contract-quantity conversion from catalog metadata.
pub mod units;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// KuCoin UTA WebSocket V2 builder.
///
/// Spot and perpetual feeds route to the documented public push hosts
/// (`x-push-spot` / `x-push-futures`) without a token. Dated futures and the
/// unsupported product/channel combinations stay rejected by capability
/// preflight.
pub struct Kucoin;

#[allow(clippy::new_ret_no_self)]
impl Kucoin {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Kucoin)
    }
}

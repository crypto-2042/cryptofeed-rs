pub mod adapter;
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// KuCoin UTA WebSocket V2 builder.
///
/// The runtime path is added incrementally; until capability preflight admits
/// KuCoin, every feed is rejected before a transport is opened.
pub struct Kucoin;

#[allow(clippy::new_ret_no_self)]
impl Kucoin {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Kucoin)
    }
}

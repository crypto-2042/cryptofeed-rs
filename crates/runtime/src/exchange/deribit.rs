#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

/// Deribit JSON-RPC v2 public derivatives builder.
///
/// Public perpetual and dated-futures data over `wss://www.deribit.com/ws/api/v2`.
/// Options, spot and authenticated methods stay rejected by capability
/// preflight.
pub struct Deribit;

#[allow(clippy::new_ret_no_self)]
impl Deribit {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Deribit)
    }
}

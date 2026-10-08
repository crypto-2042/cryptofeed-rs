#[doc(hidden)]
pub mod adapter;
#[cfg(feature = "orderbook")]
#[doc(hidden)]
pub mod book_sync;
#[doc(hidden)]
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

pub struct Gateio;

#[allow(clippy::new_ret_no_self)]
impl Gateio {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Gateio)
    }
}

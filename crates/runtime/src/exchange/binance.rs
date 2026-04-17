pub mod adapter;
pub mod book_sync;
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

pub struct Binance;

#[allow(clippy::new_ret_no_self)]
impl Binance {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Binance)
    }
}

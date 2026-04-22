pub mod adapter;
pub mod book_sync;
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

pub struct Bybit;

#[allow(clippy::new_ret_no_self)]
impl Bybit {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Bybit)
    }
}

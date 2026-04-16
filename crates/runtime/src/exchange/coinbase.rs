pub mod adapter;
pub mod parser;

use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

pub struct Coinbase;

#[allow(clippy::new_ret_no_self)]
impl Coinbase {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Coinbase)
    }
}

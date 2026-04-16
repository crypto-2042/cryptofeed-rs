use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

pub struct Coinbase;

impl Coinbase {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Coinbase)
    }
}

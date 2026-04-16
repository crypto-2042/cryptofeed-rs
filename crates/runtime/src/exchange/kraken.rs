use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

pub struct Kraken;

impl Kraken {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Kraken)
    }
}

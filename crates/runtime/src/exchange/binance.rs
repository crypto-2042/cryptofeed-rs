use cryptofeed_core::exchange::ExchangeId;

use super::ExchangeFeedBuilder;

pub struct Binance;

impl Binance {
    pub fn new() -> ExchangeFeedBuilder {
        ExchangeFeedBuilder::new(ExchangeId::Binance)
    }
}

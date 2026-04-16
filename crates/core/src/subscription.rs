use crate::exchange::{Channel, ExchangeId};
use crate::symbol::Symbol;

#[derive(Clone, Debug)]
pub struct Subscription {
    pub exchange: ExchangeId,
    pub channel: Channel,
    pub symbol: Symbol,
}

impl Subscription {
    pub fn new(exchange: ExchangeId, channel: Channel, symbol: Symbol) -> Self {
        Self { exchange, channel, symbol }
    }
}

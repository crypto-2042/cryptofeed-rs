use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ticker {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bid: Decimal,
    pub ask: Decimal,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

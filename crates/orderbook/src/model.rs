use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct L2BookSnapshot {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct L2BookDelta {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum L2Book {
    Snapshot(L2BookSnapshot),
    Delta(L2BookDelta),
}

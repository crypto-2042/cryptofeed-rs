use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ExchangeId {
    Binance,
    Bitget,
    Coinbase,
    Kraken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Channel {
    Ticker,
    Trade,
    L2Book,
}

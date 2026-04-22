use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ExchangeId {
    Binance,
    Bitget,
    Bybit,
    Coinbase,
    Kraken,
    Okx,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Channel {
    Candles,
    Funding,
    Liquidations,
    Ticker,
    Trade,
    L2Book,
}

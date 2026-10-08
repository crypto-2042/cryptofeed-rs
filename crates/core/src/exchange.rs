use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ExchangeId {
    Binance,
    Bitget,
    Bybit,
    Coinbase,
    Gateio,
    Kraken,
    Okx,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Channel {
    Candles,
    Funding,
    Index,
    L1Book,
    Liquidations,
    MarkPrice,
    OpenInterest,
    Ticker,
    Trade,
    L2Book,
}

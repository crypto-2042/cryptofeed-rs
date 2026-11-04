use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ExchangeId {
    Binance,
    Bitget,
    Bithumb,
    Bybit,
    Coinbase,
    Gateio,
    Kraken,
    Kucoin,
    Okx,
    Upbit,
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

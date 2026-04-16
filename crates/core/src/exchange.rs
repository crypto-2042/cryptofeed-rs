#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ExchangeId {
    Binance,
    Coinbase,
    Kraken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Channel {
    Ticker,
    Trade,
    L2Book,
}

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub side: Side,
    pub amount: Decimal,
    pub price: Decimal,
    pub exchange_ts: f64,
    pub received_ts: f64,
    pub id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{Side, Trade};
    use cryptofeed_core::exchange::ExchangeId;
    use cryptofeed_core::symbol::Symbol;
    use rust_decimal::Decimal;

    #[test]
    fn trade_keeps_normalized_symbol() {
        let trade = Trade {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            side: Side::Buy,
            amount: Decimal::new(1, 0),
            price: Decimal::new(100_000, 0),
            exchange_ts: 1.0,
            received_ts: 2.0,
            id: Some("1".to_owned()),
        };

        assert_eq!(trade.symbol.as_str(), "BTC-USDT");
    }
}

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

    #[test]
    fn trade_roundtrips_with_serde_json() {
        let trade = Trade {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            side: Side::Sell,
            amount: Decimal::from_str_exact("0.2501").unwrap(),
            price: Decimal::from_str_exact("64001.20").unwrap(),
            exchange_ts: 1710000001.0,
            received_ts: 1710000001.2,
            id: Some("trade-id-1".to_owned()),
        };

        let json = serde_json::to_string(&trade).unwrap();
        let decoded: Trade = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USDT");
        assert!(matches!(decoded.side, Side::Sell));
        assert_eq!(decoded.amount, Decimal::from_str_exact("0.2501").unwrap());
        assert_eq!(decoded.price, Decimal::from_str_exact("64001.20").unwrap());
        assert_eq!(decoded.id.as_deref(), Some("trade-id-1"));
    }
}

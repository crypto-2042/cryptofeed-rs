use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Candle {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub start: f64,
    pub end: f64,
    pub interval: String,
    pub trades: Option<u64>,
    pub open: Decimal,
    pub close: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub volume: Decimal,
    pub closed: Option<bool>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(test)]
mod tests {
    use super::Candle;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn candle_keeps_python_baseline_fields() {
        let candle = Candle {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            start: 1.0,
            end: 61.0,
            interval: "1m".to_owned(),
            trades: Some(10),
            open: Decimal::from_str_exact("1").unwrap(),
            close: Decimal::from_str_exact("2").unwrap(),
            high: Decimal::from_str_exact("3").unwrap(),
            low: Decimal::from_str_exact("0.5").unwrap(),
            volume: Decimal::from_str_exact("100").unwrap(),
            closed: Some(true),
            exchange_ts: 2.0,
            received_ts: 3.0,
        };

        assert_eq!(candle.symbol.as_str(), "BTC-USDT");
        assert_eq!(candle.interval, "1m");
    }

    #[test]
    fn candle_roundtrips_with_serde_json() {
        let candle = Candle {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            start: 1710000000.0,
            end: 1710000060.0,
            interval: "1m".to_owned(),
            trades: Some(42),
            open: Decimal::from_str_exact("64000").unwrap(),
            close: Decimal::from_str_exact("64010").unwrap(),
            high: Decimal::from_str_exact("64030").unwrap(),
            low: Decimal::from_str_exact("63950").unwrap(),
            volume: Decimal::from_str_exact("123.45").unwrap(),
            closed: Some(false),
            exchange_ts: 1710000060.0,
            received_ts: 1710000061.0,
        };

        let json = serde_json::to_string(&candle).unwrap();
        let decoded: Candle = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USDT");
        assert_eq!(decoded.interval, "1m");
        assert_eq!(decoded.trades, Some(42));
        assert_eq!(decoded.closed, Some(false));
    }
}

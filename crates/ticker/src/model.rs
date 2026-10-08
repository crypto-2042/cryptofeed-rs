use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Ticker {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub bid: Decimal,
    pub ask: Decimal,
    pub exchange_ts: f64,
    pub received_ts: f64,
    /// Implied volatility carried by option tickers (e.g. Binance
    /// `volatility`); `None` for non-option tickers.
    pub implied_volatility: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::Ticker;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn ticker_keeps_normalized_symbol() {
        let ticker = Ticker {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bid: Decimal::from_str_exact("64999.10").unwrap(),
            ask: Decimal::from_str_exact("65000.20").unwrap(),
            exchange_ts: 1.0,
            received_ts: 2.0,
            implied_volatility: None,
        };

        assert_eq!(ticker.symbol.as_str(), "BTC-USDT");
    }

    #[test]
    fn ticker_roundtrips_with_serde_json() {
        let ticker = Ticker {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            bid: Decimal::from_str_exact("64999.10").unwrap(),
            ask: Decimal::from_str_exact("65000.20").unwrap(),
            exchange_ts: 1710000000.5,
            received_ts: 1710000000.7,
            implied_volatility: None,
        };

        let json = serde_json::to_string(&ticker).unwrap();
        let decoded: Ticker = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USDT");
        assert_eq!(decoded.bid, Decimal::from_str_exact("64999.10").unwrap());
        assert_eq!(decoded.ask, Decimal::from_str_exact("65000.20").unwrap());
        assert_eq!(decoded.exchange_ts, 1710000000.5);
        assert_eq!(decoded.received_ts, 1710000000.7);
    }
}

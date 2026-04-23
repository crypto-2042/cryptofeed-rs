use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Funding {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub mark_price: Option<Decimal>,
    pub rate: Option<Decimal>,
    pub next_funding_time: Option<f64>,
    pub predicted_rate: Option<Decimal>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(test)]
mod tests {
    use super::Funding;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn funding_keeps_python_baseline_fields() {
        let funding = Funding {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            mark_price: Some(Decimal::from_str_exact("11185.87786614").unwrap()),
            rate: Some(Decimal::from_str_exact("0.00030000").unwrap()),
            next_funding_time: Some(1562306400.0),
            predicted_rate: None,
            exchange_ts: 1562305380.0,
            received_ts: 1562305381.0,
        };

        assert_eq!(funding.symbol.as_str(), "BTC-USDT");
        assert!(funding.rate.is_some());
    }

    #[test]
    fn funding_roundtrips_with_serde_json() {
        let funding = Funding {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            mark_price: Some(Decimal::from_str_exact("64000.1").unwrap()),
            rate: Some(Decimal::from_str_exact("0.000123").unwrap()),
            next_funding_time: Some(1710003600.0),
            predicted_rate: Some(Decimal::from_str_exact("0.000111").unwrap()),
            exchange_ts: 1710000000.0,
            received_ts: 1710000001.0,
        };

        let json = serde_json::to_string(&funding).unwrap();
        let decoded: Funding = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USDT");
        assert_eq!(
            decoded.mark_price,
            Some(Decimal::from_str_exact("64000.1").unwrap())
        );
        assert_eq!(
            decoded.predicted_rate,
            Some(Decimal::from_str_exact("0.000111").unwrap())
        );
        assert_eq!(decoded.next_funding_time, Some(1710003600.0));
    }
}

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarkPrice {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    /// Current mark price.
    pub price: Decimal,
    /// Next funding time carried by the same payload where the exchange
    /// sends it (Binance `markPriceUpdate`); `None` elsewhere.
    pub next_funding_time: Option<f64>,
    /// Predicted next funding rate, only where explicitly transmitted.
    /// Binance `markPriceUpdate.P` is a settlement price, so it leaves this unset.
    pub predicted_rate: Option<Decimal>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(test)]
mod tests {
    use super::MarkPrice;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn mark_price_roundtrips_with_serde_json() {
        let mark = MarkPrice {
            exchange: ExchangeId::Binance,
            symbol: Symbol::perpetual("btc", "usdt"),
            price: Decimal::from_str_exact("11772.81931887").unwrap(),
            next_funding_time: Some(1562306400000.0 / 1000.0),
            predicted_rate: None,
            exchange_ts: 1562305380.0,
            received_ts: 1562305381.0,
        };

        let json = serde_json::to_string(&mark).unwrap();
        let decoded: MarkPrice = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(
            decoded.price,
            Decimal::from_str_exact("11772.81931887").unwrap()
        );
        assert_eq!(decoded.next_funding_time, Some(1562306400.0));
    }
}

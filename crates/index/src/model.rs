use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexPrice {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    /// Current index price.
    pub price: Decimal,
    /// 24h rolling stats where the exchange transmits them (OKX
    /// `index-tickers`); `None` on plain index streams (Binance, Bybit).
    pub open_24h: Option<Decimal>,
    pub high_24h: Option<Decimal>,
    pub low_24h: Option<Decimal>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(test)]
mod tests {
    use super::IndexPrice;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn index_price_roundtrips_with_serde_json() {
        let index = IndexPrice {
            exchange: ExchangeId::Okx,
            symbol: Symbol::spot("btc", "usd"),
            price: Decimal::from_str_exact("65000.5").unwrap(),
            open_24h: Some(Decimal::from_str_exact("64000").unwrap()),
            high_24h: Some(Decimal::from_str_exact("65500").unwrap()),
            low_24h: Some(Decimal::from_str_exact("63500").unwrap()),
            exchange_ts: 1710000000.0,
            received_ts: 1710000001.0,
        };

        let json = serde_json::to_string(&index).unwrap();
        let decoded: IndexPrice = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USD");
        assert_eq!(decoded.price, Decimal::from_str_exact("65000.5").unwrap());
        assert_eq!(
            decoded.high_24h,
            Some(Decimal::from_str_exact("65500").unwrap())
        );
    }
}

use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenInterest {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    /// Open interest in the exchange-native unit (e.g. contracts for OKX).
    pub open_interest: Decimal,
    /// Coin-denominated quantity where explicitly transmitted (OKX `oiCcy`).
    /// This is a numeric quantity, not a currency code; `None` elsewhere.
    pub coin_quantity: Option<Decimal>,
    /// Open interest valued in USD, where the exchange transmits it.
    pub value_usd: Option<Decimal>,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(test)]
mod tests {
    use super::OpenInterest;
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn open_interest_roundtrips_with_serde_json() {
        let oi = OpenInterest {
            exchange: ExchangeId::Okx,
            symbol: Symbol::perpetual("btc", "usdt"),
            open_interest: Decimal::from_str_exact("12345.6").unwrap(),
            coin_quantity: Some(Decimal::from_str_exact("123.456").unwrap()),
            value_usd: Some(Decimal::from_str_exact("800000000").unwrap()),
            exchange_ts: 1710000000.0,
            received_ts: 1710000001.0,
        };

        let json = serde_json::to_string(&oi).unwrap();
        let decoded: OpenInterest = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USDT-PERP");
        assert_eq!(
            decoded.open_interest,
            Decimal::from_str_exact("12345.6").unwrap()
        );
        assert_eq!(
            decoded.coin_quantity,
            Some(Decimal::from_str_exact("123.456").unwrap())
        );
        assert_eq!(
            decoded.value_usd,
            Some(Decimal::from_str_exact("800000000").unwrap())
        );
    }
}

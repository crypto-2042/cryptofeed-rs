use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LiquidationStatus {
    Filled,
    Unfilled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Liquidation {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub side: String,
    pub quantity: Decimal,
    pub price: Decimal,
    pub id: Option<String>,
    pub status: LiquidationStatus,
    pub exchange_ts: f64,
    pub received_ts: f64,
}

#[cfg(test)]
mod tests {
    use super::{Liquidation, LiquidationStatus};
    use cryptofeed_core::{exchange::ExchangeId, symbol::Symbol};
    use rust_decimal::Decimal;

    #[test]
    fn liquidation_keeps_python_baseline_fields() {
        let liquidation = Liquidation {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            side: "sell".to_owned(),
            quantity: Decimal::from_str_exact("0.014").unwrap(),
            price: Decimal::from_str_exact("9910").unwrap(),
            id: None,
            status: LiquidationStatus::Filled,
            exchange_ts: 1568014460.893,
            received_ts: 1568014461.0,
        };

        assert_eq!(liquidation.symbol.as_str(), "BTC-USDT");
        assert_eq!(liquidation.status, LiquidationStatus::Filled);
    }

    #[test]
    fn liquidation_roundtrips_with_serde_json() {
        let liquidation = Liquidation {
            exchange: ExchangeId::Binance,
            symbol: Symbol::spot("btc", "usdt"),
            side: "buy".to_owned(),
            quantity: Decimal::from_str_exact("0.25").unwrap(),
            price: Decimal::from_str_exact("64050").unwrap(),
            id: Some("liq-1".to_owned()),
            status: LiquidationStatus::Unfilled,
            exchange_ts: 1710000010.0,
            received_ts: 1710000011.0,
        };

        let json = serde_json::to_string(&liquidation).unwrap();
        let decoded: Liquidation = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.symbol.as_str(), "BTC-USDT");
        assert_eq!(decoded.side, "buy");
        assert_eq!(decoded.id.as_deref(), Some("liq-1"));
        assert_eq!(decoded.status, LiquidationStatus::Unfilled);
    }
}

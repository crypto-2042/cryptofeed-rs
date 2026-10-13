//! Typed directory metadata; these reported limits are not an order validator.
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::{InstrumentKind, Symbol},
};
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MarketInfo {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub exchange_symbol: String,
    /// Explicit native price tick, never derived from decimal places.
    pub price_increment: Option<Decimal>,
    /// Explicit limit-order quantity step, in native order-size units.
    pub quantity_increment: Option<Decimal>,
    pub minimum_quantity: Option<Decimal>,
    /// Quote-denominated limit-order lower bound; not market-order applicability.
    pub minimum_notional: Option<Decimal>,
    pub price_decimal_places: Option<u32>,
    pub quantity_decimal_places: Option<u32>,
    pub native_status: Option<String>,
    pub native_contract_type: Option<String>,
    pub settlement_currency: Option<String>,
    /// Native face value and multiplier are separate and never multiplied here.
    pub contract_value: Option<Decimal>,
    pub contract_value_currency: Option<String>,
    pub contract_multiplier: Option<Decimal>,
}
impl MarketInfo {
    pub(crate) fn new(exchange: ExchangeId, symbol: Symbol, native: &str) -> Self {
        Self {
            exchange,
            symbol,
            exchange_symbol: native.to_owned(),
            price_increment: None,
            quantity_increment: None,
            minimum_quantity: None,
            minimum_notional: None,
            price_decimal_places: None,
            quantity_decimal_places: None,
            native_status: None,
            native_contract_type: None,
            settlement_currency: None,
            contract_value: None,
            contract_value_currency: None,
            contract_multiplier: None,
        }
    }
}
fn text(row: &Value, field: &str) -> Result<Option<String>> {
    match row.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(Error::MalformedData(format!(
            "market metadata {field} must be a string"
        ))),
    }
}
fn decimal(row: &Value, field: &str) -> Result<Option<Decimal>> {
    let value = match row.get(field) {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(value)) if value.is_empty() => return Ok(None),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        _ => {
            return Err(Error::MalformedData(format!(
                "invalid market metadata {field}"
            )));
        }
    };
    let parsed = if let Some((mantissa, _)) = value.split_once(['e', 'E']) {
        // from_scientific uses the rounding FromStr internally: reject an
        // inexact mantissa before using its checked exponent conversion.
        Decimal::from_str_exact(mantissa).and_then(|_| Decimal::from_scientific(&value))
    } else {
        Decimal::from_str_exact(&value)
    };
    parsed
        .ok()
        .filter(|value| *value >= Decimal::ZERO)
        .map(Some)
        .ok_or_else(|| Error::MalformedData(format!("invalid or inexact market metadata {field}")))
}
fn places(row: &Value, field: &str) -> Result<Option<u32>> {
    match row.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => value
            .parse()
            .map(Some)
            .map_err(|_| Error::MalformedData(format!("invalid market precision {field}"))),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| value.try_into().ok())
            .map(Some)
            .ok_or_else(|| Error::MalformedData(format!("invalid market precision {field}"))),
        _ => Err(Error::MalformedData(format!(
            "invalid market precision {field}"
        ))),
    }
}

fn object<'a>(row: &'a Value, field: &str) -> Result<&'a Value> {
    let value = &row[field];
    if value.is_null() || value.is_object() {
        Ok(value)
    } else {
        Err(Error::MalformedData(format!(
            "market metadata {field} must be an object"
        )))
    }
}

pub(crate) fn parse(
    exchange: ExchangeId,
    symbol: Symbol,
    native: &str,
    row: &Value,
    settlement: Option<&str>,
) -> Result<MarketInfo> {
    if !row.is_object() {
        return Err(Error::MalformedData(
            "market metadata row must be an object".into(),
        ));
    }
    let spot = symbol.kind() == InstrumentKind::Spot;
    let mut info = MarketInfo::new(exchange, symbol, native);
    match exchange {
        ExchangeId::Binance => {
            info.native_status = text(
                row,
                if !spot && row.get("contractStatus").is_some() {
                    "contractStatus"
                } else {
                    "status"
                },
            )?;
            if !spot {
                info.native_contract_type = text(row, "contractType")?;
                info.price_decimal_places = places(row, "pricePrecision")?;
                info.quantity_decimal_places = places(row, "quantityPrecision")?;
                info.settlement_currency = text(row, "marginAsset")?;
                info.contract_value = decimal(row, "contractSize")?;
            }
            if let Some(filters) = row.get("filters") {
                let filters = filters.as_array().ok_or_else(|| {
                    Error::MalformedData("market filters must be an array".into())
                })?;
                let mut seen = std::collections::HashSet::new();
                for filter in filters {
                    let kind = filter
                        .get("filterType")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            Error::MalformedData("market filter is missing filterType".into())
                        })?;
                    if matches!(
                        kind,
                        "PRICE_FILTER" | "LOT_SIZE" | "MIN_NOTIONAL" | "NOTIONAL"
                    ) && !seen.insert(kind)
                    {
                        return Err(Error::MalformedData(format!(
                            "duplicate market filter {kind}"
                        )));
                    }
                    match Some(kind) {
                        Some("PRICE_FILTER") => info.price_increment = decimal(filter, "tickSize")?,
                        Some("LOT_SIZE") => {
                            info.quantity_increment = decimal(filter, "stepSize")?;
                            info.minimum_quantity = decimal(filter, "minQty")?;
                        }
                        Some("MIN_NOTIONAL" | "NOTIONAL") => {
                            let value =
                                decimal(filter, "minNotional")?.or(decimal(filter, "notional")?);
                            info.minimum_notional =
                                info.minimum_notional.into_iter().chain(value).max();
                        }
                        _ => {}
                    }
                }
            }
        }
        ExchangeId::Bitget => {
            info.native_status = text(row, "status")?;
            info.price_decimal_places = places(row, "pricePrecision")?;
            info.quantity_decimal_places = places(row, "quantityPrecision")?;
            info.minimum_notional = decimal(row, "minOrderAmount")?;
            if !spot {
                info.native_contract_type = text(row, "type")?;
                info.price_increment = decimal(row, "priceMultiplier")?;
                info.quantity_increment = decimal(row, "quantityMultiplier")?;
                info.minimum_quantity = decimal(row, "minOrderQty")?;
            }
        }
        ExchangeId::Bybit => {
            info.native_status = text(row, "status")?;
            if !spot {
                info.native_contract_type = text(row, "contractType")?;
                info.settlement_currency = text(row, "settleCoin")?;
                info.price_decimal_places = places(row, "priceScale")?;
            }
            info.price_increment = decimal(object(row, "priceFilter")?, "tickSize")?;
            let lot = object(row, "lotSizeFilter")?;
            if spot {
                // Spot minOrderQty is deprecated in current v5; minOrderAmt is active.
                info.minimum_notional = decimal(lot, "minOrderAmt")?;
                info.quantity_increment = decimal(lot, "basePrecision")?;
            } else {
                info.quantity_increment = decimal(lot, "qtyStep")?;
                info.minimum_quantity = decimal(lot, "minOrderQty")?;
                info.minimum_notional = decimal(lot, "minNotionalValue")?;
            }
        }
        ExchangeId::Okx => {
            info.native_status = text(row, "state")?;
            info.price_increment = decimal(row, "tickSz")?;
            info.quantity_increment = decimal(row, "lotSz")?;
            info.minimum_quantity = decimal(row, "minSz")?;
            if !spot {
                info.native_contract_type = text(row, "ctType")?;
                info.settlement_currency = text(row, "settleCcy")?;
                info.contract_value = decimal(row, "ctVal")?;
                info.contract_value_currency = text(row, "ctValCcy")?;
                info.contract_multiplier = decimal(row, "ctMult")?;
            }
        }
        ExchangeId::Gateio if spot => {
            info.native_status = text(row, "trade_status")?;
            info.price_decimal_places = places(row, "precision")?;
            info.quantity_decimal_places = places(row, "amount_precision")?;
            info.minimum_quantity = decimal(row, "min_base_amount")?;
            info.minimum_notional = decimal(row, "min_quote_amount")?;
        }
        ExchangeId::Gateio => {
            info.native_contract_type = text(row, "type")?;
            info.price_increment = decimal(row, "order_price_round")?;
            info.minimum_quantity = decimal(row, "order_size_min")?;
            info.contract_multiplier = decimal(row, "quanto_multiplier")?;
            info.settlement_currency = settlement.map(str::to_ascii_uppercase);
        }
        _ => {}
    }
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn spot(exchange: ExchangeId, row: Value) -> MarketInfo {
        parse(exchange, Symbol::spot("BTC", "USDT"), "BTCUSDT", &row, None).unwrap()
    }
    #[test]
    fn binance_uses_filter_names_not_array_order_or_precision_digits() {
        let info = parse(
            ExchangeId::Binance,
            Symbol::perpetual("BTC", "USDT"),
            "BTCUSDT",
            &json!({"status":"TRADING","pricePrecision":2,"quantityPrecision":3,"filters":[
            {"filterType":"LOT_SIZE","minQty":"0.003","stepSize":"0.003"},
            {"filterType":"MIN_NOTIONAL","notional":"5"},
            {"filterType":"PRICE_FILTER","tickSize":"0.05"}]}),
            None,
        )
        .unwrap();
        let cash = spot(
            ExchangeId::Binance,
            json!({"status":"TRADING","contractStatus":"HALT","pricePrecision":true,"contractSize":"not applicable","filters":[{"filterType":"MIN_NOTIONAL","minNotional":"5"},{"filterType":"NOTIONAL","minNotional":"10"}]}),
        );
        assert_eq!(cash.native_status.as_deref(), Some("TRADING"));
        assert_eq!(cash.price_decimal_places, None);
        assert_eq!(cash.minimum_notional, Some(Decimal::from(10)));
        assert_eq!(info.price_increment, Some(Decimal::new(5, 2)));
        assert_eq!(info.quantity_increment, Some(Decimal::new(3, 3)));
        assert_eq!(info.minimum_quantity, info.quantity_increment);
        assert_eq!(info.minimum_notional, Some(Decimal::from(5)));
        assert_eq!(info.price_decimal_places, Some(2));
        let coin = parse(ExchangeId::Binance, Symbol::perpetual("BTC","USD"), "BTCUSD_PERP",
            &json!({"contractStatus":"TRADING","contractSize":100,"marginAsset":"BTC","contractType":"PERPETUAL"}), None).unwrap();
        assert_eq!(coin.contract_value, Some(Decimal::from(100)));
        assert_eq!(coin.settlement_currency.as_deref(), Some("BTC"));
        assert_eq!(coin.contract_value_currency, None); // No denomination field in this directory row.
    }
    #[test]
    fn bitget_precision_is_separate_from_futures_multipliers() {
        let row = json!({"status":"online","pricePrecision":"2","quantityPrecision":"3","priceMultiplier":"0.02","quantityMultiplier":"0.005","minOrderQty":"0.010","minOrderAmount":"5"});
        let cash = spot(ExchangeId::Bitget, row.clone());
        assert_eq!(cash.price_decimal_places, Some(2));
        assert_eq!(cash.price_increment, None);
        assert_eq!(cash.minimum_quantity, None); // Current field table scopes minOrderQty to futures.
        let future = parse(
            ExchangeId::Bitget,
            Symbol::perpetual("BTC", "USDT"),
            "BTCUSDT",
            &row,
            None,
        )
        .unwrap();
        assert_eq!(future.price_increment, Some(Decimal::new(2, 2)));
        assert_eq!(future.quantity_increment, Some(Decimal::new(5, 3)));
        assert_eq!(future.minimum_quantity, Some(Decimal::new(10, 3)));
    }
    #[test]
    fn bybit_spot_ignores_deprecated_minimum_qty_and_derivatives_use_qty_step() {
        let row = json!({"status":"Trading","settleCoin":"USDT","priceFilter":{"tickSize":"0.10"},"lotSizeFilter":{"basePrecision":"0.000001","minOrderQty":"0.001","qtyStep":"0.005","minOrderAmt":"5","minNotionalValue":"10"}});
        let cash = spot(ExchangeId::Bybit, row.clone());
        assert_eq!(cash.quantity_increment, Some(Decimal::new(1, 6)));
        assert_eq!(cash.minimum_quantity, None);
        assert_eq!(cash.minimum_notional, Some(Decimal::from(5)));
        let future = parse(
            ExchangeId::Bybit,
            Symbol::perpetual("BTC", "USDT"),
            "BTCUSDT",
            &row,
            None,
        )
        .unwrap();
        assert_eq!(future.quantity_increment, Some(Decimal::new(5, 3)));
        assert_eq!(future.minimum_quantity, Some(Decimal::new(1, 3)));
        assert_eq!(future.minimum_notional, Some(Decimal::from(10)));
    }
    #[test]
    fn okx_contract_face_value_currency_and_multiplier_remain_separate() {
        let info = parse(ExchangeId::Okx, Symbol::perpetual("BTC","USD"), "BTC-USD-SWAP",
            &json!({"state":"live","tickSz":"0.1","lotSz":"1","minSz":"1","ctVal":"100","ctValCcy":"USD","ctMult":"0.5","settleCcy":"BTC","ctType":"inverse"}), None).unwrap();
        assert_eq!(info.contract_value, Some(Decimal::from(100)));
        assert_eq!(info.contract_multiplier, Some(Decimal::new(5, 1)));
        assert_eq!(info.contract_value_currency.as_deref(), Some("USD"));
        assert_eq!(info.native_contract_type.as_deref(), Some("inverse"));
        let cash = spot(
            ExchangeId::Okx,
            json!({"state":"live","ctVal":"","ctMult":"","tickSz":"0.01","lotSz":"0.001"}),
        );
        assert_eq!(cash.contract_value, None);
        assert_eq!(cash.contract_multiplier, None);
    }
    #[test]
    fn gate_precision_does_not_guess_steps_and_zero_contract_multiplier_is_preserved() {
        let cash = spot(
            ExchangeId::Gateio,
            json!({"trade_status":"tradable","precision":6,"amount_precision":3,"min_base_amount":"0.001","min_quote_amount":null}),
        );
        assert_eq!(cash.price_decimal_places, Some(6));
        assert_eq!(cash.quantity_decimal_places, Some(3));
        assert_eq!(cash.price_increment, None);
        assert_eq!(cash.quantity_increment, None);
        assert_eq!(cash.minimum_notional, None);
        let future = parse(ExchangeId::Gateio, Symbol::perpetual("BTC","USD"), "BTC_USD",
            &json!({"type":"inverse","quanto_multiplier":"0","order_price_round":"0.1","order_size_min":1}), Some("btc")).unwrap();
        assert_eq!(future.contract_multiplier, Some(Decimal::ZERO));
        assert_eq!(future.price_increment, Some(Decimal::new(1, 1)));
        assert_eq!(future.settlement_currency.as_deref(), Some("BTC"));
    }
    #[test]
    fn decimal_numbers_scientific_values_and_invalid_metadata_never_round() {
        let row: Value =
            serde_json::from_str(r#"{"minOrderAmount":9007199254740993.123456789012}"#).unwrap();
        assert_eq!(
            spot(ExchangeId::Bitget, row)
                .minimum_notional
                .unwrap()
                .to_string(),
            "9007199254740993.123456789012"
        );
        assert_eq!(
            decimal(&json!({"value":"1e-8"}), "value").unwrap(),
            Some(Decimal::new(1, 8))
        );
        for value in [
            json!(true),
            json!("-1"),
            json!("0.00000000000000000000000000001"),
            json!("1.00000000000000000000000000001e0"),
        ] {
            assert!(decimal(&json!({"value":value}), "value").is_err());
        }
        assert!(
            parse(
                ExchangeId::Bybit,
                Symbol::spot("BTC", "USDT"),
                "BTCUSDT",
                &json!({"priceFilter":false}),
                None
            )
            .is_err()
        );
        assert!(places(&json!({"value":-1}), "value").is_err());
        assert!(
            spot(ExchangeId::Bitget, json!({}))
                .price_increment
                .is_none()
        );
        assert!(parse(ExchangeId::Binance, Symbol::spot("BTC","USDT"), "BTCUSDT", &json!({"filters":[{"filterType":"PRICE_FILTER","tickSize":"1"},{"filterType":"PRICE_FILTER","tickSize":"2"}]}), None).is_err());
    }
}

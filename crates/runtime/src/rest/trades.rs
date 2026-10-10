//! Current public recent trades. History/aggregation are separate contracts.
use super::adapter;
use crate::market_info::MarketInfo;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::InstrumentKind,
};
use cryptofeed_trade::{Side, Trade};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::Value;
use std::collections::HashSet;
use url::Url;

pub(super) struct Plan {
    pub url: Url,
    category: Option<String>,
    pub(super) aggregate: bool,
}
fn invalid() -> Error {
    Error::MalformedData("invalid public REST trade".into())
}
pub(super) fn plan(info: &MarketInfo, limit: u16) -> Result<Plan> {
    use crate::exchange::{
        binance::adapter::{BinanceProduct, product_from_normalized},
        bybit::adapter::{BybitAdapter, BybitProduct},
        gateio::adapter::{GateioProduct, product_from_symbol},
    };
    if !matches!(
        info.symbol.kind(),
        InstrumentKind::Spot | InstrumentKind::Perpetual | InstrumentKind::Futures
    ) {
        return Err(Error::UnsupportedCapability("recent trades product".into()));
    }
    let (base, symbol_key, category, maximum) = match info.exchange {
        ExchangeId::Binance => (
            match product_from_normalized(&info.symbol)? {
                BinanceProduct::Spot => "https://data-api.binance.vision/api/v3/trades",
                BinanceProduct::UsdM => "https://fapi.binance.com/fapi/v1/trades",
                BinanceProduct::CoinM => "https://dapi.binance.com/dapi/v1/trades",
                _ => {
                    return Err(Error::UnsupportedCapability(
                        "Binance recent trades product".into(),
                    ));
                }
            },
            "symbol",
            None,
            1000,
        ),
        ExchangeId::Bitget => (
            "https://api.bitget.com/api/v3/market/fills",
            "symbol",
            Some(
                crate::exchange::bitget::adapter::bitget_instrument_type(&info.symbol)
                    .to_ascii_uppercase(),
            ),
            100,
        ),
        ExchangeId::Bybit => {
            let (category, maximum) = match BybitAdapter::product_for_symbol(&info.symbol) {
                BybitProduct::Spot => ("spot", 60),
                BybitProduct::Linear => ("linear", 1000),
                BybitProduct::Inverse => ("inverse", 1000),
                _ => {
                    return Err(Error::UnsupportedCapability(
                        "Bybit recent trades product".into(),
                    ));
                }
            };
            (
                "https://api.bybit.com/v5/market/recent-trade",
                "symbol",
                Some(category.into()),
                maximum,
            )
        }
        ExchangeId::Okx => (
            "https://openapi.okx.com/api/v5/market/trades",
            "instId",
            None,
            500,
        ),
        // Gate derivative tables omit a numeric maximum; use a bounded SDK cap.
        ExchangeId::Gateio => (
            match product_from_symbol(&info.symbol)? {
                GateioProduct::Spot => "https://api.gateio.ws/api/v4/spot/trades",
                GateioProduct::UsdtPerpetual => "https://api.gateio.ws/api/v4/futures/usdt/trades",
                GateioProduct::BtcPerpetual => "https://api.gateio.ws/api/v4/futures/btc/trades",
                GateioProduct::UsdtDelivery => "https://api.gateio.ws/api/v4/delivery/usdt/trades",
            },
            if info.symbol.kind() == InstrumentKind::Spot {
                "currency_pair"
            } else {
                "contract"
            },
            None,
            1000,
        ),
        _ => return Err(Error::UnsupportedExchange(format!("{:?}", info.exchange))),
    };
    if limit == 0 || limit > maximum {
        return Err(Error::InvalidConfiguration(format!(
            "recent trades limit must be 1..={maximum}"
        )));
    }
    let mut url = Url::parse(base).expect("static public trades URL");
    url.query_pairs_mut()
        .append_pair(symbol_key, &info.exchange_symbol)
        .append_pair("limit", &limit.to_string());
    if let Some(category) = &category {
        url.query_pairs_mut().append_pair("category", category);
    }
    Ok(Plan {
        url,
        category,
        aggregate: false,
    })
}
fn id(value: &Value) -> Result<String> {
    match value {
        Value::String(value) if !value.is_empty() && value.trim() == value => Ok(value.clone()),
        Value::Number(value) if value.as_u64().is_some() => Ok(value.to_string()),
        _ => Err(invalid()),
    }
}
fn side(value: &Value) -> Result<Side> {
    match value.as_str() {
        Some(value) if value.eq_ignore_ascii_case("buy") => Ok(Side::Buy),
        Some(value) if value.eq_ignore_ascii_case("sell") => Ok(Side::Sell),
        _ => Err(invalid()),
    }
}
fn time(row: &Value, millis: &str, seconds: Option<&str>) -> Result<Decimal> {
    let time = if let Some(value) = row.get(millis).filter(|value| !value.is_null()) {
        adapter::decimal(value)?
            .checked_div(Decimal::from(1000))
            .ok_or_else(invalid)?
    } else if let Some(field) = seconds {
        adapter::decimal(&row[field])?
    } else {
        return Err(invalid());
    };
    if time.is_sign_negative() {
        return Err(invalid());
    }
    Ok(time)
}
pub(super) fn decode(
    info: &MarketInfo,
    plan: &Plan,
    payload: &Value,
    limit: u16,
    received_ts: f64,
) -> Result<Vec<Trade>> {
    Ok(decode_timed(info, plan, payload, limit, received_ts)?
        .into_iter()
        .map(|(_, trade)| trade)
        .collect())
}
pub(super) fn decode_timed(
    info: &MarketInfo,
    plan: &Plan,
    payload: &Value,
    limit: u16,
    received_ts: f64,
) -> Result<Vec<(Decimal, Trade)>> {
    let root = adapter::data(info.exchange, payload)?;
    if info.exchange == ExchangeId::Bybit && root["category"].as_str() != plan.category.as_deref() {
        return Err(Error::MalformedData("REST trade category mismatch".into()));
    }
    let rows = if info.exchange == ExchangeId::Bybit {
        &root["list"]
    } else {
        root
    };
    let rows = rows.as_array().ok_or_else(invalid)?;
    if rows.len() > usize::from(limit) {
        return Err(Error::MalformedData(
            "REST trade page exceeds requested limit".into(),
        ));
    }
    let mut seen = HashSet::new();
    let mut trades = Vec::with_capacity(rows.len());
    for row in rows {
        let (id_key, price_key, size_key, time, side) = match info.exchange {
            ExchangeId::Binance => {
                if row.get("symbol").is_some() {
                    adapter::identity(info, row, "symbol")?;
                }
                let maker = row[if plan.aggregate { "m" } else { "isBuyerMaker" }]
                    .as_bool()
                    .ok_or_else(invalid)?;
                (
                    if plan.aggregate { "a" } else { "id" },
                    if plan.aggregate { "p" } else { "price" },
                    if plan.aggregate { "q" } else { "qty" },
                    time(row, if plan.aggregate { "T" } else { "time" }, None)?,
                    if maker { Side::Sell } else { Side::Buy },
                )
            }
            ExchangeId::Bitget => {
                if row.get("symbol").is_some() {
                    adapter::identity(info, row, "symbol")?;
                }
                (
                    "execId",
                    "price",
                    "size",
                    time(row, "ts", None)?,
                    side(&row["side"])?,
                )
            }
            ExchangeId::Bybit => {
                adapter::identity(info, row, "symbol")?;
                (
                    "execId",
                    "price",
                    "size",
                    time(row, "time", None)?,
                    side(&row["side"])?,
                )
            }
            ExchangeId::Okx => {
                adapter::identity(info, row, "instId")?;
                (
                    "tradeId",
                    "px",
                    "sz",
                    time(row, "ts", None)?,
                    side(&row["side"])?,
                )
            }
            ExchangeId::Gateio => {
                let spot = info.symbol.kind() == InstrumentKind::Spot;
                adapter::identity(info, row, if spot { "currency_pair" } else { "contract" })?;
                let side = if spot {
                    side(&row["side"])?
                } else {
                    let size = adapter::decimal(&row["size"])?;
                    if size.is_sign_negative() {
                        Side::Sell
                    } else {
                        Side::Buy
                    }
                };
                (
                    "id",
                    "price",
                    if spot { "amount" } else { "size" },
                    if spot {
                        time(row, "create_time_ms", Some("create_time"))?
                    } else {
                        // Gate contract fields carry seconds despite the _ms name.
                        let value = row
                            .get("create_time_ms")
                            .filter(|value| !value.is_null())
                            .unwrap_or(&row["create_time"]);
                        let time = adapter::decimal(value)?;
                        if time.is_sign_negative() {
                            return Err(invalid());
                        }
                        time
                    },
                    side,
                )
            }
            _ => return Err(Error::UnsupportedExchange(format!("{:?}", info.exchange))),
        };
        let id = id(&row[id_key])?;
        if !seen.insert(id.clone()) {
            return Err(Error::MalformedData("duplicated REST trade ID".into()));
        }
        let price = adapter::decimal(&row[price_key])?;
        let mut amount = adapter::decimal(&row[size_key])?;
        if info.exchange == ExchangeId::Gateio && info.symbol.kind() != InstrumentKind::Spot {
            amount = amount.abs();
        }
        if price <= Decimal::ZERO || amount <= Decimal::ZERO {
            return Err(invalid());
        }
        let exchange_ts = time
            .to_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(invalid)?;
        trades.push((
            time,
            Trade {
                exchange: info.exchange,
                symbol: info.symbol.clone(),
                side,
                price,
                amount,
                exchange_ts,
                received_ts,
                id: Some(id),
                implied_volatility: None,
            },
        ));
    }
    // Stable exact timestamp order; distinct IDs at the same instant survive.
    trades.sort_by_key(|(time, _)| *time);
    Ok(trades)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_core::symbol::Symbol;
    use serde_json::json;
    fn info(exchange: ExchangeId, spot: bool) -> MarketInfo {
        MarketInfo::new(
            exchange,
            if spot {
                Symbol::spot("BTC", "USDT")
            } else {
                Symbol::perpetual("BTC", "USDT")
            },
            match exchange {
                ExchangeId::Okx if spot => "BTC-USDT",
                ExchangeId::Okx => "BTC-USDT-SWAP",
                ExchangeId::Gateio => "BTC_USDT",
                _ => "BTCUSDT",
            },
        )
    }
    fn row(exchange: ExchangeId, spot: bool, buy: bool) -> Value {
        let side = if buy { "buy" } else { "sell" };
        match exchange {
            ExchangeId::Binance => {
                json!({"id":9007199254740993u64,"price":"100.01","qty":"0.25","time":1001,"isBuyerMaker":!buy})
            }
            ExchangeId::Bitget => {
                json!({"execId":"execution-1","execLinkId":"unrelated","price":"100.01","size":"0.25","ts":"1001","side":side})
            }
            ExchangeId::Bybit => {
                json!({"execId":"execution-1","symbol":"BTCUSDT","price":"100.01","size":"0.25","time":"1001","side":if buy {"Buy"} else {"Sell"}})
            }
            ExchangeId::Okx => {
                json!({"tradeId":"execution-1","instId":if spot {"BTC-USDT"} else {"BTC-USDT-SWAP"},"px":"100.01","sz":"0.25","ts":"1001","side":side})
            }
            ExchangeId::Gateio if spot => {
                json!({"id":"execution-1","currency_pair":"BTC_USDT","price":"100.01","amount":"0.25","create_time_ms":"1001.123","create_time":"1","side":side})
            }
            ExchangeId::Gateio => {
                json!({"id":9007199254740993u64,"contract":"BTC_USDT","price":"100.01","size":if buy {"25"} else {"-25"},"create_time_ms":1.001123,"create_time":1})
            }
            _ => unreachable!(),
        }
    }
    fn payload(exchange: ExchangeId, spot: bool, rows: Vec<Value>) -> Value {
        match exchange {
            ExchangeId::Bitget => json!({"code":"00000","data":rows}),
            ExchangeId::Bybit => {
                json!({"retCode":0,"result":{"category":if spot {"spot"} else {"linear"},"list":rows}})
            }
            ExchangeId::Okx => json!({"code":"0","data":rows}),
            _ => json!(rows),
        }
    }
    #[test]
    fn five_venues_keep_taker_side_quantity_ids_and_native_times() {
        for exchange in [
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            for spot in [true, false] {
                for buy in [true, false] {
                    let info = info(exchange, spot);
                    let plan = plan(&info, 1).unwrap();
                    let p = payload(exchange, spot, vec![row(exchange, spot, buy)]);
                    let trades = decode(&info, &plan, &p, 1, 2.0).unwrap();
                    let trade = &trades[0];
                    assert_eq!(trade.symbol, info.symbol);
                    assert_eq!(trade.side, if buy { Side::Buy } else { Side::Sell });
                    assert_eq!(trade.price.to_string(), "100.01");
                    assert_eq!(
                        trade.amount.to_string(),
                        if exchange == ExchangeId::Gateio && !spot {
                            "25"
                        } else {
                            "0.25"
                        }
                    );
                    assert_eq!(
                        trade.id.as_deref(),
                        Some(
                            if exchange == ExchangeId::Binance
                                || exchange == ExchangeId::Gateio && !spot
                            {
                                "9007199254740993"
                            } else {
                                "execution-1"
                            }
                        )
                    );
                    assert_eq!(
                        trade.exchange_ts,
                        if exchange == ExchangeId::Gateio {
                            1.001123
                        } else {
                            1.001
                        }
                    );
                    assert_eq!(trade.received_ts, 2.0);
                    assert!(trade.implied_volatility.is_none());
                }
            }
        }
    }
    #[test]
    fn native_routes_and_limits_are_product_specific() {
        for (exchange, spot, max, path) in [
            (ExchangeId::Binance, true, 1000, "/api/v3/trades"),
            (ExchangeId::Binance, false, 1000, "/fapi/v1/trades"),
            (ExchangeId::Bitget, true, 100, "/api/v3/market/fills"),
            (ExchangeId::Bybit, true, 60, "/v5/market/recent-trade"),
            (ExchangeId::Bybit, false, 1000, "/v5/market/recent-trade"),
            (ExchangeId::Okx, false, 500, "/api/v5/market/trades"),
            (ExchangeId::Gateio, true, 1000, "/api/v4/spot/trades"),
            (
                ExchangeId::Gateio,
                false,
                1000,
                "/api/v4/futures/usdt/trades",
            ),
        ] {
            let info = info(exchange, spot);
            assert_eq!(plan(&info, max).unwrap().url.path(), path);
            assert!(plan(&info, 0).is_err());
            assert!(plan(&info, max + 1).is_err());
            let url = plan(&info, 1).unwrap().url;
            let params: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(params["limit"], "1");
            if exchange == ExchangeId::Bybit {
                assert_eq!(params["category"], if spot { "spot" } else { "linear" });
            }
            if exchange == ExchangeId::Bitget {
                assert_eq!(params["category"], "SPOT");
            }
        }
        let mut info = info(ExchangeId::Binance, false);
        info.symbol = Symbol::perpetual("BTC", "USD");
        assert_eq!(plan(&info, 1).unwrap().url.path(), "/dapi/v1/trades");
        info.exchange = ExchangeId::Bybit;
        assert_eq!(plan(&info, 1).unwrap().category.as_deref(), Some("inverse"));
        info.exchange = ExchangeId::Gateio;
        assert_eq!(
            plan(&info, 1).unwrap().url.path(),
            "/api/v4/futures/btc/trades"
        );
        info.symbol = Symbol::futures("BTC", "USDT", "241227");
        assert_eq!(
            plan(&info, 1).unwrap().url.path(),
            "/api/v4/delivery/usdt/trades"
        );
        info.exchange = ExchangeId::Coinbase;
        assert!(plan(&info, 1).is_err());
    }
    #[test]
    fn stable_exact_sort_keeps_distinct_ids_at_identical_times() {
        let info = info(ExchangeId::Gateio, false);
        let mut a = row(info.exchange, false, true);
        let mut b = a.clone();
        let mut c = a.clone();
        a["id"] = json!("a");
        b["id"] = json!("b");
        c["id"] = json!("c");
        a["create_time_ms"] = json!("1000000000.000000002");
        b["create_time_ms"] = json!("1000000000.000000001");
        c["create_time_ms"] = b["create_time_ms"].clone();
        let p = json!([a, b, c]);
        let trades = decode(&info, &plan(&info, 3).unwrap(), &p, 3, 0.0).unwrap();
        assert_eq!(
            trades
                .iter()
                .map(|t| t.id.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec!["b", "c", "a"]
        );
        // f64 cannot distinguish these instants; sorting precedes conversion.
        assert_eq!(trades[0].exchange_ts, trades[2].exchange_ts);
    }
    #[test]
    fn numeric_precision_and_explicit_seconds_fallback() {
        let info = info(ExchangeId::Gateio, false);
        let mut r = row(info.exchange, false, false);
        r["price"] = serde_json::from_str("100.1234567890123456789012345").unwrap();
        r["size"] = serde_json::from_str("-0.1234567890123456789012345678").unwrap();
        r.as_object_mut().unwrap().remove("create_time_ms");
        r["create_time"] = json!("1.001123");
        let trades = decode(&info, &plan(&info, 1).unwrap(), &json!([r]), 1, 2.0).unwrap();
        assert_eq!(trades[0].price.to_string(), "100.1234567890123456789012345");
        assert_eq!(
            trades[0].amount.to_string(),
            "0.1234567890123456789012345678"
        );
        assert_eq!(trades[0].exchange_ts, 1.001123);
    }
    #[test]
    fn duplicates_oversized_malformed_and_mismatched_payloads_fail() {
        let info = info(ExchangeId::Binance, true);
        let r = row(info.exchange, true, true);
        let plan = plan(&info, 2).unwrap();
        for p in [
            json!([r.clone(), r.clone()]),
            json!([r.clone(), r.clone(), r.clone()]),
            json!({"code":-1}),
            json!({}),
        ] {
            assert!(decode(&info, &plan, &p, 2, 0.0).is_err());
        }
        for (field, value) in [
            ("id", json!(9007199254740993.5)),
            ("qty", json!("-1")),
            ("price", json!("0")),
            ("time", json!("-1")),
            ("isBuyerMaker", json!("true")),
        ] {
            let mut bad = r.clone();
            bad[field] = value;
            assert!(
                decode(&info, &plan, &json!([bad]), 2, 0.0).is_err(),
                "{field}"
            );
        }
        for (exchange, key, value) in [
            (ExchangeId::Bybit, "symbol", "ETHUSDT"),
            (ExchangeId::Okx, "instId", "ETH-USDT"),
            (ExchangeId::Gateio, "currency_pair", "ETH_USDT"),
        ] {
            let info = self::info(exchange, true);
            let mut r = row(exchange, true, true);
            r[key] = json!(value);
            assert!(
                decode(
                    &info,
                    &self::plan(&info, 1).unwrap(),
                    &payload(exchange, true, vec![r]),
                    1,
                    0.0
                )
                .is_err()
            );
        }
        let info = self::info(ExchangeId::Bybit, true);
        let mut p = payload(info.exchange, true, vec![]);
        p["result"]["category"] = json!("linear");
        assert!(decode(&info, &self::plan(&info, 1).unwrap(), &p, 1, 0.0).is_err());
    }
    #[test]
    fn gate_clock_units_depend_on_product_not_field_name_or_magnitude() {
        for spot in [true, false] {
            let info = info(ExchangeId::Gateio, spot);
            let mut row = row(info.exchange, spot, true);
            row["create_time_ms"] = json!("1001.123");
            let records =
                decode(&info, &plan(&info, 1).unwrap(), &json!([row]), 1, 2000.0).unwrap();
            assert_eq!(
                records[0].exchange_ts,
                if spot { 1.001123 } else { 1001.123 }
            );
        }
    }
}

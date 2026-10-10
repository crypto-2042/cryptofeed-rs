#[cfg(any(feature = "ticker", feature = "orderbook"))]
use super::RestSnapshot;
use crate::market_info::MarketInfo;
#[cfg(any(feature = "ticker", feature = "orderbook"))]
use cryptofeed_core::symbol::InstrumentKind;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
};
use serde_json::Value;
#[cfg(any(feature = "ticker", feature = "orderbook"))]
use url::Url;

#[cfg(any(feature = "ticker", feature = "orderbook"))]
pub(super) struct Plan {
    pub url: Url,
    category: Option<String>,
}
#[cfg(any(feature = "ticker", feature = "orderbook"))]
pub(super) fn plan(info: &MarketInfo, depth: Option<u16>) -> Result<Plan> {
    use crate::exchange::{
        binance::adapter::{BinanceProduct, product_from_normalized},
        bybit::adapter::{BybitAdapter, BybitProduct},
        gateio::adapter::{GateioProduct, product_from_symbol},
    };
    if !matches!(
        info.symbol.kind(),
        InstrumentKind::Spot | InstrumentKind::Perpetual | InstrumentKind::Futures
    ) {
        return Err(Error::UnsupportedCapability("public REST product".into()));
    }
    let book = depth.is_some();
    let (base, operation, symbol_key, category, maximum, discrete) = match info.exchange {
        ExchangeId::Binance => {
            let (base, max, discrete) = match product_from_normalized(&info.symbol)? {
                BinanceProduct::Spot => ("https://data-api.binance.vision/api/v3", 5000, false),
                BinanceProduct::UsdM => ("https://fapi.binance.com/fapi/v1", 1000, true),
                BinanceProduct::CoinM => ("https://dapi.binance.com/dapi/v1", 1000, true),
                _ => {
                    return Err(Error::UnsupportedCapability(
                        "Binance public REST product".into(),
                    ));
                }
            };
            (
                base,
                if book { "depth" } else { "ticker/bookTicker" },
                "symbol",
                None,
                max,
                discrete,
            )
        }
        ExchangeId::Bitget => (
            "https://api.bitget.com/api/v3/market",
            if book { "orderbook" } else { "tickers" },
            "symbol",
            Some(
                crate::exchange::bitget::adapter::bitget_instrument_type(&info.symbol)
                    .to_ascii_uppercase(),
            ),
            1000,
            false,
        ),
        ExchangeId::Bybit => {
            let category = match BybitAdapter::product_for_symbol(&info.symbol) {
                BybitProduct::Spot => "spot",
                BybitProduct::Linear => "linear",
                BybitProduct::Inverse => "inverse",
                _ => {
                    return Err(Error::UnsupportedCapability(
                        "Bybit public REST product".into(),
                    ));
                }
            };
            (
                "https://api.bybit.com/v5/market",
                if book { "orderbook" } else { "tickers" },
                "symbol",
                Some(category.to_owned()),
                1000,
                false,
            )
        }
        ExchangeId::Okx => (
            "https://openapi.okx.com/api/v5/market",
            if book { "books" } else { "ticker" },
            "instId",
            Some(
                match info.symbol.kind() {
                    InstrumentKind::Spot => "SPOT",
                    InstrumentKind::Perpetual => "SWAP",
                    _ => "FUTURES",
                }
                .to_owned(),
            ),
            400,
            false,
        ),
        ExchangeId::Gateio => {
            let base = match product_from_symbol(&info.symbol)? {
                GateioProduct::Spot => "https://api.gateio.ws/api/v4/spot",
                GateioProduct::UsdtPerpetual => "https://api.gateio.ws/api/v4/futures/usdt",
                GateioProduct::BtcPerpetual => "https://api.gateio.ws/api/v4/futures/btc",
                GateioProduct::UsdtDelivery => "https://api.gateio.ws/api/v4/delivery/usdt",
            };
            (
                base,
                if book { "order_book" } else { "tickers" },
                if info.symbol.kind() == InstrumentKind::Spot {
                    "currency_pair"
                } else {
                    "contract"
                },
                None,
                100,
                false,
            ) // SDK cap, not an undocumented venue maximum.
        }
        _ => return Err(Error::UnsupportedExchange(format!("{:?}", info.exchange))),
    };
    if depth.is_some_and(|depth| {
        depth == 0
            || depth > maximum
            || discrete && ![5, 10, 20, 50, 100, 500, 1000].contains(&depth)
    }) {
        return Err(Error::InvalidConfiguration(
            "unsupported REST book depth".into(),
        ));
    }
    let mut url = Url::parse(&format!("{base}/{operation}")).expect("static REST URL");
    {
        let mut query = url.query_pairs_mut();
        query.append_pair(symbol_key, &info.exchange_symbol);
        if let Some(category) = &category {
            if matches!(info.exchange, ExchangeId::Bybit | ExchangeId::Bitget) {
                query.append_pair("category", category);
            }
        }
        if let Some(depth) = depth {
            query.append_pair(
                if info.exchange == ExchangeId::Okx {
                    "sz"
                } else {
                    "limit"
                },
                &depth.to_string(),
            );
        }
        if book && info.exchange == ExchangeId::Gateio {
            query.append_pair("with_id", "true");
            query.append_pair("interval", "0");
        }
    }
    Ok(Plan { url, category })
}
pub(super) fn data(exchange: ExchangeId, payload: &Value) -> Result<&Value> {
    let valid = match exchange {
        ExchangeId::Binance => payload
            .get("code")
            .is_none_or(|code| code.as_i64().is_some_and(|code| code >= 0)),
        ExchangeId::Bitget => payload["code"].as_str() == Some("00000"),
        ExchangeId::Bybit => payload["retCode"].as_i64() == Some(0),
        ExchangeId::Okx => payload["code"].as_str() == Some("0"),
        ExchangeId::Gateio => {
            payload.get("label").is_none() && payload.get("error").is_none_or(Value::is_null)
        }
        _ => false,
    };
    if !valid {
        return Err(Error::Protocol(format!(
            "{exchange:?} REST response reports failure"
        )));
    }
    Ok(match exchange {
        ExchangeId::Bybit => &payload["result"],
        ExchangeId::Bitget | ExchangeId::Okx => &payload["data"],
        _ => payload,
    })
}
#[cfg(any(feature = "ticker", feature = "orderbook"))]
fn category(plan: &Plan, row: &Value, field: &str) -> Result<()> {
    if let (Some(expected), Some(actual)) = (&plan.category, row.get(field)) {
        if !actual
            .as_str()
            .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
        {
            return Err(Error::MalformedData(
                "REST response product mismatch".into(),
            ));
        }
    }
    Ok(())
}
pub(super) fn identity(info: &MarketInfo, row: &Value, field: &str) -> Result<()> {
    if !row[field]
        .as_str()
        .is_some_and(|native| native.eq_ignore_ascii_case(&info.exchange_symbol))
    {
        return Err(Error::MalformedData(
            "REST response instrument mismatch".into(),
        ));
    }
    Ok(())
}
#[cfg(feature = "ticker")]
fn select<'a>(rows: &'a Value, info: &MarketInfo, field: &str) -> Result<&'a Value> {
    if rows.is_object() {
        identity(info, rows, field)?;
        return Ok(rows);
    }
    let rows = rows
        .as_array()
        .ok_or_else(|| Error::MalformedData("REST ticker rows missing".into()))?;
    let mut selected = rows.iter().filter(|row| {
        row[field]
            .as_str()
            .is_some_and(|native| native.eq_ignore_ascii_case(&info.exchange_symbol))
    });
    let selected_row = selected
        .next()
        .ok_or_else(|| Error::MalformedData("REST ticker instrument unavailable".into()))?;
    if selected.next().is_some() {
        return Err(Error::MalformedData(
            "REST ticker instrument duplicated".into(),
        ));
    }
    Ok(selected_row)
}
#[cfg(any(feature = "ticker", feature = "orderbook"))]
fn time(value: Option<&Value>, scale: f64) -> Result<Option<f64>> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let time = value
        .as_str()
        .and_then(|value| value.parse::<f64>().ok())
        .or_else(|| value.as_f64())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(|| Error::MalformedData("invalid REST timestamp".into()))?;
    Ok(Some(time / scale))
}
#[cfg(any(feature = "ticker", feature = "orderbook"))]
fn sequence(value: Option<&Value>) -> Result<Option<u64>> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .map(Some)
        .ok_or_else(|| Error::MalformedData("invalid REST sequence".into()))
}
pub(super) fn decimal(value: &Value) -> Result<rust_decimal::Decimal> {
    let raw = match value {
        Value::String(raw) => raw.clone(),
        Value::Number(raw) => raw.to_string(),
        _ => return Err(Error::MalformedData("invalid REST decimal".into())),
    };
    let parsed = if let Some((mantissa, _)) = raw.split_once(['e', 'E']) {
        rust_decimal::Decimal::from_str_exact(mantissa)
            .and_then(|_| rust_decimal::Decimal::from_scientific(&raw))
    } else {
        rust_decimal::Decimal::from_str_exact(&raw)
    };
    parsed.map_err(|_| Error::MalformedData("invalid or inexact REST decimal".into()))
}
#[cfg(feature = "ticker")]
pub(super) fn ticker(
    info: &MarketInfo,
    plan: &Plan,
    payload: &Value,
    received_ts: f64,
) -> Result<RestSnapshot<cryptofeed_ticker::Ticker>> {
    let root = data(info.exchange, payload)?;
    if info.exchange == ExchangeId::Bybit {
        category(plan, root, "category")?;
    }
    let rows = if info.exchange == ExchangeId::Bybit {
        &root["list"]
    } else {
        root
    };
    let field = match info.exchange {
        ExchangeId::Okx => "instId",
        ExchangeId::Gateio if info.symbol.kind() == InstrumentKind::Spot => "currency_pair",
        ExchangeId::Gateio => "contract",
        _ => "symbol",
    };
    let row = select(rows, info, field)?;
    if info.exchange == ExchangeId::Okx {
        category(plan, row, "instType")?;
    } else {
        category(plan, row, "category")?;
    }
    let (bid, ask, exchange_ts) = match info.exchange {
        ExchangeId::Binance => ("bidPrice", "askPrice", time(row.get("time"), 1000.0)?),
        ExchangeId::Bitget => ("bid1Price", "ask1Price", time(row.get("ts"), 1000.0)?),
        ExchangeId::Bybit => ("bid1Price", "ask1Price", time(payload.get("time"), 1000.0)?),
        ExchangeId::Okx => ("bidPx", "askPx", time(row.get("ts"), 1000.0)?),
        ExchangeId::Gateio => ("highest_bid", "lowest_ask", None),
        _ => unreachable!(),
    };
    Ok(RestSnapshot {
        data: cryptofeed_ticker::Ticker {
            exchange: info.exchange,
            symbol: info.symbol.clone(),
            bid: decimal(&row[bid])?,
            ask: decimal(&row[ask])?,
            exchange_ts: exchange_ts.unwrap_or(received_ts),
            received_ts,
            implied_volatility: None,
        },
        exchange_ts,
        received_ts,
        sequence: sequence(row.get("lastUpdateId"))?,
    })
}
#[cfg(feature = "orderbook")]
fn levels(value: &Value, bids: bool, depth: u16) -> Result<Vec<cryptofeed_orderbook::PriceLevel>> {
    let rows = value
        .as_array()
        .ok_or_else(|| Error::MalformedData("REST book levels missing".into()))?;
    let mut levels = std::collections::BTreeMap::new();
    for row in rows {
        let (price, amount) = if row.is_array() {
            (decimal(&row[0])?, decimal(&row[1])?)
        } else {
            (decimal(&row["p"])?, decimal(&row["s"])?)
        };
        if amount < rust_decimal::Decimal::ZERO || levels.insert(price, amount).is_some() {
            return Err(Error::MalformedData(
                "negative or duplicated REST book level".into(),
            ));
        }
    }
    let mut levels: Vec<_> = levels
        .into_iter()
        .filter(|(_, amount)| !amount.is_zero())
        .map(|(price, amount)| cryptofeed_orderbook::PriceLevel { price, amount })
        .collect();
    if bids {
        levels.reverse();
    }
    levels.truncate(usize::from(depth));
    Ok(levels)
}
#[cfg(feature = "orderbook")]
pub(super) fn book(
    info: &MarketInfo,
    plan: &Plan,
    payload: &Value,
    depth: u16,
    received_ts: f64,
) -> Result<RestSnapshot<cryptofeed_orderbook::L2BookSnapshot>> {
    let root = data(info.exchange, payload)?;
    let row = if let Some(rows) = root.as_array() {
        if rows.len() != 1 {
            return Err(Error::MalformedData(
                "REST book response must have one snapshot".into(),
            ));
        }
        &rows[0]
    } else {
        root
    };
    if !row.is_object() {
        return Err(Error::MalformedData("REST book snapshot missing".into()));
    }
    for key in ["symbol", "s", "instId", "currency_pair", "contract"] {
        if row.get(key).is_some() {
            identity(info, row, key)?;
        }
    }
    category(plan, row, "category")?;
    let (bids, asks, exchange_ts, seq) = match info.exchange {
        ExchangeId::Binance => (
            "bids",
            "asks",
            time(
                row.get("T")
                    .filter(|value| !value.is_null())
                    .or_else(|| row.get("E")),
                1000.0,
            )?,
            sequence(row.get("lastUpdateId"))?,
        ),
        ExchangeId::Bitget => ("b", "a", time(row.get("ts"), 1000.0)?, None),
        ExchangeId::Bybit => (
            "b",
            "a",
            time(
                row.get("cts")
                    .filter(|value| !value.is_null())
                    .or_else(|| row.get("ts")),
                1000.0,
            )?,
            sequence(row.get("u"))?,
        ),
        ExchangeId::Okx => (
            "bids",
            "asks",
            time(row.get("ts"), 1000.0)?,
            sequence(row.get("seqId"))?,
        ),
        ExchangeId::Gateio => (
            "bids",
            "asks",
            time(
                row.get("update")
                    .filter(|value| !value.is_null())
                    .or_else(|| row.get("current")),
                if info.symbol.kind() == InstrumentKind::Spot {
                    1000.0
                } else {
                    1.0
                },
            )?,
            sequence(row.get("id"))?,
        ),
        _ => unreachable!(),
    };
    Ok(RestSnapshot {
        data: cryptofeed_orderbook::L2BookSnapshot {
            exchange: info.exchange,
            symbol: info.symbol.clone(),
            bids: levels(&row[bids], true, depth)?,
            asks: levels(&row[asks], false, depth)?,
            exchange_ts: exchange_ts.unwrap_or(received_ts),
            received_ts,
        },
        exchange_ts,
        received_ts,
        sequence: seq,
    })
}

#[cfg(all(test, feature = "ticker", feature = "orderbook"))]
mod tests {
    use super::*;
    use cryptofeed_core::symbol::Symbol;
    use serde_json::json;
    fn info(exchange: ExchangeId, kind: InstrumentKind) -> MarketInfo {
        let symbol = if kind == InstrumentKind::Spot {
            Symbol::spot("BTC", "USDT")
        } else {
            Symbol::perpetual("BTC", "USDT")
        };
        let native = match exchange {
            ExchangeId::Okx if kind == InstrumentKind::Spot => "BTC-USDT",
            ExchangeId::Okx => "BTC-USDT-SWAP",
            ExchangeId::Gateio => "BTC_USDT",
            _ => "BTCUSDT",
        };
        MarketInfo::new(exchange, symbol, native)
    }
    #[test]
    fn request_plans_use_current_product_endpoints_and_exact_native_query() {
        for exchange in [
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            for kind in [InstrumentKind::Spot, InstrumentKind::Perpetual] {
                let info = info(exchange, kind);
                let ticker = plan(&info, None).unwrap();
                let book = plan(&info, Some(20)).unwrap();
                assert!(
                    ticker
                        .url
                        .query_pairs()
                        .any(|(_, value)| value == info.exchange_symbol)
                );
                assert!(book.url.query_pairs().any(|(_, value)| value == "20"));
                assert!(plan(&info, Some(0)).is_err());
                assert!(plan(&info, Some(6000)).is_err());
                if exchange == ExchangeId::Bitget {
                    assert!(ticker.url.path().starts_with("/api/v3/"));
                }
                if exchange == ExchangeId::Binance && kind == InstrumentKind::Spot {
                    assert_eq!(book.url.host_str(), Some("data-api.binance.vision"));
                }
            }
        }
        let coin = MarketInfo::new(
            ExchangeId::Binance,
            Symbol::perpetual("BTC", "USD"),
            "BTCUSD_PERP",
        );
        assert_eq!(
            plan(&coin, None).unwrap().url.path(),
            "/dapi/v1/ticker/bookTicker"
        );
        assert!(plan(&coin, Some(3)).is_err());
        let gate = MarketInfo::new(
            ExchangeId::Gateio,
            Symbol::futures("BTC", "USDT", "261225"),
            "BTC_USDT_20261225",
        );
        assert_eq!(
            plan(&gate, None).unwrap().url.path(),
            "/api/v4/delivery/usdt/tickers"
        );
        let bybit = MarketInfo::new(
            ExchangeId::Bybit,
            Symbol::futures("BTC", "USD", "261225"),
            "BTCUSDZ26",
        );
        assert!(
            plan(&bybit, None)
                .unwrap()
                .url
                .query_pairs()
                .any(|(key, value)| key == "category" && value == "inverse")
        );
    }
    #[test]
    fn ticker_families_keep_catalog_identity_native_timestamps_and_missing_time() {
        let cases = [
            (
                ExchangeId::Binance,
                r#"{"symbol":"BTCUSDT","bidPrice":"100.01","askPrice":"101.02"}"#,
                None,
            ),
            (
                ExchangeId::Bitget,
                r#"{"code":"00000","data":[{"symbol":"BTCUSDT","category":"SPOT","bid1Price":"100.01","ask1Price":"101.02","ts":"1710000000123"}]}"#,
                Some(1710000000.123),
            ),
            (
                ExchangeId::Bybit,
                r#"{"retCode":0,"time":1710000000123,"result":{"category":"spot","list":[{"symbol":"BTCUSDT","bid1Price":"100.01","ask1Price":"101.02"}]}}"#,
                Some(1710000000.123),
            ),
            (
                ExchangeId::Okx,
                r#"{"code":"0","data":[{"instId":"BTC-USDT","instType":"SPOT","bidPx":"100.01","askPx":"101.02","ts":"1710000000123"}]}"#,
                Some(1710000000.123),
            ),
            (
                ExchangeId::Gateio,
                r#"[{"currency_pair":"BTC_USDT","highest_bid":"100.01","lowest_ask":"101.02"}]"#,
                None,
            ),
        ];
        for (exchange, raw, native_time) in cases {
            let info = info(exchange, InstrumentKind::Spot);
            let reply = ticker(
                &info,
                &plan(&info, None).unwrap(),
                &serde_json::from_str(raw).unwrap(),
                1710000001.5,
            )
            .unwrap();
            assert_eq!(reply.data.symbol, info.symbol);
            assert_eq!(reply.data.exchange, exchange);
            assert_eq!(reply.data.bid.to_string(), "100.01");
            assert_eq!(reply.data.ask.to_string(), "101.02");
            assert_eq!(reply.exchange_ts, native_time);
            assert_eq!(reply.data.exchange_ts, native_time.unwrap_or(1710000001.5));
            assert_eq!(reply.received_ts, reply.data.received_ts);
        }
        let coin = MarketInfo::new(
            ExchangeId::Binance,
            Symbol::perpetual("BTC", "USD"),
            "BTCUSD_PERP",
        );
        let reply=ticker(&coin,&plan(&coin,None).unwrap(),&json!([{"symbol":"BTCUSD_PERP","bidPrice":"100","askPrice":"101","time":1710000000000i64,"lastUpdateId":9007199254740993u64}]),1710000001.5).unwrap();
        assert_eq!(reply.sequence, Some(9007199254740993));
        assert_eq!(reply.data.symbol, coin.symbol);
        let gate = info(ExchangeId::Gateio, InstrumentKind::Perpetual);
        assert!(
            ticker(
                &gate,
                &plan(&gate, None).unwrap(),
                &json!([{"contract":"BTC_USDT","highest_bid":"100","lowest_ask":"101"}]),
                1710000001.5
            )
            .is_ok()
        );
    }
    #[test]
    fn book_families_keep_precision_sequence_and_sorted_native_levels() {
        let cases = [
            (
                ExchangeId::Binance,
                r#"{"lastUpdateId":9007199254740993,"bids":[["99","2"],["100","1"]],"asks":[["102","2"],["101","1"]]}"#,
            ),
            (
                ExchangeId::Bitget,
                r#"{"code":"00000","data":{"b":[[99,2],[100,1]],"a":[[102,2],[101,1]],"ts":"1710000000123"}}"#,
            ),
            (
                ExchangeId::Bybit,
                r#"{"retCode":0,"result":{"s":"BTCUSDT","b":[["99","2"],["100","1"]],"a":[["102","2"],["101","1"]],"ts":1710000000123,"cts":1710000000100,"u":9007199254740993}}"#,
            ),
            (
                ExchangeId::Okx,
                r#"{"code":"0","data":[{"bids":[["99","2","0","1"],["100","1","0","1"]],"asks":[["102","2","0","1"],["101","1","0","1"]],"ts":"1710000000123","seqId":9007199254740993}]}"#,
            ),
            (
                ExchangeId::Gateio,
                r#"{"id":9007199254740993,"current":1710000000500,"update":1710000000400,"bids":[["99","2"],["100","1"]],"asks":[["102","2"],["101","1"]]}"#,
            ),
        ];
        for (exchange, raw) in cases {
            let info = info(exchange, InstrumentKind::Spot);
            let reply = book(
                &info,
                &plan(&info, Some(20)).unwrap(),
                &serde_json::from_str(raw).unwrap(),
                20,
                1710000001.5,
            )
            .unwrap();
            assert_eq!(reply.data.symbol, info.symbol);
            assert_eq!(reply.data.bids[0].price.to_string(), "100");
            assert_eq!(reply.data.asks[0].price.to_string(), "101");
            if exchange == ExchangeId::Gateio {
                assert_eq!(reply.exchange_ts, Some(1710000000.4));
            }
            if matches!(
                exchange,
                ExchangeId::Binance | ExchangeId::Bybit | ExchangeId::Okx | ExchangeId::Gateio
            ) {
                assert_eq!(reply.sequence, Some(9007199254740993));
            }
        }
        let info = info(ExchangeId::Bitget, InstrumentKind::Spot);
        let payload:Value=serde_json::from_str(r#"{"code":"00000","data":{"b":[[100.12345678901234567890123456,0.1234567890123456789012345678]],"a":[],"ts":"1710000000000"}}"#).unwrap();
        let reply = book(
            &info,
            &plan(&info, Some(20)).unwrap(),
            &payload,
            20,
            1710000001.5,
        )
        .unwrap();
        assert_eq!(
            reply.data.bids[0].price.to_string(),
            "100.12345678901234567890123456"
        );
        assert_eq!(
            reply.data.bids[0].amount.to_string(),
            "0.1234567890123456789012345678"
        );
        let gate = info_for_gate();
        let reply = book(
            &gate,
            &plan(&gate, Some(20)).unwrap(),
            &json!({"id":1,"bids":[{"p":"100","s":"0.25"}],"asks":[],"update":1710000000.5}),
            20,
            1710000001.5,
        )
        .unwrap();
        assert_eq!(reply.data.bids[0].amount.to_string(), "0.25");
    }
    #[test]
    fn gate_book_time_units_follow_product_schema_not_number_magnitude() {
        let spot = info(ExchangeId::Gateio, InstrumentKind::Spot);
        let row = json!({"id":1,"current":4500,"update":4000,"bids":[],"asks":[]});
        assert_eq!(
            book(&spot, &plan(&spot, Some(20)).unwrap(), &row, 20, 5.0)
                .unwrap()
                .exchange_ts,
            Some(4.0)
        );
        let future = info_for_gate();
        let row = json!({"id":1,"current":4.5,"update":4.0,"bids":[],"asks":[]});
        assert_eq!(
            book(&future, &plan(&future, Some(20)).unwrap(), &row, 20, 5.0)
                .unwrap()
                .exchange_ts,
            Some(4.0)
        );
    }
    fn info_for_gate() -> MarketInfo {
        info(ExchangeId::Gateio, InstrumentKind::Perpetual)
    }
    #[test]
    fn foreign_duplicate_failed_or_malformed_replies_do_not_become_snapshots() {
        let info = info(ExchangeId::Bybit, InstrumentKind::Spot);
        let plan = plan(&info, None).unwrap();
        for payload in [
            json!({"retCode":10006}),
            json!({"retCode":0,"result":{"category":"linear","list":[]}}),
            json!({"retCode":0,"result":{"category":"spot","list":[{"symbol":"ETHUSDT"}]}}),
        ] {
            assert!(ticker(&info, &plan, &payload, 1710000001.5).is_err());
        }
        let info = info_for_gate();
        let plan = super::plan(&info, Some(20)).unwrap();
        assert!(
            book(
                &info,
                &plan,
                &json!({"bids":[{"p":"100","s":"1"},{"p":"100","s":"2"}],"asks":[]}),
                20,
                1710000001.5
            )
            .is_err()
        );
        assert!(
            book(
                &info,
                &plan,
                &json!({"bids":[{"p":"100","s":"-1"}],"asks":[]}),
                20,
                1710000001.5
            )
            .is_err()
        );
        assert!(
            book(
                &info,
                &plan,
                &json!({"contract":"FOREIGN","bids":[],"asks":[]}),
                20,
                1710000001.5
            )
            .is_err()
        );
    }
}

//! Bounded candle history. Time windows advance even across empty source ranges.
use super::{adapter, received_time};
use crate::market_info::MarketInfo;
use chrono::{Datelike, Months, TimeZone, Utc};
use cryptofeed_candles::Candle;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::{InstrumentKind, Symbol},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, future::Future};
use url::Url;

#[derive(Clone, Debug)]
pub struct CandleHistoryQuery {
    start_ms: u64,
    end_ms: u64,
    interval: String,
    page_size: u16,
    max_pages: u16,
    cursor: Option<CandleHistoryCursor>,
}
impl CandleHistoryQuery {
    /// Select bars by open time in [start_ms, end_ms). Interval is normalized.
    pub fn new(start_ms: u64, end_ms: u64, interval: impl Into<String>) -> Result<Self> {
        if start_ms >= end_ms || end_ms > i64::MAX as u64 {
            return Err(Error::InvalidConfiguration(
                "invalid candle history range".into(),
            ));
        }
        let interval = interval.into();
        if crate::exchange::binance::adapter::candle_interval_wire(&interval).is_none() {
            return Err(Error::InvalidConfiguration(
                "invalid candle history interval".into(),
            ));
        }
        Ok(Self {
            start_ms,
            end_ms,
            interval,
            page_size: 100,
            max_pages: 10,
            cursor: None,
        })
    }
    /// Common SDK cap: 100 bars per page, 100 requests per call.
    pub fn limits(mut self, page_size: u16, max_pages: u16) -> Result<Self> {
        if !(1..=100).contains(&page_size) || !(1..=100).contains(&max_pages) {
            return Err(Error::InvalidConfiguration(
                "candle history limits must be 1..=100".into(),
            ));
        }
        self.page_size = page_size;
        self.max_pages = max_pages;
        Ok(self)
    }
    pub fn resume(mut self, cursor: CandleHistoryCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
    pub fn maximum_rows(&self) -> usize {
        usize::from(self.page_size) * usize::from(self.max_pages)
    }
}
/// Versioned JSON cursor bound to the exact catalog mapping and query.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandleHistoryCursor {
    version: u16,
    exchange: ExchangeId,
    symbol: Symbol,
    native: String,
    start_ms: u64,
    end_ms: u64,
    interval: String,
    page_size: u16,
    before_ms: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CandleHistoryStop {
    /// All requested windows queried; does not guarantee retention/completeness.
    RangeBoundary,
    BudgetReached,
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CandleHistory {
    /// Ascending within this batch. Subsequent batches contain older bars.
    pub records: Vec<Candle>,
    pub pages: usize,
    pub scanned_rows: usize,
    pub stop: CandleHistoryStop,
    pub next: Option<CandleHistoryCursor>,
}
fn invalid() -> Error {
    Error::MalformedData("invalid REST candle history data".into())
}
fn millis(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|time| *time <= i64::MAX as u64)
        .ok_or_else(invalid)
}
fn duration(interval: &str) -> Result<u64> {
    let (number, unit) = interval.split_at(interval.len() - 1);
    let scale = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return Err(invalid()),
    };
    number
        .parse::<u64>()
        .map_err(|_| invalid())?
        .checked_mul(scale)
        .ok_or_else(invalid)
}
fn date(time: u64) -> Result<chrono::DateTime<Utc>> {
    chrono::DateTime::from_timestamp_millis(time as i64).ok_or_else(invalid)
}
fn month_end(start: u64) -> Result<u64> {
    date(start)?
        .checked_add_months(Months::new(1))
        .and_then(|end| u64::try_from(end.timestamp_millis()).ok())
        .ok_or_else(invalid)
}
fn window_start(q: &CandleHistoryQuery, before: u64) -> Result<u64> {
    let lower = if q.interval == "1M" {
        let last = date(before - 1)?;
        let first = Utc
            .with_ymd_and_hms(last.year(), last.month(), 1, 0, 0, 0)
            .single()
            .ok_or_else(invalid)?;
        first
            .checked_sub_months(Months::new(u32::from(q.page_size - 1)))
            .map_or(0, |date| date.timestamp_millis().max(0) as u64)
    } else {
        before.saturating_sub(duration(&q.interval)? * u64::from(q.page_size))
    };
    // COIN-M permits at most 200 days per request, including monthly windows.
    Ok(lower
        .max(before.saturating_sub(200 * 86_400_000))
        .max(q.start_ms))
}
fn category(info: &MarketInfo) -> Result<&'static str> {
    use crate::exchange::bybit::adapter::{BybitAdapter, BybitProduct};
    match BybitAdapter::product_for_symbol(&info.symbol) {
        BybitProduct::Spot => Ok("spot"),
        BybitProduct::Linear => Ok("linear"),
        BybitProduct::Inverse => Ok("inverse"),
        _ => Err(Error::UnsupportedCapability(
            "Bybit candle history product".into(),
        )),
    }
}
fn plan(info: &MarketInfo, q: &CandleHistoryQuery, lower: u64, before: u64) -> Result<Url> {
    use crate::exchange::binance::adapter::{BinanceProduct, product_from_normalized};
    use crate::exchange::bybit::adapter::BybitAdapter;
    if !matches!(
        info.symbol.kind(),
        InstrumentKind::Spot | InstrumentKind::Perpetual | InstrumentKind::Futures
    ) {
        return Err(Error::UnsupportedCapability(
            "candle history product".into(),
        ));
    }
    let (base, interval, start_key, end_key) = match info.exchange {
        ExchangeId::Binance => (
            match product_from_normalized(&info.symbol)? {
                BinanceProduct::Spot => "https://data-api.binance.vision/api/v3/klines",
                BinanceProduct::UsdM => "https://fapi.binance.com/fapi/v1/klines",
                BinanceProduct::CoinM => "https://dapi.binance.com/dapi/v1/klines",
                _ => {
                    return Err(Error::UnsupportedCapability(
                        "Binance candle history product".into(),
                    ));
                }
            },
            q.interval.as_str(),
            "startTime",
            "endTime",
        ),
        ExchangeId::Bybit => (
            "https://api.bybit.com/v5/market/kline",
            BybitAdapter::candle_interval_wire(&q.interval).ok_or_else(|| {
                Error::UnsupportedCapability("Bybit candle history interval".into())
            })?,
            "start",
            "end",
        ),
        _ => {
            return Err(Error::UnsupportedCapability(
                "candle history currently supports Binance and Bybit".into(),
            ));
        }
    };
    let mut url = Url::parse(base).expect("static candle history URL");
    let mut params = url.query_pairs_mut();
    params
        .append_pair("symbol", &info.exchange_symbol)
        .append_pair("interval", interval)
        .append_pair("limit", &q.page_size.to_string())
        .append_pair(start_key, &lower.to_string())
        .append_pair(end_key, &(before - 1).to_string());
    if info.exchange == ExchangeId::Bybit {
        params.append_pair("category", category(info)?);
    }
    drop(params);
    Ok(url)
}
fn decode(
    info: &MarketInfo,
    q: &CandleHistoryQuery,
    row: &Value,
    received_ts: f64,
) -> Result<(u64, Candle)> {
    let row = row.as_array().ok_or_else(invalid)?;
    let binance = info.exchange == ExchangeId::Binance;
    if row.len() != if binance { 12 } else { 7 } {
        return Err(invalid());
    }
    let start = millis(&row[0])?;
    let end = if binance {
        millis(&row[6])?
    } else {
        (if q.interval == "1M" {
            month_end(start)?
        } else {
            start
                .checked_add(duration(&q.interval)?)
                .ok_or_else(invalid)?
        }) - 1
    };
    if end < start {
        return Err(invalid());
    }
    let open = adapter::decimal(&row[1])?;
    let high = adapter::decimal(&row[2])?;
    let low = adapter::decimal(&row[3])?;
    let close = adapter::decimal(&row[4])?;
    let volume = adapter::decimal(&row[5])?;
    if low > high
        || open < low
        || open > high
        || close < low
        || close > high
        || volume.is_sign_negative()
    {
        return Err(invalid());
    }
    Ok((
        start,
        Candle {
            exchange: info.exchange,
            symbol: info.symbol.clone(),
            start: start as f64 / 1000.0,
            end: end as f64 / 1000.0,
            interval: q.interval.clone(),
            trades: if binance {
                Some(millis(&row[8])?)
            } else {
                None
            },
            open,
            high,
            low,
            close,
            volume,
            closed: None,
            exchange_ts: start as f64 / 1000.0,
            received_ts,
        },
    ))
}
pub(super) async fn collect<F, Fut>(
    info: &MarketInfo,
    q: CandleHistoryQuery,
    mut fetch: F,
) -> Result<CandleHistory>
where
    F: FnMut(Url) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    let mut before = q.end_ms;
    if let Some(c) = &q.cursor {
        if c.version != 1
            || c.exchange != info.exchange
            || c.symbol != info.symbol
            || c.native != info.exchange_symbol
            || c.start_ms != q.start_ms
            || c.end_ms != q.end_ms
            || c.interval != q.interval
            || c.page_size != q.page_size
            || c.before_ms <= q.start_ms
            || c.before_ms >= q.end_ms
        {
            return Err(Error::InvalidConfiguration(
                "candle history cursor scope/position mismatch".into(),
            ));
        }
        before = c.before_ms;
    }
    let mut records = BTreeMap::new();
    let mut pages = 0;
    let mut scanned_rows = 0;
    while pages < usize::from(q.max_pages) && before > q.start_ms {
        let lower = window_start(&q, before)?;
        let payload = fetch(plan(info, &q, lower, before)?).await?;
        let root = adapter::data(info.exchange, &payload)?;
        let rows = if info.exchange == ExchangeId::Bybit {
            adapter::identity(info, root, "symbol")?;
            if root["category"].as_str() != Some(category(info)?) {
                return Err(invalid());
            }
            &root["list"]
        } else {
            root
        };
        let rows = rows.as_array().ok_or_else(invalid)?;
        if rows.len() > usize::from(q.page_size) {
            return Err(invalid());
        }
        let received_ts = received_time();
        for row in rows {
            let (time, candle) = decode(info, &q, row, received_ts)?;
            if time < lower || time >= before || records.insert(time, candle).is_some() {
                return Err(invalid());
            }
        }
        pages += 1;
        scanned_rows += rows.len();
        before = lower;
    }
    let next = (before > q.start_ms).then(|| CandleHistoryCursor {
        version: 1,
        exchange: info.exchange,
        symbol: info.symbol.clone(),
        native: info.exchange_symbol.clone(),
        start_ms: q.start_ms,
        end_ms: q.end_ms,
        interval: q.interval,
        page_size: q.page_size,
        before_ms: before,
    });
    Ok(CandleHistory {
        records: records.into_values().collect(),
        pages,
        scanned_rows,
        stop: if next.is_some() {
            CandleHistoryStop::BudgetReached
        } else {
            CandleHistoryStop::RangeBoundary
        },
        next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn info(exchange: ExchangeId) -> MarketInfo {
        MarketInfo::new(exchange, Symbol::perpetual("BTC", "USDT"), "BTCUSDT")
    }
    fn payload(exchange: ExchangeId, times: &[u64]) -> Value {
        let rows: Vec<_> = times
            .iter()
            .map(|t| {
                if exchange == ExchangeId::Binance {
                    json!([
                        t,
                        "100.0000000000000000000000001",
                        "102",
                        "99",
                        "101",
                        "3.25",
                        t + 59_999,
                        "328",
                        7,
                        "1",
                        "101",
                        "0"
                    ])
                } else {
                    json!([
                        t.to_string(),
                        "100.0000000000000000000000001",
                        "102",
                        "99",
                        "101",
                        "3.25",
                        "328"
                    ])
                }
            })
            .collect();
        if exchange == ExchangeId::Bybit {
            json!({"retCode":0,"result":{"symbol":"BTCUSDT","category":"linear","list":rows}})
        } else {
            json!(rows)
        }
    }
    #[test]
    fn normalization_preserves_native_fields_without_inferred_finality() {
        for exchange in [ExchangeId::Binance, ExchangeId::Bybit] {
            let p = payload(exchange, &[60_000]);
            let row = if exchange == ExchangeId::Binance {
                &p[0]
            } else {
                &p["result"]["list"][0]
            };
            let (_, candle) = decode(
                &info(exchange),
                &CandleHistoryQuery::new(0, 120_000, "1m").unwrap(),
                row,
                123.0,
            )
            .unwrap();
            assert_eq!(candle.start, 60.0);
            assert_eq!(candle.end, 119.999);
            assert_eq!(candle.open.to_string(), "100.0000000000000000000000001");
            assert_eq!(candle.volume.to_string(), "3.25");
            assert_eq!(
                candle.trades,
                (exchange == ExchangeId::Binance).then_some(7)
            );
            assert_eq!(candle.closed, None);
            assert_eq!(candle.exchange_ts, 60.0);
            assert_eq!(candle.received_ts, 123.0);
        }
    }
    #[tokio::test]
    async fn backward_windows_resume_without_duplicates_in_either_source_order() {
        for exchange in [ExchangeId::Binance, ExchangeId::Bybit] {
            let q = CandleHistoryQuery::new(0, 300_000, "1m")
                .unwrap()
                .limits(2, 1)
                .unwrap();
            let first = collect(&info(exchange), q.clone(), |url| async move {
                let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
                assert_eq!(
                    params[if exchange == ExchangeId::Binance {
                        "startTime"
                    } else {
                        "start"
                    }],
                    "180000"
                );
                assert_eq!(
                    params[if exchange == ExchangeId::Binance {
                        "endTime"
                    } else {
                        "end"
                    }],
                    "299999"
                );
                Ok(payload(exchange, &[240_000, 180_000]))
            })
            .await
            .unwrap();
            assert_eq!(
                first.records.iter().map(|c| c.start).collect::<Vec<_>>(),
                vec![180.0, 240.0]
            );
            assert_eq!(first.stop, CandleHistoryStop::BudgetReached);
            let cursor =
                serde_json::from_str(&serde_json::to_string(&first.next.unwrap()).unwrap())
                    .unwrap();
            let second = collect(&info(exchange), q.resume(cursor), |_| async {
                Ok(payload(exchange, &[60_000, 120_000]))
            })
            .await
            .unwrap();
            assert_eq!(
                second.records.iter().map(|c| c.start).collect::<Vec<_>>(),
                vec![60.0, 120.0]
            );
            assert!(second.next.is_some());
        }
    }
    #[tokio::test]
    async fn empty_and_short_windows_advance_with_finite_budget() {
        let q = CandleHistoryQuery::new(0, 600_000, "1m")
            .unwrap()
            .limits(2, 2)
            .unwrap();
        let mut requests = 0;
        let result = collect(&info(ExchangeId::Binance), q, |_| {
            requests += 1;
            async { Ok(json!([])) }
        })
        .await
        .unwrap();
        assert_eq!(requests, 2);
        assert_eq!(result.scanned_rows, 0);
        assert_eq!(result.next.unwrap().before_ms, 360_000);
        let q = CandleHistoryQuery::new(1, 120_000, "1m")
            .unwrap()
            .limits(2, 2)
            .unwrap();
        let result = collect(&info(ExchangeId::Binance), q, |_| async {
            Ok(payload(ExchangeId::Binance, &[60_000]))
        })
        .await
        .unwrap();
        assert_eq!(result.stop, CandleHistoryStop::RangeBoundary);
        assert!(result.next.is_none());
    }
    #[test]
    fn monthly_calendar_and_coin_m_window_cap() {
        let feb = 1_706_745_600_000;
        assert_eq!(month_end(feb).unwrap(), 1_709_251_200_000); // leap-year March 1
        let q = CandleHistoryQuery::new(0, 1_714_521_600_000, "1M")
            .unwrap()
            .limits(2, 1)
            .unwrap();
        assert_eq!(window_start(&q, q.end_ms).unwrap(), 1_709_251_200_000); // March+April
        let q = q.limits(100, 1).unwrap();
        assert_eq!(
            q.end_ms - window_start(&q, q.end_ms).unwrap(),
            200 * 86_400_000
        );
        let mut cm = info(ExchangeId::Binance);
        cm.symbol = Symbol::perpetual("BTC", "USD");
        cm.exchange_symbol = "BTCUSD_PERP".into();
        assert!(
            plan(&cm, &q, window_start(&q, q.end_ms).unwrap(), q.end_ms)
                .unwrap()
                .as_str()
                .starts_with("https://dapi.binance.com/dapi/v1/klines?")
        );
        let row = json!([feb.to_string(), "100", "102", "99", "101", "3", "303"]);
        let (_, candle) = decode(&info(ExchangeId::Bybit), &q, &row, 0.0).unwrap();
        assert_eq!(candle.end, 1_709_251_199.999);
    }
    #[tokio::test]
    async fn cursor_scope_and_unsupported_requests_fail_before_fetch() {
        let q = CandleHistoryQuery::new(0, 600_000, "1m")
            .unwrap()
            .limits(2, 1)
            .unwrap();
        let first = collect(&info(ExchangeId::Binance), q.clone(), |_| async {
            Ok(json!([]))
        })
        .await
        .unwrap();
        let cursor = serde_json::to_value(first.next.unwrap()).unwrap();
        for (key, value) in [
            ("version", json!(2)),
            ("native", json!("OTHER")),
            ("exchange", json!("Bybit")),
            (
                "symbol",
                serde_json::to_value(Symbol::spot("ETH", "USDT")).unwrap(),
            ),
            ("interval", json!("1h")),
            ("start_ms", json!(1)),
            ("end_ms", json!(600001)),
            ("page_size", json!(3)),
            ("before_ms", json!(600000)),
        ] {
            let mut changed = cursor.clone();
            changed[key] = value;
            let cursor: CandleHistoryCursor = serde_json::from_value(changed).unwrap();
            assert!(
                collect(
                    &info(ExchangeId::Binance),
                    q.clone().resume(cursor),
                    |_| async { panic!("must not fetch") }
                )
                .await
                .is_err(),
                "{key}"
            );
        }
        for exchange in [ExchangeId::Bitget, ExchangeId::Okx, ExchangeId::Gateio] {
            assert!(
                collect(&info(exchange), q.clone(), |_| async {
                    panic!("must not fetch")
                })
                .await
                .is_err()
            );
        }
        assert!(
            collect(
                &info(ExchangeId::Bybit),
                CandleHistoryQuery::new(0, 600_000, "8h").unwrap(),
                |_| async { panic!("must not fetch") }
            )
            .await
            .is_err()
        );
        assert!(
            CandleHistoryQuery::new(0, 1, "1m")
                .unwrap()
                .limits(101, 1)
                .is_err()
        );
        assert!(CandleHistoryQuery::new(1, 1, "1m").is_err());
    }
    #[tokio::test]
    async fn invalid_native_payloads_fail_without_skipping_records() {
        let exchange = ExchangeId::Binance;
        let q = CandleHistoryQuery::new(60_000, 180_000, "1m")
            .unwrap()
            .limits(2, 1)
            .unwrap();
        for value in [
            payload(exchange, &[0]),
            payload(exchange, &[180_000]),
            payload(exchange, &[60_000, 60_000]),
            payload(exchange, &[60_000, 120_000, 150_000]),
            json!({"code":-1}),
        ] {
            assert!(
                collect(&info(exchange), q.clone(), |_| std::future::ready(Ok(
                    value.clone()
                )))
                .await
                .is_err()
            );
        }
        let mut p = payload(ExchangeId::Bybit, &[60_000]);
        p["result"]["symbol"] = json!("ETHUSDT");
        assert!(
            collect(&info(ExchangeId::Bybit), q.clone(), |_| std::future::ready(
                Ok(p.clone())
            ))
            .await
            .is_err()
        );
        p["result"]["symbol"] = json!("BTCUSDT");
        p["result"]["category"] = json!("spot");
        assert!(
            collect(&info(ExchangeId::Bybit), q, |_| std::future::ready(Ok(
                p.clone()
            )))
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn dropping_query_cancels_in_flight_fetch_without_background_work() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct DropMark(Arc<AtomicBool>);
        impl Drop for DropMark {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let info = info(ExchangeId::Binance);
        let q = CandleHistoryQuery::new(0, 600_000, "1m").unwrap();
        let fetch = |_| {
            let mark = DropMark(dropped.clone());
            async move {
                let _mark = mark;
                std::future::pending::<Result<Value>>().await
            }
        };
        let mut future = Box::pin(collect(&info, q, fetch));
        assert!(futures::poll!(future.as_mut()).is_pending());
        drop(future);
        assert!(dropped.load(Ordering::SeqCst));
    }
}

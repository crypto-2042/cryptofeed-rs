//! Bounded candle history. Time windows advance even across empty source ranges.
use super::{adapter, received_time};
use crate::market_info::MarketInfo;
use chrono::{Datelike, FixedOffset, Months, TimeZone, Utc};
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
        if crate::exchange::binance::adapter::candle_interval_wire(&interval).is_none()
            && !matches!(interval.as_str(), "10s" | "3M")
        {
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
fn gate_millis(value: &Value) -> Result<u64> {
    let time = adapter::decimal(value)?
        .checked_mul(rust_decimal::Decimal::from(1000))
        .ok_or_else(invalid)?;
    if !time.fract().is_zero() {
        return Err(invalid());
    }
    time.normalize()
        .to_string()
        .parse::<u64>()
        .ok()
        .filter(|time| *time <= i64::MAX as u64)
        .ok_or_else(invalid)
}
fn duration(interval: &str) -> Result<u64> {
    let (number, unit) = interval.split_at(interval.len() - 1);
    let scale = match unit {
        "s" => 1_000,
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
fn month_offset(info: &MarketInfo) -> FixedOffset {
    FixedOffset::east_opt(if info.exchange == ExchangeId::Okx {
        8 * 3600
    } else {
        0
    })
    .expect("static calendar offset")
}
fn months(interval: &str) -> u32 {
    if interval == "3M" { 3 } else { 1 }
}
fn month_end(info: &MarketInfo, interval: &str, start: u64) -> Result<u64> {
    date(start)?
        .with_timezone(&month_offset(info))
        .checked_add_months(Months::new(months(interval)))
        .and_then(|end| u64::try_from(end.timestamp_millis()).ok())
        .ok_or_else(invalid)
}
fn window_start(info: &MarketInfo, q: &CandleHistoryQuery, before: u64) -> Result<u64> {
    let lower = if q.interval.ends_with('M') {
        let offset = month_offset(info);
        let last = date(before - 1)?.with_timezone(&offset);
        let step = months(&q.interval);
        let month = (last.month0() / step) * step + 1;
        let first = offset
            .with_ymd_and_hms(last.year(), month, 1, 0, 0, 0)
            .single()
            .ok_or_else(invalid)?;
        first
            .checked_sub_months(Months::new(u32::from(q.page_size - 1) * step))
            .map_or(0, |date| date.timestamp_millis().max(0) as u64)
    } else {
        before.saturating_sub(duration(&q.interval)? * u64::from(q.page_size))
    };
    // Bitget limits history windows to 90 days; COIN-M permits 200 days.
    let (end, days) = if info.exchange == ExchangeId::Bitget {
        let period = duration(&q.interval)?;
        (
            ((before - 1) / period + 1)
                .checked_mul(period)
                .ok_or_else(invalid)?,
            90,
        )
    } else {
        (before, 200)
    };
    Ok(lower
        .max(end.saturating_sub(days * 86_400_000))
        .max(q.start_ms))
}
fn interval_wire<'a>(info: &MarketInfo, interval: &'a str) -> Result<&'a str> {
    use crate::exchange::{
        binance::adapter::candle_interval_wire, bitget::adapter::BitgetAdapter,
        bybit::adapter::BybitAdapter, gateio::adapter::GateioAdapter, okx::adapter::OkxAdapter,
    };
    let wire = match info.exchange {
        ExchangeId::Binance => candle_interval_wire(interval),
        ExchangeId::Bybit => BybitAdapter::candle_interval_wire(interval),
        ExchangeId::Bitget => BitgetAdapter::candle_interval_wire(interval),
        ExchangeId::Okx => OkxAdapter::candle_interval_wire(interval),
        // REST uses 1d, whereas the established WS adapter uses 24h.
        ExchangeId::Gateio => {
            if interval == "1d" {
                Some("1d")
            } else {
                GateioAdapter::candle_interval_wire(interval)
            }
        }
        _ => None,
    };
    wire.ok_or_else(|| Error::UnsupportedCapability("candle history exchange/interval".into()))
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
fn gate_first_start(info: &MarketInfo, q: &CandleHistoryQuery, lower: u64) -> Result<u64> {
    if q.interval == "1M" {
        let date = date(lower)?;
        let first = Utc
            .with_ymd_and_hms(date.year(), date.month(), 1, 0, 0, 0)
            .single()
            .ok_or_else(invalid)?;
        if u64::try_from(first.timestamp_millis()).map_err(|_| invalid())? == lower {
            Ok(lower)
        } else {
            first
                .checked_add_months(Months::new(1))
                .and_then(|next| u64::try_from(next.timestamp_millis()).ok())
                .ok_or_else(invalid)
        }
    } else {
        let period = duration(&q.interval)?;
        // Spot 7d opens Monday; perpetual 7d is explicitly epoch-aligned.
        let anchor = if q.interval == "1w" && info.symbol.kind() == InstrumentKind::Spot {
            4 * 86_400_000
        } else {
            0
        };
        lower
            .saturating_sub(anchor)
            .div_ceil(period)
            .checked_mul(period)
            .and_then(|time| time.checked_add(anchor))
            .ok_or_else(invalid)
    }
}
fn plan(info: &MarketInfo, q: &CandleHistoryQuery, lower: u64, before: u64) -> Result<Url> {
    use crate::exchange::{
        binance::adapter::{BinanceProduct, product_from_normalized},
        gateio::adapter::{GateioProduct, product_from_symbol},
    };
    if !matches!(
        info.symbol.kind(),
        InstrumentKind::Spot | InstrumentKind::Perpetual | InstrumentKind::Futures
    ) {
        return Err(Error::UnsupportedCapability(
            "candle history product".into(),
        ));
    }
    let interval = interval_wire(info, &q.interval)?;
    let base = match info.exchange {
        ExchangeId::Binance => match product_from_normalized(&info.symbol)? {
            BinanceProduct::Spot => "https://data-api.binance.vision/api/v3/klines",
            BinanceProduct::UsdM => "https://fapi.binance.com/fapi/v1/klines",
            BinanceProduct::CoinM => "https://dapi.binance.com/dapi/v1/klines",
            _ => {
                return Err(Error::UnsupportedCapability(
                    "Binance candle history product".into(),
                ));
            }
        },
        ExchangeId::Bybit => "https://api.bybit.com/v5/market/kline",
        ExchangeId::Bitget => "https://api.bitget.com/api/v3/market/history-candles",
        ExchangeId::Okx => "https://openapi.okx.com/api/v5/market/history-candles",
        ExchangeId::Gateio => match product_from_symbol(&info.symbol)? {
            GateioProduct::Spot => "https://api.gateio.ws/api/v4/spot/candlesticks",
            GateioProduct::UsdtPerpetual => {
                "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
            }
            GateioProduct::BtcPerpetual => "https://api.gateio.ws/api/v4/futures/btc/candlesticks",
            GateioProduct::UsdtDelivery => {
                return Err(Error::UnsupportedCapability(
                    "Gate delivery candle history is undocumented".into(),
                ));
            }
        },
        _ => return Err(Error::UnsupportedExchange(format!("{:?}", info.exchange))),
    };
    let mut url = Url::parse(base).expect("static candle history URL");
    let mut params = url.query_pairs_mut();
    let symbol_key = match info.exchange {
        ExchangeId::Okx => "instId",
        ExchangeId::Gateio if info.symbol.kind() == InstrumentKind::Spot => "currency_pair",
        ExchangeId::Gateio => "contract",
        _ => "symbol",
    };
    params
        .append_pair(symbol_key, &info.exchange_symbol)
        .append_pair(
            if info.exchange == ExchangeId::Okx {
                "bar"
            } else {
                "interval"
            },
            interval,
        );
    if info.exchange != ExchangeId::Gateio {
        params.append_pair("limit", &q.page_size.to_string());
    }
    match info.exchange {
        ExchangeId::Binance => {
            params
                .append_pair("startTime", &lower.to_string())
                .append_pair("endTime", &(before - 1).to_string());
        }
        ExchangeId::Bybit => {
            params
                .append_pair("start", &lower.to_string())
                .append_pair("end", &(before - 1).to_string())
                .append_pair("category", category(info)?);
        }
        ExchangeId::Bitget => {
            // Exact end boundaries avoid the documented extra earlier interval.
            let duration = duration(&q.interval)?;
            let end = ((before - 1) / duration + 1)
                .checked_mul(duration)
                .ok_or_else(invalid)?;
            params
                .append_pair("startTime", &lower.to_string())
                .append_pair("endTime", &end.to_string())
                .append_pair(
                    "category",
                    &crate::exchange::bitget::adapter::bitget_instrument_type(&info.symbol)
                        .to_ascii_uppercase(),
                );
        }
        ExchangeId::Okx => {
            params.append_pair("after", &before.to_string());
            // before is exclusive; zero is safe because epoch has no older bar.
            params.append_pair("before", &lower.saturating_sub(1).to_string());
        }
        ExchangeId::Gateio => {
            params
                .append_pair(
                    "from",
                    &(gate_first_start(info, q, lower)? / 1000).to_string(),
                )
                .append_pair("to", &((before - 1) / 1000).to_string());
        }
        _ => unreachable!(),
    }
    drop(params);
    Ok(url)
}
fn closed(value: &Value) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::String(value) if value == "true" || value == "1" => Ok(true),
        Value::String(value) if value == "false" || value == "0" => Ok(false),
        _ => Err(invalid()),
    }
}
fn decode(
    info: &MarketInfo,
    q: &CandleHistoryQuery,
    row: &Value,
    received_ts: f64,
) -> Result<(u64, Candle)> {
    let gate = info.exchange == ExchangeId::Gateio;
    let gate_spot = gate && info.symbol.kind() == InstrumentKind::Spot;
    let binance = info.exchange == ExchangeId::Binance;
    let okx = info.exchange == ExchangeId::Okx;
    let (start, open, high, low, close, volume, completion) = if gate && !gate_spot {
        let start = gate_millis(&row["t"])?;
        (
            start, &row["o"], &row["h"], &row["l"], &row["c"], &row["v"], None,
        )
    } else {
        let rows = row.as_array().ok_or_else(invalid)?;
        let count = if binance {
            12
        } else if okx {
            9
        } else if gate_spot {
            8
        } else {
            7
        };
        if rows.len() != count {
            return Err(invalid());
        }
        if gate_spot {
            let start = gate_millis(&rows[0])?;
            (
                start,
                &rows[5],
                &rows[3],
                &rows[4],
                &rows[2],
                &rows[6],
                Some(closed(&rows[7])?),
            )
        } else {
            (
                millis(&rows[0])?,
                &rows[1],
                &rows[2],
                &rows[3],
                &rows[4],
                &rows[5],
                if okx { Some(closed(&rows[8])?) } else { None },
            )
        }
    };
    let end = if binance {
        millis(&row[6])?
    } else {
        let boundary = if q.interval.ends_with('M') {
            month_end(info, &q.interval, start)?
        } else {
            start
                .checked_add(duration(&q.interval)?)
                .ok_or_else(invalid)?
        };
        if info.exchange == ExchangeId::Bybit {
            boundary - 1
        } else {
            boundary
        }
    };
    if end < start {
        return Err(invalid());
    }
    let open = adapter::decimal(open)?;
    let high = adapter::decimal(high)?;
    let low = adapter::decimal(low)?;
    let close = adapter::decimal(close)?;
    let volume = adapter::decimal(volume)?;
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
            closed: completion,
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
        let lower = window_start(info, &q, before)?;
        let url = plan(info, &q, lower, before)?;
        if info.exchange == ExchangeId::Gateio && gate_first_start(info, &q, lower)? >= before {
            before = lower;
            continue;
        }
        let payload = fetch(url).await?;
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
            return Err(Error::MalformedData(format!(
                "candle history page exceeds row budget: {} > {}",
                rows.len(),
                q.page_size
            )));
        }
        let received_ts = received_time();
        let mut earlier_overlap = false;
        for row in rows {
            let (time, candle) = decode(info, &q, row, received_ts)?;
            // Bitget documents one extra earlier interval and was observed to
            // backfill it when the newest historical bar is not yet available.
            // Count it against the raw budget; a later window may emit it.
            if info.exchange == ExchangeId::Bitget
                && time < lower
                && time >= lower.saturating_sub(duration(&q.interval)?)
                && !earlier_overlap
            {
                earlier_overlap = true;
                continue;
            }
            if time < lower || time >= before {
                return Err(Error::MalformedData(format!(
                    "candle open {time} outside requested window [{lower}, {before})"
                )));
            }
            if records.insert(time, candle).is_some() {
                return Err(Error::MalformedData("duplicated candle open time".into()));
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
        MarketInfo::new(
            exchange,
            Symbol::perpetual("BTC", "USDT"),
            match exchange {
                ExchangeId::Okx => "BTC-USDT-SWAP",
                ExchangeId::Gateio => "BTC_USDT",
                _ => "BTCUSDT",
            },
        )
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
        assert_eq!(
            month_end(&info(ExchangeId::Bybit), "1M", feb).unwrap(),
            1_709_251_200_000
        ); // leap-year March 1
        let q = CandleHistoryQuery::new(0, 1_714_521_600_000, "1M")
            .unwrap()
            .limits(2, 1)
            .unwrap();
        assert_eq!(
            window_start(&info(ExchangeId::Binance), &q, q.end_ms).unwrap(),
            1_709_251_200_000
        ); // March+April
        let q = q.limits(100, 1).unwrap();
        assert_eq!(
            q.end_ms - window_start(&info(ExchangeId::Binance), &q, q.end_ms).unwrap(),
            200 * 86_400_000
        );
        let mut cm = info(ExchangeId::Binance);
        cm.symbol = Symbol::perpetual("BTC", "USD");
        cm.exchange_symbol = "BTCUSD_PERP".into();
        assert!(
            plan(
                &cm,
                &q,
                window_start(&info(ExchangeId::Binance), &q, q.end_ms).unwrap(),
                q.end_ms
            )
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
        for exchange in [ExchangeId::Coinbase, ExchangeId::Kraken] {
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
    fn new_payload(exchange: ExchangeId, times: &[u64], spot: bool) -> Value {
        let rows: Vec<_> = times
            .iter()
            .map(|t| match exchange {
                ExchangeId::Bitget => {
                    json!([t.to_string(), "100", "102", "99", "101", "3.25", "328"])
                }
                ExchangeId::Okx => json!([
                    t.to_string(),
                    "100",
                    "102",
                    "99",
                    "101",
                    "3.25",
                    "0.325",
                    "328",
                    "1"
                ]),
                ExchangeId::Gateio if spot => json!([
                    (t / 1000).to_string(),
                    "328",
                    "101",
                    "102",
                    "99",
                    "100",
                    "3.25",
                    "true"
                ]),
                ExchangeId::Gateio => {
                    json!({"t":t/1000,"o":"100","h":"102","l":"99","c":"101","v":325,"sum":"328"})
                }
                _ => unreachable!(),
            })
            .collect();
        match exchange {
            ExchangeId::Bitget => json!({"code":"00000","data":rows}),
            ExchangeId::Okx => json!({"code":"0","data":rows}),
            _ => json!(rows),
        }
    }
    #[tokio::test]
    async fn new_venues_normalize_and_resume_exact_bar_windows() {
        for (exchange, spot) in [
            (ExchangeId::Bitget, false),
            (ExchangeId::Okx, false),
            (ExchangeId::Gateio, false),
            (ExchangeId::Gateio, true),
        ] {
            let mut info = info(exchange);
            if spot {
                info.symbol = Symbol::spot("BTC", "USDT");
            }
            let q = CandleHistoryQuery::new(0, 300_000, "1m")
                .unwrap()
                .limits(2, 1)
                .unwrap();
            let first = collect(&info, q.clone(), |_| async {
                Ok(new_payload(exchange, &[240_000, 180_000], spot))
            })
            .await
            .unwrap();
            let bar = &first.records[0];
            assert_eq!(bar.symbol, info.symbol);
            assert_eq!(bar.start, 180.0);
            assert_eq!(bar.end, 240.0);
            assert_eq!(
                bar.volume.to_string(),
                if exchange == ExchangeId::Gateio && !spot {
                    "325"
                } else {
                    "3.25"
                }
            );
            assert_eq!(
                bar.closed,
                if exchange == ExchangeId::Okx || spot {
                    Some(true)
                } else {
                    None
                }
            );
            let cursor =
                serde_json::from_str(&serde_json::to_string(&first.next.unwrap()).unwrap())
                    .unwrap();
            let next = collect(&info, q.resume(cursor), |_| async {
                Ok(new_payload(exchange, &[120_000, 60_000], spot))
            })
            .await
            .unwrap();
            assert_eq!(
                next.records.iter().map(|c| c.start).collect::<Vec<_>>(),
                vec![60.0, 120.0]
            );
            assert_eq!(next.pages, 1);
        }
    }
    #[test]
    fn native_parameters_use_current_versions_units_and_exclusive_bounds() {
        let q = CandleHistoryQuery::new(1, 180_001, "1m")
            .unwrap()
            .limits(3, 1)
            .unwrap();
        for exchange in [ExchangeId::Bitget, ExchangeId::Okx, ExchangeId::Gateio] {
            let info = info(exchange);
            let lower = window_start(&info, &q, q.end_ms).unwrap();
            let url = plan(&info, &q, lower, q.end_ms).unwrap();
            let params: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            match exchange {
                ExchangeId::Bitget => {
                    assert_eq!(url.path(), "/api/v3/market/history-candles");
                    assert_eq!(params["endTime"], "240000");
                    assert_eq!(params["startTime"], "1");
                    assert_eq!(params["category"], "USDT-FUTURES");
                }
                ExchangeId::Okx => {
                    assert_eq!(params["after"], "180001");
                    assert_eq!(params["before"], "0");
                    assert_eq!(params["instId"], "BTC-USDT-SWAP");
                }
                ExchangeId::Gateio => {
                    assert_eq!(params["from"], "60");
                    assert_eq!(params["to"], "180");
                    assert!(!params.contains_key("limit"));
                    assert_eq!(url.path(), "/api/v4/futures/usdt/candlesticks");
                    assert_eq!(interval_wire(&info, "1d").unwrap(), "1d");
                    assert_eq!(interval_wire(&info, "1M").unwrap(), "30d");
                }
                _ => unreachable!(),
            }
        }
        let info = info(ExchangeId::Bitget);
        let q = CandleHistoryQuery::new(0, 200 * 86_400_000 + 1, "1d").unwrap();
        let lower = window_start(&info, &q, q.end_ms).unwrap();
        let url = plan(&info, &q, lower, q.end_ms).unwrap();
        let p: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(
            p["endTime"].parse::<u64>().unwrap() - p["startTime"].parse::<u64>().unwrap(),
            90 * 86_400_000
        );
    }
    #[test]
    fn okx_quarters_and_gate_months_preserve_calendar_semantics() {
        let okx = info(ExchangeId::Okx);
        let q = CandleHistoryQuery::new(0, 1_714_492_800_000, "3M")
            .unwrap()
            .limits(1, 1)
            .unwrap();
        // 2024-04-30 16:00 UTC is May 1 in UTC+8; quarter opens April 1.
        assert_eq!(window_start(&okx, &q, q.end_ms).unwrap(), 1_711_900_800_000);
        let jan = 1_704_038_400_000; // Jan 1 00:00 UTC+8
        assert_eq!(month_end(&okx, "3M", jan).unwrap(), 1_711_900_800_000);
        let gate = info(ExchangeId::Gateio);
        let feb = 1_706_745_600_000;
        assert_eq!(month_end(&gate, "1M", feb).unwrap(), 1_709_251_200_000);
        assert_eq!(gate_millis(&json!(60.0)).unwrap(), 60_000);
        assert_eq!(gate_millis(&json!("60.001")).unwrap(), 60_001);
        assert!(gate_millis(&json!("60.0001")).is_err());
        let q = CandleHistoryQuery::new(0, 1_714_521_600_000, "1M").unwrap();
        assert_eq!(
            gate_first_start(&gate, &q, feb + 1).unwrap(),
            1_709_251_200_000
        );
        assert_eq!(gate_first_start(&gate, &q, feb).unwrap(), feb);
        let q = CandleHistoryQuery::new(0, 1_788_825_600_000, "1w").unwrap();
        let mut spot = gate.clone();
        spot.symbol = Symbol::spot("BTC", "USDT");
        assert_eq!(
            gate_first_start(&spot, &q, 1_788_220_800_000).unwrap(),
            1_788_739_200_000
        );
        assert_eq!(
            gate_first_start(&gate, &q, 1_788_220_800_000).unwrap(),
            1_788_393_600_000
        );
    }
    #[tokio::test]
    async fn gate_subsecond_empty_window_and_delivery_reject_before_http() {
        let info = info(ExchangeId::Gateio);
        let q = CandleHistoryQuery::new(60_001, 60_002, "10s").unwrap();
        let result = collect(&info, q, |_| async {
            panic!("empty second range must not fetch")
        })
        .await
        .unwrap();
        assert_eq!(result.pages, 0);
        assert_eq!(result.stop, CandleHistoryStop::RangeBoundary);
        let mut delivery = info.clone();
        delivery.symbol = Symbol::futures("BTC", "USDT", "241227");
        let q = CandleHistoryQuery::new(0, 180_000, "1m").unwrap();
        assert!(
            collect(&delivery, q, |_| async { panic!("undocumented endpoint") })
                .await
                .is_err()
        );
    }
    #[test]
    fn completion_and_volume_shapes_fail_without_guessing() {
        let mut gate = info(ExchangeId::Gateio);
        gate.symbol = Symbol::spot("BTC", "USDT");
        let q = CandleHistoryQuery::new(0, 180_000, "1m").unwrap();
        let old = json!(["60", "328", "101", "102", "99", "100", "true"]);
        assert!(decode(&gate, &q, &old, 0.0).is_err()); // no base quantity in retired example
        let mut row = new_payload(ExchangeId::Gateio, &[60_000], true)[0].clone();
        row[7] = json!(false);
        assert_eq!(decode(&gate, &q, &row, 0.0).unwrap().1.closed, Some(false));
        row[7] = json!("finished");
        assert!(decode(&gate, &q, &row, 0.0).is_err());
        let okx = info(ExchangeId::Okx);
        let mut row = new_payload(ExchangeId::Okx, &[60_000], false)["data"][0].clone();
        row[8] = json!("0");
        assert_eq!(decode(&okx, &q, &row, 0.0).unwrap().1.closed, Some(false));
        row[8] = Value::Null;
        assert!(decode(&okx, &q, &row, 0.0).is_err());
    }
    #[tokio::test]
    async fn bitget_one_earlier_interval_counts_against_budget_without_losing_resume() {
        let info = info(ExchangeId::Bitget);
        let q = CandleHistoryQuery::new(1, 300_001, "1m")
            .unwrap()
            .limits(2, 1)
            .unwrap();
        let first = collect(&info, q.clone(), |_| async {
            Ok(new_payload(ExchangeId::Bitget, &[180_000, 240_000], false))
        })
        .await
        .unwrap();
        assert_eq!(first.scanned_rows, 2);
        assert_eq!(
            first.records.iter().map(|c| c.start).collect::<Vec<_>>(),
            vec![240.0]
        );
        let second = collect(&info, q.clone().resume(first.next.unwrap()), |_| async {
            Ok(new_payload(ExchangeId::Bitget, &[120_000, 180_000], false))
        })
        .await
        .unwrap();
        assert_eq!(
            second.records.iter().map(|c| c.start).collect::<Vec<_>>(),
            vec![120.0, 180.0]
        );
        for times in [
            &[120_000, 240_000][..],
            &[180_000, 180_000][..],
            &[240_000, 360_000][..],
        ] {
            assert!(
                collect(&info, q.clone(), |_| std::future::ready(Ok(new_payload(
                    ExchangeId::Bitget,
                    times,
                    false
                ))))
                .await
                .is_err()
            );
        }
    }
}

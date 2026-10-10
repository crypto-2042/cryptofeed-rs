//! Bounded funding-settlement history with explicit continuation/termination.
use super::{adapter, received_time};
use crate::market_info::MarketInfo;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::{InstrumentKind, Symbol},
};
use cryptofeed_funding::Funding;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, future::Future};
use url::Url;

#[derive(Clone, Debug)]
pub struct FundingHistoryQuery {
    start_ms: u64,
    end_ms: u64,
    page_size: u16,
    max_pages: u16,
    cursor: Option<FundingHistoryCursor>,
}
impl FundingHistoryQuery {
    /// Half-open UTC range [start_ms, end_ms); no floating timestamp rounding.
    pub fn new(start_ms: u64, end_ms: u64) -> Result<Self> {
        if start_ms >= end_ms || end_ms > i64::MAX as u64 {
            return Err(Error::InvalidConfiguration(
                "invalid funding history millisecond range".into(),
            ));
        }
        Ok(Self {
            start_ms,
            end_ms,
            page_size: 100,
            max_pages: 10,
            cursor: None,
        })
    }
    /// SDK common page cap 100 and per-call page cap 100 (at most 10,000 rows).
    pub fn limits(mut self, page_size: u16, max_pages: u16) -> Result<Self> {
        if !(1..=100).contains(&page_size) || !(1..=100).contains(&max_pages) {
            return Err(Error::InvalidConfiguration(
                "funding history limits must be 1..=100".into(),
            ));
        }
        self.page_size = page_size;
        self.max_pages = max_pages;
        Ok(self)
    }
    pub fn resume(mut self, cursor: FundingHistoryCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
    pub fn maximum_rows(&self) -> usize {
        usize::from(self.page_size) * usize::from(self.max_pages)
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Position {
    Forward {
        start_ms: u64,
    },
    Backward {
        end_ms: u64,
    },
    Paged {
        page: u16,
        previous_min_ms: Option<u64>,
    },
}
/// Versioned cursor bound to exchange, exact catalog mapping, range/page size.
/// Deserialized cursors are validated before any HTTP request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingHistoryCursor {
    version: u16,
    exchange: ExchangeId,
    symbol: Symbol,
    native: String,
    start_ms: u64,
    end_ms: u64,
    page_size: u16,
    position: Position,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HistoryStop {
    /// Paging reached the requested boundary; not a guarantee of source completeness.
    RangeBoundary,
    /// Server returned an empty/short page; retention may limit available data.
    SourceExhausted,
    BudgetReached,
    /// Native page cursor ceiling; cannot resume beyond it.
    SourceLimit,
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FundingHistory {
    pub records: Vec<Funding>,
    pub pages: usize,
    pub scanned_rows: usize,
    pub stop: HistoryStop,
    pub next: Option<FundingHistoryCursor>,
}
fn initial(info: &MarketInfo, q: &FundingHistoryQuery) -> Position {
    match info.exchange {
        ExchangeId::Binance => Position::Forward {
            start_ms: q.start_ms,
        },
        ExchangeId::Bitget => Position::Paged {
            page: 1,
            previous_min_ms: None,
        },
        _ => Position::Backward { end_ms: q.end_ms },
    }
}
fn validate(info: &MarketInfo, q: &FundingHistoryQuery) -> Result<Position> {
    if info.symbol.kind() != InstrumentKind::Perpetual {
        return Err(Error::UnsupportedCapability(
            "funding history is perpetual-only".into(),
        ));
    }
    if !matches!(
        info.exchange,
        ExchangeId::Binance
            | ExchangeId::Bitget
            | ExchangeId::Bybit
            | ExchangeId::Okx
            | ExchangeId::Gateio
    ) {
        return Err(Error::UnsupportedExchange(format!("{:?}", info.exchange)));
    }
    let Some(cursor) = &q.cursor else {
        return Ok(initial(info, q));
    };
    let scope = cursor.version == 1
        && cursor.exchange == info.exchange
        && cursor.symbol == info.symbol
        && cursor.native == info.exchange_symbol
        && cursor.start_ms == q.start_ms
        && cursor.end_ms == q.end_ms
        && cursor.page_size == q.page_size;
    let position = match (&cursor.position, info.exchange) {
        (Position::Forward { start_ms }, ExchangeId::Binance) => {
            *start_ms >= q.start_ms && *start_ms < q.end_ms
        }
        (
            Position::Paged {
                page,
                previous_min_ms,
            },
            ExchangeId::Bitget,
        ) => {
            (1..=100).contains(page)
                && if *page == 1 {
                    previous_min_ms.is_none()
                } else {
                    previous_min_ms.is_some_and(|time| time > q.start_ms && time <= i64::MAX as u64)
                }
        }
        (
            Position::Backward { end_ms },
            ExchangeId::Bybit | ExchangeId::Okx | ExchangeId::Gateio,
        ) => *end_ms > q.start_ms && *end_ms <= q.end_ms,
        _ => false,
    };
    if !scope || !position {
        return Err(Error::InvalidConfiguration(
            "funding history cursor scope/position mismatch".into(),
        ));
    }
    Ok(cursor.position.clone())
}
fn request(info: &MarketInfo, q: &FundingHistoryQuery, position: &Position) -> Result<Url> {
    use crate::exchange::{
        binance::adapter::{BinanceProduct, product_from_normalized},
        bybit::adapter::{BybitAdapter, BybitProduct},
        gateio::adapter::{GateioProduct, product_from_symbol},
    };
    let base = match info.exchange {
        ExchangeId::Binance => match product_from_normalized(&info.symbol)? {
            BinanceProduct::UsdM => "https://fapi.binance.com/fapi/v1/fundingRate",
            BinanceProduct::CoinM => "https://dapi.binance.com/dapi/v1/fundingRate",
            _ => {
                return Err(Error::UnsupportedCapability(
                    "Binance funding history product".into(),
                ));
            }
        },
        ExchangeId::Bitget => "https://api.bitget.com/api/v3/market/history-fund-rate",
        ExchangeId::Bybit => "https://api.bybit.com/v5/market/funding/history",
        ExchangeId::Okx => "https://openapi.okx.com/api/v5/public/funding-rate-history",
        ExchangeId::Gateio => match product_from_symbol(&info.symbol)? {
            GateioProduct::UsdtPerpetual => {
                "https://api.gateio.ws/api/v4/futures/usdt/funding_rate"
            }
            GateioProduct::BtcPerpetual => "https://api.gateio.ws/api/v4/futures/btc/funding_rate",
            _ => {
                return Err(Error::UnsupportedCapability(
                    "Gate funding history product".into(),
                ));
            }
        },
        _ => return Err(Error::UnsupportedExchange(format!("{:?}", info.exchange))),
    };
    let mut url = Url::parse(base).expect("static history URL");
    {
        let mut params = url.query_pairs_mut();
        params.append_pair(
            match info.exchange {
                ExchangeId::Okx => "instId",
                ExchangeId::Gateio => "contract",
                _ => "symbol",
            },
            &info.exchange_symbol,
        );
        params.append_pair("limit", &q.page_size.to_string());
        match (info.exchange, position) {
            (ExchangeId::Binance, Position::Forward { start_ms }) => {
                params.append_pair("startTime", &start_ms.to_string());
                params.append_pair("endTime", &(q.end_ms - 1).to_string());
            }
            (ExchangeId::Bitget, Position::Paged { page, .. }) => {
                params.append_pair(
                    "category",
                    &crate::exchange::bitget::adapter::bitget_instrument_type(&info.symbol)
                        .to_ascii_uppercase(),
                );
                params.append_pair("cursor", &page.to_string());
            }
            (ExchangeId::Bybit, Position::Backward { end_ms }) => {
                params.append_pair(
                    "category",
                    match BybitAdapter::product_for_symbol(&info.symbol) {
                        BybitProduct::Linear => "linear",
                        BybitProduct::Inverse => "inverse",
                        _ => {
                            return Err(Error::UnsupportedCapability(
                                "Bybit funding history category".into(),
                            ));
                        }
                    },
                );
                params.append_pair("endTime", &(end_ms - 1).to_string());
            }
            (ExchangeId::Okx, Position::Backward { end_ms }) => {
                params.append_pair("after", &end_ms.to_string());
            }
            (ExchangeId::Gateio, Position::Backward { end_ms }) => {
                params.append_pair("to", &((end_ms - 1) / 1000).to_string());
            }
            _ => {
                return Err(Error::InvalidConfiguration(
                    "invalid funding cursor kind".into(),
                ));
            }
        }
    }
    Ok(url)
}
fn integer(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| Error::MalformedData("invalid funding settlement timestamp".into()))
}
fn rows<'a>(info: &MarketInfo, payload: &'a Value) -> Result<&'a [Value]> {
    let root = adapter::data(info.exchange, payload)?;
    let rows = match info.exchange {
        ExchangeId::Bitget => &root["resultList"],
        ExchangeId::Bybit => &root["list"],
        _ => root,
    };
    if info.exchange == ExchangeId::Bybit {
        if let Some(category) = root.get("category") {
            let expected =
                if crate::exchange::bybit::adapter::BybitAdapter::product_for_symbol(&info.symbol)
                    == crate::exchange::bybit::adapter::BybitProduct::Linear
                {
                    "linear"
                } else {
                    "inverse"
                };
            if category.as_str() != Some(expected) {
                return Err(Error::MalformedData(
                    "funding response product mismatch".into(),
                ));
            }
        }
    }
    rows.as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| Error::MalformedData("funding history rows missing".into()))
}
fn timestamp(info: &MarketInfo, row: &Value) -> Result<u64> {
    for key in ["symbol", "instId", "contract"] {
        if row.get(key).is_some() {
            adapter::identity(info, row, key)?;
        }
    }
    let key = match info.exchange {
        ExchangeId::Bybit | ExchangeId::Bitget => "fundingRateTimestamp",
        ExchangeId::Gateio => "t",
        _ => "fundingTime",
    };
    let raw = integer(&row[key])?;
    let millis = if info.exchange == ExchangeId::Gateio {
        raw.checked_mul(1000)
            .ok_or_else(|| Error::MalformedData("funding timestamp overflow".into()))?
    } else {
        raw
    };
    if millis > i64::MAX as u64 {
        return Err(Error::MalformedData(
            "funding timestamp exceeds protocol range".into(),
        ));
    }
    Ok(millis)
}
fn record(info: &MarketInfo, row: &Value, millis: u64, received_ts: f64) -> Result<Funding> {
    if info.exchange == ExchangeId::Binance
        && row
            .get("rateType")
            .is_some_and(|kind| kind.as_str() != Some("Regular"))
    {
        return Err(Error::UnsupportedCapability(
            "special/non-regular funding settlements are not supported".into(),
        ));
    }
    let key = match info.exchange {
        ExchangeId::Gateio => "r",
        ExchangeId::Okx => "realizedRate",
        _ => "fundingRate",
    };
    if row
        .get(key)
        .is_none_or(|value| value.is_null() || value.as_str() == Some(""))
    {
        return Err(Error::Protocol(
            "actual settled funding rate unavailable in history row".into(),
        ));
    }
    let rate = adapter::decimal(&row[key])?;
    let mark_price = if info.exchange == ExchangeId::Binance {
        match row.get("markPrice") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if value.is_empty() => None,
            Some(value) => Some(adapter::decimal(value)?),
        }
    } else {
        None
    };
    Ok(Funding {
        exchange: info.exchange,
        symbol: info.symbol.clone(),
        mark_price,
        rate: Some(rate),
        next_funding_time: None,
        predicted_rate: None,
        exchange_ts: millis as f64 / 1000.0,
        received_ts,
    })
}
fn cursor(info: &MarketInfo, q: &FundingHistoryQuery, position: Position) -> FundingHistoryCursor {
    FundingHistoryCursor {
        version: 1,
        exchange: info.exchange,
        symbol: info.symbol.clone(),
        native: info.exchange_symbol.clone(),
        start_ms: q.start_ms,
        end_ms: q.end_ms,
        page_size: q.page_size,
        position,
    }
}
pub(super) async fn collect<F, Fut>(
    info: &MarketInfo,
    q: FundingHistoryQuery,
    mut fetch: F,
) -> Result<FundingHistory>
where
    F: FnMut(Url) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    let mut position = validate(info, &q)?;
    let mut records: BTreeMap<u64, Funding> = BTreeMap::new();
    let mut pages = 0usize;
    let mut scanned_rows = 0usize;
    let (stop, next) = loop {
        let url = request(info, &q, &position)?;
        let payload = fetch(url).await?;
        pages += 1;
        let rows = rows(info, &payload)?;
        if rows.len() > usize::from(q.page_size) {
            return Err(Error::MalformedData(
                "funding page exceeds requested row budget".into(),
            ));
        }
        scanned_rows += rows.len();
        if rows.is_empty() {
            break (HistoryStop::SourceExhausted, None);
        }
        let mut minimum = u64::MAX;
        let mut maximum = 0;
        let lower = match position {
            Position::Forward { start_ms } => start_ms,
            _ => q.start_ms,
        };
        let upper = match position {
            Position::Backward { end_ms } => end_ms,
            Position::Paged {
                previous_min_ms: Some(time),
                ..
            } => q.end_ms.min(time),
            _ => q.end_ms,
        };
        let received_ts = received_time();
        for row in rows {
            let millis = timestamp(info, row)?;
            if matches!(position, Position::Forward { .. }) && (millis < lower || millis >= upper)
                || matches!(position, Position::Backward { .. }) && millis >= upper
            {
                return Err(Error::Protocol(
                    "funding source ignored requested time boundary".into(),
                ));
            }
            minimum = minimum.min(millis);
            maximum = maximum.max(millis);
            if millis < lower || millis >= upper {
                continue;
            }
            let next_record = record(info, row, millis, received_ts)?;
            if records.get(&millis).is_some_and(|previous| {
                previous.rate != next_record.rate || previous.mark_price != next_record.mark_price
            }) {
                return Err(Error::MalformedData(
                    "conflicting funding records at one settlement time".into(),
                ));
            }
            records.entry(millis).or_insert(next_record);
        }
        let boundary = match position {
            Position::Forward { .. } => maximum >= q.end_ms - 1,
            _ => minimum <= q.start_ms,
        };
        if boundary {
            break (HistoryStop::RangeBoundary, None);
        }
        let next_position = match position {
            Position::Forward { start_ms } if maximum >= start_ms => Position::Forward {
                start_ms: maximum + 1,
            },
            Position::Backward { end_ms } if minimum < end_ms => {
                Position::Backward { end_ms: minimum }
            }
            Position::Paged {
                page,
                previous_min_ms,
            } if previous_min_ms.is_none_or(|previous| minimum < previous) => {
                if page == 100 {
                    break (HistoryStop::SourceLimit, None);
                }
                Position::Paged {
                    page: page + 1,
                    previous_min_ms: Some(minimum),
                }
            }
            _ if rows.len() < usize::from(q.page_size) => {
                break (HistoryStop::SourceExhausted, None);
            }
            _ => {
                return Err(Error::Protocol(
                    "funding history pagination made no progress".into(),
                ));
            }
        };
        if rows.len() < usize::from(q.page_size) {
            break (HistoryStop::SourceExhausted, None);
        }
        if pages >= usize::from(q.max_pages) {
            break (
                HistoryStop::BudgetReached,
                Some(cursor(info, &q, next_position)),
            );
        }
        position = next_position;
    };
    Ok(FundingHistory {
        records: records.into_values().collect(),
        pages,
        scanned_rows,
        stop,
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
        let rows:Vec<_>=times.iter().map(|time|match exchange {
            ExchangeId::Binance=>json!({"symbol":"BTCUSDT","fundingTime":time,"fundingRate":"-0.0001","markPrice":"100","rateType":"Regular"}),
            ExchangeId::Bitget=>json!({"symbol":"BTCUSDT","fundingRateTimestamp":time.to_string(),"fundingRate":"-0.0001"}),
            ExchangeId::Bybit=>json!({"symbol":"BTCUSDT","fundingRateTimestamp":time.to_string(),"fundingRate":"-0.0001"}),
            ExchangeId::Okx=>json!({"instId":"BTC-USDT-SWAP","fundingTime":time.to_string(),"fundingRate":"0.009","realizedRate":"-0.0001"}),
            ExchangeId::Gateio=>json!({"t":time/1000,"r":"-0.0001"}),
            _=>unreachable!(),
        }).collect();
        match exchange {
            ExchangeId::Bitget => json!({"code":"00000","data":{"resultList":rows}}),
            ExchangeId::Bybit => json!({"retCode":0,"result":{"category":"linear","list":rows}}),
            ExchangeId::Okx => json!({"code":"0","data":rows}),
            _ => json!(rows),
        }
    }
    #[tokio::test]
    async fn every_venue_normalizes_actual_settlements_and_native_time_units() {
        for exchange in [
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            let query = FundingHistoryQuery::new(1000, 10000)
                .unwrap()
                .limits(2, 1)
                .unwrap();
            let data = payload(
                exchange,
                if exchange == ExchangeId::Binance {
                    &[1000, 2000]
                } else {
                    &[9000, 8000]
                },
            );
            let result = collect(&info(exchange), query, move |_| {
                let data = data.clone();
                async move { Ok(data) }
            })
            .await
            .unwrap();
            assert_eq!(result.pages, 1);
            assert_eq!(result.scanned_rows, 2);
            for record in &result.records {
                assert_eq!(record.exchange, exchange);
                assert_eq!(record.rate, Some(rust_decimal::Decimal::new(-1, 4)));
                assert_eq!(record.predicted_rate, None);
                assert_eq!(record.next_funding_time, None);
                assert!(record.exchange_ts >= 1.0 && record.exchange_ts < 10.0);
            }
            assert!(result.records[0].exchange_ts < result.records[1].exchange_ts);
        }
    }
    #[tokio::test]
    async fn forward_budget_cursor_roundtrips_and_resumes_without_repeating_records() {
        let info = info(ExchangeId::Binance);
        let query = FundingHistoryQuery::new(0, 10000)
            .unwrap()
            .limits(2, 1)
            .unwrap();
        assert_eq!(query.maximum_rows(), 2);
        let first = collect(&info, query.clone(), |url| async move {
            assert!(
                url.query_pairs()
                    .any(|(key, value)| key == "startTime" && value == "0")
            );
            Ok(payload(ExchangeId::Binance, &[1000, 2000]))
        })
        .await
        .unwrap();
        assert_eq!(first.stop, HistoryStop::BudgetReached);
        let encoded = serde_json::to_vec(&first.next.unwrap()).unwrap();
        let cursor: FundingHistoryCursor = serde_json::from_slice(&encoded).unwrap();
        let next = collect(&info, query.resume(cursor), |url| async move {
            assert!(
                url.query_pairs()
                    .any(|(key, value)| key == "startTime" && value == "2001")
            );
            Ok(payload(ExchangeId::Binance, &[3000]))
        })
        .await
        .unwrap();
        assert_eq!(next.stop, HistoryStop::SourceExhausted);
        assert!(next.next.is_none());
        assert_eq!(next.records.len(), 1);
        assert_eq!(next.records[0].exchange_ts, 3.0);
    }
    #[tokio::test]
    async fn descending_and_offset_cursors_advance_using_native_boundaries() {
        for exchange in [
            ExchangeId::Bitget,
            ExchangeId::Bybit,
            ExchangeId::Okx,
            ExchangeId::Gateio,
        ] {
            let info = info(exchange);
            let query = FundingHistoryQuery::new(1000, 10000)
                .unwrap()
                .limits(2, 1)
                .unwrap();
            let first = collect(&info, query.clone(), |_| async move {
                Ok(payload(exchange, &[9000, 8000]))
            })
            .await
            .unwrap();
            let next = collect(&info, query.resume(first.next.unwrap()), |url| async move {
                let pairs: std::collections::HashMap<_, _> = url
                    .query_pairs()
                    .map(|(key, value)| (key.into_owned(), value.into_owned()))
                    .collect();
                match exchange {
                    ExchangeId::Bitget => {
                        assert_eq!(pairs["cursor"], "2");
                        assert!(!pairs.contains_key("pageNo"));
                    }
                    ExchangeId::Bybit => {
                        assert_eq!(pairs["endTime"], "7999");
                        assert!(!pairs.contains_key("startTime"));
                    }
                    ExchangeId::Okx => assert_eq!(pairs["after"], "8000"),
                    ExchangeId::Gateio => assert_eq!(pairs["to"], "7"),
                    _ => unreachable!(),
                }
                Ok(payload(exchange, &[7000]))
            })
            .await
            .unwrap();
            assert_eq!(next.records.len(), 1);
            assert_eq!(next.records[0].exchange_ts, 7.0);
        }
    }
    #[tokio::test]
    async fn wrong_scope_or_budget_is_rejected_before_fetch() {
        assert!(FundingHistoryQuery::new(1, 1).is_err());
        assert!(
            FundingHistoryQuery::new(0, 10)
                .unwrap()
                .limits(0, 1)
                .is_err()
        );
        assert!(
            FundingHistoryQuery::new(0, 10)
                .unwrap()
                .limits(1, 101)
                .is_err()
        );
        let info = info(ExchangeId::Binance);
        let query = FundingHistoryQuery::new(0, 10000)
            .unwrap()
            .limits(1, 1)
            .unwrap();
        let result = collect(&info, query.clone(), |_| async {
            Ok(payload(ExchangeId::Binance, &[1000]))
        })
        .await
        .unwrap();
        let valid = result.next.unwrap();
        let mut invalid = Vec::new();
        let mut cursor = valid.clone();
        cursor.exchange = ExchangeId::Bybit;
        invalid.push(cursor);
        let mut cursor = valid.clone();
        cursor.version = 2;
        invalid.push(cursor);
        let mut cursor = valid.clone();
        cursor.symbol = Symbol::perpetual("ETH", "USDT");
        invalid.push(cursor);
        let mut cursor = valid.clone();
        cursor.native = "ETHUSDT".into();
        invalid.push(cursor);
        let mut cursor = valid.clone();
        cursor.start_ms += 1;
        invalid.push(cursor);
        let mut cursor = valid.clone();
        cursor.end_ms -= 1;
        invalid.push(cursor);
        let mut cursor = valid.clone();
        cursor.page_size = 2;
        invalid.push(cursor);
        let mut cursor = valid;
        cursor.position = Position::Backward { end_ms: 9000 };
        invalid.push(cursor);
        for cursor in invalid {
            assert!(
                collect(&info, query.clone().resume(cursor), |_| async {
                    panic!("must not fetch")
                })
                .await
                .is_err()
            );
        }
        let spot = MarketInfo::new(ExchangeId::Binance, Symbol::spot("BTC", "USDT"), "BTCUSDT");
        assert!(
            collect(&spot, FundingHistoryQuery::new(0, 10).unwrap(), |_| async {
                panic!("must not fetch")
            })
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn missing_actual_rate_foreign_records_conflicts_and_oversized_pages_fail() {
        let info = info(ExchangeId::Okx);
        let query = FundingHistoryQuery::new(0, 10000)
            .unwrap()
            .limits(2, 1)
            .unwrap();
        let missing = json!({"code":"0","data":[{"instId":"BTC-USDT-SWAP","fundingTime":"1000","fundingRate":"0.1","realizedRate":""}]});
        assert!(
            collect(&info, query.clone(), |_| {
                let data = missing.clone();
                async move { Ok(data) }
            })
            .await
            .is_err()
        );
        let foreign = json!({"code":"0","data":[{"instId":"ETH-USDT-SWAP","fundingTime":"1000","realizedRate":"0.1"}]});
        assert!(
            collect(&info, query.clone(), |_| {
                let data = foreign.clone();
                async move { Ok(data) }
            })
            .await
            .is_err()
        );
        let conflict = json!({"code":"0","data":[{"instId":"BTC-USDT-SWAP","fundingTime":"1000","realizedRate":"0.1"},{"instId":"BTC-USDT-SWAP","fundingTime":"1000","realizedRate":"0.2"}]});
        assert!(
            collect(&info, query.clone(), |_| {
                let data = conflict.clone();
                async move { Ok(data) }
            })
            .await
            .is_err()
        );
        assert!(
            collect(&info, query, |_| async {
                Ok(payload(ExchangeId::Okx, &[9000, 8000, 7000]))
            })
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn repeated_full_pages_fail_instead_of_looping() {
        let info = info(ExchangeId::Bitget);
        let query = FundingHistoryQuery::new(0, 10000)
            .unwrap()
            .limits(1, 10)
            .unwrap();
        let mut calls = 0;
        assert!(
            collect(&info, query, |_| {
                calls += 1;
                async { Ok(payload(ExchangeId::Bitget, &[9000])) }
            })
            .await
            .is_err()
        );
        assert_eq!(calls, 2);
    }
    #[tokio::test]
    async fn gate_half_open_window_filters_second_granularity_and_native_page_ceiling_is_explicit()
    {
        let info = info(ExchangeId::Gateio);
        let query = FundingHistoryQuery::new(1500, 2500)
            .unwrap()
            .limits(2, 1)
            .unwrap();
        let result = collect(&info, query, |url| async move {
            assert!(
                url.query_pairs()
                    .any(|(key, value)| key == "to" && value == "2")
            );
            Ok(payload(ExchangeId::Gateio, &[2000, 1000]))
        })
        .await
        .unwrap();
        assert_eq!(result.stop, HistoryStop::RangeBoundary);
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].exchange_ts, 2.0);
        let info = super::tests::info(ExchangeId::Bitget);
        let mut query = FundingHistoryQuery::new(0, 10000)
            .unwrap()
            .limits(1, 1)
            .unwrap();
        query.cursor = Some(cursor(
            &info,
            &query,
            Position::Paged {
                page: 100,
                previous_min_ms: Some(9000),
            },
        ));
        let result = collect(&info, query, |_| async {
            Ok(payload(ExchangeId::Bitget, &[8000]))
        })
        .await
        .unwrap();
        assert_eq!(result.stop, HistoryStop::SourceLimit);
        assert!(result.next.is_none());
    }
    #[tokio::test]
    async fn cancellation_drops_pending_page_and_starts_no_more_requests() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let task = {
            let calls = calls.clone();
            let dropped = dropped.clone();
            let started = started.clone();
            tokio::spawn(async move {
                collect(
                    &info(ExchangeId::Binance),
                    FundingHistoryQuery::new(0, 10000)
                        .unwrap()
                        .limits(1, 5)
                        .unwrap(),
                    move |_| {
                        let index = calls.fetch_add(1, Ordering::SeqCst);
                        let dropped = dropped.clone();
                        let started = started.clone();
                        async move {
                            if index == 0 {
                                Ok(payload(ExchangeId::Binance, &[1000]))
                            } else {
                                let _guard = DropFlag(dropped);
                                started.notify_one();
                                std::future::pending::<Result<Value>>().await
                            }
                        }
                    },
                )
                .await
            })
        };
        started.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn exact_negative_json_rate_and_empty_page_have_explicit_semantics() {
        let info = info(ExchangeId::Binance);
        let query = FundingHistoryQuery::new(0, 10000).unwrap();
        let value:Value=serde_json::from_str(r#"[{"symbol":"BTCUSDT","fundingTime":1000,"fundingRate":-0.1234567890123456789012345678}]"#).unwrap();
        let result = collect(&info, query.clone(), |_| {
            let value = value.clone();
            async move { Ok(value) }
        })
        .await
        .unwrap();
        assert_eq!(
            result.records[0].rate.unwrap().to_string(),
            "-0.1234567890123456789012345678"
        );
        let result = collect(&info, query, |_| async { Ok(json!([])) })
            .await
            .unwrap();
        assert_eq!(result.stop, HistoryStop::SourceExhausted);
        assert_eq!(result.scanned_rows, 0);
        assert!(result.records.is_empty());
    }
}

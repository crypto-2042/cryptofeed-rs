//! ID-aware public trade history; never advance a dense page by timestamp alone.
use super::{received_time, trades};
use crate::market_info::MarketInfo;
use cryptofeed_core::{
    error::{Error, Result},
    exchange::ExchangeId,
    symbol::{InstrumentKind, Symbol},
};
use cryptofeed_trade::Trade;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{cmp::Ordering, collections::HashSet, future::Future};
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeHistoryKind {
    Individual,
    Aggregate,
}
#[derive(Clone, Debug)]
pub struct TradeHistoryQuery {
    start_ms: u64,
    end_ms: u64,
    kind: TradeHistoryKind,
    page_size: u16,
    max_pages: u16,
    cursor: Option<TradeHistoryCursor>,
}
impl TradeHistoryQuery {
    /// Half-open integer millisecond range; callers explicitly select granularity.
    pub fn new(start_ms: u64, end_ms: u64, kind: TradeHistoryKind) -> Result<Self> {
        if start_ms >= end_ms || end_ms > i64::MAX as u64 {
            return Err(Error::InvalidConfiguration(
                "invalid trade history range".into(),
            ));
        }
        Ok(Self {
            start_ms,
            end_ms,
            kind,
            page_size: 100,
            max_pages: 10,
            cursor: None,
        })
    }
    /// At most 100 rows/page and 100 requests/call, including filtered boundary rows.
    pub fn limits(mut self, page_size: u16, max_pages: u16) -> Result<Self> {
        if !(1..=100).contains(&page_size) || !(1..=100).contains(&max_pages) {
            return Err(Error::InvalidConfiguration(
                "trade history limits must be 1..=100".into(),
            ));
        }
        self.page_size = page_size;
        self.max_pages = max_pages;
        Ok(self)
    }
    pub fn resume(mut self, cursor: TradeHistoryCursor) -> Self {
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
    Binance {
        window_start_ms: u64,
        next_id: Option<u64>,
    },
    Okx {
        after_id: Option<String>,
    },
    Gate {
        page: u32,
        last_min_id: Option<String>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeHistoryCursor {
    version: u16,
    exchange: ExchangeId,
    symbol: Symbol,
    native: String,
    start_ms: u64,
    end_ms: u64,
    kind: TradeHistoryKind,
    page_size: u16,
    position: Position,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TradeHistoryStop {
    /// Boundary reached or seed windows queried; not proof of retained completeness.
    RangeBoundary,
    /// Empty/short source page; retention may constrain availability.
    SourceExhausted,
    /// Per-call request budget reached; a continuation is supplied.
    BudgetReached,
    /// Native/SDK cursor ceiling; no continuation past this point.
    SourceLimit,
}
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TradeHistory {
    pub records: Vec<Trade>,
    pub kind: TradeHistoryKind,
    pub pages: usize,
    pub scanned_rows: usize,
    pub stop: TradeHistoryStop,
    pub next: Option<TradeHistoryCursor>,
}
fn malformed() -> Error {
    Error::MalformedData("invalid or non-progressing trade history page".into())
}
fn numeric_id(id: &str) -> Result<&str> {
    if id.is_empty() || !id.bytes().all(|c| c.is_ascii_digit()) {
        return Err(malformed());
    }
    let id = id.trim_start_matches('0');
    Ok(if id.is_empty() { "0" } else { id })
}
fn compare_ids(a: &str, b: &str) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}
fn initial(info: &MarketInfo, q: &TradeHistoryQuery) -> Result<Position> {
    match (info.exchange, q.kind) {
        (ExchangeId::Binance, TradeHistoryKind::Aggregate) => Ok(Position::Binance {
            window_start_ms: q.start_ms,
            next_id: None,
        }),
        (ExchangeId::Okx, TradeHistoryKind::Individual) => Ok(Position::Okx { after_id: None }),
        (ExchangeId::Gateio, TradeHistoryKind::Individual) => Ok(Position::Gate {
            page: 1,
            last_min_id: None,
        }),
        _ => Err(Error::UnsupportedCapability(
            "public trade history exchange/product/granularity".into(),
        )),
    }
}
fn validate(info: &MarketInfo, q: &TradeHistoryQuery) -> Result<Position> {
    let initial = initial(info, q)?;
    let Some(c) = &q.cursor else {
        return Ok(initial);
    };
    let position = match &c.position {
        Position::Binance {
            window_start_ms,
            next_id,
        } => {
            matches!(initial, Position::Binance { .. })
                && *window_start_ms >= q.start_ms
                && *window_start_ms < q.end_ms
                && next_id.is_none_or(|id| id <= i64::MAX as u64)
        }
        Position::Okx { after_id } => {
            matches!(initial, Position::Okx { .. })
                && after_id.as_ref().is_some_and(|id| numeric_id(id).is_ok())
        }
        Position::Gate { page, last_min_id } => {
            matches!(initial, Position::Gate { .. })
                && info.symbol.kind() != InstrumentKind::Futures
                && *page > 1
                && u64::from(page - 1) * u64::from(q.page_size) <= 100_000
                && last_min_id
                    .as_ref()
                    .is_some_and(|id| numeric_id(id).is_ok())
        }
    };
    if c.version != 1
        || c.exchange != info.exchange
        || c.symbol != info.symbol
        || c.native != info.exchange_symbol
        || c.start_ms != q.start_ms
        || c.end_ms != q.end_ms
        || c.kind != q.kind
        || c.page_size != q.page_size
        || !position
    {
        return Err(Error::InvalidConfiguration(
            "trade history cursor scope/position mismatch".into(),
        ));
    }
    Ok(c.position.clone())
}
fn plan(info: &MarketInfo, q: &TradeHistoryQuery, p: &Position) -> Result<trades::Plan> {
    let mut plan = trades::plan(info, q.page_size)?;
    match p {
        Position::Binance {
            window_start_ms,
            next_id,
        } => {
            let path = plan.url.path().replace("/trades", "/aggTrades");
            plan.url.set_path(&path);
            plan.aggregate = true;
            if let Some(id) = next_id {
                plan.url
                    .query_pairs_mut()
                    .append_pair("fromId", &id.to_string());
            } else {
                let end = window_start_ms.saturating_add(3_600_000).min(q.end_ms);
                plan.url
                    .query_pairs_mut()
                    .append_pair("startTime", &window_start_ms.to_string())
                    .append_pair("endTime", &(end - 1).to_string());
            }
        }
        Position::Okx { after_id } => {
            plan.url.set_path("/api/v5/market/history-trades");
            plan.url
                .query_pairs_mut()
                .append_pair("type", if after_id.is_some() { "1" } else { "2" })
                .append_pair(
                    "after",
                    &after_id.clone().unwrap_or_else(|| q.end_ms.to_string()),
                );
        }
        Position::Gate { page, .. } => {
            // Include the enclosing second at both ends, then filter exactly.
            plan.url
                .query_pairs_mut()
                .append_pair("from", &(q.start_ms / 1000).to_string())
                .append_pair("to", &q.end_ms.div_ceil(1000).to_string());
            if info.symbol.kind() != InstrumentKind::Futures {
                plan.url.query_pairs_mut().append_pair(
                    if info.symbol.kind() == InstrumentKind::Spot {
                        "page"
                    } else {
                        "offset"
                    },
                    &if info.symbol.kind() == InstrumentKind::Spot {
                        page.to_string()
                    } else {
                        (u64::from(page - 1) * u64::from(q.page_size)).to_string()
                    },
                );
            }
        }
    }
    Ok(plan)
}
pub(super) async fn collect<F, Fut>(
    info: &MarketInfo,
    q: TradeHistoryQuery,
    mut fetch: F,
) -> Result<TradeHistory>
where
    F: FnMut(Url) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    let mut position = validate(info, &q)?;
    let mut records = Vec::new();
    let mut seen = HashSet::new();
    let mut pages = 0;
    let mut scanned_rows = 0;
    let lower = Decimal::from(q.start_ms) / Decimal::from(1000);
    let upper = Decimal::from(q.end_ms) / Decimal::from(1000);
    let stop = loop {
        let plan = plan(info, &q, &position)?;
        let payload = fetch(plan.url.clone()).await?;
        let mut rows = trades::decode_timed(info, &plan, &payload, q.page_size, received_time())?;
        pages += 1;
        scanned_rows += rows.len();
        if rows.is_empty() {
            if let Position::Binance {
                window_start_ms,
                next_id: None,
            } = &position
            {
                let next = window_start_ms.saturating_add(3_600_000).min(q.end_ms);
                if next == q.end_ms {
                    break TradeHistoryStop::RangeBoundary;
                }
                position = Position::Binance {
                    window_start_ms: next,
                    next_id: None,
                };
            } else {
                break TradeHistoryStop::SourceExhausted;
            }
        } else {
            // Numeric ID ordering is arbitrary-width and independent of f64 clocks.
            let mut ids: Vec<_> = rows
                .iter()
                .map(|(_, trade)| {
                    numeric_id(trade.id.as_deref().ok_or_else(malformed)?).map(str::to_owned)
                })
                .collect::<Result<_>>()?;
            ids.sort_by(|a, b| compare_ids(a, b));
            if ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(malformed());
            }
            let min_id = ids.first().expect("nonempty").clone();
            let max_id = ids.last().expect("nonempty").clone();
            let min_time = rows.first().expect("sorted nonempty").0;
            let max_time = rows.last().expect("sorted nonempty").0;
            match &position {
                Position::Binance {
                    window_start_ms,
                    next_id,
                } => {
                    if min_time < lower
                        || next_id.is_some_and(|id| {
                            compare_ids(&min_id, &id.to_string()) == Ordering::Less
                        })
                    {
                        return Err(malformed());
                    }
                    if next_id.is_none() {
                        let end = window_start_ms.saturating_add(3_600_000).min(q.end_ms);
                        if min_time < Decimal::from(*window_start_ms) / Decimal::from(1000)
                            || max_time >= Decimal::from(end) / Decimal::from(1000)
                        {
                            return Err(malformed());
                        }
                    }
                }
                Position::Okx { after_id } => {
                    if max_time >= upper
                        || after_id.as_ref().is_some_and(|after| {
                            compare_ids(&max_id, numeric_id(after).expect("validated"))
                                != Ordering::Less
                        })
                    {
                        return Err(malformed());
                    }
                }
                Position::Gate { last_min_id, .. } => {
                    let native_lower = Decimal::from(q.start_ms / 1000);
                    let native_upper = Decimal::from(q.end_ms.div_ceil(1000) + 1);
                    if min_time < native_lower
                        || max_time >= native_upper
                        || last_min_id.as_ref().is_some_and(|after| {
                            compare_ids(&max_id, numeric_id(after).expect("validated"))
                                != Ordering::Less
                        })
                    {
                        return Err(malformed());
                    }
                }
            }
            let count = rows.len();
            for (time, trade) in rows.drain(..) {
                if !seen.insert(trade.id.clone()) {
                    return Err(malformed());
                }
                if time >= lower && time < upper {
                    records.push((time, trade));
                }
            }
            match &position {
                Position::Binance {
                    window_start_ms,
                    next_id,
                } => {
                    if max_time >= upper {
                        break TradeHistoryStop::RangeBoundary;
                    }
                    if count < usize::from(q.page_size) && next_id.is_none() {
                        let next = window_start_ms.saturating_add(3_600_000).min(q.end_ms);
                        if next == q.end_ms {
                            break TradeHistoryStop::RangeBoundary;
                        }
                        position = Position::Binance {
                            window_start_ms: next,
                            next_id: None,
                        };
                    } else if count < usize::from(q.page_size) {
                        break TradeHistoryStop::SourceExhausted;
                    } else {
                        let Some(id) = max_id
                            .parse::<u64>()
                            .ok()
                            .and_then(|id| id.checked_add(1))
                            .filter(|id| *id <= i64::MAX as u64)
                        else {
                            break TradeHistoryStop::SourceLimit;
                        };
                        position = Position::Binance {
                            window_start_ms: *window_start_ms,
                            next_id: Some(id),
                        };
                    }
                }
                Position::Okx { .. } => {
                    if min_time < lower {
                        break TradeHistoryStop::RangeBoundary;
                    }
                    if count < usize::from(q.page_size) {
                        break TradeHistoryStop::SourceExhausted;
                    }
                    position = Position::Okx {
                        after_id: Some(min_id),
                    };
                }
                Position::Gate { page, .. } => {
                    if min_time < lower {
                        break TradeHistoryStop::RangeBoundary;
                    }
                    if count < usize::from(q.page_size) {
                        break TradeHistoryStop::SourceExhausted;
                    }
                    if info.symbol.kind() == InstrumentKind::Futures {
                        break TradeHistoryStop::SourceLimit;
                    }
                    if u64::from(*page) * u64::from(q.page_size) > 100_000 {
                        break TradeHistoryStop::SourceLimit;
                    }
                    position = Position::Gate {
                        page: page + 1,
                        last_min_id: Some(min_id),
                    };
                }
            }
        }
        if pages >= usize::from(q.max_pages) {
            break TradeHistoryStop::BudgetReached;
        }
    };
    records.sort_by_key(|(time, _)| *time);
    let next = (stop == TradeHistoryStop::BudgetReached).then(|| TradeHistoryCursor {
        version: 1,
        exchange: info.exchange,
        symbol: info.symbol.clone(),
        native: info.exchange_symbol.clone(),
        start_ms: q.start_ms,
        end_ms: q.end_ms,
        kind: q.kind,
        page_size: q.page_size,
        position,
    });
    Ok(TradeHistory {
        records: records.into_iter().map(|(_, trade)| trade).collect(),
        kind: q.kind,
        pages,
        scanned_rows,
        stop,
        next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cryptofeed_core::symbol::Symbol;
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
    fn kind(exchange: ExchangeId) -> TradeHistoryKind {
        if exchange == ExchangeId::Binance {
            TradeHistoryKind::Aggregate
        } else {
            TradeHistoryKind::Individual
        }
    }
    fn payload(exchange: ExchangeId, ids: &[u64], time_ms: u64) -> Value {
        let rows:Vec<_>=ids.iter().map(|id|match exchange {
            ExchangeId::Binance=>json!({"a":id,"p":"100.01","q":"0.25","T":time_ms,"m":true,"f":id,"l":id}),
            ExchangeId::Okx=>json!({"instId":"BTC-USDT-SWAP","tradeId":id.to_string(),"px":"100.01","sz":"0.25","ts":time_ms.to_string(),"side":"sell"}),
            ExchangeId::Gateio=>json!({"id":id,"contract":"BTC_USDT","price":"100.01","size":"-25","create_time_ms":serde_json::from_str::<Value>(&(Decimal::from(time_ms)/Decimal::from(1000)).to_string()).unwrap()}),
            _=>unreachable!(),
        }).collect();
        if exchange == ExchangeId::Okx {
            json!({"code":"0","data":rows})
        } else {
            json!(rows)
        }
    }
    #[tokio::test]
    async fn native_id_cursors_preserve_multiple_pages_at_identical_milliseconds() {
        for exchange in [ExchangeId::Binance, ExchangeId::Okx, ExchangeId::Gateio] {
            let info = info(exchange);
            let q = TradeHistoryQuery::new(1000, 2000, kind(exchange))
                .unwrap()
                .limits(2, 1)
                .unwrap();
            let first = collect(&info, q.clone(), |_| async {
                Ok(payload(
                    exchange,
                    &[9007199254740994, 9007199254740993],
                    1500,
                ))
            })
            .await
            .unwrap();
            assert_eq!(first.records.len(), 2);
            assert_eq!(first.scanned_rows, 2);
            assert_eq!(first.records[0].price.to_string(), "100.01");
            assert_eq!(
                first.records[0].amount.to_string(),
                if exchange == ExchangeId::Gateio {
                    "25"
                } else {
                    "0.25"
                }
            );
            assert_eq!(first.records[0].side, cryptofeed_trade::Side::Sell);

            let cursor: TradeHistoryCursor =
                serde_json::from_str(&serde_json::to_string(&first.next.unwrap()).unwrap())
                    .unwrap();
            let next_ids = if exchange == ExchangeId::Binance {
                vec![9007199254740995, 9007199254740996]
            } else {
                vec![9007199254740992, 9007199254740991]
            };
            let next = collect(&info, q.resume(cursor), |url| {
                let p: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
                match exchange {
                    ExchangeId::Binance => {
                        assert_eq!(p["fromId"], "9007199254740995");
                        assert!(!p.contains_key("startTime"));
                        assert!(!p.contains_key("endTime"));
                    }
                    ExchangeId::Okx => {
                        assert_eq!(p["type"], "1");
                        assert_eq!(p["after"], "9007199254740993");
                    }
                    ExchangeId::Gateio => assert_eq!(p["offset"], "2"),
                    _ => unreachable!(),
                }
                let ids = next_ids.clone();
                async move { Ok(payload(exchange, &ids, 1500)) }
            })
            .await
            .unwrap();
            assert_eq!(next.records.len(), 2);
            assert!(
                first
                    .records
                    .iter()
                    .chain(&next.records)
                    .all(|t| t.exchange_ts == 1.5)
            );
            assert_eq!(
                first
                    .records
                    .iter()
                    .chain(&next.records)
                    .map(|t| t.id.clone())
                    .collect::<HashSet<_>>()
                    .len(),
                4
            );
            assert_eq!(next.kind, kind(exchange));
        }
    }
    #[tokio::test]
    async fn binance_empty_and_short_seed_windows_advance_without_timestamp_skips() {
        let info = info(ExchangeId::Binance);
        let q = TradeHistoryQuery::new(0, 7_200_000, TradeHistoryKind::Aggregate)
            .unwrap()
            .limits(2, 2)
            .unwrap();
        let mut calls = 0;
        let result = collect(&info, q, |url| {
            let p: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(p["startTime"], if calls == 0 { "0" } else { "3600000" });
            assert_eq!(p["endTime"], if calls == 0 { "3599999" } else { "7199999" });
            calls += 1;
            std::future::ready(Ok(if calls == 1 {
                json!([])
            } else {
                payload(ExchangeId::Binance, &[1], 4_000_000)
            }))
        })
        .await
        .unwrap();
        assert_eq!(result.pages, 2);
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.stop, TradeHistoryStop::RangeBoundary);
        assert!(result.next.is_none());
    }
    #[tokio::test]
    async fn boundary_rows_are_filtered_without_consuming_same_time_ids() {
        let binance = info(ExchangeId::Binance);
        let q = TradeHistoryQuery::new(1000, 2000, TradeHistoryKind::Aggregate)
            .unwrap()
            .limits(2, 1)
            .unwrap();
        let first = collect(&binance, q.clone(), |_| async {
            Ok(payload(ExchangeId::Binance, &[1, 2], 1500))
        })
        .await
        .unwrap();
        let next = collect(&binance, q.resume(first.next.unwrap()), |_| async {
            Ok(payload(ExchangeId::Binance, &[3, 4], 2000))
        })
        .await
        .unwrap();
        assert!(next.records.is_empty());
        assert_eq!(next.stop, TradeHistoryStop::RangeBoundary);
        let gate = info(ExchangeId::Gateio);
        let q = TradeHistoryQuery::new(1501, 2501, TradeHistoryKind::Individual)
            .unwrap()
            .limits(2, 1)
            .unwrap();
        let first = collect(&gate, q.clone(), |_| async {
            Ok(payload(ExchangeId::Gateio, &[4, 3], 2900))
        })
        .await
        .unwrap();
        assert_eq!(first.scanned_rows, 2);
        assert!(first.records.is_empty());
        assert!(first.next.is_some());
        let next = collect(&gate, q.resume(first.next.unwrap()), |_| async {
            Ok(payload(ExchangeId::Gateio, &[2, 1], 2000))
        })
        .await
        .unwrap();
        assert_eq!(next.records.len(), 2);
    }
    #[tokio::test]
    async fn stalled_duplicate_and_wrong_direction_pages_fail() {
        for exchange in [ExchangeId::Binance, ExchangeId::Okx, ExchangeId::Gateio] {
            let info = info(exchange);
            let q = TradeHistoryQuery::new(1000, 2000, kind(exchange))
                .unwrap()
                .limits(2, 1)
                .unwrap();
            let first = collect(&info, q.clone(), |_| async {
                Ok(payload(exchange, &[1, 2], 1500))
            })
            .await
            .unwrap();
            assert!(
                collect(&info, q.resume(first.next.unwrap()), |_| async {
                    Ok(payload(exchange, &[1, 2], 1500))
                })
                .await
                .is_err()
            );
            let q = TradeHistoryQuery::new(1000, 2000, kind(exchange))
                .unwrap()
                .limits(2, 1)
                .unwrap();
            assert!(
                collect(&info, q, |_| async {
                    Ok(payload(exchange, &[1, 2, 3], 1500))
                })
                .await
                .is_err()
            );
        }
    }
    #[tokio::test]
    async fn json_scope_and_position_corruption_fail_before_fetch() {
        let info = info(ExchangeId::Okx);
        let q = TradeHistoryQuery::new(1000, 2000, TradeHistoryKind::Individual)
            .unwrap()
            .limits(2, 1)
            .unwrap();
        let first = collect(&info, q.clone(), |_| async {
            Ok(payload(ExchangeId::Okx, &[1, 2], 1500))
        })
        .await
        .unwrap();
        let original = serde_json::to_value(first.next.unwrap()).unwrap();
        for (field, value) in [
            ("version", json!(2)),
            ("native", json!("OTHER")),
            ("start_ms", json!(1001)),
            ("end_ms", json!(2001)),
            ("kind", json!("aggregate")),
            ("page_size", json!(3)),
            ("exchange", json!("Gateio")),
            (
                "symbol",
                serde_json::to_value(Symbol::spot("ETH", "USDT")).unwrap(),
            ),
            ("position", json!({"kind":"okx","after_id":"bad"})),
        ] {
            let mut value_json = original.clone();
            value_json[field] = value;
            let cursor = serde_json::from_value(value_json).unwrap();
            assert!(
                collect(&info, q.clone().resume(cursor), |_| async {
                    panic!("must not fetch")
                })
                .await
                .is_err(),
                "{field}"
            );
        }
        for exchange in [ExchangeId::Bybit, ExchangeId::Bitget] {
            assert!(
                collect(&self::info(exchange), q.clone(), |_| async {
                    panic!("unsupported history")
                })
                .await
                .is_err()
            );
        }
    }
    #[tokio::test]
    async fn gate_offset_ceiling_and_empty_source_are_explicit() {
        let info = info(ExchangeId::Gateio);
        let mut q = TradeHistoryQuery::new(1000, 2000, TradeHistoryKind::Individual)
            .unwrap()
            .limits(100, 1)
            .unwrap();
        q.cursor = Some(TradeHistoryCursor {
            version: 1,
            exchange: info.exchange,
            symbol: info.symbol.clone(),
            native: info.exchange_symbol.clone(),
            start_ms: 1000,
            end_ms: 2000,
            kind: q.kind,
            page_size: 100,
            position: Position::Gate {
                page: 1001,
                last_min_id: Some("1000".into()),
            },
        });
        let ids: Vec<_> = (0..100).collect();
        let result = collect(&info, q, |_| async {
            Ok(payload(ExchangeId::Gateio, &ids, 1500))
        })
        .await
        .unwrap();
        assert_eq!(result.stop, TradeHistoryStop::SourceLimit);
        assert!(result.next.is_none());
        let q = TradeHistoryQuery::new(1000, 2000, TradeHistoryKind::Individual).unwrap();
        let result = collect(&info, q, |_| async { Ok(json!([])) })
            .await
            .unwrap();
        assert_eq!(result.stop, TradeHistoryStop::SourceExhausted);
    }
    #[test]
    fn request_profiles_and_arbitrary_width_numeric_ids_are_precise() {
        let q = TradeHistoryQuery::new(1501, 2501, TradeHistoryKind::Individual).unwrap();
        let info = info(ExchangeId::Okx);
        let plan = plan(&info, &q, &Position::Okx { after_id: None }).unwrap();
        let p: std::collections::BTreeMap<_, _> = plan.url.query_pairs().into_owned().collect();
        assert_eq!(p["type"], "2");
        assert_eq!(p["after"], "2501");
        assert_eq!(plan.url.path(), "/api/v5/market/history-trades");
        let mut info = self::info(ExchangeId::Gateio);
        info.symbol = Symbol::spot("BTC", "USDT");
        let plan = self::plan(
            &info,
            &q,
            &Position::Gate {
                page: 2,
                last_min_id: Some("999".into()),
            },
        )
        .unwrap();
        let p: std::collections::BTreeMap<_, _> = plan.url.query_pairs().into_owned().collect();
        assert_eq!(p["page"], "2");
        assert_eq!(p["from"], "1");
        assert_eq!(p["to"], "3");
        assert!(!p.contains_key("last_id"));
        assert_eq!(
            compare_ids(
                numeric_id("184467440737095516160").unwrap(),
                numeric_id("184467440737095516159").unwrap()
            ),
            Ordering::Greater
        );
    }
    #[tokio::test]
    async fn dropping_later_page_cancels_fetch_without_background_paging() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Mark(Arc<AtomicBool>);
        impl Drop for Mark {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let mut calls = 0;
        let info = info(ExchangeId::Binance);
        let q = TradeHistoryQuery::new(1000, 2000, TradeHistoryKind::Aggregate)
            .unwrap()
            .limits(2, 3)
            .unwrap();
        let fetch = |_| {
            calls += 1;
            let call = calls;
            let mark = (call > 1).then(|| Mark(dropped.clone()));
            async move {
                if call == 1 {
                    Ok(payload(ExchangeId::Binance, &[1, 2], 1500))
                } else {
                    let _mark = mark;
                    std::future::pending::<Result<Value>>().await
                }
            }
        };
        let mut future = Box::pin(collect(&info, q, fetch));
        assert!(futures::poll!(future.as_mut()).is_pending());
        assert!(!dropped.load(Ordering::SeqCst));
        drop(future);
        assert_eq!(calls, 2);
        assert!(dropped.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn delivery_range_full_page_reports_source_limit_without_retired_paging() {
        let mut info = info(ExchangeId::Gateio);
        info.symbol = Symbol::futures("BTC", "USDT", "241227");
        info.exchange_symbol = "BTC_USDT_20241227".into();
        let q = TradeHistoryQuery::new(1000, 2000, TradeHistoryKind::Individual)
            .unwrap()
            .limits(2, 10)
            .unwrap();
        let mut payload = payload(ExchangeId::Gateio, &[1, 2], 1500);
        for row in payload.as_array_mut().unwrap() {
            row["contract"] = json!(info.exchange_symbol);
            row["size"] = json!(-25);
        }
        let result = collect(&info, q, |url| {
            assert_eq!(url.path(), "/api/v4/delivery/usdt/trades");
            let params: std::collections::BTreeMap<_, _> = url.query_pairs().into_owned().collect();
            for key in ["offset", "page", "last_id"] {
                assert!(!params.contains_key(key));
            }
            std::future::ready(Ok(payload.clone()))
        })
        .await
        .unwrap();
        assert_eq!(result.records.len(), 2);
        assert_eq!(result.records[0].amount.to_string(), "25");
        assert_eq!(result.records[0].symbol, info.symbol);
        assert_eq!(result.pages, 1);
        assert_eq!(result.stop, TradeHistoryStop::SourceLimit);
        assert!(result.next.is_none());
    }
}

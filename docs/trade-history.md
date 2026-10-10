# Bounded public trade history

With the `trade` feature, `PublicRestClient::trade_history` returns bounded
historical batches with scope-bound JSON continuation. Granularity is explicit:
Binance supports `Aggregate`; OKX and Gate support `Individual`.
Use `supported_trade_history_kinds()` separately from recent-trade capabilities.
Bybit v5 and Bitget v3 expose recent fills in the current implemented public
surfaces, not this historical cursor API. Gate delivery supports a bounded historical time range with no current page/offset
cursor: a full page returns `SourceLimit` without continuation, while a short
page reports `SourceExhausted`. Its old last_id is explicitly no longer supported. No private API is substituted.

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = PublicRestClient::load(ExchangeId::Binance, InstrumentKind::Spot).await?;
let symbol = Symbol::spot("BTC", "USDT");
let query = TradeHistoryQuery::new(1_791_640_000_000, 1_791_640_060_000,
    TradeHistoryKind::Aggregate)?.limits(100, 2)?;
let batch = client.trade_history(&symbol, query.clone()).await?;
if let Some(cursor) = batch.next {
    let saved = serde_json::to_string(&cursor)?;
    let restored: TradeHistoryCursor = serde_json::from_str(&saved)?;
    let next = client.trade_history(&symbol, query.resume(restored)).await?;
    println!("{} more {:?} executions", next.records.len(), next.kind);
}
# Ok(()) }
```

The range selects exact native trade times in `[start_ms, end_ms)`. Limits are
1..100 rows/page and 1..100 requests/call (at most 10,000 scanned raw rows,
including filtered boundary records); defaults 100/10. Results sort by exact
native time before conversion to model f64 seconds. Distinct same-time IDs
survive. Price/amount units and taker direction follow [recent trades](recent-trades.md).
All HTTP requests share existing proxy, timeout/body-size, admission and cooldown
policies. Dropping the query cancels pending fetches, with no background pager.
Errors return no partial batch/cursor. These queries do not publish WS events.

| Source | Native progression | Scope/limits |
| --- | --- | --- |
| Binance Spot/UM/CM | Initial aggTrades start/end time window, then fromId=max aggregate ID+1, without simultaneous time parameters | Time seeds span at most one hour (inclusive end is window end−1ms). Empty/short seeds advance to the next window. Derivative official retention is currently 48h. |
| OKX Spot/swap/futures | history-trades type=2 after=end_ms for the first page, then type=1 after=minimum tradeId | Native ID paging preserves same-millisecond executions; documented retention last three months, maximum 100 rows/page. |
| Gate Spot | Fixed enclosing-second from/to plus limit&page | Native IDs must progress backwards; native offset bound limit×(page−1)≤100,000. |
| Gate USDT delivery | Fixed enclosing-second from/to, one bounded page | Full page stops with SourceLimit; no retired last_id or invented offset/page. |
| Gate USDT/BTC perpetual | Fixed enclosing-second from/to plus limit&offset | offset=(page−1)×limit; SDK applies the same 100,000 offset ceiling. No deprecated last_id. |

Binance batches continue forward in time; OKX/Gate continue older data. Every
batch is ascending internally. Binance aggregate IDs are distinct from its
recent `/trades` individual execution IDs; `kind` is present in both query and
result. Never deduplicate across those namespaces. Aggregate quantities are not
expanded into individual executions or assumed equal to event counts.

Gate native bounds use seconds, while requested bounds remain exact integer
milliseconds. The implementation queries enclosing seconds and filters exact
native Decimal timestamps locally; boundary rows still count against the scan
budget. A batch can contain zero selected records while retaining a valid cursor.
Gate Spot `create_time_ms` is a millisecond count with fractions; Gate contracts
use second-valued `create_time_ms` with fractional precision. This is product
knowledge verified against current public services, not magnitude-based guessing.

Version-1 cursors bind exchange, normalized/native mapping, range, granularity,
page size and native position. Invalid scope/version/position is rejected before
HTTP. Per-call page budget may change on resume; page size cannot. Numeric OKX/
Gate IDs compare as arbitrary-width decimal strings, independent of floats/u64.
Duplicate, stalled/wrong-direction, oversized, malformed and out-of-native-bound
pages fail explicitly. Fixed time ranges/page offsets are not provider snapshot
tokens: late inserts/reordering can invalidate a traversal; no atomic history
claim is made.

Stop reasons are `BudgetReached` (with next cursor), `RangeBoundary`,
`SourceExhausted`, and `SourceLimit` (no continuation past a native/SDK ceiling).
None proves complete historical retention. Empty/short source pages, server
retention and provider errors can limit available history. Unsupported granularity,
unknown symbols and unavailable source profiles fail instead of silently switching
endpoints. Archived-file ingestion remains unimplemented.

Official references checked on 2026-10-10:
[Binance Spot aggTrades](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#compressedaggregate-trades-list),
[USD-M](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[COIN-M](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data),
[OKX history-trades](https://app.okx.com/docs-v5/en/#order-book-trading-market-data-get-trades-history),
[Gate Spot](https://www.gate.com/docs/developers/apiv4/en/spot/),
[Gate perpetual](https://www.gate.com/docs/developers/apiv4/en/futures/),
[Gate delivery](https://www.gate.com/docs/developers/apiv4/en/delivery/).

Offline: `cargo test -p cryptofeed-rs --lib rest::trade_history`.
Manual: `cargo run -p cryptofeed-rs --example trade_history_public`.

[Six-product public evidence](reports/live-smoke-trade-history-2026-10-10.md) records native first/resume success and preserves earlier observations.

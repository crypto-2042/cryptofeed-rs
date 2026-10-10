# Bounded candle history

With the `candles` feature, `PublicRestClient::candle_history` queries Binance
Spot/USD-M/COIN-M and Bybit v5 Spot/linear/inverse. Use
`supported_channels().contains(&Channel::Candles)` to check this implemented
subset; a WebSocket candle capability alone does not imply REST-history support.
Bitget v3, OKX and Gate candle history remain pending. No authentication is used.

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = PublicRestClient::load(ExchangeId::Binance, InstrumentKind::Spot).await?;
let symbol = Symbol::spot("BTC", "USDT");
let query = CandleHistoryQuery::new(1_710_000_000_000, 1_710_003_600_000, "1m")?
    .limits(100, 2)?;
let batch = client.candle_history(&symbol, query.clone()).await?;
if let Some(cursor) = batch.next {
    let saved = serde_json::to_string(&cursor)?;
    let restored: CandleHistoryCursor = serde_json::from_str(&saved)?;
    let older = client.candle_history(&symbol, query.resume(restored)).await?;
    println!("{} older candles", older.records.len());
}
# Ok(()) }
```

Ranges select **bar open times** in `[start_ms, end_ms)`, using integer UTC
milliseconds. Interval names match Rust WS configuration; Bybit rejects unsupported
intervals (for example `8h`) before HTTP. Each call permits 1..100 rows per page
and 1..100 pages, with a maximum of 10,000 scanned rows. Defaults are 100/10.
HTTP uses the existing proxy, timeout, response-size and shared admission/backoff
policies. Cancellation drops the pending request; no background paging is spawned.
Errors return no partial result or advanced cursor.

Queries walk backwards through bounded time windows. Each batch sorts records
ascending; a resumed batch contains **older** bars. Empty/short windows advance
instead of claiming source exhaustion. Monthly windows use calendar months;
requests span at most 200 days, respecting COIN-M's range limit even for monthly
bars. Large sparse ranges may consume the request budget without returning bars.
`RangeBoundary` means all requested windows were queried, not that the exchange
retains all historical data. `BudgetReached` includes a cursor. Version-1 JSON
cursors bind exchange, normalized/native symbol, range, interval and page size;
validation occurs before HTTP. The per-call page budget can change on resume.

| Field | Contract |
| --- | --- |
| OHLC/volume | Exact Decimal conversion; native quantity units preserved. COIN-M volume is contracts; Bybit inverse volume is quote currency. |
| `start` | Native open time, converted from milliseconds to seconds. |
| `end` | Binance native inclusive close time; Bybit next interval boundary minus 1ms, including calendar month boundaries. |
| `trades` | Binance native count; Bybit `None`. |
| `closed` | `None` on both REST surfaces; scheduled end/response time is not proof of finality. Apply strict `CandlePolicy` semantics in caller code if needed. |
| `exchange_ts` | Bar open time; these REST rows have no individual event-generation timestamp. |
| `received_ts` | Local receipt time, shared by rows from a page. |

Duplicate or out-of-window rows, oversized pages, invalid envelopes, mismatched
Bybit symbol/category and malformed values fail explicitly. No record is silently
skipped. Candles are standalone observations, do not publish WS events, and do not
change feed counters or book recovery state. Sources may revise unclosed bars;
a cursor is a query continuation, not an exchange snapshot token.

Current official references checked on 2026-10-10:
[Binance Spot klines](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data),
[USD-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[COIN-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data),
[Bybit v5 kline](https://bybit-exchange.github.io/docs/v5/market/kline).

Offline tests: `cargo test -p cryptofeed-rs --lib rest::candles`.
Manual public smoke: `cargo run -p cryptofeed-rs --example candle_history_public`.

[Four-product public smoke evidence](reports/live-smoke-candle-history-2026-10-10.md) records first/resume success; inverse/dated/monthly live runs are not claimed.

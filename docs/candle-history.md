# Bounded candle history

With the `candles` feature, `PublicRestClient::candle_history` queries current
Binance Spot/USD-M/COIN-M, Bitget v3 Spot/contracts, Bybit v5 Spot/linear/inverse,
OKX v5 Spot/swap/futures and Gate v4 Spot/USDT/BTC perpetual and USDT delivery surfaces.
`supported_channels().contains(&Channel::Candles)` checks the implemented REST
subset. Gate delivery uses the documented `/api/v4/delivery/usdt/candlesticks`
endpoint with contract quantities and unknown completion. No authentication is used.

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
milliseconds. Interval names follow the union of existing Rust WS vocabularies,
including Gate `10s` and OKX `3M`. Each venue rejects unsupported intervals before
HTTP (for example Bybit `8h` or Bitget `1M`). Each call permits 1..100 rows per page
and 1..100 pages, with a maximum of 10,000 scanned rows. Defaults are 100/10.
HTTP uses the existing proxy, timeout, response-size and shared admission/backoff
policies. Cancellation drops the pending request; no background paging is spawned.
Errors return no partial result or advanced cursor.

Queries walk backwards through bounded time windows. Each batch sorts records
ascending; a resumed batch contains **older** bars. Empty/short windows advance
instead of claiming source exhaustion. Monthly windows use calendar months;
requests span at most 200 days (Bitget 90 days), respecting native range limits
even for monthly bars. Bitget aligns its native end to the interval boundary;
its documented single earlier interval is counted against the scan budget and
filtered from that window, so a later window can emit it. Gate rounds native
`from` down: Rust sends the first legal bar boundary, converts bounds to seconds,
and omits the incompatible `limit` parameter. A sub-window with no possible bar
opens performs no HTTP request. Large sparse ranges may consume the request budget without returning bars.
`RangeBoundary` means all requested windows were queried, not that the exchange
retains all historical data. `BudgetReached` includes a cursor. Version-1 JSON
cursors bind exchange, normalized/native symbol, range, interval and page size;
validation occurs before HTTP. The per-call page budget can change on resume.

| Field | Contract |
| --- | --- |
| OHLC/volume | Exact Decimal conversion; native quantity units preserved. COIN-M, Gate perpetual and OKX derivative volume is contracts; Bybit inverse volume is quote currency. Gate Spot uses base volume, never substitutes quote turnover. |
| `start` | Native open time, converted from milliseconds to seconds. |
| `end` | Binance native inclusive close time; Bybit next interval boundary minus 1ms. Others use the next interval boundary. Monthly/quarterly calculations are calendar-based: OKX unsuffixed bars use UTC+8, Gate/Bybit use UTC. Gate `30d` means a calendar month. |
| `trades` | Binance native count; others `None`. |
| `closed` | OKX native confirm and Gate Spot completion flag; `None` on Binance, Bybit, Bitget and Gate perpetual. Scheduled end/response time is not proof of finality. |
| `exchange_ts` | Bar open time; these REST rows have no individual event-generation timestamp. |
| `received_ts` | Local receipt time, shared by rows from a page. |

Duplicate or out-of-window rows, oversized pages, invalid envelopes, mismatched
Bybit symbol/category and malformed values fail explicitly. The only overlap
exception is one documented Bitget interval immediately before the lower bound;
multiple earlier records or any upper-bound violation fail. Gate Spot requires
the current eight-field schema; the seven-field example lacking base volume is
rejected instead of guessing quantity units. Candles are standalone observations, do not publish WS events, and do not
change feed counters or book recovery state. Sources may revise unclosed bars;
a cursor is a query continuation, not an exchange snapshot token.

Current official references checked on 2026-10-10:
[Binance Spot klines](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data),
[USD-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[COIN-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data),
[Bybit v5 kline](https://bybit-exchange.github.io/docs/v5/market/kline),
[Bitget v3 market data](https://www.bitget.com/docs/catalog/market/market-data),
[OKX history candles](https://app.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks-history),
[Gate Spot](https://www.gate.com/docs/developers/apiv4/en/spot/),
[Gate perpetual](https://www.gate.com/docs/developers/apiv4/en/futures/),
[Gate delivery](https://www.gate.com/docs/developers/apiv4/en/delivery/).

Offline tests: `cargo test -p cryptofeed-rs --lib rest::candles`.
Manual public smoke: `cargo run -p cryptofeed-rs --example candle_history_public`.

[Public smoke evidence](reports/live-smoke-candle-history-2026-10-10.md) preserves the initial four-product result and subsequent all-venue attempts, including failures. Inverse/dated/monthly live runs are not claimed.

Gate weekly boundary verification: current Spot `7d` bars open Monday (epoch
remainder four days), while perpetual `7d` bars are epoch-aligned, as documented
for futures. Request rounding uses the product-specific anchor. Public September
2026 probes confirmed both grids and UTC-midnight daily opens. Monthly Spot and
perpetual `30d` probes each returned the September 1 UTC open. These observations
are separate from the ten-product 1m smoke; offline tests assert both week grids.

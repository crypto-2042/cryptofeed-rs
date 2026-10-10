# Public REST market snapshots

`PublicRestClient` provides normalized, unauthenticated `ticker` and `l2_book`
queries for the five active exchanges' existing Spot/Perpetual/Futures profiles.
It is independent of FeedHandler and creates no subscription, polling task,
callback or recovery anchor. Ticker means the SDK's best bid/ask model, not all
native 24-hour statistics. Bounded [funding history](funding-history.md) and
[five-venue candle history](candle-history.md) are also implemented. [Recent trades](recent-trades.md) adds bounded five-venue batches. [Native trade history](trade-history.md) adds scoped continuation
for Binance aggregate and OKX/Gate individual executions; Gate delivery candles use the documented delivery endpoint.
`supported_channels()` reports
implemented REST methods in the current Cargo build, separately from
MarketCatalog's WS capabilities.

```rust,no_run
use cryptofeed_rs::prelude::*;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let client = PublicRestClient::load(ExchangeId::Bybit, InstrumentKind::Spot).await?;
    let symbol = Symbol::spot("BTC", "USDT");
    let quote = client.ticker(&symbol).await?;
    let book = client.l2_book(&symbol, 20).await?;
    println!("{} {} / {}", symbol.as_str(), quote.data.bid, quote.data.ask);
    println!("{} bid levels, native sequence {:?}", book.data.bids.len(), book.sequence);
    Ok(())
}
```

`load_with_transport(exchange, product, transport)` shares the explicit
[TransportConfig](transport.md) for catalog and queries. `from_catalog` accepts an
existing immutable catalog and transport; clones share that catalog/client pool.
Unknown normalized names fail before HTTP. To follow new listings/mappings,
refresh a catalog and construct a new client from it; query calls do not guess
native names or automatically refresh the symbol universe.

Ticker, book, funding-history, candle-history and recent-trade methods are gated
by `ticker`, `orderbook`, `funding`, `candles` and `trade`, respectively.
The catalog/context API remains available in builds without those categories.
Existing capability cells, private feeds, options/MARGIN, trading and the Binance
contract-OI deferral are unchanged. This does not claim every newly introduced
native product beyond the declared SDK routing profiles.

## Current request surfaces and depth

All queries specify the exact catalog-native instrument; no implicit all-symbol
request is made. Product selection reuses current runtime routing helpers.

| Exchange | Ticker | Book | Requested depth policy |
| --- | --- | --- | --- |
| Binance Spot | data-api.binance.vision `/api/v3/ticker/bookTicker` | `/api/v3/depth` | 1–5000 |
| Binance USD-M | fapi `/fapi/v1/ticker/bookTicker` | `/fapi/v1/depth` | 5, 10, 20, 50, 100, 500, 1000 |
| Binance COIN-M | dapi `/dapi/v1/ticker/bookTicker` | `/dapi/v1/depth` | 5, 10, 20, 50, 100, 500, 1000 |
| Bitget v3 | `/api/v3/market/tickers` | `/api/v3/market/orderbook` | 1–1000, category-qualified |
| Bybit v5 | `/v5/market/tickers` | `/v5/market/orderbook` | 1–1000, spot/linear/inverse |
| OKX v5 | openapi `/api/v5/market/ticker` | `/api/v5/market/books` | 1–400 |
| Gate v4 | Spot/futures settlement/delivery `tickers` | matching `order_book` | 1–100 SDK cap, not a claimed official maximum |

Zero and unsupported depth fail before network work. Gate uses unmerged depth
and requests native IDs. The book result sorts bids descending/asks ascending,
omits zero-size levels, rejects duplicate/negative sizes, and returns at most the
requested depth. Prices and quantities use exact Decimal conversion, including
JSON numbers/scientific notation; no float price/size conversion or inferred
contract-to-base conversion is applied.

The Binance v1 futures book-ticker route is the current verified official BBO
surface; a v2 last-price ticker is a different endpoint, not a version substitute.
Bitget uses v3 rather than Python's legacy API. Venue/native response identity,
product categories and success envelopes are checked before normalization.
Ambiguous/foreign/failed/malformed replies do not become snapshots.

## Time and sequence semantics

`RestSnapshot<T>` contains normalized `data`, optional native `exchange_ts`, local
`received_ts` (response decoded/available), and optional native `sequence`.
Where native time is absent, the existing nonoptional model's exchange_ts uses
received_ts for compatibility; the wrapper remains None so callers can distinguish
that fallback. A native response-generation time is not necessarily the last
trade or quote-change time.

| Response | Native time used |
| --- | --- |
| Binance ticker | `time` in milliseconds when present; Spot normally absent |
| Binance book | `T`, otherwise `E`, milliseconds when present; Spot normally absent |
| Bitget ticker/book | data row/object `ts`, milliseconds |
| Bybit ticker | envelope `time`, response-generation milliseconds |
| Bybit book | `cts`, otherwise `ts`, milliseconds |
| OKX ticker/book | `ts`, milliseconds |
| Gate ticker | unavailable |
| Gate Spot book | `update`, otherwise `current`, milliseconds |
| Gate derivative book | `update`, otherwise `current`, seconds |

Gate's units are selected by the product schema, never guessed from numeric
magnitude. Native IDs remain u64, preserving values beyond JavaScript's exact
integer range. They are provider/method-scoped IDs, not L2BookHandle's local
revision anchors. A standalone HTTP book cannot be spliced into a running WS
stream without native sequence bridging; use [L2 recovery](l2-recovery.md) for
atomic synchronized consumer recovery. Queries do not mutate feed caches,
readiness, counters or handler state. Separate ticker/book requests are not one
atomic market observation.

## Shared HTTP admission, errors and cancellation

Directory requests, standalone REST snapshots and Binance/Gate bootstrap/resnapshot
now share one process-local pool: at most four active network requests and at
least one second between starts. Waiting on pacing/cooldown does not occupy active
network slots. Catalog requests retain their 45-second/32-MiB limits and cache;
REST/bootstrap retains 15 seconds/8 MiB, measured after admission. Dropping the
query future cancels its wait or request and releases capacity; applications can
wrap a total deadline around admission plus network work.

HTTP failures expose `Error::HttpStatus { status, retry_after }`. HTTP 418/429,
or other unsuccessful HTTP responses with a parsed Retry-After, defer further
requests to that exchange. Both seconds and HTTP-date formats are supported.
Absent/invalid Retry-After on 418/429 uses a 60-second SDK cooldown; the error's
optional field still describes the parsed header. Requests already admitted
cannot be recalled. Cooldown is conservative across this SDK's routes for one
exchange; another exchange does not inherit it. Waiters recheck extended cooldown
before starting and do not park global slots. Shared catalog failures preserve
structured error fields for concurrent callers.

REST queries do not automatically retry. Catalog HTTP statuses also return
without an immediate retry; its existing one retry remains for non-HTTP request/
JSON/envelope failures. Permanent HTTP request/configuration statuses fail fast
in WS bootstrap supervision, while transient HTTP failures remain reconnectable.
Error bodies are not echoed by the HTTP-status path. Authentication is only
explicit proxy authentication, never exchange account credentials.

This is bounded SDK admission and HTTP-header backoff, not complete exchange-
weight/native-code accounting, a distributed quota service or a guarantee about
other applications sharing an IP. Large depth requests have higher native cost;
weighted budgets and provider-native quota/error-code adaptation remain pending.

## Verification and sources

Offline tests cover routing/depth, exact identity/category/envelope validation,
native time/missing time, price/quantity precision, native IDs, book ordering and
malformed data. Paused-clock/HTTP-response doubles cover Retry-After dates,
queued cooldown extension, venue isolation, cancellation, bounded bodies and
safe HTTP errors. They use no listening server/external network. Matching
sanitized captures are described in [provenance](../sample_data/SOURCES.md#2026-10-10-public-rest-snapshots).
The separate [live report](reports/live-smoke-rest-2026-10-10.md) records twenty
queries and the corrected Gate Spot unit issue. Run `rest_public` for the same
public Spot/Perpetual observation; its active-BTC time check is an example sanity
check, not a generic quote-freshness rule.

Primary references: [Binance Spot/market-data-only](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md),
[Spot REST](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md),
[USD-M REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data),
[COIN-M REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data),
[Bitget market data](https://www.bitget.com/docs/catalog/market/market-data),
[Bybit ticker](https://bybit-exchange.github.io/docs/v5/market/tickers),
[Bybit book](https://bybit-exchange.github.io/docs/v5/market/orderbook),
[OKX market data](https://app.okx.com/docs-v5/en/#order-book-trading-market-data),
[Gate Spot](https://www.gate.com/docs/developers/apiv4/en/spot/),
[Gate futures](https://www.gate.com/docs/developers/apiv4/en/futures/),
[Gate delivery](https://www.gate.com/docs/developers/apiv4/en/delivery/),
[HTTP Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after).

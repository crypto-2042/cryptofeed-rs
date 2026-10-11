# cryptofeed-rs

Public-first Rust workspace for normalized cryptocurrency exchange market data.

This project is a ground-up Rust counterpart to the Python
[`cryptofeed`](https://github.com/bmoscon/cryptofeed) project. It preserves the
normalized-feed model while targeting current stable exchange APIs where legacy
Python adapters no longer match the live protocol. The Python project is a
semantic migration reference, not the protocol source of truth.

The SDK is currently an experimental public-market-data release. Authenticated
feeds and trading are out of scope.

## Crates

The workspace splits by data category; exchange protocol and transport live
in the runtime crate:

- `cryptofeed-core` — shared abstractions (`ExchangeId`, `Channel`, `Symbol`,
  `Side`, error types)
- `cryptofeed-ticker`, `cryptofeed-trade`, `cryptofeed-orderbook`,
  `cryptofeed-candles`, `cryptofeed-funding`, `cryptofeed-liquidations`,
  `cryptofeed-openinterest`, `cryptofeed-index`, `cryptofeed-markprice` —
  normalized public models and handler traits per data category
- `cryptofeed-rs` — the runtime: exchange adapters, websocket/HTTP transport,
  routing, reconnect/shutdown, parsing, and orchestration

## Using as a Library

The workspace requires Rust 1.85 or newer. All public-data features are enabled
by default. The crates have not been published by this project; for a local
checkout use a path dependency:

```toml
[dependencies]
cryptofeed-rs = { path = "path/to/cryptofeed-rs/crates/runtime" }
async-trait = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

A Git dependency from
[the project repository](https://github.com/crypto-2042/cryptofeed-rs) can use `cryptofeed-rs = { git = "https://github.com/crypto-2042/cryptofeed-rs" }`.
Use a registry version such as `"0.1"` only after the crates are actually
published.

Subscribe to Binance USD-M perpetual funding, index, and mark price alongside
the ticker/trade/L2 baseline:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use cryptofeed_rs::prelude::*;

struct PrintHandler;

#[async_trait]
impl TickerHandler for PrintHandler {
    async fn on_ticker(&self, ticker: Ticker) {
        println!("ticker {}", ticker.symbol.as_str());
    }
}

#[async_trait]
impl TradeHandler for PrintHandler {
    async fn on_trade(&self, trade: Trade) {
        println!("trade {} {}", trade.symbol.as_str(), trade.price);
    }
}

#[async_trait]
impl OrderBookHandler for PrintHandler {
    async fn on_l2_book(&self, book: L2Book) {
        println!("l2_book {} levels={}", book.symbol().as_str(), book.bids().len());
    }
    async fn on_l1_book(&self, book: L1Book) {
        println!("l1_book {} bid={}", book.symbol.as_str(), book.bid.price);
    }
}

#[async_trait]
impl CandleHandler for PrintHandler {
    async fn on_candle(&self, candle: Candle) {
        println!("candle {} {}", candle.symbol.as_str(), candle.close);
    }
}

#[async_trait]
impl FundingHandler for PrintHandler {
    async fn on_funding(&self, funding: Funding) {
        println!("funding {} rate={:?}", funding.symbol.as_str(), funding.rate);
    }
}

#[async_trait]
impl IndexPriceHandler for PrintHandler {
    async fn on_index_price(&self, index: IndexPrice) {
        println!("index {} {}", index.symbol.as_str(), index.price);
    }
}

#[async_trait]
impl MarkPriceHandler for PrintHandler {
    async fn on_mark_price(&self, mark: MarkPrice) {
        println!("mark {} {}", mark.symbol.as_str(), mark.price);
    }
}

#[tokio::main]
async fn main() {
    let mut handler = FeedHandler::new();
    let feed = Binance::new()
        .ticker()
        .trade()
        .candles()
        .l2_book()
        .l1_book()
        .funding()
        .index()
        .mark_price()
        .symbol("BTC-USDT-PERP")
        .exchange_symbol("BTCUSDT")
        .ticker_handler(Arc::new(PrintHandler))
        .trade_handler(Arc::new(PrintHandler))
        .candle_handler(Arc::new(PrintHandler))
        .orderbook_handler(Arc::new(PrintHandler))
        .funding_handler(Arc::new(PrintHandler))
        .index_price_handler(Arc::new(PrintHandler))
        .mark_price_handler(Arc::new(PrintHandler))
        .build();
    handler.add_feed(feed);
    handler.run().await.expect("run");
}
```

Key API surface:

- `FeedHandler::new()` + `add_feed(...)` + `run()` — the user-facing entrypoint
  (Ctrl-C shuts down cleanly).
- Event-stream mode: `FeedHandler::subscribe()` returns a bounded
  `broadcast::Receiver<FeedEvent>` covering all normalized event types
  (multiple consumers, non-blocking; a slow consumer loses the oldest events
  by design). `FeedHandler::event_count(channel)` reports normalized events
  produced, not per-consumer delivery. Obtain `handler.event_counters()`
  before `run(self)` to retain a shared handle and read `counters.count(channel)`
  while the runtime runs or after it shuts down.
- Handler callbacks execute sequentially within the feed and should return
  quickly. Use the bounded event stream for slower application processing;
  lagged broadcast consumers must handle `RecvError::Lagged`. After losing L2
  deltas, suspend use of the local book until a replacement snapshot/restarted
  feed restores it; the event stream does not promise gap-free delivery.
- Reconnect attempts emit `tracing` warnings with the error and retry delay;
  install a tracing subscriber in your application to observe transient failures.
- `FeedHandler::subscribe_status()` exposes terminal feed failures
  programmatically while healthy feeds continue running independently.
- Per-exchange builders: `Binance`, `Bitget`, `Bybit`, `Okx`, `Gateio`
  (Coinbase/Kraken builders exist but are rejected by capability validation).
- `ExchangeFeedBuilder` methods: `symbol("BTC-USDT")` /
  `symbol("BTC-USDT-PERP")` / `symbol("BTC-USDT-240628")` (normalized forms),
  `instrument(Symbol)` (typed product identity; option/MARGIN variants are
  retained for fixtures and future work but rejected in the 0.1 runtime),
  `exchange_symbol(...)` (explicit native mapping), `candles_interval(...)`,
  `l2_book_depth(...)`, `l2_book_interval(...)` (Binance only), one
  `*_handler(...)` per data category and `add_*_handler(...)` for additional
  serial callbacks. Each callback defaults to a five-second timeout; catchable panic or
  timeout continues to the next callback. See [handler semantics](docs/handlers.md).
  `build()` warns when a subscribed
  channel has no registered handler.
- `OpenInterest` preserves exchange-native `open_interest`, optional Decimal
  `coin_quantity` (OKX `oiCcy`), and optional `value_usd`. `oiCcy` is an amount,
  not a currency code. Binance's `P` is an estimated settlement price and
  never becomes `Funding.predicted_rate` or `MarkPrice.predicted_rate`.
- `FeedEvent` is non-exhaustive: downstream matches need a wildcard arm.
  Order-book synchronization state is runtime-owned; consume normalized book
  events instead of mutating exchange sync state.
- Normalized models: `Ticker`, `Trade`, `L2Book`, `L1Book`, `Candle`, `Funding`,
  `Liquidation`, `OpenInterest`, `IndexPrice`, `MarkPrice`.
- Unsupported product/channel combinations and malformed or mixed normalized
  products fail preflight. Catalog discovery rejects unknown/ambiguous symbols.
  Explicit native pairs are caller-supplied mappings; the exchange may reject
  an incorrect native name during subscription.

## Supported Exchanges and Instruments

Capability matrix by instrument type (✓ = verified public channel; ✗ = not
supported). The 0.1 release supports spot, perpetuals/swaps, and dated futures.
Options and MARGIN remain implementation references and fail preflight.

### 现货 (Spot)

| Exchange | Ticker | Trades | L2 Book | L1 Book | Candles | Funding | Liquidations | Open Interest | Index | Mark price |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| Bitget v3 | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| Bybit v5 | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| OKX v5 | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✓ (explicit index symbol) | ✗ |
| Gate.io v4 | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |

### 合约 (Perpetual / Dated Futures)

| Exchange | Ticker | Trades | L2 Book | L1 Book | Candles | Funding | Liquidations | Open Interest | Index | Mark price |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance USD-M / coin-M perpetual | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ | ✓ |
| Binance USD-M / coin-M dated futures | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ | ✗ | ✓ | ✓ |
| Bitget v3 perpetual (USDT/USDC/coin) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bitget v3 dated futures | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ | ✓ | ✓ | ✓ |
| Bybit v5 linear / inverse perpetual | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bybit v5 linear / inverse dated futures | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ | ✓ | ✓ | ✓ |
| OKX v5 SWAP | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| OKX v5 FUTURES | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ | ✓ | ✓ | ✓ |
| Gate.io v4 (USDT/BTC perpetual) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Gate.io v4 (USDT delivery) | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ | ✓ |

### 期权 (Options)

| Exchange | Ticker | Trades | L2 Book | L1 Book | Candles | Implied volatility |
| --- | --- | --- | --- | --- | --- | --- |
| Binance European options | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| Bybit v5 options | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| OKX v5 options | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

### MARGIN (OKX leveraged spot)

| Exchange | Ticker | Trades | L2 Book | L1 Book | Candles | Liquidations | Mark price |
| --- | --- | --- | --- | --- | --- | --- | --- |
| OKX v5 MARGIN | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

Notes:

- Bitget derivative funding, open interest, index, and mark price share the
  `ticker` subscription; Bybit and Gate.io use `tickers.{symbol}` and
  `futures.tickers`. Funding is enabled only for perpetual/swap products;
  all dated-futures Funding requests fail preflight, even when prices share
  the same ticker/mark-price transport. Bybit derivative ticker deltas rebuild
  the latest known model fields per connection; snapshots and reconnects reset
  that state, and absent fields never receive fabricated initial values.
- Bitget spot/contract L1 uses snapshot-only `books1`, independently of L2.
  Its current v3 candle wire intervals use `1H/4H/6H/12H/1D`; normalized inputs
  remain `1h/4h/6h/12h/1d`. `3d/1w/1M` are rejected because they are not in the
  current official v3 interval table.
- Binance contract Index uses `markPriceUpdate.i` and event time `E`. Index,
  Funding, and MarkPrice share one `@markPrice@1s` subscription when Index is
  requested; Funding/MarkPrice without Index retain the venue's default cadence.
- OKX monthly/quarterly candle ends use calendar arithmetic at the documented
  UTC+8 opening boundary; Gate.io's normalized `1M` maps to its fixed `30d` bar.
- OKX contract Index derives the base/quote index ID from the native contract
  ID (`BTC-USDT-SWAP` → `BTC-USDT`), deduplicates subscriptions, and dispatches
  one IndexPrice per matching configured contract. The explicit spot index
  symbol entrypoint remains available.
- Gate.io perpetual public liquidations use `futures.public_liquidates`
  without authentication; delivery liquidations remain rejected. This is a
  sampled stream (at most one order per contract per second), not a complete
  liquidation ledger.
- The additions above have deterministic offline regression coverage.
  [The 2026-10-08 smoke](docs/reports/live-smoke-2026-10-08.md) received Bitget
  spot/perpetual L1, derivative ticker fields, and OKX SWAP index events.
  Gate.io liquidation subscription was accepted, but no liquidation occurred
  in the short observation window.
- Gate.io delivery endpoints serve `futures.*` channel names (verified live
  2026-08-07).
- Options on every exchange and OKX MARGIN are rejected by preflight in the
  0.1 release scope.
- Binance contract OI is explicitly deferred (2026-10-08). No official native
  perpetual/dated-futures OI WS path was found; current OI is available via
  single-symbol REST requests (weight 1). Multi-symbol polling can exhaust
  the shared request budget, so neither guessed WS topics nor REST polling
  are enabled. Options OI WS is a separate product. See the
  [decision, rationale, and official API references](docs/binance-open-interest-decision.md).
- Liquidation `side` is normalized to the liquidation order/aggressor side.
  Bitget v3 quote-denominated liquidation amounts are converted to base
  quantity; Gate.io public liquidation sizes retain the native contract unit.
  Other trade and book quantities preserve the exchange-native unit.
- L3 order books have no public stream across the active exchanges and are
  deferred (see [the protocol baseline](docs/exchange-protocol-baseline.md#l3-order-book-scope)).
- OKX v5 uses default WebSocket TLS port 443 and the recommended
  `openapi.okx.com` REST domain. Dated API verification and migration details
  are recorded in the [protocol baseline](docs/exchange-protocol-baseline.md#api-currency-review--2026-10-09).
- Authenticated data and trading are out of scope.

## Per-channel symbol subscriptions

Use `.subscription(channel, symbols)` when channels need different symbols:

```rust
use cryptofeed_rs::prelude::*;

let feed = Binance::new()
    .subscription(Channel::Trade, ["BTC-USDT", "ETH-USDT"])
    .subscription(Channel::L2Book, ["BTC-USDT"])
    .build();
```

Pass this feed to `FeedHandler::add_feed` as usual. Repeated entries for a
channel merge and deduplicate symbols. `.subscription_instruments(channel,
selected_symbols)` accepts typed instruments returned by a catalog.

This mode cannot be mixed with `.trade()` / `.ticker()` / other channel
shortcuts or shared `.symbol()` / `.symbols()` / `.instrument()` methods.
Each channel needs at least one symbol, all symbols must share a product kind,
and every requested channel must pass capability/feature preflight. Handler
and interval/depth settings are unchanged. Explicit `.exchange_symbol(...)`
entries correspond to the first-seen deduplicated union in `feed.symbols`.

The runtime resolves this union once and preserves exact channel/symbol pairs
while sharing native connections, then shards the first-seen union by actual
subscription budgets. For example, Trade BTC/ETH plus L2 BTC shares one Spot
connection without subscribing to ETH depth. Product and public/business routes
still split when required. [Connection planning and budgets](docs/connection-planning.md)
documents packing, queued sends and shared admission/snapshot pacing. `feed_count()`
counts logical feeds; managed snapshots identify physical connections and books.
Low-level adapter callers must still plan each `feed.connection_feeds()?` shard
and the adapter's endpoint plans; exact map support alone does not enforce capacity.

## Managed runtime updates

Enable `handler.control_handle()` before starting the handler to retain a
cloneable runtime controller. It supports `add_feed`, `replace_feed`,
`remove_feed`, `feeds` and `shutdown`. `add_feed_with_id` exposes IDs for initial
feeds; existing `add_feed` and the default strict startup remain compatible.
Control mode starts initial logical feeds independently.

`subscribe_identified()` emits `FeedEnvelope { identity, event }`. Replacement
keeps the feed ID and advances its configuration generation; stale queued events
remain identifiable. Candidate validation failure leaves the old feed running.
A successful command acknowledges validated task launch. Query
`control.state(id).await?` for a retained `FeedSnapshot`; `.is_ready()` requires
all connection subscriptions confirmed and requested L2 books initialized.
State queries also work after lifecycle notifications lag.

See [managed runtime control](docs/runtime-control.md) for cancellation, command
queues, graceful/forced removal, lifecycle states, event lag and remaining scope.
Run the public add/replace/remove example with:

```bash
cargo run -p cryptofeed-rs --example managed_public
```

[Readiness semantics](docs/readiness.md) distinguish confirmations, local book
state, data observations, reconnect epochs and stopping. Run the five-exchange
public Trade/L2 observation with `cargo run -p cryptofeed-rs --example readiness_public`.

## Recoverable L2 consumption

Retain `handler.l2_book_handle()` before startup to enable managed L2 recovery.
`books.recover(identity, &symbol)` atomically returns a full local snapshot and
subsequent bounded updates with identity/connection/epoch/revision anchors.
After lag, replace the old receiver and local book with a fresh recovery result.
Disconnect/resync/stop withdraws unusable books. See [L2 recovery](docs/l2-recovery.md)
and `book_recovery_public` for the consumer loop and resource/lifecycle limits.

## Candle completion

Use `.candle_policy(CandlePolicy::ClosedOnly)` to deliver only explicitly final
candles. The compatible default `All` delivers every update; `ClosedOrUnknown`
omits known unfinished bars but preserves unknown completion as `None`.
The filter applies to handlers, streams and counters. Bitget currently has
unknown completion, so strict mode delivers no candles there. See
[candle delivery](docs/candle-delivery.md) for policies and Python differences.

## Public REST snapshots

`PublicRestClient::load(exchange, product).await?` exposes feature-gated
`ticker(&symbol)` and `l2_book(&symbol, depth)` on current public endpoints.
Results reuse normalized models, with optional native timestamp/sequence metadata.
Catalog identity, proxy routing and bounded HTTP admission are shared; queries
create no WS subscription and do not change live recovery state. See
[public REST](docs/public-rest.md) for native depth/time rules, HTTP backoff and
examples. Feature-gated `funding_history(&symbol, query)` now provides bounded settlement
pages and versioned continuation, with actual rates and explicit stop reasons.
See [funding history](docs/funding-history.md). Five-venue
[candle history](docs/candle-history.md) adds bounded time windows and JSON
continuation. [Recent trades](docs/recent-trades.md) now provides bounded
five-venue batches with exact IDs and native quantity units. [Native trade history](docs/trade-history.md) now adds scoped continuation
for Binance aggregate and OKX/Gate individual executions; Gate delivery candles use the documented delivery endpoint.

## Event recording and offline replay

Opt-in `recording` adds bounded, versioned JSONL capture of identified normalized
public events and sequential offline replay callbacks, with exact Decimal data,
original model clocks, immediate/recorded timing and explicit failure/stop behavior.
Broadcast lag is an error; incomplete files are rejected. A separate
[sanitized WS observation channel](docs/raw-capture.md) now captures text before
control/market parsing. [Raw WS files](docs/raw-recording.md) now persist and validate those observations.
[Native parser/state replay](docs/raw-replay.md) now supports five public families
and WS-native L2 for Bybit/OKX/Bitget. HTTP bootstrap capture and Binance/Gate L2
replay remain pending. See [recording](docs/recording.md) for limits, file schema and the
end-to-end example.

## HTTP and WebSocket proxy

Use `.transport(TransportConfig::http_proxy("http://127.0.0.1:8080")?)` to route
catalog discovery, REST book bootstrap/resync and WebSockets consistently.
Basic proxy authentication uses `.basic_auth(username, password)?`; credentials
are excluded from URLs and debug output. Default routing is explicitly direct,
including HTTP; system proxy environment inference is not used. See
[transport configuration](docs/transport.md) for standalone catalogs, discovery,
cache isolation, supported proxy types and migration details.

## Symbol discovery and service shutdown

Load a product-qualified catalog, then select explicit normalized symbols:

```rust
use cryptofeed_rs::prelude::*;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = MarketCatalog::load(ExchangeId::Binance, InstrumentKind::Spot).await?;
    let selected = catalog.select(&["BTC-*", "ETH-USDT"])?;
    let feed = Binance::new().trade().instruments(selected).build();
    let mut handler = FeedHandler::new();
    handler.add_feed(feed);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown_rx));
    // Keep shutdown_tx while the application runs; at service shutdown:
    shutdown_tx.send(true)?;
    running.await??;
    Ok(())
}
```

`MarketCatalog::symbols()` lists normalized instruments. `select` supports `*`
and `?`, ignores ASCII case, sorts and deduplicates matches, and rejects empty
input or any unmatched pattern. It uses the existing 24-hour catalog cache;
use `MarketCatalog::refresh(exchange, product).await` to bypass cached responses,
including paginated catalogs. Concurrent requests for the same URL share
in-flight work. Refresh does not change existing catalog snapshots or running
subscriptions. Optional `DiscoveryFeed` periodically refreshes and reconciles a
managed feed; see [automatic symbol reconciliation](docs/discovery.md). Patterns
are expanded explicitly before building a feed, not by `.symbol("*-USDT")`.
Broad matches are split by native subscription budgets; configurations exceeding
100 planned physical connections per exchange are rejected before WS startup.
Existing capability preflight still applies. These process-local limits do not
account for other clients sharing your IP.

`catalog.market(&symbol)` and sorted `catalog.markets()` expose `MarketInfo`:
reported ticks/lot steps, minimum quantity/notional, separate decimal-place counts,
native status/type and contract value/currency/multiplier. Missing/inapplicable
fields are None; steps and denominations are not guessed. `exchange()`, `product()`
and `supported_channels()` expose catalog/build context. See
[market metadata](docs/market-metadata.md) for current field mappings, deprecated
constraints, refresh behavior and limits; this is not an order validator.

For explicit lists, `.symbols(["BTC-USDT", "ETH-USDT"])` appends names like
repeated `.symbol(...)` calls. `run()` still installs Ctrl-C shutdown;
`run_with_shutdown` uses your watch signal and installs no signal handler.
Managed add/remove/replace is available through a retained control handle.
Readiness is available through the managed state API; remaining policy work is tracked in the
[Python usage alignment plan](docs/python-usage-alignment.md).

## Development

```bash
cargo check --workspace
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo check --workspace --no-default-features
make features
```

## Examples

Run from the workspace root:

```bash
cargo run -p cryptofeed-rs --example binance_public
cargo run -p cryptofeed-rs --example bitget_public
cargo run -p cryptofeed-rs --example bybit_public
cargo run -p cryptofeed-rs --example okx_public
cargo run -p cryptofeed-rs --example gateio_public
cargo run -p cryptofeed-rs --example derivatives_public
cargo run -p cryptofeed-rs --example release_smoke
cargo run -p cryptofeed-rs --example channel_completion_smoke
```

`derivatives_public` demonstrates the extended derivative channels (Binance
USD-M funding / index / mark price / L1) and the
event-stream consumer mode (`FeedHandler::subscribe()` instead of handler
traits). `release_smoke` is the bounded-output multi-exchange counter used for
dated manual release reports.

Example imports use the runtime prelude:

```rust
use async_trait::async_trait;
use cryptofeed_rs::prelude::*;
```

## Symbol Resolution

- Runtime preloads product-specific exchange symbol catalogs over HTTP before
  WebSocket subscription.
- Subscription symbols are resolved bidirectionally from normalized forms such
  as `BTC-USDT`, `BTC-USDT-PERP`, and `BTC-USD-240628`.
- Spot, perpetual, and dated-futures identities remain distinct even when an
  exchange reuses the same native symbol.
- Catalog discovery rejects unknown/ambiguous symbols; mixed normalized
  products and unsupported channel combinations fail before connecting.
  The runtime does not guess quote currencies. Explicit mappings assert native
  identity on the caller's behalf and bypass market-catalog existence checks.
- Explicit exchange-native symbols remain available through
  `ExchangeFeedBuilder::exchange_symbol(...)` and must map one-to-one to the
  normalized symbols in the feed.

Runtime connections implement exchange application heartbeats, idle detection,
indefinite transient reconnection with bounded backoff, bounded shutdown, feed
failure isolation, batch event delivery, and product-aware L2 snapshot/gap
recovery. [RuntimeOptions](docs/runtime-options.md) adds per-connection retry
limits, startup delay, receipt idle policy and handshake/callback deadlines. Handler callbacks default to five
seconds so a stuck callback
cannot permanently stop socket reads; high-volume consumers should prefer the
bounded event-stream API.

The offline evidence and parity workflow is documented in
[the harness guide](docs/harness.md). See the [documentation index](docs/README.md)
for protocol references, decisions, and dated live-validation reports.

## Exchange Policy

New CEX integrations must use the latest stable official exchange API rather than legacy versions.

- Bitget integrations should target the current official v3 API surface.
- When adding or updating an exchange adapter, verify the current official REST and WebSocket documentation before implementing.
- If an older API version already exists in the Python codebase, treat it as migration reference only, not as the Rust source of truth.

## Origin, License, and Maturity

- **Origin**: `cryptofeed-rs` is a ground-up Rust workspace modeled on the
  Python [`cryptofeed`](https://github.com/bmoscon/cryptofeed) project by
  Bryant Moscon, preserving its normalized-feed model and `FeedHandler`
  entrypoint.
- **Rewrite rationale**: the Rust implementation targets the current stable
  official exchange APIs where legacy Python adapters no longer match the live
  protocol. The Python project is a semantic migration reference, not the
  protocol source of truth.
- **Repository**: https://github.com/crypto-2042/cryptofeed-rs.
- **License**: this workspace currently declares XFree86-1.1 (see `LICENSE`).
  The maintainer must confirm licensing and ownership before the first formal
  release. This declaration is not a claim that the current Python
  upstream has the same license; any reused material needs provenance tied
  to its exact source revision.
- **Maturity**: experimental public-market-data release. The API surface and
  the supported exchange scope are still evolving; `PROGRESS.md` tracks the
  current implementation state and the remaining release work.

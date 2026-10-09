# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Product-qualified `MarketCatalog` discovery and explicit normalized-symbol
  pattern selection (`*`/`?`), bulk `.symbols` / `.instruments` builders, and
  a `FeedHandler::run_with_shutdown` facade for service-owned shutdown.
- Source-backed Python usage gap inventory and a phased improvement plan.

- Bitget v3 spot/contract L1 and derivative ticker Funding/OpenInterest/Index/
  MarkPrice; Gate.io public perpetual liquidations; OKX contract index mapping
  and shared-index fan-out. Sourced fixtures, per-feature regressions, and a
  targeted live-smoke example cover the completed public scope.

- Event-stream consumer mode: `FeedHandler::subscribe()` returns a bounded
  `broadcast::Receiver<FeedEvent>` covering all normalized event types,
  complementing the handler-trait callbacks; `FeedHandler::event_count(...)`
  exposes per-channel producer counters.
- Builder diagnostics: `ExchangeFeedBuilder::build()` warns when a subscribed
  channel has no registered handler; users can instead consume the event
  stream, and the warning now describes both valid consumer paths.
- `L2Book` convenience accessors: `symbol()`, `bids()`, `asks()`,
  `exchange_ts()`, `received_ts()`.
- README: support matrix grouped by instrument type (spot / perpetual-futures
  / options / MARGIN) and a verified library-usage example.
- CHANGELOG.md.

### Fixed

- OKX v5 WebSockets use default TLS port 443 ahead of the announced 8443
  shutdown; discovery uses the recommended Global REST domain. Non-live spot
  directory records are filtered, and empty spot identity returns an error
  instead of panicking.

- Binance Index uses documented mark-price `i/E` fields, with a deduplicated
  1s topic when Index is requested; native COIN-M contract identities are retained.
- Bybit derivative ticker snapshots/deltas are reconstructed per connection,
  preserving unchanged BBO/funding/OI/index/mark fields and clearing stale values.
- Dated Funding requests are rejected for all five exchanges. Bitget v3 uses
  current uppercase hourly/daily wire intervals and rejects undocumented longer
  periods; OKX monthly/quarterly ends use UTC+8 calendar boundaries.
- Binance estimated settlement price (`P`) no longer becomes a predicted
  funding rate in Funding or MarkPrice.
- OKX numeric `oiCcy` is exposed as Decimal `OpenInterest.coin_quantity`;
  the incorrect pre-release `oi_currency: Option<String>` contract is removed.
- Bitget dated-futures L2 and liquidation dispatch now preserve configured
  native-symbol identity.
- JSON numeric prices/quantities preserve wire decimal precision before
  normalization; Cargo resolver 3 honors Rust 1.85 during fresh resolution.
- All runtime features are tested independently in CI; AGENTS/CLAUDE equality
  is checked; crate metadata points to the confirmed GitHub repository.

- Updated Bybit v5 `allLiquidation` (`T/s/S/v/p`) and option ticker
  (`bidPrice`/`askPrice`/`markPriceIv`) parsing to the current official wire
  contracts; refreshed parity fixtures.
- Normalized Bybit and Bitget liquidation position sides to aggressor side;
  Bitget quote-denominated liquidation amounts are converted to base quantity.
- Gate.io reconnects now regenerate subscription timestamps, reject
  `event=subscribe` responses with non-null `error`, and require a buffered
  sequence anchor before accepting id-less REST book snapshots.
- Runtime feature boundaries: `liquidations` compiles independently, Binance
  mark price works without `funding`, Gate.io L1 works without `ticker`, and
  no-default tests/examples no longer compile unavailable targets.
- Multiplexed exchange streams now dispatch only channels requested by the
  feed and only requested symbols; ticker-only feeds no longer leak
  L1/funding/index/mark events, and market-wide liquidation streams cannot
  publish unrelated instruments.
- Gate.io open interest now uses the current official `futures.tickers`
  `total_size` field instead of stale `open_interest`/`open_interest_usd`
  fixture fields.
- Catalog fetches reject HTTP/error envelopes before caching, enforce a 32 MiB
  response limit, and use an in-process cache instead of predictable files in
  the shared temporary directory.
- Transient WebSocket failures reconnect indefinitely with bounded backoff;
  symbol hydration is shutdown-aware and handler callbacks have a bounded
  deadline.
- Release engineering now includes all-target Clippy, per-feature/MSRV/package
  CI, dependency audit, package README/LICENSE files, and docs.rs metadata.
- Added `FeedHandler::subscribe_status()`, runtime-owned interval strings, and
  a multi-exchange `release_smoke` counter; the 2026-08-17 candidate smoke
  passed five spot feeds plus Binance USD-M.
- Binance index-price stream: the live USD-M stream emits `e: "IndexUpdate"`
  (docs example writes `indexPriceUpdate`); both event names are now
  accepted (measured live 2026-08-07).
- Cargo packaging: workspace path dependencies now carry explicit versions
  so `cargo package` can verify manifests.
- Binance spot partial-depth streams (`@depth5/10/20`): every push
  (`{lastUpdateId,bids,asks}` without `e`/`s`) previously errored the
  session; pushes now resolve the instrument from the stream name and
  replace the top-N book through book sync, bootstrapped from a snapshot of
  the same width.
- Gate.io `full: true` order-book pushes previously errored the session;
  they now replace the local book and re-anchor the sequence at the push's
  `u` (in-session, bounded resnapshot semantics unchanged).
- Bitget kline intervals: hourly/daily/weekly/monthly bars (`1h`..`1M`)
  silently produced zero candles; the duration map now mirrors the v3 wire
  vocabulary.
- Binance idle detection: the 90s idle timeout was shorter than the server's
  3-minute ping cadence and terminally failed quiet feeds; the timeout is
  now 240s.
- Bybit option feeds: the `/option` connection plan was classified as spot,
  subscribing to an empty args list; option symbols now route to the option
  URL.
- L2 snapshot fetches (Binance, Gate.io) now use a 15s HTTP timeout, and
  buffered deltas are bounded (oldest dropped past a cap); repeated failed
  bootstraps trigger a bounded in-session resnapshot before the session
  retry.
- OKX `books` checksum reconstruction now interleaves bids and asks per
  index (the official format) instead of all bids then all asks.
- OKX candles: `end` is derived from the wire interval (a 1H bar is no
  longer 60s long) and the interval is normalized (`1H` → `1h`).
- OKX VIP4 tick-by-tick depth levels (50/400) were subscribed but never
  parsed; they are now rejected explicitly at preflight until wired.
- Bybit option base-coin trade batches resolve each row's own series instead
  of stamping the first row's symbol on every entry.
- Gate.io `parse_symbol` no longer panics on dash-less input.
- Public API: models derive `PartialEq`; `Liquidation.side` is the shared
  `Side` enum (`Trade.side` and `Liquidation.side` both serialize
  lowercase); `Symbol` implements `Display`/`FromStr`; public enums are
  `#[non_exhaustive]`; `Error::UnsupportedExchange/Channel` carry `String`.

### Changed

- `FeedEvent` is non-exhaustive and exchange order-book synchronization state
  is internal to the runtime. `FeedHandler::event_counters()` exposes a shared
  producer-counter handle that remains usable when `run(self)` owns the handler.
- Transient reconnect failures emit tracing diagnostics; handler warnings
  correctly describe the event-stream alternative.
- Binance contract OI, including REST polling, is explicitly deferred due to
  multi-symbol rate-budget exposure (2026-10-08).

- The 0.1 release scope is now limited to spot, perpetual/swap, and dated
  futures. Bybit/OKX options and OKX MARGIN are rejected by capability
  preflight; their parser, catalog, fixture, and regression code is retained
  for future product work.
- Bybit options discovery pages `instruments-info?category=option` (the
  tickers endpoint requires a `baseCoin`); `optionsType` is cross-checked
  against the `{C|P}` suffix (verified live 2026-08-07).
- Binance L2 book update interval is parameterized via
  `l2_book_interval(...)`: spot `100ms`/`1000ms`, USD-M/COIN-M
  `100ms`/`250ms`/`500ms`.
- Gate.io derivative funding, open interest, index price, and mark price are
  normalized from the `futures.tickers` stream; `book_ticker` doubles as the
  L1 top-of-book channel (spot and derivatives); the 10s candle interval is
  supported.
- Removed the unused `cryptofeed_core::subscription::Subscription` type.

### Known issues

- Binance contract OI is explicitly deferred, including REST polling; options
  remain out of scope and both fail capability preflight.
- Rust author attribution and license/provenance remain maintainer decisions
  before a formal release. The GitHub repository and private vulnerability
  reporting channel are confirmed; crates.io publication has not been performed.

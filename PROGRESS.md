# Progress

Last updated: 2026-10-08

## Current State

`cryptofeed-rs` has a verified public runtime baseline for Binance, Bitget v3,
Bybit v5, OKX v5, and Gate.io v4 across supported spot, perpetual, and dated
futures products. Product-qualified symbol discovery, capability preflight,
heartbeats, reconnect, feed isolation, batch delivery, and product-aware L2
recovery are in place.

## Completed

- 2026-10-08 public-channel completion: Bitget spot/contract L1 and derivative
  ticker funding/OI/index/mark price; Gate.io perpetual public liquidations;
  OKX swap/futures index mapping with shared-index fan-out. Official sourced
  fixtures, inline parity cases, runtime dispatch regressions, and isolated
  feature tests cover these additions. Binance OI remains unchanged.
- 2026-10-08 targeted live smoke: Bitget spot/perpetual L1, perpetual
  funding/OI/index/mark price, and OKX SWAP index delivered normalized events.
  Gate.io perpetual public liquidation subscription succeeded, with no
  liquidation events in the short window; see `docs/live-smoke-2026-10-08.md`.

- Pure Rust workspace split into core and normalized public data categories.
- Stable `FeedHandler` entrypoint and exchange builders.
- Normalized ticker, trade, L2 book, candle, funding, and liquidation models and
  handler traits.
- Product-qualified bidirectional symbol catalogs for the five active exchanges.
- Explicit exchange/product/channel capability validation without heuristic
  quote guessing or silent no-op feeds.
- Spot, perpetual, and dated-futures routing for:
  - Binance Spot, USD-M, and coin-margined products.
  - Bitget v3 Spot, USDT, USDC, and coin futures.
  - Bybit v5 Spot, linear, and inverse products.
  - OKX v5 Spot, swap, and futures products.
  - Gate.io v4 Spot, USDT/BTC perpetual, and USDT delivery products.
- Current Bitget v3 kline, trade batch, depth, and instruments protocols.
- Current Binance spot book-ticker shape and USD-M public/market WebSocket split.
- Bitget v3 subscription acknowledgements classified before market-data
  dispatch.
- Bybit v5 spot BBO Ticker sourced from level-1 order book while level-50 remains
  the independent L2 source.
- Current Gate.io subscription timestamps, trade objects, connection plans, and
  derivative REST depth bootstrap; non-identity zero contract multipliers do not
  block catalog discovery.
- Batch normalization for supported multi-entry messages.
- Exchange application heartbeats, idle detection, clean-close reconnect,
  graceful bounded shutdown, and independent feed failure isolation.
- Product-aware L2 bootstrap, replacement-snapshot reset, stale update discard,
  sequence gap detection, and resync.
- Sanitized official protocol references with per-file provenance in
  `sample_data/SOURCES.md`.
- Deterministic public parity assertions for all five active exchanges.
- Full-session WebSocket doubles for all five active exchanges: deterministic
  duplex-stream session tests covering subscribe framing, control
  acknowledgements, market-data delivery, clean shutdown, and, for Binance, an
  injected REST snapshot bootstrap with buffered-depth bridging.
- OKX full-book checksum validation: IEEE CRC-32 over the first 25 bids/asks
  with transmitted string scale preserved; validated on the `books` channel
  only (top-N channel checksums cover only transmitted levels), mismatch is a
  recoverable parse error.
- Deterministic Gate.io bootstrap-failure evidence: the non-bridging buffered
  delta failure signature and fresh-snapshot recovery, covered without
  weakening sequence validation.
- Gate.io bounded in-session resnapshot: a failed bootstrap re-fetches the
  snapshot up to `GATEIO_MAX_RESNAPSHOTS` per symbol while preserving the
  buffered deltas, then falls back to the session-level retry; the bootstrap
  dispatch now contains the snapshot and the applied buffered updates (matching
  the Binance event sequence).
- Gate.io derivative funding, open interest, and index price extracted from the
  `futures.tickers` stream (no standalone channel), plus the 10s candle
  interval; parity fixtures and capability-matrix cells added (2026-08-06).
- Bybit options catalog discovery fixed (2026-08-07): `tickers?category=option`
  requires `baseCoin` (PARAMS_ERROR otherwise); discovery now pages
  `instruments-info?category=option` via `nextPageCursor` with the
  `optionsType` field cross-checked against the `{C|P}` suffix.
- Binance L2 update intervals parameterized (2026-08-07):
  `l2_book_interval(...)` with spot `100ms`/`1000ms` and USD-M/COIN-M
  `100ms`/`250ms`/`500ms` preflight validation and wire mapping.
- Gate.io mark price and L1 top-of-book added (2026-08-07): mark price rides
  the derivative `futures.tickers` stream; `book_ticker` doubles as the L1
  channel with best bid/ask sizes (same dual-event pattern as Binance).
- Gate.io delivery channel-prefix question resolved (2026-08-07, live
  verification): the delivery WebSocket serves `futures.*` channel names;
  `delivery.*` is rejected as an unknown channel, so no adapter change was
  needed.
- Library-usage hardening (2026-08-07): event-stream consumer mode
  (`FeedHandler::subscribe()` + `FeedEvent` + per-channel delivery counters),
  builder warnings for subscriptions without registered handlers, `L2Book`
  convenience accessors, `CHANGELOG.md`, and a derivatives extended-channel
  example (`derivatives_public`), all verified live.
- Binance index-price stream event name fixed (2026-08-07): live USD-M emits
  `e: "IndexUpdate"`; both that and the docs-name `indexPriceUpdate` are
  accepted. Binance `@openInterest` produced no messages on 2026-08-07
  (three windows, >21 min) while REST returned data; the unverified WebSocket
  capability is now rejected by preflight.
- Repository license material (`LICENSE`, XFree86-1.1) and README origin,
  license, and maturity sections.
- Gate.io bootstrap-failure forensics: the resnapshot path logs the failing
  snapshot id and the complete buffered `U/u` sequence at `warn` level so a
  live smoke run can capture the acceptance evidence (see
  `docs/harness.md`).
- Release metadata: per-crate `description` fields, README origin/license/
  maturity sections, and a Phase F consistency check (all example channel
  combinations are inside the capability matrix, and every documented
  capability claim has a corresponding preflight or normalization assertion;
  options and MARGIN have explicit rejection tests plus retained parser fixtures).
- Feature-safe compilation with no-default and individual public-data features.
- Binance index price and L1 top-of-book are wired through parsing, dispatch,
  capability preflight, and parity assertions. The historical open-interest
  parser fixture remains, but live capability is disabled until a current
  official WebSocket stream is verified.
- Bybit index price: the derivative `tickers.{symbol}` stream's `indexPrice`
  is normalized into `IndexPrice` events (current official docs expose no
  standalone index-price channel; verified 2026-08-06).
- Parameterized candle intervals: `.candles_interval(...)` validates against
  each exchange's official interval set during preflight and maps to the wire
  form (Bybit `kline.{1..720,D,W,M}`, OKX `candle{1m..3M}`, Bitget v3 kline
  arg, Gate.io `24h/7d/30d`, Binance `kline_{1m..1M}`); the default stays
  `1m`.
- Parameterized L2 depth levels: `.l2_book_depth(...)` with official partial
  books (Binance `@depth{5,10,20}@100ms` plus matching snapshot width, Bybit
  `orderbook.{50,200,1000}`, OKX `books5` (50/400 tick-by-tick channels are
  VIP4+-gated and rejected), Bitget `books1/5/50`); Gate.io depth stays
  full-depth and rejects explicit levels.
- Mark price channel: new `cryptofeed-markprice` crate and
  `Channel::MarkPrice`; Binance `@markPrice` doubles Funding and MarkPrice
  events, Bybit derivative `tickers` carries `markPrice`, OKX `mark-price`
  channel (`markPx`/`ts`); capability rows on Binance/Bybit/OKX derivatives.
- OKX option candle coverage and Bybit's lack of option klines remain recorded
  in fixtures/baseline, but 0.1 rejects both option products before connecting.
- OKX MARGIN implementation reference: `InstrumentKind::Margin` + `Symbol::margin`
  (explicit `.instrument(...)` only; input text can never infer margin
  because the instId form matches spot). Historical parser coverage includes
  candles, liquidations, and mark price. Events are
  rebound to the feed instrument via `parse_messages_for_feed`, fixing the
  Spot-kind symbols the payload parser would otherwise produce; symbol
  discovery for margin requires explicit `exchange_symbol` pairs. The 0.1
  capability matrix now rejects MARGIN before connection.
- OKX VIP4+ tick-by-tick depth: `books-l2-tbt` (400) and `books50-l2-tbt`
  (50) with the documented VIP gate (error 64003 otherwise) and the
  transmitted-levels-only checksum skip rule shared with `books5`.

## Pre-push engineering review (2026-10-08)

- Corrected Binance settlement-price/funding-rate semantics, OKX numeric
  coin-quantity modeling, Bitget dated book/liquidation identity, and JSON
  numeric precision, each backed by a reproduced regression.
- Added MSRV-aware resolver 3, full single-feature CI tests, instruction-file
  equality checking, repository metadata, and docs.rs all-feature metadata.
- Final local gates: 383 tests (263 runtime / 82 parity / 9 feature-boundary /
  29 model/core), all-target Clippy, rustdoc, Rust 1.85, per-feature/no-default,
  11 package-content checks, and zero-vulnerability dependency audit. Current
  and 403 historical blob credential-pattern scans found no matches.
- Aggregate 10-configuration smoke and post-fix Bybit/Binance/Bitget targeted
  checks exited cleanly; see `docs/live-smoke-pre-push-2026-10-08.md`.
- GitHub repository confirmed: https://github.com/crypto-2042/cryptofeed-rs.
  It was public and empty at the engineering review, with ADMIN permission.
- The final review also covers derivative ticker delta reconstruction,
  dated Funding preflight, official candle interval vocabulary, calendar-month
  ends, and documented Binance index extraction. See
  `docs/pre-push-review-2026-10-08.md` for exact checks and limitations.
- The original engineering review did not commit or push. On 2026-10-08 the
  maintainer authorized source submission to GitHub; final engineering gates
  were rerun and private vulnerability reporting was enabled and verified.
- The current source remains an unreleased 0.1 candidate. No release tag or
  crates.io publication is included. License/provenance remains a maintainer
  decision before a formal release; authorship is intentionally unset.

## Current Milestone (complete)

- The 0.1 public runtime scope is frozen to spot, perpetual/swap, and dated
  futures. Existing options and MARGIN catalog/parser work stays covered as a
  future implementation reference but is absent from the authoritative
  capability matrix and fails preflight.

## Deferred

- Binance USD-M/COIN-M perpetual and dated-futures OI: explicitly deferred by
  the user on 2026-10-08. No official native contract OI WS path was found;
  weight-1 single-symbol REST polling scales with symbol count/frequency and
  can exhaust shared IP request budgets. No polling implementation is
  scheduled. See `docs/binance-open-interest-decision.md`.

- Private/authenticated support and order entry.
- Options and MARGIN public runtime support; reopening either requires a new
  explicit product-model and live-validation plan.
- Coinbase and Kraken live runtime paths.
- Gate.io live bootstrap capture validation: the forensics logging is in
  place, but confirming the bounded resnapshot path against a captured real
  failure requires an intermittent live failure; tracked passively.
- L3 order books: none of the five active exchanges exposes a public
  order-by-order depth stream (verified 2026-08-05); public depth is
  price-level only. See `docs/plan-advanced-public-channels.md` Section 2.2.

## Verification Baseline

Run from the workspace root:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace
cargo check --workspace --no-default-features
```

Also compile the runtime with each individual public-data feature when changing
feature boundaries.

## Recent Milestones

- `4189b5e` `feat: add cryptofeed-rs workspace skeleton`
- `618c6c6` `feat: add cryptofeed core abstractions`
- `d6c0758` `feat: add public category crates`
- `a384bfc` `feat: add feedhandler runtime shell`
- `1efdef8` `feat: connect binance websocket runtime`
- `53d5c6e` `feat: add runtime reconnect backoff`
- `4b1768b` `feat: add bitget v3 public runtime`
- `be87f2e` `feat: add bybit okx public scaffolding`
- `449ff49` `feat: add candles public parity`
- `0f53a06` `feat: add binance funding liquidation parity`
- `(unreleased)` `feat: complete current public exchange protocol baseline`
- `(unreleased)` `fix: align live exchange routing and control frames`
- `(unreleased)` `feat: add okx full-book checksum validation`
- `(unreleased)` `test: add full-session websocket doubles for all exchanges`
- `(unreleased)` `test: document gateio non-bridging bootstrap failure and recovery`
- `(unreleased)` `docs: add license material and maturity note`
- `(unreleased)` `feat: add gateio bounded in-session resnapshot and bootstrap event dispatch`
- `(unreleased)` `feat: log gateio bootstrap failure forensics for live capture`
- `(unreleased)` `docs: add release metadata and phase F consistency check`
- `(unreleased)` `feat: add bybit v5 funding and liquidation channels`
- `(unreleased)` `feat: add okx v5 funding-rate and liquidation-orders channels`
- `(unreleased)` `feat: add bitget v3 liquidation channel`
- `(unreleased)` `feat: add open interest channel (okx standalone, bybit tickers stream)`
- `(unreleased)` `feat: add okx index-tickers channel with explicit index symbol`
- `(unreleased)` `docs: defer L3 with verified no-public-stream analysis`
- `(unreleased)` `feat: add l1 top-of-book channel (bybit orderbook.1, okx bbo-tbt)`
- `(unreleased)` `feat: add option product kind and bybit publicOption routing`
- `(unreleased)` `feat: add bybit option discovery and base-stream trades, okx option support`
- `(unreleased)` `test: retain binance option parser fixtures while disabling unverified live capability`
- `(unreleased)` `feat: add binance index price and l1 book; reject unverified websocket open interest`
- `(unreleased)` `feat: add bybit index price from derivative ticker stream`
- `(unreleased)` `feat: parameterize candle intervals and l2 depth levels with preflight validation`
- `(unreleased)` `feat: add mark price channel (binance, bybit, okx)`
- `(unreleased)` `feat: add okx option candles; verify bybit option kline unsupported`
- `(unreleased)` `feat: add okx margin product category with feed-context symbol rebind`
- `(unreleased)` `feat: add okx vip tick-by-tick depth channels`
- `(unreleased)` `fix: adapt to live protocol changes (bybit funding on tickers, okx liquidation instType scope, okx zero checksum, gateio order_book schema, binance eapi underlying)`
- `(unreleased)` `fix: resilient catalog hydration with bounded response size, status validation, retry, and 24h in-process cache`
- `(unreleased)` `fix: gateio resnapshot receiver replacement and cross-anchor buffered replay`
- `(unreleased)` `fix: current bybit liquidation/option ticker contracts and liquidation side/unit normalization`
- `(unreleased)` `fix: gateio fresh reconnect timestamps, rejected subscribe detection, and anchored id-less snapshots`
- `(unreleased)` `ci: add all-target lint, single-feature, MSRV, package-content, and security gates`

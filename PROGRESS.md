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
  liquidation events in the short window; see `docs/reports/live-smoke-2026-10-08.md`.

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
  checks exited cleanly; see `docs/reports/live-smoke-pre-push-2026-10-08.md`.
- GitHub repository confirmed: https://github.com/crypto-2042/cryptofeed-rs.
  It was public and empty at the engineering review, with ADMIN permission.
- The final review also covers derivative ticker delta reconstruction,
  dated Funding preflight, official candle interval vocabulary, calendar-month
  ends, and documented Binance index extraction. See
  `docs/reports/live-smoke-pre-push-2026-10-08.md` for manual evidence and
  limitations; automated gate results are recorded above.
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
  price-level only. See `docs/exchange-protocol-baseline.md` (L3 order-book scope).

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

## Python usage alignment — 2026-10-09

- Added a source-backed workflow gap inventory and phased improvement plan in
  `docs/python-usage-alignment.md`, excluding exchange/product expansion.
- Implemented product-qualified `MarketCatalog` discovery, sorted/deduplicated
  explicit pattern selection (`*`/`?`), and bulk builder symbols/instruments.
- Added `FeedHandler::run_with_shutdown` as a facade for the existing runtime
  watch-signal shutdown; this is not a new dynamic subscription mechanism.
- Force refresh, per-channel symbol maps, connection sizing, and runtime
  add/remove/replace remain planned. Catalogs retain the 24-hour cache policy;
  patterns do not automatically follow listings.
- Verification: 390 workspace tests passed; strict all-target Clippy, rustdoc,
  Rust 1.85, no-default catalog tests, all nine feature checks/tests, formatting,
  README example compilation, and local documentation links passed. No new
  live-service evidence or exchange wire changes are claimed.

## API currency review — 2026-10-09

- Rechecked selected enabled endpoint families/subscription contracts against
  current official sources for all five active exchanges; sources and scope
  are recorded in `docs/exchange-protocol-baseline.md`.
- Migrated OKX public/business WebSockets from retiring port 8443 to default
  TLS 443, and discovery to the recommended Global REST domain.
- Reproduced and corrected OKX SPOT catalog panic on preopen empty-identity
  records: non-live rows are filtered and malformed live rows return errors.
- Updated sanitized endpoint/catalog references and offline regressions;
  exchange capabilities and deferred Binance OI remain unchanged.
- Verification: 393 workspace tests, strict all-target Clippy, all nine feature
  checks, no-default compilation, Rust 1.85, and formatting passed. A 35-second
  OKX spot run received Ticker/Trade/L2/Candles and exited cleanly; see
  `docs/reports/live-smoke-2026-10-09.md` for counts and limitations.

## Usage alignment: catalog refresh and request sharing — 2026-10-09

- Added `MarketCatalog::refresh` with capability preflight and cache bypass
  propagated through every catalog category and Bybit pagination request.
- Coalesced overlapping same-URL fetches, including failure results; unrelated
  URLs remain concurrent. Reused the discovery HTTP client.
- Failed HTTP/JSON/envelope validation preserves prior cached responses.
  Cancelled leaders release the gate; later calls can retry normally.
- This is per-response caching, not atomic replacement of a multi-page catalog.
  Existing snapshots and subscriptions are not modified. Per-channel symbol
  maps, connection sharding and snapshot concurrency budgets remain pending.
- Verification: 398 workspace tests, strict all-target Clippy, rustdoc, all nine
  single-feature checks/tests, no-default catalog tests, Rust 1.85 and formatting
  passed. No endpoint/schema changes or new live-service evidence are claimed.

## Usage alignment: per-channel symbols — 2026-10-10

- Added channel-specific normalized and typed symbol configuration; repeated
  channels merge and deduplicate symbols. Shared configuration cannot be mixed
  with mapped configuration. Empty sets, unsupported channel/features and
  mixed products fail before catalog hydration.
- Resolve the logical symbol union once, preserving global explicit-native
  ambiguity checks, then group channels with identical symbol sets for existing
  exchange sessions. Exact pair filtering applies to callbacks/events/counters
  and L2 state. Existing feed_count remains a count of logical configurations.
- Distinct sets may open more sockets. Connection packing, capacity-based
  sharding, snapshot concurrency budgets and runtime hot updates remain pending.
- Offline tests cover concrete plans across all five active exchanges and
  runtime dispatch/state behavior; no endpoint/payload changes or new live
  service verification are claimed.
- Verification: 407 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation,
  formatting and local documentation links passed.

## Usage alignment: connection and snapshot resource planning — 2026-10-10

- Added native subscription-budget sharding after symbol-set grouping, retaining
  explicit native mappings and counting shared topics only once. Oversized
  singleton subscriptions and over-budget connection plans fail explicitly.
- Bybit spot subscribe frames batch at ten args; session queues pace control
  sends while keeping reads/heartbeats/shutdown active. Gate timestamps refresh
  at send. Current public APIs remain unchanged except the new batch helper.
- Handshake slots and start pacing are process-local; Binance/Gate snapshots
  share a reused HTTP client with four concurrent admissions and paced starts.
- Official limits, SDK choices and remaining weighted/global-IP/optimal-packing
  boundaries are recorded in `docs/connection-planning.md`. No new live load
  verification or exchange/channel capability expansion is claimed.
- Runtime controls, readiness/generation identity, consumer recovery and
  ecosystem features remain incomplete under the full alignment objective.
- Verification: 428 workspace tests, strict all-target Clippy, rustdoc, Rust 1.85,
  all nine feature checks/tests, no-default test compilation, formatting and
  local documentation links passed. Whole alignment remains in progress.
- Gate bootstrap now defers initial REST until its first requested depth delta
  is buffered. This prevents paced subscription queues from prefetching before
  a symbol begins streaming; full book pushes still establish state directly.

## Usage alignment: managed runtime commands — 2026-10-10

- Added retained RuntimeControl with bounded command queues, add/remove/replace,
  registry queries and shutdown. Core commands validate/hydrate candidates
  without stopping old generations; admitted replacement drains old tasks first.
- Added process-local FeedId, per-attempt configuration generations and tagged
  broadcast envelopes. SDK state resets on new generations; old queue entries
  remain identifiable. Scoped lifecycle includes concrete-group degradation.
- Controlled initial feeds start independently; legacy strict startup remains
  the default. Connection admission reserves old/new capacity during transitions.
- Offline tests cover lifecycle/cancellation, invalid/busy operations, state and
  source identity, confirmed forced child cancellation and panic recovery.
- Manual OKX BTC-to-ETH replacement observed one stable ID and two generations,
  zero registered feeds after removal, and exit 0; see
  `docs/reports/live-smoke-managed-2026-10-10.md`.
- Readiness/status snapshots, periodic listing reconciliation and phases 4–5
  remain incomplete. This is not completion of the overall alignment goal.
- Final verification: 444 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation and
  formatting passed. The deletion registry/ack ordering also has a multi-thread
  regression. The overall goal remains active with readiness/discovery and
  consumer/ecosystem requirements still unimplemented.

## Usage alignment: retained readiness — 2026-10-10

- Added independent `control.state(id)` snapshots, preserving Copy FeedInfo
  registry entries. Snapshots include lifecycle, connection attempts, native
  confirmation coverage, local L2 readiness and local receipt/event diagnostics.
- Managed Binance sessions use explicit SUBSCRIBE acknowledgements on existing
  routed endpoints. Bybit/Gate requests correlate IDs; Bitget/OKX confirmations
  match requested arguments. Unsent/unknown/duplicate/stale replies do not advance.
- Reconnect/resync clears relevant readiness and connection-owned book state;
  other connection books remain intact. Stop publication cannot be followed by
  stale Ready. Initial failed configured IDs remain queryable and repairable.
- Offline tests cover correlation, aggregation, timeout/rejection, stale epochs,
  book initialization, cache isolation, concurrent stop, status lag and failures.
- Public follow-up smoke reached Ready plus Trade/L2 observations on all five
  exchanges with exit 0; the earlier Bybit TLS failure is retained separately in
  `docs/reports/live-smoke-readiness-2026-10-10.md`.
- Listing reconciliation, consumer recovery/multiple handlers/candle filtering
  and ecosystem alignment remain unfinished under the overall goal.
- Final verification: 459 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation,
  formatting and documentation links passed. Overall alignment remains active.


## Usage alignment: automatic directory reconciliation — 2026-10-10

- Added opt-in DiscoveryFeed/Handle with product-qualified per-channel patterns,
  forced sequential refresh, SDK one-minute minimum and exponential backoff.
  Native mappings come from the exact catalog snapshot; unchanged healthy
  selections do not restart. Directory success remains distinct from readiness.
- Listing/removal/native alias changes replace the managed configuration;
  empty total selection/failures preserve the last nonempty feed. Worker-side
  compare-and-replace protects concurrent manual changes. Stop/drop ends future
  polling; explicit stop waits for accepted replacement and retains the feed.
- Current official catalog statuses filter unavailable Binance/Bitget/Bybit/Gate
  entries with matching sanitized references and inline regressions. Existing
  OKX live and Gate derivative delisting rules remain intact.
- Public OKX smoke observed two refreshes, no replacement, generation 1 Ready,
  27 events, explicit stop/removal and process exit 0. New listing/removal paths
  were scripted offline, not falsely attributed to a real listing event.
- Final verification: 471 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation and
  formatting passed. Consumer semantics/recovery, needed runtime policy,
  market metadata and optional ecosystem work remain under the active goal.


## Usage alignment: candle completion policy — 2026-10-10

- Added builder CandlePolicy All/ClosedOnly/ClosedOrUnknown at the shared
  dispatcher for every active exchange. All preserves existing Rust behavior;
  Python's inspected closed-only default is documented as an explicit difference.
- Filtered events do not reach handlers, raw/identified streams, counters or
  normalized observation metrics. Unknown remains None; receive time/end/reconnect
  never synthesizes finality. Bitget strict mode emits no candles with its current
  unknown-completion parser; the caller can explicitly retain unknown bars.
- Regression exercises all flags/policies and delivery surfaces with unchanged
  normalized values/identity and receipt-after-end behavior. No protocol/parser
  changes or new live-service claims were made.
- Final verification: 472 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation and
  formatting passed. Multiple handlers/error/deadline semantics, recoverable L2
  consumers and remaining policy/ecosystem increments are still active work.


## Usage alignment: multiple handlers — 2026-10-10

- Added add_*_handler for every data category. Existing setters still replace
  only the primary; primary runs first, followed by appended registrations.
  Add-only configuration is recognized; models are independently cloned.
- Book application, normalized observations, event publication and counting
  occur once per event before callbacks. Each invocation has its own existing
  five-second timeout; catchable construction/poll panics and timeout continue
  to later callbacks. Existing () traits keep business-error handling caller-owned.
- Documented serial backpressure, concurrent shared handlers across sessions,
  cancellation/partial effects and panic-abort/non-yielding limits. Bounded
  runtime shutdown can cancel slow callbacks before later registrations.
- Offline regressions cover order/replacement/add-only, independent values,
  single publication/count, panic and real timeout continuation, subsequent
  events and shutdown while a handler is pending. No exchange parser changed.
- Final verification: 475 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation,
  formatting and documentation links passed. Recoverable L2, remaining needed
  runtime policy, metadata and optional ecosystem work remain active.


## Usage alignment: recoverable L2 consumption — 2026-10-10

- Added opt-in FeedHandler.l2_book_handle and atomically acquired full local
  snapshots/subsequent bounded updates. Anchors contain configuration identity,
  physical connection, retry epoch and contiguous local revision; they are not
  native exchange sequence numbers. Legacy callbacks/streams are unchanged.
- Cache normalizes dispatch order independently of ahead-of-dispatch bootstrap
  state. Deltas require initialization; recovery assembles full arrays only when
  requested. Lag recovery replaces the old queue and local state atomically.
- In-session resync, disconnect/drop and stop/failure/removal withdraw affected
  cache state. Stale epochs/generations cannot alter newer views; cancelling a
  candidate does not retire the current generation or healthy sibling books.
- Regressions cover revisions/deletion, lag, resync, owner isolation, retirement,
  concurrent snapshot/subscription acquisition and runtime lifecycle hooks.
  Seven older test cases now declare the features their unchanged assertions
  require; the orderbook-only lib suite passes 209 tests without changing runtime
  capability preflight or removing full-feature assertions.
- Public OKX observed three consecutive L2 updates/revision 3, reconstructed
  400 bids/400 asks, recovery snapshot at the same revision and unavailable cache
  after removal, then exit 0. Offline lag is not presented as live packet loss.
- Final verification: 482 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation,
  orderbook-only lib tests, formatting and documentation links passed.
- Needed runtime policies, full market metadata and optional REST/history,
  recording/replay, sinks/aggregation and NBBO remain in the active overall goal.


## Usage alignment: runtime budgets and retry reset — 2026-10-10

- Added validated feed RuntimeOptions: optional finite transient retries,
  connection establishment timeout and per-callback timeout. Defaults stay None,
  20 seconds and five seconds; options follow concrete planning/replacements.
- Verified the sibling Python connection handler resets retries/delay after
  successful subscription writes. Runtime supervisors now reset their private
  counter/backoff at the same public-session initialization boundary: all queued
  writes completed, or first non-control text for legacy implicit Binance URLs.
  Partial writes do not reset; permanent rejection still fails immediately.
- Finite limits are per physical connection, exclude the initial attempt and do
  not cap lifetime healthy reconnects. Handshake budget excludes admission and
  other HTTP/ack/idle budgets. Shutdown grace and protocol pacing remain intact.
- Regressions cover defaults/zero rejection, initial-versus-retry counts,
  successful-init reset/permanent errors, partial/full subscribe queue progress,
  stalled handshake/result preservation, actual callback cancellation and option
  retention across channel partitioning. No wire parser or endpoint changed.
- Final verification: 488 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation,
  formatting and documentation links passed. No new live protocol claim.
- Explicit HTTP/WS proxy, caller idle policy and start delay remain unfinished;
  full market metadata and optional ecosystem work remain in the active goal.


## Usage alignment: startup delay and idle receipt policy — 2026-10-10

- RuntimeOptions adds zero-default startup delay and IdlePolicy ExchangeDefault,
  positive After(duration), or Disabled. Incoming transport receipts anchor idle
  deadlines; outgoing heartbeats do not. Overrides do not rewrite heartbeat
  payload/cadence, Ping/Pong handling or subscription-confirmation deadlines.
- Startup delay runs once per physical supervisor after hydration/before admission;
  retries use backoff and replacement generations delay again. Shutdown/removal
  cancels the wait, and false watch notifications do not restart its timer.
- Offline regressions cover custom idle expiry, disabled idle beyond the previous
  deadline with continuing heartbeat/shutdown, zero rejection, option retention,
  startup cancellation before work and false watch notifications. No protocol
  fixture or external endpoint changed; no new live evidence is claimed.
- Final verification: 492 workspace tests, strict all-target Clippy, rustdoc,
  Rust 1.85, all nine feature checks/tests, no-default test compilation,
  formatting and documentation links passed. Explicit HTTP/WS proxy, full market
  metadata and optional ecosystem work remain under the active overall goal.


## Usage alignment: explicit HTTP/WS proxy transport — 2026-10-10

- Added cloneable TransportConfig for direct or HTTP proxy routing and separate
  Basic authentication. Feed hydration/pagination, standalone/periodic catalogs,
  every WS and Binance/Gate bootstrap/resnapshot use the same route. Clones reuse
  a client/cache identity; distinct/direct/auth configurations isolate caches.
- Default HTTP now explicitly bypasses environment/system proxy inference to
  match WS direct behavior; users relying on HTTP-only inference must configure
  routing explicitly. Unsupported schemes/userinfo/path/query/fragment fail
  before network; proxy failure never silently falls back to direct.
- CONNECT preserves target TLS verification, bounds headers, handles interim
  statuses/IPv6 and does not consume tunnel bytes. Debug/header/error handling
  excludes credentials and proxy reason/body echoes; CONNECT 407 is permanent
  configuration failure. Existing HTTP limits, admission and cancellation remain.
- Duplex regressions cover auth, limits/errors/redaction, IPv6/interim success,
  non-overread and cache/client/planning preservation. No default test requires
  a listening proxy or external connection. No exchange parser/fixture changed.
- Authenticated local-forwarder smoke observed 1,375 Binance spot catalog symbols,
  authenticated api/WS tunnels, Trade/L2 Ready with 9 events and revision 8 local
  book (1,004 bids/1,001 asks), clean removal/shutdown and exit 0. Forwarder stopped;
  neither proxy credentials nor TLS contents were retained in the public report.
- Final verification: 497 workspace tests, strict all-target Clippy, rustdoc,
  all nine feature checks/tests, no-default test compilation, formatting/links,
  and Rust 1.85 against fresh no-lockfile dependency resolution passed. Base64
  0.22 is an explicit small dependency; Cargo.lock remains untracked.
- Full market metadata, optimized unequal-set packing/remaining resource policy
  and optional REST/history, recording/replay, sinks/aggregation and NBBO remain
  unfinished under the overall alignment goal. HTTPS/SOCKS/PAC/custom CA and
  general Python config-file parity are not promised by this HTTP proxy increment.


## Usage alignment: exact sparse native packing — 2026-10-10

- Replaced equal-symbol-set grouping with exact pair-aware native subscription
  generation across all five adapters. Unequal sets share endpoint connections;
  contiguous union slices respect native budgets and trim empty channel rules.
  First-seen normalized/native order, global ambiguity and capability checks stay.
- L2 REST URL lists, initial snapshot jobs, Gate resnapshot indexing, retained
  readiness and recovery ownership now use only the L2 subset. Unrequested depth
  cannot create sync/cache/bootstrap work. A manually incomplete public symbol
  union with remaining private channel rules is rejected before connection.
- Exact request parity and sanitized references cover five Spot adapters and
  required Binance USD-M, OKX business/public and Gate settlement-product splits.
  Capacity tests assert pair uniqueness, native alignment and empty-channel removal;
  existing handler/event/counter/book filtering remains covered.
- Five-exchange Spot smoke reached Ready with one connection, three confirmations,
  one synchronized BTC book and three observed pairs each. Every BTC recovery view
  was present and ETH absent; all epochs were 1, no last errors, process exit 0.
  Event observations: Binance 200, Bitget 418, Bybit 134, OKX 86, Gate 145.
- Final verification: 502 workspace tests (87 public parity), strict all-target
  Clippy, rustdoc, all nine feature checks/tests, no-default test compilation,
  Rust 1.85/fresh resolution, formatting and documentation links passed.
- This is deterministic contiguous packing, not a proven global minimum solver.
  Full market metadata, global allocation/configurable/weighted resource policies
  and optional public REST/history, recording/replay, sinks/aggregation and NBBO
  remain under the active overall goal. No exchange capability cell expanded.


## Usage alignment: typed market metadata — 2026-10-10

- MarketCatalog now exposes immutable MarketInfo records, sorted iteration,
  exchange/product identity and current-build public WS capabilities. Existing
  exact native lookup delegates to the same metadata record, preventing drift.
- Existing directory payloads supply explicit increments, separate decimal-place
  counts, reported limit minima, native status/type and independent contract
  value/currency/multiplier/settlement fields. No per-symbol request is added.
- Current field applicability is verified: Binance filters by name rather than
  position/precision, Bitget futures multipliers separate from counts, deprecated
  Bybit Spot minQty omitted, OKX contract fields independent, Gate zeros retained.
  Inapplicable fields stay None; no price step, currency or event-unit conversion
  is inferred. Metadata is not an order validator or authenticated capability.
- Exact JSON/scientific Decimal conversion rejects inexact/malformed known data;
  duplicate metadata conflicts fail while native ambiguity preserves its typed
  error. Previous snapshots remain immutable; metadata-only discovery updates do
  not restart unchanged subscriptions. Sanitized helper fixtures/assertions agree.
- Live follow-up loaded all ten Spot/Perpetual catalogs, checked BTC metadata/native
  identity and exited 0. Earlier Binance COIN-M request failure and curl TLS
  failure are preserved separately, not hidden or labeled an obsolete endpoint.
- Final verification: 511 workspace tests, strict all-target Clippy, rustdoc,
  all nine feature checks/tests, no-default compilation/metadata execution,
  Rust 1.85 against fresh dependency resolution, formatting/links passed.
- Global allocation/resource policies and optional public REST/history,
  recording/replay, sinks/aggregation and NBBO remain unfinished under the active
  overall goal. Full trading-rule/account validation is outside this SDK scope.


## Usage alignment: public REST snapshots and HTTP backoff — 2026-10-10

- Added PublicRestClient catalog-bound ticker/L2 snapshots on current five-exchange
  public routes, with category feature gates, native identity/product validation
  and normalized existing models. Unknown symbols/depth fail before HTTP; no
  subscription, account/trading operation or OI polling was added.
- RestSnapshot exposes optional native time/sequence separately from received
  time and the existing model fallback. Book prices/sizes and IDs remain exact;
  levels are sorted/bounded, duplicate/negative sizes fail. Native quantities
  are not converted or joined to live recovery revision anchors.
- Directory, REST and bootstrap now share four active HTTP slots/one-second
  starts, per-venue Retry-After cooldown, cancellation and bounded bodies. Cooldown
  waiters recheck extensions without occupying slots or blocking other venues.
  Structured HTTP errors survive catalog coalescing; HTTP statuses are not
  immediately retried. Existing network/JSON/envelope retry remains one.
- First live query run exposed a Gate Spot timestamp-unit bug despite successful
  HTTP results. Verified current Spot millisecond versus derivative second fields,
  corrected by product, and updated narrow assertions/sanitized references.
  Final twenty Spot/Perpetual ticker/book queries passed identity/time checks,
  each book had 20 bids/20 asks and the process exited 0; earlier bad time output
  remains documented rather than counted as a passing normalization check.
- Final verification: 524 workspace tests, strict all-target Clippy, rustdoc,
  all nine feature checks/tests, no-default compilation/capability execution,
  Rust 1.85/fresh dependency resolution, formatting and documentation links passed.
  httpdate is a small parsing dependency; HTTP-response and Tokio paused-clock
  test support remain dev-only. Cargo.lock is untracked.
- Trade/candle/funding history and bounded pagination, recording/replay,
  sinks/aggregation, NBBO and weighted/native-code/distributed resource policies
  remain unfinished under the active full alignment goal. Snapshot REST is not
  claimed as complete Python REST/history or trading-rule parity.


## Usage alignment: bounded funding settlement history — 2026-10-10

- Added feature-gated FundingHistoryQuery/result and version-1 JSON continuation
  cursor to PublicRestClient, on five current perpetual/swap history surfaces.
  Half-open integer-ms ranges, 1..100 rows/pages and <=10,000 scanned rows/call
  bound work/memory. Scope/version/position/native mapping validate before HTTP.
- Implemented native ascending/backward/Bitget-v3 page-cursor progression, exact
  signed actual rates and explicit Gate second conversion. OKX realizedRate is
  actual, never replaced by forecast fundingRate; no next time/rate is inferred.
  Non-Regular Binance types/Spot/delivery funding remain explicitly unsupported.
- Stop reasons distinguish budget, requested boundary, source exhaustion and
  native cursor ceiling; no full-retention boolean is offered. Conflicts,
  oversized/stalled/mismatched pages fail. Cancellation drops pending fetch;
  errors do not return undisclosed partial success. Returned batches sort by time.
- Regressions cover JSON resume, scope edits, native request boundaries/shape,
  rate/time semantics, negative numeric precision, empty/capped/stalled/conflicting
  data and later-page cancellation. Matching sanitized references/provenance agree.
- Public five-venue first/resume smoke observed 10+10 records and 2+2 pages each,
  no repeated identity/time, all BudgetReached with further cursors, exit 0.
  This is 20 pages/100 per-exchange records, not a complete seven-day history claim.
- Final verification: 533 workspace tests, strict all-target Clippy, rustdoc,
  all nine feature checks/tests, no-default compilation, funding-only history
  execution, Rust 1.85/fresh resolution, formatting/documentation links passed.
- Trade/candle history, recording/replay, sinks/aggregation, NBBO and advanced
  resource policies remain unfinished under the active full alignment goal.
  Private/trading/instrument-type coverage and Binance OI deferral stay unchanged.

## Bounded candle history: Binance/Bybit — 2026-10-10

- Added candle-feature query/result/scoped JSON continuation, backward time windows,
  common 100-row/100-page caps and at most 200-day requests. Empty windows advance
  within the budget; RangeBoundary means queried windows, not complete retention.
- Current Spot/USD-M/COIN-M and Bybit v5 spot/linear/inverse routes reuse exact
  catalog mappings. OHLCV stays Decimal, native volume units stay intact, Binance
  count/end preserved, Bybit monthly ends use calendar UTC and inclusive 1ms ends.
  Both REST surfaces have unknown finality; no clock-based close inference.
- Offline tests cover normalization, boundaries, scope mutation, empty/duplicate/
  malformed pages, leap-year months, JSON resume and cancellation. Coverage for
  Bitget/OKX/Gate candle history and trade history remains pending.
- Manual four-product public smoke: first and JSON resume each returned 10 bars
  over 2 pages, no repeated open times, all BudgetReached with cursors; exit 0.
  See [evidence](docs/reports/live-smoke-candle-history-2026-10-10.md).
- Verification: 540 workspace tests, 7 candle-only history tests, nine individual
  feature checks/boundary-test runs, no-default compilation, strict all-target
  Clippy, rustdoc, formatting/relative links and Rust 1.85 with fresh resolution
  passed. Single-feature builds retain existing unrelated unused-code warnings.

## Five-venue candle history — 2026-10-10

- Extended current public candle-history queries to Bitget v3, OKX v5 and Gate
  v4 Spot/perpetual. Gate delivery was initially omitted after an incomplete document inspection;
  the later full-reference recheck restores its documented candle route/capability.
- Bitget aligns native end boundaries, limits requests to 90 days and accepts
  exactly one documented earlier overlap, counted against raw scan limits.
  Gate sends the first legal bar open, second-valued from/to, no conflicting
  limit; Spot base volume/completion and perpetual contracts stay distinct.
- OKX calendar months/quarters use UTC+8, native confirm is preserved; Gate 30d
  is a calendar month. Gate perpetual and Bitget finality stay unknown.
- Offline regressions cover all new row families and JSON continuation, native
  request routing/units, 90-day caps, UTC+8 quarters, completion/volume shapes,
  Gate empty subsecond windows/delivery rejection and Bitget overlap continuity.
- Final ten-product Spot/perpetual public first/resume smoke passed: 10+10 bars,
  2+2 pages per product, no duplicate opens, BudgetReached/next=true; 40 pages
  and 200 per-product records, exit 0. Earlier Gate/Bitget failures remain in
  the [dated report](docs/reports/live-smoke-candle-history-2026-10-10.md).
- Gates passed: 547 workspace tests; focused candle-only REST tests; nine
  individual feature checks and no-default compilation; strict all-target Clippy,
  rustdoc, formatting/local links and Rust 1.85 with fresh dependency resolution.
  Recording/replay, trade history, sinks/aggregation, NBBO and advanced resource
  policies remain under the active alignment objective.
- Additional Gate day/week/month public probes confirmed UTC daily/calendar
  month opens and caught different weekly grids: Spot Monday versus perpetual
  epoch alignment. Product-specific request rounding and regressions cover both.

## Public recent trades — 2026-10-10

- Added trade-feature PublicRestClient::recent_trades on five current public
  routes, category/product-qualified and bounded by per-source/SDK row caps.
  Invalid inputs fail before HTTP; no paging or completeness claim is made.
- Prices/amounts use exact Decimal, signed Gate contract quantity becomes absolute
  with explicit side, Binance maker flag converts to taker side. Native IDs stay
  exact (including >2^53); Bitget uses execId, not execLinkId. Same-time executions
  survive and sort stably by exact native time before model f64 conversion.
- Recent Binance individual trade IDs are distinct from Python/Rust WS aggregate
  IDs; the guide documents granularity and prohibits cross-namespace dedup claims.
  Historical pagination/aggregate retrieval remains the next increment.
- Offline tests cover five venue/product row families, both sides, caps/routing,
  signed/numeric precision, explicit Gate seconds fallback, stable same-time IDs,
  duplicates/malformed/oversized/mismatched payloads and pre-HTTP validation.
- Public Spot/perpetual smoke passed for all five venues: 5 records/unique IDs per
  product, ascending time and matching identity, ten queries/50 executions, exit 0.
  See [evidence](docs/reports/live-smoke-recent-trades-2026-10-10.md).
- Trade-only focused REST tests and the Gate full-session double now compile and
  pass without orderbook. Fixed that existing double's unconditional book-only
  arguments with matching cfg attributes, retaining its trade-only coverage.
  Nine individual feature checks and no-default compilation passed.
- Final verification: 553 workspace tests, strict all-target Clippy, rustdoc,
  formatting/local links, trade-only REST/session tests and Rust 1.85 all-target
  checks with fresh resolution passed. Historical trade pagination, recording/
  replay, sinks/aggregation, NBBO and advanced resource policies remain active.

## Native trade-history continuation — 2026-10-10

- Added explicit Aggregate/Individual queries/results, bounded raw scan/page
  budgets, scope-bound JSON cursors and separate history-capability reporting.
  Binance current aggregate time seeds advance via IDs, OKX time seeds switch
  to native ID paging, Gate fixed ranges use current page/offset parameters.
- Same-millisecond execution IDs survive continuation; no timestamp+1 skip.
  Gate Spot/perpetual are pageable; delivery time ranges are one native page,
  SourceLimit if full, without its retired last_id or an invented offset. Bybit/Bitget
  current implemented public surfaces remain recent-only; no private fallback.
- Corrected Gate contract create_time_ms to fractional seconds, distinct from
  Spot millisecond counts. Matching capture and near-epoch regression prevent
  magnitude inference. Recent smoke now verifies a five-minute age lower bound.
- Tests cover native same-time ID progression, JSON scope edits, bounded empty
  seeds/offset ceilings, precision/granularity, boundary filtering, stalls and
  later-page cancellation. Source/budget/boundary/ceiling stops are explicit;
  none guarantees complete retention or an exchange-atomic historical view.
- Public six-product first/JSON-resume smoke passed: each batch 10 selected/
  scanned records over two pages, unique IDs/range identity checked, all budget-
  limited with further cursors. 24 pages/120 executions; earlier observations
  are retained in [evidence](docs/reports/live-smoke-trade-history-2026-10-10.md).
  Corrected ten-product recent smoke also passed with the added age lower bound.
- Full Gate delivery documentation recheck corrected the earlier candle omission:
  current USDT delivery candle path/capability and contract normalization now have
  a focused offline regression. Historical delivery ranges are bounded one-page
  queries, SourceLimit if full; no delivery live result is claimed.
- Final gates passed: 564 workspace tests, nine focused history regressions,
  trade-only/candle-only REST suites, all nine feature checks, no-default build,
  strict all-target Clippy, rustdoc, formatting/local links and Rust 1.85 all-target
  fresh resolution. Recording/replay, sinks/aggregation, NBBO and advanced resource
  policies remain under the active full-alignment objective.

## Normalized recording/replay — 2026-10-11

- Added opt-in recording feature: version-1 JSONL header/event/footer, contiguous
  sequence and source feed/generation labels, preserved normalized models/Decimal
  strings and original clocks. No transport/config/headers/raw/error text captured.
- Bounded events/bytes/lines, footer reserve, size-limited encoding/read buffers,
  five-second I/O deadlines and poisoned state after partial cancellation/failure.
  Broadcast lag is explicit and produces no successful footer. Limited/stopped
  prefixes are distinct from natural source closure; no fsync promise is made.
- Offline replay invokes sequential callbacks with immediate or absolute recorded
  timing, callback deadline and catchable panic/error handling. Cancellation/drop
  cannot resume and skip a pending event. No network/live feed/cache mutation.
- Focused tests cover all enabled categories, L2 snapshot/delta, committed format
  reference/inline fixture, numeric precision, truncation/version/sequence/scope,
  resource bounds, lag/quiet limits, false-stop signals, I/O deadlines/poisoning,
  ordered timing, callback errors/panics and replay cancellation.
- Normalized replay is one part of the larger objective; sanitized raw protocol
  capture/replay, sinks/aggregation, NBBO and advanced resource policies remain.
- Public OKX Spot workflow captured 20 identified normalized trades (6,738 bytes),
  stopped the managed runtime, then replayed 20 models offline with source/model
  identity and footer/count checks; exit 0. See [evidence](docs/reports/live-smoke-recording-2026-10-11.md).
- Final gates passed: 577 all-feature workspace tests, 13 recording/Trade-only
  focused cases plus disabled-category/boundary tests, every recording/category
  combination and no-default build, strict all-target Clippy/rustdoc, formatting/
  local links and Rust 1.85 all-feature all-target fresh resolution. CI/Makefile
  now include the optional feature boundaries. No capture output/lockfile committed.

## Public raw WS observation boundary — 2026-10-11

- Added recording-feature raw_capture_channel and exchange-builder attachment,
  actual successful-connection/send/inbound-text/closure hooks on all five runtime
  consumers. Observe input before heartbeat/readiness/control/market filtering.
- Shared immutable public feed/session context includes native mappings, sparse
  channel rules, policy and source labels, never transport URL/credentials/headers.
  JSON numeric literals preserve precision; known credential/diagnostic fields
  redact, private auth/topics reject, and non-JSON text is limited to ping/pong.
- Bounded nonblocking queue/text/depth, sticky overflow/closed/private/malformed
  failure, serialized global enqueue order and fresh reconnect session IDs.
  Capture failure does not fail healthy feed processing. Receiver owns no sender.
- WS normalization now reuses the exact text-read receipt clock. Narrow WS double
  verifies capture before pong filtering; no native exchange timestamp changed.
- Raw persisted file/HTTP bootstrap capture and complete parser/session/L2 replay
  remain pending, distinct from the existing normalized JSONL workflow.
- Public OKX smoke observed 20 market packets/24 observations and 20 normalized
  trades, exact shared receipt clocks/sequence/source verified, exit 0; see
  [evidence](docs/reports/live-smoke-raw-capture-2026-10-11.md).
- Final gates passed: 584 all-feature workspace tests, recording-only raw tests,
  each data feature alone and paired with recording, no-default build, strict
  Clippy/rustdoc, format/local links, package contents and Rust 1.85 all-feature
  all-target fresh resolution. Final tests/lints/docs also passed on that resolved
  dependency set. Actual public payloads and Cargo.lock are not committed.

## Raw WS segment files — 2026-10-11

- Added separate version-1 raw-ws JSONL writer/reader/stream capture, sharing
  bounded encoding/line I/O with normalized recording. Explicit header/footer,
  quotas, session ceiling and partial-I/O poison/deadline behavior are preserved.
- Validate contiguous global observation order, monotonic elapsed time, finite
  clocks, public mapping/sparse scopes, immutable context, fresh connection IDs
  and Connected/Sent/Received/Closed lifecycle. Natural Complete requires closed
  sessions; Stopped/LimitReached are deliberate prefixes with open sessions allowed.
- Revalidate privacy on writes/reads: unsafe or private records fail rather than
  becoming file output/callback input. Reader interns validated context. No native
  parser or network is invoked by file validation; HTTP/L2 replay remains pending.
- Tests cover exact JSON-number roundtrip, unsafe metadata/payload, truncation/
  versions/gaps/extra records, frozen context/lifecycle, resource/session limits,
  source queue failure, partial cancellation and I/O deadlines.
- Public OKX capture accepted 24 observations/18 trade-channel packets/15,672 bytes,
  shut down runtime, then validated the saved file offline; LimitReached, exit 0.
  See [evidence](docs/reports/live-smoke-raw-recording-2026-10-11.md).
- Final gates: 592 all-feature workspace tests, eight raw-file regressions,
  recording-only/category combinations and no-default checks, strict Clippy/
  rustdoc, formatting/local links, package contents and Rust 1.85 all-feature
  all-target fresh resolution passed. Final tests/lints/docs also passed with
  that dependency resolution. Native parser/session/HTTP/L2 replay remains active.

## Offline native WS parser/state replay — 2026-10-11

- Added fresh-reader raw replay API with Started/Market/Ended callbacks and
  source/session/observation labels. Reuses existing runtime parsers, mapping,
  scoped dispatch and CandlePolicy with local caches, no network/hydration/live
  counters/recovery-store publication. Disabled channels fail at connection start.
- Bybit ticker snapshot/delta and Bybit/OKX/Bitget WS-native book sync reset per
  session; strict sequences/checksums/bridges remain unchanged. Binance/Gate L2
  explicitly reject missing HTTP bootstrap instead of fetching live data.
- Output batch/total budgets fail before partial model delivery from an oversized
  frame. Sequential immediate/absolute-time callbacks retain original clocks;
  error/panic/deadline/cancellation/drop cannot resume after skipping an input.
- Tests cover five trade families, derivative/dated identity, sparse/candle policy,
  ticker/book reset, valid/invalid WS sync and nonzero CRC, timing, output limits,
  callbacks/poisoning and disabled/missing-HTTP preflight. Bitget's initially
  mistaken gap test was corrected to a true disjoint [12,13] first bridge; no
  native validator/expected output was relaxed.
- Pure offline prior OKX file replay produced all 17 trade rows from 24 observations.
  Five new Spot captures replayed 60 observations/93 models; every Trade exactly
  matched live fields/clocks, exit 0. See [evidence](docs/reports/live-smoke-raw-replay-2026-10-11.md).
- HTTP/bootstrap and Binance/Gate L2 replay, segment rotation/merge, sinks/
  aggregation, NBBO and advanced resource policies remain under the full objective.
- Final gates passed: 602 all-feature workspace tests, ten native replay cases
  plus disabled-channel recording-only coverage, individual recording/category
  checks and no-default compilation, strict Clippy/rustdoc, formatting/local
  links, package contents and Rust 1.85 all-feature all-target fresh resolution.
  Final tests/lints/docs also passed on that dependency set. No real raw payload
  or Cargo.lock was committed; remaining HTTP/L2/ecosystem scope stays active.

## Consumed HTTP and causal L2 replay — 2026-10-11

- New raw protocol v2 preserves v1 reading while adding explicit WS Processing
  references and consumed HTTP snapshot/failure records. Received is not proof
  of processing; native snapshot polling occurs between receive and processing.
- Recording-only response slots retain safe JSON/original HTTP receipt time until
  actual consumption. No URL/headers/error text enter snapshots. Safe categories/
  status propagate for no-body failures; superseded/cancelled results are not dumped.
- Binance/Gate raw L2 now reuses native processing/bootstrap/poll/reset/resnapshot
  code, injecting recorded ready results. Offline schedules pending placeholders
  with no spawned HTTP task. No bridge/gap/checksum rule was changed.
- Regressions compare all native/replay models and originating observations,
  id-less Gate anchoring, strict overlap resync, missing pending response/HTTP429,
  causal/version corruption, unprocessed prefixes and legacy v1 behavior.
- Public Spot L2: each profile captured 200 observations/2 HTTP snapshots/97
  processing markers; Binance replayed 90 models, Gate 98, each exactly matching
  live fields/clocks after network shutdown; exit 0. See [evidence](docs/reports/live-smoke-http-l2-replay-2026-10-11.md).
- Generic catalogs/HTTP capture, rotation/merge, broader product/concurrency edge
  audits, sinks/aggregation, NBBO and advanced resources remain active.
- Final gates passed: 608 all-feature workspace tests, recording-only and
  recording/orderbook HTTP regressions, all standalone/recording-category feature
  combinations and no-default build, strict Clippy/rustdoc, format/local links,
  package contents and Rust 1.85 all-feature all-target fresh resolution. Final
  tests/lints/docs also passed on that dependency set. Real responses/lockfile
  remain uncommitted; broader isolation/ecosystem scope is not marked complete.

## Physical connection state isolation — 2026-10-11

- Fixed Bybit product and OKX public/business sparse feed projections: retained
  channels, normalized/native symbol unions and exact rules now agree. Previously
  stale rules could invalidate recorded session contexts.
- Reconnect resets explicitly target owned L2 pairs. Preserved existing Binance
  snapshot-route guards and Gate per-plan instrument scope; unrelated books stay
  initialized and can consume subsequent increments.
- Seven offline regressions cover projection, native-name alignment, raw file
  roundtrips and cache ownership. No exchange protocol or fixture interpretation
  changed; no new live smoke certification is claimed.
- 615 all-feature workspace tests, strict Clippy/rustdoc, no-default and individual
  category checks and fresh-resolution Rust 1.85 all-target/all-feature check passed.
  Generic HTTP capture, rotation, broader replay audits,
  sinks/aggregation, NBBO and advanced resource policies remain pending.

## Sequential event sinks and JSONL adapter — 2026-10-11

- Added EventSink/run_sink over identified normalized events, without a hidden
  task/queue/retry or new dependency. Owned envelopes preserve source generations,
  decimals and wire clocks; elapsed consumer time is a separate monotonic value.
- Sequential writes and consuming finalization have configurable positive
  five-second default deadlines and catchable panic handling. Lag/errors/uncertain
  in-flight cancellation return errors and drop the sink without a success footer.
  Shutdown between writes finalizes Stopped; source closure drains then Complete.
- RecordingWriter implements the contract using the existing bounded JSONL format.
  Summary counts reflect the actual accepted prefix, including an oversized input
  rejected before writing and the exact event cap. Flush does not promise fsync.
- Ten focused tests cover order, generations, precision, slow-consumer lag,
  deadlines, errors, panics, stop/drop and bounded JSONL reader roundtrips.
  Database/message-bus adapters, aggregation and NBBO remain pending.
- Public OKX Spot sink smoke: 20 events/6,765 bytes, LimitReached, managed
  shutdown then 20 offline replay callbacks; exit 0. Raw output stays temporary;
  [sanitized report](docs/reports/live-smoke-sink-2026-10-11.md) records scope.
- Final gates: 625 all-feature workspace tests, minimal trade/recording-trade
  sink tests, no-default/every standalone feature, strict Clippy/rustdoc and
  fresh-resolution Rust 1.85 all-feature/all-target check passed. CI now runs
  the minimal sink feature tests. Cargo.lock/target are not committed.

## Caller-clocked trade OHLCV — 2026-10-11

- Added trade-feature Ohlcv/TradeBar, independent of candles/storage. Preserved
  Python OHLCV arrival-order OHLC/amount-weighted VWAP semantics with explicit
  monotonic, whole-second elapsed windows aligned to zero rather than wall time.
- State is isolated by feed ID/generation/exchange/symbol and bounded to at most
  4096 configured series. Timer advance closes windows without new trades; large
  jumps skip empty bars; consuming finish returns an explicitly partial tail.
- Native quantity units remain intact, including contracts. Checked Decimal
  arithmetic keeps finite precision/rounding explicit; invalid inputs, backwards
  consumer clocks, capacity and unrepresentable arithmetic/bounds fail atomically.
- Twelve regressions cover OHLCV, precision, native units, source/generation
  isolation, boundary/idle/late-clock behavior, limits, atomic failures and full
  bar-field equality between direct input and normalized recording replay.
- Throttle/Renko, custom aggregation, NBBO, broader storage adapters and remaining
  recording/resource-policy scope stay active; this is not exchange candle parity.
- Offline example over the earlier 20-trade public OKX recording produced 11
  populated one-second bars (10 closed, 1 partial), with input/output counts and
  native amount sums matching; exit 0. [Report](docs/reports/offline-ohlcv-2026-10-11.md).
- Gates passed: 637 all-feature workspace tests, minimal trade and recording/trade
  aggregation tests, no-default/every standalone feature, strict Clippy/rustdoc.
  Fresh-resolution Rust 1.85 all-feature/all-target compilation also passed;
  resolved dependencies match the tested set. CI now includes the minimal
  aggregation feature tests.

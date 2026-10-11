# Python Baseline vs Rust Parity Checklist

Last updated: 2026-10-10

## Goal

`cryptofeed-rs` aligns core normalized public-market behavior with Python
`cryptofeed` while using the current official protocols for Binance, Bitget,
Bybit, OKX, and Gate.io. Python remains a semantic migration reference; official
exchange documentation and sourced fixtures define protocol correctness.

## Verified Baseline

- [x] Preserve the `FeedHandler` user entrypoint.
- [x] Keep normalized public models in category crates and exchange protocols in
  `crates/runtime`.
- [x] Preserve decimal price and quantity precision and separate exchange and
  receive timestamps.
- [x] Preserve spot, perpetual, and dated-futures product identity.
- [x] Resolve symbols bidirectionally with product-qualified catalogs.
- [x] Reject unknown, ambiguous, mixed-product, and unsupported configurations
  before connecting.
- [x] Process every entry in supported batched market-data messages.
- [x] Implement application heartbeat, idle timeout, clean-close reconnect,
  graceful shutdown, and independent feed failure isolation.
- [x] Reset L2 state on replacement snapshots and preserve product-aware
  snapshot/gap recovery.
- [x] Drive every active exchange through a full-session WebSocket double:
  subscribe framing, control acknowledgement, market-data delivery, injected
  snapshot bootstrap, and clean shutdown.
- [x] Back normalization assertions with sanitized, sourced official protocol
  references.

## Usage alignment

Phase 1 exposes product-qualified catalog discovery, explicit `*`/`?` selection,
bulk symbol configuration, and the existing watch-signal shutdown via
`FeedHandler`. Deterministic catalog tests cover ordering, overlaps, unmatched
patterns, unsupported products, and typed configuration. Exchange/channel
capabilities are unchanged. Managed runtime controls/readiness and opt-in
[automatic directory reconciliation](docs/discovery.md) are implemented.
[CandlePolicy](docs/candle-delivery.md) also supports confirmed-final or
final/unknown delivery while preserving the all-update default.
[Multiple handlers](docs/handlers.md) now execute serially with per-invocation
timeout and catchable panic isolation; legacy primary setters remain compatible.
[L2 recovery](docs/l2-recovery.md) adds opt-in full snapshots plus anchored
updates and scoped invalidation; legacy broadcasts remain bounded/lossy.
[RuntimeOptions](docs/runtime-options.md) exposes retry/connection/callback
budgets, startup delay and receipt watchdog overrides/disable, preserving
heartbeat and confirmation policies.
[TransportConfig](docs/transport.md) shares explicit proxy routing across
catalogs, REST book snapshots/resync and WS, with scoped caches and direct defaults.
Unequal channel/symbol sets now share native connections, with exact request
args, L2-only snapshot ownership and capacity-preserving rule slices.
[Five-exchange sparse smoke](docs/reports/live-smoke-packing-2026-10-10.md) reached
Ready on one Spot socket each; required endpoint/product splits remain asserted.
[MarketInfo](docs/market-metadata.md) now exposes verified directory constraints
and contract fields with exact Decimal/applicability rules and build-specific
channel context. No event units or capability cell changed.
[PublicRestClient](docs/public-rest.md) adds ticker/book snapshots with optional
native times/IDs and shared HTTP-status backoff. [Funding history](docs/funding-history.md)
adds bounded actual-settlement batches/cursors; trade/candle history remains pending.
Scripted regressions cover listings/removals, native-name changes, empty/failing
refreshes, stop/drop and concurrent manual ownership changes. Remaining workflow gaps and acceptance criteria are
tracked in [the usage alignment plan](docs/python-usage-alignment.md).

## Capability Matrix

| Exchange | Products | Ticker | Trades | L2 | L1 | 1m candles | Funding | Liquidations | Open interest | Index | Mark price |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance | spot | yes | yes | yes | yes | yes | no | no | no | no | no |
| Binance | USD-M / coin-margined perpetual and futures | yes | yes | yes | yes | yes | yes (perpetual only) | yes | no | yes | yes |
| Binance | European options | no | no | no | no | no | no | no | no | no | no |
| Bitget v3 | spot, USDT/USDC/coin futures | yes | yes | yes | yes | yes | yes (perpetual only) | yes (derivatives) | yes (derivatives) | yes (derivatives) | yes (derivatives) |
| Bybit v5 | spot, linear, inverse | yes | yes | yes | yes | yes | yes (perpetual only) | yes (derivatives) | yes (derivatives) | yes (derivatives) | yes (derivatives) |
| Bybit v5 | options | no | no | no | no | no | no | no | no | no | no |
| OKX v5 | options | no | no | no | no | no | no | no | no | no | no |
| OKX v5 | spot, swap, futures | yes | yes | yes | yes | yes | yes (swap) | yes (swap/futures) | yes (swap/futures) | yes (spot explicit index; swap/futures mapped index) | yes (swap/futures) |
| OKX v5 | MARGIN (explicit symbols) | no | no | no | no | no | no | no | no | no | no |
| Gate.io v4 | spot, USDT/BTC perpetual, USDT delivery | yes | yes | yes | yes | yes | yes (perpetual only) | yes (perpetual only) | yes (derivatives) | yes (derivatives) | yes (derivatives) |

Bybit funding rides the derivative `tickers.{symbol}` stream (the standalone
`funding.{symbol}` channel was removed by Bybit; verified live 2026-08-06).
Option and MARGIN products are outside the 0.1 release scope and are rejected
by capability preflight. Their protocol parsers, catalog helpers, fixtures, and
tests remain as implementation references for a future explicit plan. Mark
price rides the funding/mark-price streams: Binance `@markPrice`, Bybit
derivative `tickers`, and OKX `mark-price` (swap/futures).

OKX tick-by-tick depth (`books-l2-tbt` 400, `books50-l2-tbt` 50)
is VIP4+-gated (error 64003 otherwise).

Candles are parameterized: the normalized interval (default `1m`) is
validated per exchange during preflight (`1m`..`1M` subsets; Gate.io maps to
`10s`/`24h`/`7d`/`30d`). L2 depth levels are parameterized where the exchange
exposes partial books: Binance `@depth5/10/20@100ms`, Bybit
`orderbook.{50,200,1000}` for spot/linear/inverse,
OKX `books5` (the 50/400 tick-by-tick channels are VIP4+-gated and stay
unsupported), Bitget `books1/5/50`; Gate.io WebSocket depth stays full-depth.
Binance L2 update intervals are parameterized via `l2_book_interval(...)`:
spot `100ms`/`1000ms`, USD-M/COIN-M `100ms`/`250ms`/`500ms` (default `100ms`).

Gate.io derivative funding, open interest, index price, and mark price ride
the `futures.tickers` stream (there is no standalone channel; the funding
normalization carries rate and mark price without an applicable funding
time, mirroring the Python baseline). Gate.io L1 top-of-book is the
`book_ticker` stream (best bid/ask with sizes), which doubles as the BBO
Ticker source — the same dual-event pattern as Binance. Gate.io perpetual
liquidations use the separate public `futures.public_liquidates` channel;
`futures.liquidates` is the authenticated user channel and is never subscribed.
Delivery liquidations stay unsupported pending product-specific evidence. The delivery WebSocket serves
`futures.*` channel names (verified live 2026-08-07; `delivery.*` is
unknown).

The runtime capability preflight is authoritative. A combination absent from
this matrix must fail explicitly rather than creating an empty subscription.

New-channel live smoke on 2026-10-08 received Bitget spot/perpetual L1,
perpetual funding/OI/index/mark price, and OKX SWAP index events. Gate.io
public perpetual liquidation subscription was acknowledged successfully;
no liquidation event occurred in the 45-second window. Dated-product fan-out,
other Bitget derivative categories, and Gate.io liquidation normalization
remain offline-verified in this increment. See
[the smoke report](docs/reports/live-smoke-2026-10-08.md).

## Exchange Protocol Gates

### Binance

- [x] Contract Index reads documented `markPriceUpdate.i` / `E`; shared 1s
  mark-price subscription works for USD-M and COIN-M perpetual/dated symbols,
  including index-only feature builds. Legacy index-stream fixtures remain
  parser references.
- [x] Funding is perpetual-only across all five exchanges; dated requests fail
  preflight instead of opening a transport with no applicable funding data.

- [x] `markPriceUpdate.P` does not populate predicted funding rate; sourced
  settlement-price payloads cover both Funding and MarkPrice.

- [x] Product-specific Spot, USD-M, and coin-margined WebSocket routing.
- [x] Spot book ticker accepts the documented shape without `e`/`E`.
- [ ] European options remain disabled in capability preflight: the legacy
  `nbstream` endpoint returned 404 and no current endpoint has passed live
  verification. Parser fixtures remain as migration references only.
- [x] USD-M book/depth and trade/kline/funding/liquidation split across the
  current public and market WebSocket services.
- [x] Product-specific REST depth snapshots.
- [x] Non-USDT spot and derivative symbols normalize without guessing or panic.
- [x] Spot bridge sequencing and derivative previous-update continuity.
- [x] Bootstrap dispatch contains the snapshot and applied buffered updates.
- [x] Spot partial-depth streams (`@depth5/10/20`) replace the top-N book
  from `lastUpdateId` pushes and bootstrap from a same-width snapshot.
- [x] Snapshot fetches carry an HTTP timeout; buffered deltas are bounded and
  repeated bootstrap failures trigger a bounded in-session resnapshot.
- [x] Binance contract OI remains rejected by capability preflight. On
  2026-10-08 the user explicitly deferred REST polling because multi-symbol
  request volume can exhaust the shared rate budget. No official native
  perpetual/dated-futures OI WS path was found; options OI WS is separate.
  See [the decision record](docs/binance-open-interest-decision.md).

### Bitget v3

- [x] Dated L2 sync and platform-wide liquidation rows retain the configured
  catalog identity through runtime dispatch; unrelated series are filtered.

- [x] Spot/contract L1 `books1` snapshots, independent L2 routing, and shared
  `books1` L1/L2 subscription deduplication.
- [x] Derivative ticker funding/open interest/index/mark-price extraction,
  batch normalization, native-to-configured dated-product binding, and
  independent single-feature parsing without the `ticker` feature.

- [x] V3 instruments endpoint and product categories.
- [x] V3 `topic: kline` plus separate interval and current object payload.
- [x] Batched public trades and product-aware normalization.
- [x] Full-depth snapshot/update distinction and seq/pseq gap handling.
- [x] Current documented v3 intervals parse; normalized hours/days map to
  uppercase `1H/4H/6H/12H/1D`. Longer `3d/1w/1M` inputs fail preflight;
  older raw-parser helpers remain references, not enabled runtime intervals.
- [x] String ping/pong lifecycle.
- [x] Successful subscription acknowledgements bypass market-data parsing while
  error acknowledgements remain visible.
- [x] Derivative `liquidation` (instType-scoped, no symbol in the subscription)
  normalizes per-row symbols with product identity from `instType`,
  `status: Filled`, and no order id; spot liquidations fail preflight.

### Bybit v5

- [x] Per-connection derivative ticker state reconstructs snapshot/delta
  payloads; single-side BBO and USD-value-only OI changes are retained.
  Replacement snapshots and reconnects clear previous values; the cache is
  bounded to configured symbols.

- [x] Spot, linear, and inverse endpoint routing.
- [x] Batched trade and candle normalization.
- [x] JSON heartbeat and subscription-error handling.
- [x] Replacement snapshot and service-restart book reset.
- [x] Spot BBO Ticker uses `orderbook.1`; spot 24-hour ticker statistics are not
  misrepresented as BBO and level-50 remains the L2 source.
- [x] Derivative funding rides the `tickers.{symbol}` stream (the standalone
  `funding.{symbol}` channel was removed by Bybit; verified live 2026-08-06)
  and normalizes rate, mark price, and `next_funding_time`.
- [x] Option parser/catalog references retain `/option` routing,
  base-coin `publicTrade.{base}` row resolution, and level-25 book behavior;
  option feeds themselves are rejected by 0.1 capability preflight.
- [x] Derivative `allLiquidation.{symbol}` normalizes side, size, and price
  with `status: Filled` and no order id; spot funding/liquidation fails
  preflight.
- [x] Derivative open interest is sourced from the `tickers.{symbol}` stream
  (`openInterest`/`openInterestValue`) because Bybit has no standalone channel;
  spot open interest fails preflight.
- [x] `orderbook.1` doubles as the independent L1 channel: `L1Book` events
  carry the best bid/ask with sizes alongside the BBO Ticker mapping.
- [x] The deferred option implementation routes to `publicOption`; discovery
  pages the
  `instruments-info?category=option` catalog (verified live 2026-08-07:
  `tickers?category=option` requires a `baseCoin` and errors otherwise;
  `instruments-info` pages without one) with the `optionsType` field
  cross-checked against the `{C|P}` symbol suffix; settlement derives from
  the documented `-USDT` suffix; trades use the base-coin
  `publicTrade.{BASE}` stream with per-row symbol resolution.

### OKX v5

- [x] Monthly/quarterly candle ends follow UTC+8 calendar boundaries, including
  leap February and 31-day months.

- [x] Spot, swap, and futures identity.
- [x] Public and business WebSocket routing.
- [x] Batched ticker, trade, candle, and book processing.
- [x] Text ping/pong and sequence/reset recovery.
- [x] Full-book checksum validation on the `books` channel: CRC-32 over the
  first 25 levels interleaved per index (`bid1:sz1:ask1:sz1:...`) with
  transmitted string scale preserved; top-N channel checksums are not
  compared to the local full book.
- [x] Candle `end` derives from the wire interval and the interval
  normalizes to the shared lowercase vocabulary; VIP4 tick-by-tick depth
  (50/400) is rejected at preflight until wired.
- [x] SWAP `funding-rate` normalizes rate, next-funding time, and predicted
  rate; futures funding fails preflight because the channel is SWAP-only.
- [x] `liquidation-orders` normalizes side, size, price, and `ordId` as the
  order id with `status: Filled`.
- [x] `open-interest` normalizes `oi`, numeric `oiCcy` into Decimal
  `coin_quantity`, and `oiUsd` for SWAP and
  FUTURES; spot open interest fails preflight.
- [x] `index-tickers` normalizes `idxPx` plus 24h open/high/low; the index
  instId can be supplied explicitly for spot index feeds; SWAP/FUTURES derive
  the index ID from the native base/quote and fan out to matching contracts.
- [x] `bbo-tbt` normalizes as `L1Book` (best bid/ask with sizes) and is kept
  out of the L2 book-sync path.
- [x] Deferred `OPTION` catalog/parser helpers normalize the documented
  `{BASE}-{QUOTE}-{EXPIRY}-{STRIKE}-{C|P}` instId form; 0.1 preflight rejects
  the product.

### Gate.io v4

- [x] Public perpetual liquidation subscription, signed-size normalization,
  row-by-row multi-contract resolution, and spot/delivery negative preflight.

- [x] Fresh subscription timestamps and official public trade object shape.
- [x] Spot, USDT/BTC perpetual, and USDT delivery connection plans.
- [x] Product-aware REST bootstrap, WebSocket depth, and gap resync.
- [x] Delivery expiry and perpetual settlement identity validation.
- [x] Contract discovery does not use `quanto_multiplier` as an identity or
  positive-value gate.
- [x] Bootstrap dispatch contains the snapshot and the applied buffered updates.
- [x] Bounded in-session resnapshot: a failed bootstrap (non-bridging buffered
  deltas) re-fetches the snapshot up to three times per symbol while preserving
  the buffered deltas, then falls back to the session-level retry; the bridge
  rule is never weakened.
- [x] `full: true` order-book pushes replace the local book in-session and
  re-anchor the sequence at the push's `u` instead of erroring the session.

## Verification Baseline

Run from the workspace root:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace
cargo check --workspace --no-default-features
```

The deterministic parity harness is:

```bash
cargo test -p cryptofeed-rs --test public_parity
```

## Deferred Scope

- Binance contract OI, including REST polling: explicit 2026-10-08 decision
  based on multi-symbol rate-limit exposure; current REST availability does
  not enable a capability cell. See [the decision record](docs/binance-open-interest-decision.md).

- Coinbase and Kraken live runtime paths.
- Authenticated feeds and trading.
- Options and MARGIN as public runtime products. Their existing protocol
  helpers and fixtures are retained, but all such feeds fail 0.1 preflight.
- Gate.io live bootstrap capture validation (forensics logging in place; the
  bounded resnapshot path is implemented and deterministically covered).
- L3 order books: no public order-by-order depth stream across the five active
  exchanges (verified 2026-08-05); see
  [the protocol baseline](docs/exchange-protocol-baseline.md#l3-order-book-scope).
- Deeper full-session WebSocket/REST doubles and remaining checksum coverage.

Funding and liquidations (Bitget v3, Bybit v5, OKX v5, Gate.io perpetual
liquidations), open interest (Bitget, Bybit, OKX, Gate.io), index price
(OKX explicit index/contract mapping, Binance derivative index-price stream,
Bitget/Bybit/Gate.io derivative tickers), and L1 (all five active exchanges) are implemented for spot/contract scope. Options, MARGIN, and L3 are
deferred. Further expansion requires a new explicit plan and current official
protocol evidence. See [the harness guide](docs/harness.md) for acceptance
criteria and [PROGRESS.md](PROGRESS.md) for remaining release work.

## API currency review — 2026-10-09

- Checked enabled endpoint families and subscription contracts against current
  official sources; see `docs/exchange-protocol-baseline.md` for per-exchange links.
- Migrated OKX public/business WebSockets to default TLS port 443 and Global
  instrument discovery to the recommended REST domain; mixed-feed endpoint
  parity and catalog identity regressions cover the corrections.
- No exchange/channel capability expansion or legacy Python protocol fallback.

## Catalog refresh alignment — 2026-10-09

- `MarketCatalog::refresh` forces response/page fetches without changing the
  capability matrix or wire protocol. Offline tests cover cache bypass, failed
  refresh preservation, concurrent success/failure sharing, independent URLs,
  cancellation recovery, and unsupported-product preflight.
- No new live-service verification is claimed. Subscription hot updates,
  connection sizing and snapshot concurrency budgets remain pending.

## Per-channel subscription alignment — 2026-10-10

- Channel-specific symbol sets compile into concrete groups before adapter
  planning; unchanged protocol paths retain product/endpoint behavior.
- Offline regressions verify all five exchange planners, global native identity,
  mode/empty-set/product validation, feature boundaries and exact dispatch to
  callbacks, broadcast counters and L2 state.
- No exchange/channel capability expansion or new live-service evidence.

## Connection/resource alignment — 2026-10-10

- Native subscription budgets split concrete feeds after grouping and preserve
  aligned mappings. Offline tests cover exact limits, overflow, topic aliases,
  oversized instruments and aggregate connection counts.
- Bybit spot request batching has inline parity plus sanitized send references.
  Session doubles verify paced subscriptions coexist with reads, shutdown and
  fresh Gate timestamps. Snapshot admission tests cover pacing/concurrency and
  cancelled active/waiting work. No new live service/load claim is made.

## Managed lifecycle alignment — 2026-10-10

- Opt-in core runtime commands and source identity use existing exchange session
  paths. Actor/public-API tests cover lifecycle, cancellation, invalid candidate
  preservation, independent startup, fresh state, admission and scoped failure.
- Raw model parity and feature gates remain intact. Public OKX smoke evidence
  covers one add/replace/remove/close sequence, not the full exchange matrix.
- Remote-ready state, automatic listing reconciliation and consumer/ecosystem
  alignment remain unfinished under the full objective.

## Readiness alignment — 2026-10-10

- Explicit Binance subscription planning has public parity and sanitized control
  references. Current Bybit/Gate IDs and Bitget/OKX arguments correlate replies.
- Retained state tests reject stale/unknown/duplicate/unsent confirmation, revoke
  readiness on reconnect/gap, and require local L2 initialization. A Bybit
  session double verifies confirmation before book readiness.
- Follow-up public spot Trade/L2 evidence reached Ready on all five exchanges;
  this is not a new all-product, rare-channel or throughput certification.

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

## Throttle and Python-rule Renko — 2026-10-11

- Added feature-independent Throttle with a caller-owned monotonic clock, first
  input acceptance and the Python strict-greater-than interval boundary. Dropped
  input does not extend the interval; backwards clocks fail atomically.
- Added trade-feature RenkoFixed/RenkoBrick with per-source/generation/exchange/
  symbol extrema/anchors. Exact threshold triggers, continuations use prior close,
  reversals use prior open, and gaps emit one current-price brick, as in Python.
- Corrected the Python wrapper's delayed notification/initial empty emission in
  Rust: return immediately on trigger and never emit an initial/partial brick.
  Cap active series to 1..=4096 and expose explicit removal for lifecycle/gaps.
- Eleven new tests (23 aggregation tests total) cover boundaries, large clocks,
  decimal increments, both directions, gaps, source isolation, retirement, atomic
  invalid input and full-brick/throttle-decision equality after recording replay.
- Evaluated sibling Python RenkoFixed._agg for the documented ten-price sequence;
  all five open/close/direction triples match the Rust golden assertions. This
  uses only local source and synthetic prices, without wire fixture changes.
- Custom aggregation, NBBO, broader storage adapters and the remaining recording/
  resource-policy scope remain active. No new live validation is claimed.
- Final gates: 648 all-feature workspace tests; no-data/trade/recording-trade
  aggregation suites (3/21/23 tests); every standalone feature and no-default
  compilation; strict Clippy/rustdoc; fresh-resolution Rust 1.85 all-target/all-
  feature compilation. Resolved dependencies match the tested set. CI now also
  tests no-data aggregation. Existing OHLCV source/tests moved byte-for-byte into
  their own feature-gated submodule; no OHLCV behavior changed.

## Scoped cross-exchange NBBO — 2026-10-11

- Added orderbook-feature Nbbo over complete L1/reconstructed L2 tops: highest
  bid/lowest ask, selected native sizes and configured-order ties. One exact
  public symbol, at most 64 distinct feed IDs and pinned generations.
- Explicit per-leg Base/Quote/Contracts labels retain native units without
  aggregating or converting contract sizes; caller verifies price/unit metadata.
  Cross-venue crossed quotes are flagged, invalid local snapshots rejected.
- Separate source-observation and consumer clocks account for queued age. Inclusive
  expiry produces fallback/last-source withdrawal, timer advance works without
  packets, and errors leave state unchanged. Availability suspension blocks queued
  input; newer-generation replacement rejects the old scope.
- Twelve regressions cover prices/sizes, ties, expiry, availability, queued input,
  replacement, product/unit scope, cross/invalid/clock/configuration boundaries
  and all emitted fields after identical normalized-recording replay inputs.
- Ran sibling Python NBBO._update on synthetic two-exchange books; Rust price,
  selected size and source golden assertions match. No native protocol changed.
- Caller owns L2 reconstruction/anchors, status signals, clock mapping and output
  delivery. Normalized files alone do not prove live ingress-age/lifecycle replay.
  Custom aggregation, broader storage and remaining recording/resource work stay active.
- Public Binance/OKX Spot L1 smoke observed both sources and 20 best changes;
  explicit source suspension produced the asserted final withdrawal before clean
  managed shutdown/exit 0. [Report](docs/reports/live-smoke-nbbo-2026-10-11.md).
- 660 all-feature workspace tests, minimal orderbook/recording-orderbook tests,
  every standalone/no-default feature compilation, strict Clippy/rustdoc passed.
  CI now includes the two minimal NBBO test combinations.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed; resolved
  dependencies match the tested set. Source/protocol fixtures are unchanged and
  Cargo.lock/target remain uncommitted. Current sibling guides now link the
  implemented sinks/aggregation/NBBO instead of reporting them as wholly pending.

## Typed custom window aggregation — 2026-10-11

- Added feature-independent CustomAggregate/AggregateWindow with caller-defined
  initialization/reduction over identified FeedEnvelope input. Fixed elapsed
  [start,end) windows support subsecond clocks, timer closure, skipped empty output
  and consuming partial tails without I/O/tasks/queues or automatic serialization.
- Cap accepted inputs per window at configured 1..=1,000,000. Caller owns custom
  state memory, group caps, source/units and calculation cost; no generic byte
  bound or independent allocation can be inferred from arbitrary user state.
- Invalid clocks/caps/bounds leave state unchanged. Initializer/reducer errors or
  catchable panics poison further output, including finish, because rollback of
  arbitrary mutable state/side effects is impossible. Pending prior windows are
  not falsely certified after rollover failure; explicit advance can deliver a
  completed window before reducing a later input.
- Eleven tests cover no-data construction/empty resets, validation, subsecond
  windows, source-preserving state, partial tails, callback errors/panics/poison,
  cap resets and all custom state/window fields after deterministic recording replay.
- Broader storage adapters and remaining recording/resource-policy work stay active.
- Offline custom example over the earlier 20-trade OKX recording produced 10
  closed/1 partial one-second windows; per-source buy/sell sums matched every
  window and all 20 input events. [Report](docs/reports/offline-custom-aggregation-2026-10-11.md).
- Gates passed: 671 all-feature workspace tests, no-data/trade/recording-trade
  custom suites (4/10/11 tests), every standalone/no-default feature compilation,
  strict Clippy/rustdoc. Existing CI aggregation jobs include the new helper.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed, including
  the custom replay example. Resolved dependencies match the tested set; no new
  dependency/protocol/fixture or Cargo.lock policy change was introduced.

## Raw series rotation and continuous replay — 2026-10-11

- Added RawSeriesWriter/Reader/record_raw_series behind recording. Physical files
  contain record-aligned fragments of one unchanged logical raw v2 stream, with
  an ordered bounded manifest of byte counts/FNV-1a64 and logical footer summary.
- Preserve global sequence/session metadata/Processing references/consumed HTTP
  across boundaries. Reader lazily feeds the existing validator/native replay;
  no per-part restart, online fallback or relaxed L2 bridge rule.
- create_new reserves the manifest and each part, staging one record at a time.
  Default 16 MiB parts, at most 1024; existing logical budgets apply globally.
  Physical ceiling/I/O errors are terminal; logical cutoffs retain LimitReached.
  Manifest finalizes after data flush; checksums are not authentication/fsync.
- Ten regressions cover exact observations, ordered composition, missing/extra/
  truncated/reordered/corrupt files, manifest/summary mismatch, limits, overwrite
  protection, cancellation/error poisoning and native trade replay. A Binance
  HTTP bootstrap/Processing/L2 scenario matches all single-file outputs/summary
  across more than two parts.
- Independent-capture merging, generic catalog/HTTP capture, broader replay audits
  and advanced resource-policy work remain active.
- Public OKX capture: 40 observations/23,676 logical bytes across 3 physical
  files; managed shutdown then 17 offline trade models, validated manifest/footer,
  exit 0. [Report](docs/reports/live-smoke-raw-series-2026-10-11.md). Captures stay temporary.
- Gates passed: 681 all-feature workspace tests; recording-only/trade/orderbook
  series suites (7/9/8); every standalone/no-default feature compilation and
  strict Clippy/rustdoc. CI adds recording/orderbook series coverage alongside
  existing recording-only/trade jobs. No new dependency/protocol fixture change.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed, including
  the public rotation example. Resolved dependencies match the tested set;
  Cargo.lock/target and unsanitized capture artifacts remain uncommitted.

## Public HTTP attempt/result recording — 2026-10-11

- Added recording-feature HttpCapture/HttpObservation and bounded version-1
  public-http JSONL codec over shared directory/REST/bootstrap GET entrypoints.
  Request starts precede admission; matched results/safe failures and cancellation
  retain invocation IDs despite reordered completion. No headers/proxy auth/error
  bodies or successful status/HTTP wire bytes are captured.
- Public endpoint/query gates, existing credential/diagnostic sanitation and file
  revalidation preserve exact JSON numeric literals. Observer failures are sticky
  and do not veto healthy queries. Catalog cache namespaces are capture-scoped,
  shared by clones; proxy authentication rebuilding preserves the observer.
- Complete requires every started request to finish/cancel. Explicit prefixes may
  retain pending starts, never silently match them to later responses. HTTP
  response injection/offline catalog/REST replay remains a separate active step.
- Fixed HTTP and raw WS capture failure wakeup: rejected input without an enqueued
  record now wakes an already waiting consumer, even while producers stay alive.
  Nine new tests cover IDs/order/cancel, precision/redaction, strict codec/privacy/
  lifecycle/bounds, URL scope, cache identity/proxy retention and both wakeups.
- Public OKX workflow discovered 1,152 markets and returned ticker/book/recent
  trades. Four attempts/four JSON results, zero failures/cancels, 8 observations/
  1,252,409 bytes/Complete; offline ledger validation and exit 0. [Report](docs/reports/live-smoke-http-recording-2026-10-11.md).
- Gates passed: 690 all-feature workspace tests, recording-only/trade/orderbook
  HTTP tests and raw failure wakeup, every standalone/no-default feature checks,
  strict Clippy/rustdoc. CI adds recording/orderbook HTTP coverage.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed, including
  the new public HTTP example. Resolved dependencies match the tested set; no new
  dependency/native endpoint/protocol fixture or lockfile policy change occurred.

## Offline HTTP payload injection — 2026-10-11

- Added fresh-file into_replay/HttpReplay/HttpReplayReport and explicit transport
  replay mode. Entire bounded ledger/privacy/footer validates before responses
  are available. Keys are exact exchange/URL and outcomes follow invocation IDs,
  even when completion reordered; cloned cursors are shared, instance caches isolated.
- Existing directory/REST methods now work from archived payloads. Shared fetch
  intercepts before HTTP admission/client access; HTTP client and WS entry guards
  prevent fallback, including proxy authentication rebuilding. No live quota/cooldown
  is consumed. Unknown/pending/exhausted/over-budget attempts consume nothing and
  fail strict audit; unrelated keys remain usable.
- Cancellations/configuration mismatches terminate the current catalog operation
  rather than retrying or sharing it as completed HTTP work. A later explicit
  call can consume its response; captured real transport failures still retry.
- Twelve new regressions cover order, source/cursor/budget checks, pending starts, failures,
  cancellation/retry boundaries, fresh-file validation, no-network entry guards,
  cache isolation and existing catalog/ticker/book/recent-trade normalization.
- This is immediate JSON payload injection; receive clocks are SDK execution time.
  Original cache expiry, failure coalescing/scheduling and cancellation timers are
  not simulated. Independent-capture merging and advanced resource work remain active.
- HTTP file v2 adds optional permanent configuration-error classification, retaining
  v1 read compatibility. Re-recording a replay cancellation must not lose its
  terminal class and revive catalog retry. Tests also prove waiting callers do
  not inherit cancelled work as an HTTP completion; genuine transient failures retry.
- Archived public OKX data re-created 1,152 symbols, ticker, five bids/five asks
  and five recent trades using normal SDK calls; 4/4 consumed, no unfinished/
  cancelled/mismatched requests, exit 0. [Report](docs/reports/offline-http-sdk-replay-2026-10-11.md).
  The synthetic v1 reference also passed the same offline CLI with one symbol/trade.
- Gates passed: 702 all-feature workspace tests, recording-only/trade/orderbook
  HTTP/replay suites plus the waiter test, every standalone/no-default compilation,
  strict Clippy/rustdoc. CI now includes minimal waiter coverage.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed. Resolved
  dependencies match the tested set; no new dependency, native endpoint or
  normalization rule changed. Original receipt clocks/cache timers remain out of
  this payload mode's claims; exact normalized tape replay is already separate.

## Bounded directory retention — 2026-10-12

- Replaced unbounded globally retained Value trees with serialized immutable byte
  buffers. Process cache caps 256 responses/128 MiB JSON+key bytes and 32 MiB per
  response; LRU touches, 24-hour boundary expiry and global sweeps bound old
  capture/replay/proxy namespaces. Wall-clock rollback invalidates future entries.
- Encoding is bounded; decoding runs outside the global lock and preserves exact
  numeric JSON. Active reads and caller-owned catalog/feed snapshots survive
  eviction. Failed fetch/envelope validation preserves eligible prior data;
  uncacheable successful replacement invalidates the older same-key entry.
- Replay remembers successful catalog fetches per transport namespace. A later
  non-refresh cache miss fails without consuming a future archived refresh;
  explicit refresh remains available. No unbounded second global cache is added.
- Seven new regressions cover retention/accounting/LRU/TTL/rollback, reference
  lifetime, precision, 10,000 namespaces and actual SDK replay eviction ordering.
- Configurable/weighted/distributed quotas, minimum packing and independent-capture
  merging remain under the overall alignment work. No new live run is claimed.
- Gates passed: 709 all-feature workspace tests, no-default cache and minimal
  recording replay tests, every standalone feature compilation, strict Clippy/
  rustdoc. Existing failed-refresh/coalescing/replay suites passed. CI adds the
  no-default cache regressions. No native endpoint/fixture/dependency changed.
- The saved public HTTP tape also re-created 1,152 markets/ticker/five bid/ask
  levels/five trades; an additional cached catalog load matched all symbols
  without consuming another response. Four requests consumed, zero mismatches,
  exit 0. This is offline use of earlier data, not a new exchange smoke run.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed, including
  the updated offline cache-hit example. Resolved dependencies match the tested
  set; Cargo.lock/target and old capture artifacts remain uncommitted.
- Fresh dependency resolution updated cc 1.7.0→1.8.0 and serde_json
  1.0.152→1.0.154. Re-ran all 709 workspace tests, strict Clippy/rustdoc,
  no-default/standalone feature and minimal cache/replay checks on that exact
  resolved set; all passed, including numeric precision regressions.

## Caller-shared stricter resource scopes — 2026-10-12

- Added ResourcePolicy/ResourceBudget and transport attachment. Clones share local
  HTTP capacity/start clocks and per-exchange WS live/queued capacity/start clocks;
  independent groups remain under unchanged process/native guards. Invalid or
  weaker policy bounds fail during construction; policies stay immutable.
- Composite admission checks/advances both clocks at actual SDK start, avoiding
  bunched starts after global waiting. HTTP pacing releases network permits;
  cancellation and session drop release both layers. Proxy auth keeps the group.
- Known plans preflight scoped capacity. Managed admission reserves scope as well
  as exchange: max old/new for same scope, both on scope/provider change until
  drain. Excess add/replacement is rejected without stopping healthy feeds.
- Offline HTTP bypasses both budgets and WS rejects before slot reservation.
  Twelve new tests cover policy bounds/clones, composite pacing, exhaustion/drop/
  cancel, planning, managed reservations/actors and offline guards.
- Unit-cost/process-local policy is implemented; endpoint weights/native quota
  codes, distributed coordination, minimum packing and independent capture merging
  remain active. No protocol endpoint or normalization change occurred.
- Public OKX trade/REST smoke rejected excess add, retained the healthy feed,
  replaced on the same ID, removed/reused a live slot and completed directory/
  ticker/book queries under one group; managed shutdown and exit 0. [Report](docs/reports/live-smoke-resource-budget-2026-10-12.md).
- Gates passed: 721 all-feature workspace tests, no-default/scoped admission and
  minimal recording/trade resource/actor tests, every standalone feature checks,
  strict Clippy/rustdoc. CI adds minimal scope coverage. No new dependency/native
  endpoint or protocol fixture change.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed. Resolution
  updated syn 3.0.7→3.0.8; all 721 tests, strict Clippy/rustdoc and standalone/
  minimal feature suites were repeated on that resolved set and passed. No
  declared dependency or lockfile policy change; Cargo.lock/target stay uncommitted.

## Native public REST quota cooldowns — 2026-10-12

- Verified official Binance/Bybit/OKX/Bitget code forms and added exchange-scoped
  successful-HTTP JSON recognition. Gate remains on HTTP 429; private order/WS
  and convert labels are not guessed into public REST scope.
- Preserve the original JSON/result semantics while deferring shared admission.
  Prefer Retry-After, then valid future Bybit epoch-ms reset, else SDK 60-second
  fallback; malformed/past hints fall back, existing longer waits never shorten.
  No diagnostic ban text parsing, fabricated HTTP 429 or automatic REST retry.
- Known quota envelopes fail before directory cache writes, including numeric
  Binance errors. Standalone validators and bounded directory retry remain in
  place; HTTP/scoped slots stay free during cooldown and other exchanges proceed.
- Nine new deterministic regressions cover code/type/scope, reset/header timing,
  body identity, fallback/extension, slot/isolation, no live cooldown during replay
  and no cached native-error directory. No live throttling was intentionally induced.
- Endpoint weights, native WS controls, distributed coordination, minimum packing
  and independent capture merging remain active; no endpoint/normalization changed.
- Gates passed: 730 all-feature workspace tests, no-default/recording/orderbook
  native-rate suites and every standalone feature check, strict Clippy/rustdoc.
  CI adds the no-default native-code classifier. COIN-M's current official error
  page was verified via its redirect; no native endpoint or dependency was changed.
- Fresh-resolution Rust 1.85 all-feature/all-target compilation passed; resolved
  dependencies match the tested set. JSON/cache/recording codecs and native API
  versions stay unchanged; Cargo.lock/target remain uncommitted.

## Binance weighted HTTP admission — 2026-10-12

- Verified current official Spot/USD-M/COIN-M public GET costs; current Spot
  recent trades costs 25, derivative depth/klines use their own tiers, and USD-M
  funding uses its separate request window. No endpoint or payload shape changed.
- Process rolling windows enforce Spot 6000/60s, conservatively shared futures
  2400/60s and USD-M funding 500/300s. Live directory minute limits only tighten.
  ResourcePolicy can lower each bucket's threshold against total process usage.
- Weights, scoped/global clocks and cooldowns compose at actual admission.
  Waiting frees both slots; admitted work is charged even on failure/cancel,
  waiting cancellation is free. Replay neither charges nor learns live limits.
- Eleven new regressions; 741 all-feature workspace tests, strict Clippy/rustdoc,
  no-default and each individual feature compile, and fresh-resolution Rust 1.85
  all-target compilation passed. No live rate limit was intentionally triggered.
- [Sources and boundaries](docs/http-weights.md): other venue weights, native WS
  quotas, distributed coordination, minimum packing and independent capture
  merging remain unfinished. No shared-IP guarantee or used-weight feedback.

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

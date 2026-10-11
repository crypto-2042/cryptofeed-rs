# Python usage alignment

Status: active improvement plan, updated 2026-10-10 against the sibling Python
checkout at commit `3a6d3ca`. Phase 1 and phase 2 catalog refresh/request sharing and per-channel subscriptions
are implemented; conservative connection sizing and paced sends are now implemented.
Phase 3 core runtime commands/identity, retained readiness and opt-in directory reconciliation are implemented;
phases 4–5 and the remaining design work below remain unfinished. Exchange count and instrument-type coverage are
excluded. Authenticated feeds and trading remain outside the 0.1 scope.

## Goal and compatibility

Align public-market workflows and semantics while preserving Rust's typed
models, async handlers, feature gates, and `FeedHandler` entrypoint. Do not copy
legacy Python exchange protocols or introduce dynamic conversion to mimic
Python syntax. Existing builders and `run()` remain supported.

Python evidence below refers to paths under its `cryptofeed/` package, except
where an example path is specified. These are source observations from the
local checkout, not claims about every upstream version.

## Gap inventory

| Workflow | Python evidence | Rust assessment / treatment |
| --- | --- | --- |
| Symbol discovery | `exchange.py`: `symbols`, `info`, `symbol_mapping(refresh=...)` | `MarketCatalog::load` exposes sorted symbols; `refresh` bypasses cached responses, including pagination. Exact native lookup and typed MarketInfo now expose verified tick/lot/minimum/precision/status/contract fields, plus product/build WS capabilities; order validation is not claimed. |
| Pattern selection | `feed.py` resolves each supplied name by exact mapping; no general glob expansion found | Phase 1 adds explicit catalog `select` with `*` and `?` as a convenience extension, not Python parity. |
| Batch configuration | `feed.py`: `symbols` plus `channels` | Multi-symbol feeds already work; phase 1 adds bulk `.symbols` and typed `.instruments`. |
| Per-channel symbol sets | `feed.py`: `subscription={channel: symbols}` | Implemented `.subscription` / `.subscription_instruments`; exact unequal sets now share native connections and retain per-pair filtering through capacity shards. |
| Connection sizing | `feed.py`: `connect` / `limit_sub`, endpoint-specific limits | Implemented native-topic/message budget sharding, per-request Bybit batches, paced sends, connection budgets and snapshot admission. Unequal-set packing is implemented; a global minimum solver and configurable/weighted policies remain pending. |
| Embedding and shutdown | `feedhandler.py`: `run(start_loop=False, install_signal_handlers=False)`, `stop_async` | Existing `runtime::run_with_shutdown` supports a caller-owned watch signal. Phase 1 adds a `FeedHandler` facade; it installs no Ctrl-C handler. |
| Runtime additions | `feedhandler.py`: `add_feed` starts a new feed when running; `examples/demo_loop.py` | Implemented opt-in RuntimeControl; retained handles can add feeds after startup. Legacy strict startup remains available. |
| Updating an existing subscription | No general public update/unsubscribe API found in the inspected Python core | Implemented controlled remove/replace with candidate validation and old-task drain. In-place exchange WS updates remain a separate optimization. |
| Automatically following listings | Python catalog refresh is explicit; no core periodic discover-and-resubscribe loop found | Rust now offers opt-in DiscoveryFeed with forced refresh, per-channel patterns, replacement, backoff and ownership guards; this extends the inspected Python core. |
| Callback fan-out | `feed.py`: callback lists; `callback.py`: async/sync callback wrappers | Implemented primary plus add_*_handler registrations, serial order, independent five-second deadlines and catchable panic isolation. Traits return (); business errors remain caller-owned. Broadcast remains lossy. |
| Candle completion | `feed.py`: `candle_closed_only`; Binance applies the flag | Implemented CandlePolicy All/ClosedOnly/ClosedOrUnknown at the common dispatcher; Rust retains its existing All default and never infers unknown completion. |
| Book consumption | `feed.py`: book callbacks, depth/checksum/cross checks; Python book objects expose deltas | Implemented opt-in L2BookHandle atomic full-snapshot/subscription recovery with local identity/connection/epoch/revision anchors and resync/disconnect/stop invalidation. Quantity units remain exchange-native normalized units. |
| Runtime settings | `feed.py`: timeout/retry/start delay/proxy settings; `config.py` | Implemented feed-level retry limits and handshake/callback deadlines with compatible defaults and successful-subscription reset. Idle overrides/disable and cancellation-aware startup delay are now implemented; shared explicit HTTP/WS proxy routing is implemented through TransportConfig; fixed protocol/resource policies stay intact. |
| Public REST/history | `exchange.py`: ticker/trades/candles/funding/book methods and sync wrappers | Implemented PublicRestClient ticker/book snapshots on current five-exchange routes, normalized models and optional native time/IDs; bounded funding settlement history/cursors are now implemented; five-venue candle history is implemented; five-venue recent trade batches are implemented; Binance/OKX/Gate historical pagination is implemented; Bybit/Bitget remain recent-only; Gate delivery history is range-only with SourceLimit on a full page; Gate delivery candles use the documented delivery endpoint. |
| Recording/replay | `raw_data_collection.py`: recording and playback | Implemented opt-in normalized JSONL recording/replay with explicit format, source labels, limits, timing and strict lag/truncation/cancellation behavior. Pre-parser sanitized WS observation is implemented; bounded raw WS file capture/validation is implemented; native WS parser/state replay (including three WS-L2 families) is implemented; v2 consumed HTTP bootstrap and Binance/Gate L2 replay are implemented; generic HTTP/catalog capture and broader edge audits remain pending. |
| Storage/aggregation | `backends/`: database/message-bus/socket adapters, aggregate callbacks | No bundled Rust backends or OHLCV/throttle/Renko adapters. Phase 5 starts with a small sink contract and one justified adapter, avoiding a dependency-heavy default SDK. |
| Cross-exchange NBBO | `feedhandler.py`: `add_nbbo`; `nbbo.py` | No Rust aggregation helper. Later opt-in work, with stale-source and symbol/unit compatibility rules. |

Decimal prices/quantities, separate receive/exchange timestamps, multi-symbol
subscriptions, reconnects, and clean shutdown already exist. Their exact Python
function signatures need not be duplicated in Rust. Current public model units
and consumer delivery limits remain documented in the project README.

## Phase 1 — discovery and startup ergonomics (implemented)

- `MarketCatalog::load(exchange, product)` reuses existing product-qualified
  catalog fetchers and the 24-hour HTTP cache; unsupported products fail before
  network access. This snapshot is not a promise that every discovered symbol
  is tradable or compatible with every channel. Feed preflight still applies.
- `.symbols()` returns sorted normalized instruments. `.select(&[patterns])`
  returns their sorted, deduplicated union, with case-insensitive `*` and `?`.
  Patterns match the entire normalized name; brackets and escaping have no
  special meaning. Empty input or any unmatched pattern fails explicitly.
- Feed builder `.symbols(...)` and `.instruments(...)` append to existing
  configuration, preserving the semantics of repeated singular calls.
- `FeedHandler::run_with_shutdown(watch_receiver)` exposes existing lifecycle
  behavior. A true watch value or sender closure requests shutdown. Already
  signalled shutdown skips catalog hydration.

Typed [market metadata](market-metadata.md) now extends this phase with current
field applicability, exact Decimal conversion and product/build capability context.

Acceptance: deterministic selection/order/overlap/error tests, typed bulk
configuration tests, unsupported-catalog rejection without HTTP, and pre-signalled
shutdown without hydration. Workspace tests, strict Clippy, no-default and
single-feature compilation, rustdoc, formatting, and Rust 1.85 must pass.
No exchange wire parser or endpoint changes are part of this phase.

## Phase 2 — subscription planning and refresh (in progress)

1. **Implemented: catalog refresh and request sharing.**
   `MarketCatalog::refresh` bypasses cached responses, including every requested
   page. Concurrent callers share in-flight work for the same URL and transport
   configuration; unrelated URLs/routes remain independent. Reusable HTTP clients
   serve requests within each transport scope.
   Failed HTTP/JSON or exchange-envelope validation preserves the previous
   cached response. Cancellation releases the request gate and later calls can
   retry. This is per-response caching, not an atomic transaction across all
   pages or full parsed instrument validation; existing snapshots/subscriptions
   are unchanged. Snapshot requests now share a separately paced four-slot budget.
2. **Implemented: per-channel symbol sets.**
   `.subscription(channel, names)` / `.subscription_instruments(channel, symbols)`
   are mutually exclusive with shared channel/symbol shortcuts. Repeated channel
   entries merge; duplicates are removed. Empty sets, unsupported channels or
   features, and mixed products fail preflight. The runtime resolves the
   first-seen union once (including global native-mapping ambiguity checks),
   then retains exact channel/symbol rules through native-topic packing and
   capacity shards. Dispatch and book bootstrap/sync/readiness/recovery check
   exact membership. Explicit native names remain aligned to the first-seen union.
   Unequal sets can share a physical socket; required product/public/business
   routes remain separate. Low-level adapter users still call `connection_feeds`
   for capacity partitioning and then plan each shard's endpoint routes.
3. **Implemented: conservative connection sizing and paced admission.**
   Concrete groups split by actual native topic/message budgets after adapter
   deduplication. Bybit spot args batch at ten per request; a session-owned queue
   paces sends while retaining reads/heartbeats/shutdown. Process-local connection
   slots/start pacing and shared snapshot concurrency/start pacing bound bursts.
   [Connection planning](connection-planning.md) separates official limits from
   SDK choices and records cancellation and scope. Unequal channel sets now share
   native connections using deterministic contiguous-union packing; global minimum
   allocation, configurable budgets and distributed/weighted quotas remain
   separate improvements, not implied guarantees.

Completed subscription tests cover five-exchange concrete planning, native
mapping preservation/ambiguity, mode conflicts, typed symbols, and exact
handler/broadcast/counter/book filtering after runtime hydration. Feature
preflight is tested in every isolated build. No exchange wire shape changed.

Connection/resource tests cover exact/overflow boundaries, topic deduplication,
Bybit request batches, native-name preservation, cancellation and live socket
reads during queued sends. Full-session doubles still cover all five exchanges.
Future policy changes must retain these checks and refresh-failure regressions. Each exchange protocol change requires sourced fixtures and official
verification; limits must not be inferred from old Python constants.

## Phase 3 — runtime control and symbol updates (implemented core)

Core add/remove/replace/list/shutdown commands, independent initial startup,
IDs/configuration generations, scoped events and lifecycle transitions are
implemented. [The runtime-control guide](runtime-control.md) defines results,
cancellation/commit points, error aggregation and remaining readiness scope.
A manual OKX run observed generation 1 BTC trade followed by generation 2 ETH
trade on the same feed ID, then removal and exit 0. Retained state/readiness now includes matching subscription evidence across
all connections, synchronized L2 counts and reconnect epochs. The follow-up
public smoke reached Ready on all five spot Trade/L2 feeds, including Bybit
on its second attempt; the earlier TLS failure remains documented. Periodic
listing reconciliation is now opt-in through [DiscoveryFeed](discovery.md). See [readiness semantics](readiness.md).


- Keep `run()` compatible and add a caller-retained control handle with stable
  feed IDs. Commands: add, remove, replace, and shutdown. Each command needs an
  explicit result; invalid catalog/capability requests must not disturb healthy
  feeds. Bound the command queue and make hydration cancellable.
- Implement replacement first by validating the candidate, stopping/draining
  the old session, and starting a fresh session with fresh L2/ticker state.
  Document the replacement gap; do not claim exchange-atomic switching or
  uninterrupted books. If new startup fails after stopping the old feed, report
  that failure explicitly. Readiness is exposed through scoped status events and retained snapshots.
- Define when removal completes and how already queued events are identified;
  consumers need feed/generation identity to reject events from an old session.
- Implemented opt-in periodic catalog reconciliation with a one-minute minimum,
  exponential backoff, explicit listing/removal/empty-selection policy, and shared
  bootstrap budgets. Unchanged directories avoid restarts; compare-and-replace
  ownership guards protect manual updates. No polling by default or Binance OI fallback.

Acceptance: doubles cover commands during hydration/retry, invalid replacement,
concurrent shutdown, pending snapshots, stale generations, state reset, and
healthy-feed isolation. In-place subscribe/unsubscribe can follow only where
current exchange protocols and acknowledgement handling are verified.

## Phase 4 — event and handler semantics

Implemented [closed-only candles and explicit unknown-completion policy](candle-delivery.md),
with compatible all-update defaults and filtering before all delivery surfaces.
Implemented [multiple handler registrations](handlers.md) with primary/append ordering,
per-callback timeout, catchable panic isolation and documented business-error semantics.
Implemented [L2 recovery](l2-recovery.md) with atomic full snapshots and revision-anchored
updates, lag recovery and scoped cache invalidation. Existing lossy broadcast
must remain explicit. Document which books are snapshots versus changes and
which quantities use contracts versus base units. Add only needed transport
settings without hiding protocol-specific constraints.
[RuntimeOptions](runtime-options.md) now covers finite retries, handshake/callback
deadlines, successful-subscription retry/backoff reset, idle policy and startup
delay. [TransportConfig](transport.md) now covers explicit HTTP/WS proxy routing,
including standalone/periodic catalogs and REST bootstrap/resync. General Python
config-file loading is not claimed.

Acceptance: tests cover unfinished/unknown candles, handler ordering and failure,
lag-and-recovery continuity, and shutdown with slow consumers. Preserve existing
defaults unless a documented migration intentionally changes them.

## Phase 5 — optional ecosystem features

[Public REST ticker/book snapshots](public-rest.md) and shared HTTP-status backoff
are implemented. [Funding history](funding-history.md) now adds bounded settlement
batches with scope-bound JSON cursors and explicit stop reasons. [Five-venue candle history](candle-history.md) now provides bounded time windows
and scoped JSON continuation. [Recent trades](recent-trades.md) now provides bounded five-venue batches.
[Native trade history](trade-history.md) is implemented for Binance/OKX/Gate Spot/perpetual; Bybit/Bitget remain recent-only; Gate delivery history has no native continuation; Gate delivery candles use the documented delivery endpoint. [Normalized recording/replay](recording.md) is now implemented as a bounded
optional workflow. [Sanitized WS observation](raw-capture.md) is now attached at actual connection
text boundaries. [Raw WS segments](raw-recording.md) now persist/validate observations. [Native replay](raw-replay.md) now executes current parsers and three WS-native
book sync paths. [Consumed HTTP/L2 replay](http-l2-replay.md) now supports Binance/Gate without
online fallback. [Physical route isolation](connection-planning.md#physical-connection-state-isolation--2026-10-11)
now covers sparse Bybit/OKX projections, raw contexts and route-owned cache resets.
Generic HTTP/catalog capture, broader replay audits,
sinks/aggregation and NBBO remain
separate increments after the subscription/lifecycle contract is stable.
Do not promise full Python backend parity as part of the 0.1 core SDK. Each
increment needs a concrete caller workflow, bounded resource behavior, and
independent tests; protocol-facing work needs current official sources.

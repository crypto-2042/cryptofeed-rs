# Python usage alignment

Status: active improvement plan, updated 2026-10-10 against the sibling Python
checkout at commit `3a6d3ca`. Phase 1 and phase 2 catalog refresh/request sharing and per-channel subscriptions
are implemented; conservative connection sizing and paced sends are now implemented.
Phase 3 core runtime commands/identity are implemented; readiness/discovery,
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
| Symbol discovery | `exchange.py`: `symbols`, `info`, `symbol_mapping(refresh=...)` | `MarketCatalog::load` exposes sorted symbols; `refresh` bypasses cached responses, including pagination. Public market metadata remains pending. |
| Pattern selection | `feed.py` resolves each supplied name by exact mapping; no general glob expansion found | Phase 1 adds explicit catalog `select` with `*` and `?` as a convenience extension, not Python parity. |
| Batch configuration | `feed.py`: `symbols` plus `channels` | Multi-symbol feeds already work; phase 1 adds bulk `.symbols` and typed `.instruments`. |
| Per-channel symbol sets | `feed.py`: `subscription={channel: symbols}` | Implemented `.subscription` / `.subscription_instruments`; equal symbol sets share a concrete group. Distinct sets may use more connections until packing is optimized. |
| Connection sizing | `feed.py`: `connect` / `limit_sub`, endpoint-specific limits | Implemented native-topic/message budget sharding, per-request Bybit batches, paced sends, connection budgets and snapshot admission. Optimal packing and configurable/weighted policies remain pending. |
| Embedding and shutdown | `feedhandler.py`: `run(start_loop=False, install_signal_handlers=False)`, `stop_async` | Existing `runtime::run_with_shutdown` supports a caller-owned watch signal. Phase 1 adds a `FeedHandler` facade; it installs no Ctrl-C handler. |
| Runtime additions | `feedhandler.py`: `add_feed` starts a new feed when running; `examples/demo_loop.py` | Implemented opt-in RuntimeControl; retained handles can add feeds after startup. Legacy strict startup remains available. |
| Updating an existing subscription | No general public update/unsubscribe API found in the inspected Python core | Implemented controlled remove/replace with candidate validation and old-task drain. In-place exchange WS updates remain a separate optimization. |
| Automatically following listings | Python catalog refresh is explicit; no core periodic discover-and-resubscribe loop found | Neither implementation guarantees this. Optional discovery reconciliation comes after runtime controls and force refresh. |
| Callback fan-out | `feed.py`: callback lists; `callback.py`: async/sync callback wrappers | Rust registers one handler per category. Broadcast offers fan-out with loss on lag; multiple reliable handlers need separate semantics. Phase 4. |
| Candle completion | `feed.py`: `candle_closed_only`; Binance applies the flag | Rust exposes `Candle.closed` but lacks a builder-level closed-only filter. Phase 4, with an explicit policy for unknown completion. |
| Book consumption | `feed.py`: book callbacks, depth/checksum/cross checks; Python book objects expose deltas | Rust has normalized snapshots/deltas and exchange sync, but a lagged broadcast consumer cannot request a synchronized recovery snapshot. Phase 4 prioritizes recovery and documents native-unit differences. |
| Runtime settings | `feed.py`: timeout/retry/start delay/proxy settings; `config.py` | Rust uses fixed supervision policies and tracing. Add only demonstrated public-service settings after lifecycle controls; preserve safe defaults and bounded shutdown. |
| Public REST/history | `exchange.py`: ticker/trades/candles/funding/book methods and sync wrappers | Rust REST currently serves discovery and book bootstrap. Public history clients are a later workstream; verify current official endpoints before implementation. |
| Recording/replay | `raw_data_collection.py`: recording and playback | Rust has deterministic inline fixtures/session doubles, but no user recording/replay API. Phase 5, with sanitization and an explicit file format. |
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

Acceptance: deterministic selection/order/overlap/error tests, typed bulk
configuration tests, unsupported-catalog rejection without HTTP, and pre-signalled
shutdown without hydration. Workspace tests, strict Clippy, no-default and
single-feature compilation, rustdoc, formatting, and Rust 1.85 must pass.
No exchange wire parser or endpoint changes are part of this phase.

## Phase 2 — subscription planning and refresh (in progress)

1. **Implemented: catalog refresh and request sharing.**
   `MarketCatalog::refresh` bypasses cached responses, including every requested
   page. Concurrent callers share in-flight work for the same URL; unrelated
   URLs remain independent. One reusable HTTP client serves discovery requests.
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
   then groups channels by identical normalized symbol sets and uses existing
   adapter/session paths. Dispatch checks exact channel/symbol membership.
   Explicit native names follow the union's order, then are rebound per group.
   This prioritizes correctness over minimizing sockets: distinct sets can open
   additional connections, and existing status reports identify only exchanges.
   Low-level adapter users must call `connection_feeds` and plan each group;
   this compilation is automatic through FeedHandler.
3. **Implemented: conservative connection sizing and paced admission.**
   Concrete groups split by actual native topic/message budgets after adapter
   deduplication. Bybit spot args batch at ten per request; a session-owned queue
   paces sends while retaining reads/heartbeats/shutdown. Process-local connection
   slots/start pacing and shared snapshot concurrency/start pacing bound bursts.
   [Connection planning](connection-planning.md) separates official limits from
   SDK choices and records cancellation and scope. Optimal packing of unequal
   channel sets, configurable budgets and distributed/weighted quotas remain
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

## Phase 3 — runtime control and symbol updates (in progress)

Core add/remove/replace/list/shutdown commands, independent initial startup,
IDs/configuration generations, scoped events and lifecycle transitions are
implemented. [The runtime-control guide](runtime-control.md) defines results,
cancellation/commit points, error aggregation and remaining readiness scope.
A manual OKX run observed generation 1 BTC trade followed by generation 2 ETH
trade on the same feed ID, then removal and exit 0. Remote-ready state and
periodic listing reconciliation remain unfinished.


- Keep `run()` compatible and add a caller-retained control handle with stable
  feed IDs. Commands: add, remove, replace, and shutdown. Each command needs an
  explicit result; invalid catalog/capability requests must not disturb healthy
  feeds. Bound the command queue and make hydration cancellable.
- Implement replacement first by validating the candidate, stopping/draining
  the old session, and starting a fresh session with fresh L2/ticker state.
  Document the replacement gap; do not claim exchange-atomic switching or
  uninterrupted books. If new startup fails after stopping the old feed, report
  that failure explicitly. Remote readiness needs separate status events.
- Define when removal completes and how already queued events are identified;
  consumers need feed/generation identity to reject events from an old session.
- Only then add opt-in periodic catalog reconciliation with minimum intervals,
  backoff, listing/removal policy, and bounded bootstrap concurrency. No polling
  by default and no Binance OI polling fallback.

Acceptance: doubles cover commands during hydration/retry, invalid replacement,
concurrent shutdown, pending snapshots, stale generations, state reset, and
healthy-feed isolation. In-place subscribe/unsubscribe can follow only where
current exchange protocols and acknowledgement handling are verified.

## Phase 4 — event and handler semantics

Add closed-only candles with unknown-completion policy, multiple handler
registrations with documented ordering/error/deadline behavior, and a recoverable
L2 consumer interface with snapshot revision anchors. Existing lossy broadcast
must remain explicit. Document which books are snapshots versus changes and
which quantities use contracts versus base units. Add only needed transport
settings without hiding protocol-specific constraints.

Acceptance: tests cover unfinished/unknown candles, handler ordering and failure,
lag-and-recovery continuity, and shutdown with slow consumers. Preserve existing
defaults unless a documented migration intentionally changes them.

## Phase 5 — optional ecosystem features

Public REST/history, sanitized recording/replay, sinks and aggregation, and NBBO
are separate increments after the subscription/lifecycle contract is stable.
Do not promise full Python backend parity as part of the 0.1 core SDK. Each
increment needs a concrete caller workflow, bounded resource behavior, and
independent tests; protocol-facing work needs current official sources.

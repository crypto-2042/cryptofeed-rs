# Python usage alignment

Status: active improvement plan, reviewed 2026-10-09 against the sibling Python
checkout at commit `3a6d3ca`. Phase 1 is implemented; later phases below are
planned, not supported APIs. Exchange count and instrument-type coverage are
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
| Symbol discovery | `exchange.py`: `symbols`, `info`, `symbol_mapping(refresh=...)` | Phase 1 exposes `MarketCatalog::load` and sorted normalized symbols. Force refresh and public market metadata remain pending. |
| Pattern selection | `feed.py` resolves each supplied name by exact mapping; no general glob expansion found | Phase 1 adds explicit catalog `select` with `*` and `?` as a convenience extension, not Python parity. |
| Batch configuration | `feed.py`: `symbols` plus `channels` | Multi-symbol feeds already work; phase 1 adds bulk `.symbols` and typed `.instruments`. |
| Per-channel symbol sets | `feed.py`: `subscription={channel: symbols}` | Rust currently uses one symbol set for every channel in a feed. Multiple feeds are a workaround; phase 2 adds a subscription map. |
| Connection sizing | `feed.py`: `connect` / `limit_sub`, endpoint-specific limits | Product/endpoint splitting exists, but generic subscription-limit sharding and paced subscribe batches do not. Phase 2. |
| Embedding and shutdown | `feedhandler.py`: `run(start_loop=False, install_signal_handlers=False)`, `stop_async` | Existing `runtime::run_with_shutdown` supports a caller-owned watch signal. Phase 1 adds a `FeedHandler` facade; it installs no Ctrl-C handler. |
| Runtime additions | `feedhandler.py`: `add_feed` starts a new feed when running; `examples/demo_loop.py` | Rust consumes a fixed feed list at startup. Phase 3 adds a runtime control handle. |
| Updating an existing subscription | No general public update/unsubscribe API found in the inspected Python core | Rust also has none. Phase 3 offers controlled feed replacement; in-place exchange WS updates are a separate optimization. |
| Automatically following listings | Python catalog refresh is explicit; no core periodic discover-and-resubscribe loop found | Neither implementation guarantees this. Optional discovery reconciliation comes after runtime controls and force refresh. |
| Callback fan-out | `feed.py`: callback lists; `callback.py`: async/sync callback wrappers | Rust registers one handler per category. Broadcast offers fan-out with loss on lag; multiple reliable handlers need separate semantics. Phase 4. |
| Candle completion | `feed.py`: `candle_closed_only`; Binance applies the flag | Rust exposes `Candle.closed` but lacks a builder-level closed-only filter. Phase 4, with an explicit policy for unknown completion. |
| Book consumption | `feed.py`: book callbacks, depth/checksum/cross checks; Python book objects expose deltas | Rust has normalized snapshots/deltas and exchange sync, but a lagged broadcast consumer cannot request a synchronized recovery snapshot. Phase 4 prioritizes recovery and documents native-unit differences. |
| Runtime settings | `feed.py`: timeout/retry/start delay/proxy settings; `config.py` | Rust uses fixed supervision policies and tracing. Add only demonstrated public-service settings after lifecycle controls; preserve safe defaults and bounded shutdown. |
| Public REST/history | `exchange.py`: ticker/trades/candles/funding/book methods and sync wrappers | Rust REST currently serves discovery and book bootstrap. Public history clients are a later workstream; verify current official endpoints before implementation. |
| Recording/replay | `raw_data_collection.py`, `util/playback.py` | Rust has deterministic inline fixtures/session doubles, but no user recording/replay API. Phase 5, with sanitization and an explicit file format. |
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

## Phase 2 — subscription planning and refresh (next)

1. Add explicit force-refresh to catalog loading, including paginated catalogs.
   Bypass only the requested catalog responses; replace cache entries only
   after validated fetches and never mutate an existing subscription implicitly.
2. Add mutually exclusive channel-symbol-map and existing channels-plus-symbols
   configuration. Validate every selected pair and route only requested pairs;
   use a canonical subscription set for planning and dispatch.
3. Add exchange-specific connection/subscription sizing from current official
   documentation. Count native topics after shared-stream deduplication; enforce
   connection limits and paced subscription batches. Large patterns must not
   blindly create an oversized socket or an unbounded REST bootstrap burst.

Acceptance: offline doubles verify heterogeneous channel-symbol routing,
unsupported pairs, topic deduplication, exact limit boundaries, and refresh
failures. Each exchange protocol change requires sourced fixtures and official
verification; limits must not be inferred from old Python constants.

## Phase 3 — runtime control and symbol updates

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

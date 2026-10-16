# Documentation

Start with the [project README](../README.md) for installation and examples,
and [PARITY.md](../PARITY.md) for the authoritative support and parity gates.

- [Dependency transport validation](reports/dependency-transport-smoke-2026-10-13.md):
  bounded HTTPS/WSS observations and compatibility checks for the HTTP/WS upgrades.

## Release preparation

- [Release-package verification](reports/release-package-verification-2026-10-13.md):
  eleven real package builds, extracted consumers/tests, security and license evidence.

- [Compatibility policy](compatibility.md): 0.1 API, MSRV, feature and data contracts.
- [Release verification and sustained-run plan](release-validation.md): packaging,
  independent consumer checks and measurable live acceptance criteria.

- [Issue #3 remediation evidence](reports/issue-3-event-stream-2026-10-14.md):
  retained Linux failure, bounded-stream changes and deterministic regression scope.
- [Issue #4 remediation evidence](reports/issue-4-gateio-recovery-2026-10-15.md):
  preserved Ready failure, sequence fixes, bounded recovery and verification limits.
- [Open-issue audit and Bitget diagnostics](reports/issue-audit-2026-10-16.md):
  closure gates, safe failure classification and remaining Linux evidence.
- [Gateio L2 recovery](gateio-book-recovery.md): ID bridges, cumulative recovery
  budgets, idle polling, epoch-qualified diagnostics and issue #4 evidence.
- [Event delivery and receiver diagnostics](event-streams.md): shared retention,
  explicit lag, configurable event rings, backlog and final drain accounting.

## Reference and contribution guides

- [Exchange protocol baseline](exchange-protocol-baseline.md): official wire
  contracts, dated observations, and normalization boundaries.
- [Public parity harness](harness.md): sanitized fixtures, deterministic replay,
  and acceptance criteria.
- [AI coding workflow](ai-coding.md): contributor workflow and verification rules.
- [Binance contract OI decision](binance-open-interest-decision.md): deferral,
  official API references, and multi-symbol rate-budget rationale.
- [Feature status](feature-status.html): Chinese visual support report; download
  and open the HTML locally to view it.

- [Python requirement audit](python-alignment-audit.md): implementation/test
  evidence, remaining book policies and completion criteria.

- [Python usage alignment](python-usage-alignment.md): source-backed gap inventory,
  implementation phases, and supported discovery/lifecycle conveniences.

- [Per-session WS send budgets](ws-send-budgets.md): shared text/heartbeat/Pong
  admission, native hourly control windows and cancellation.

- [Connection planning, state isolation and budgets](connection-planning.md): native subscription
  limits, paced sending/handshakes, snapshot admission, and cancellation.

- [Managed runtime control](runtime-control.md): command results, feed identity,
  configuration generations, cancellation, startup policy and lifecycle scope.

- [Readiness and retained state](readiness.md): confirmation evidence, reconnect
  epochs, book synchronization, diagnostics and authoritative queries.

- [Automatic symbol reconciliation](discovery.md): opt-in patterns, listing/removal
  policy, refresh backoff, ownership and stopping.

- [Candle delivery](candle-delivery.md): final/unfinished/unknown completion,
  filtering surfaces and compatible defaults.

- [Multiple handlers](handlers.md): registration order, per-callback deadlines,
  panic/error semantics, backpressure and shutdown.

- [Maximum delivered L2 depth](book-delivery-depth.md): consistent top-N
  snapshots/deltas, deletion/promotion and recovery/replay semantics.

- [Optional crossed-L2 policy](book-cross-check.md): exact best-price checks
  before publication, recovery withdrawal and recorded replay configuration.

- [Recoverable L2 consumption](l2-recovery.md): atomic snapshots/subscriptions,
  continuity anchors, lag recovery and cache invalidation.

- [Runtime budgets](runtime-options.md): retry limits/reset, handshake and callback
  deadlines, fixed protocol policies and remaining transport settings.

- [Explicit transport/proxy](transport.md): shared HTTP/WS routing, authentication,
  direct defaults, cache isolation and TLS/deadline behavior.

- [Market metadata](market-metadata.md): typed directory information, current
  field mappings, exact precision, capability context and refresh/applicability.

- [Public REST snapshots](public-rest.md): normalized ticker/book methods, native
  time/sequence metadata, depth limits, shared HTTP admission and backoff.

- [Funding history](funding-history.md): bounded settlement batches, exact actual
  rates, versioned cursors, native pagination/retention and termination semantics.

- [HTTP snapshot/L2 replay](http-l2-replay.md): v2 causal processing markers,
  consumed bootstrap bodies/failures, offline receiver scheduling and scope.

- [Native raw replay](raw-replay.md): current parser/dispatch reuse, per-session
  ticker/WS-book state, lifecycle delivery, limits and unsupported HTTP-bootstrap paths.

- [Public HTTP frequency audit](public-http-frequency-audit.md): verified count
  limits for current non-Binance routes and the existing pacing boundary.

- [Binance HTTP weights](http-weights.md): current endpoint tiers, rolling windows,
  minute header feedback, stricter shared usage thresholds and replay/cancellation semantics.
- [Replay contract audit](replay-contract-audit.md): model/delivery/cache clock
  domains, exact cursor consumption, request cohorts and terminal state evidence.
- [Proven minimum connection planning](minimum-connections.md): optional exact
  packing, native lower bounds, search limits and cancellable runtime execution.
- [Independent raw capture replay](raw-merge.md): explicit common-axis offsets,
  namespaced identity, isolated native/HTTP/L2 state and aggregate bounds.
- [WS handshake and close failures](ws-lifecycle-failures.md): HTTP delay hints,
  scoped close codes, retry decisions and establishment-slot reuse.
- [Public WS control failures](ws-control-errors.md): native frequency/service codes,
  permanent rejection boundaries, shared cooldown and replay semantics.
- [Native REST rate limits](native-rate-limits.md): verified native codes, public
  header hints, scoped cooldown, cache rejection and offline replay isolation.

- [Shared resource budgets](resource-budgets.md): stricter HTTP/WS groups, joint
  pacing, cancellation, scoped plan admission and managed replacement reservations.

- [Directory cache bounds](catalog-cache.md): shared entry/byte/LRU/TTL retention,
  snapshot lifetime, numeric fidelity and explicit offline cache-loss handling.

- [Offline HTTP replay](http-replay.md): validated payload injection into existing
  directory/REST APIs, request-order matching, strict audit and no network fallback.

- [Public HTTP recording](http-recording.md): scoped GET attempts/results/cancels,
  shared transport integration, sanitized bounded JSONL and remaining replay work.

- [Raw series rotation](raw-series.md): record-aligned physical parts, ordered
  manifest composition, size/checksum gates and continuous native replay.

- [Raw WS files](raw-recording.md): bounded segments, privacy revalidation,
  lifecycle/context integrity and explicit prefix/footer semantics.

- [Public WS observation](raw-capture.md): pre-parser input/output capture,
  sanitization, bounded failure signaling, source context and remaining replay scope.

- [Cross-exchange NBBO](nbbo.md): exact-symbol best bid/ask, declared native size
  units, source generations, age expiry and lifecycle withdrawal.

- [Throttle and trade aggregation](aggregation.md): leading-edge sampling, bounded
  arrival-clock OHLCV, threshold Renko and custom window calculations with typed
  state, source grouping and replay reproducibility.

- [Event sinks](sinks.md): sequential fallible storage integration, bounded JSONL
  adapter, lag/deadline/cancellation and finalization contracts.

- [Normalized recording/replay](recording.md): versioned JSONL, source identity,
  bounded strict capture, offline callbacks, timing/cancellation and format scope.

- [Trade history](trade-history.md): explicit granularity, ID-aware paging,
  scoped continuation, native retention and bounded stop reasons.

- [Recent trades](recent-trades.md): bounded five-venue public queries, taker side,
  exact native IDs/quantity units, sorting and granularity differences.

- [Candle history](candle-history.md): bounded five-venue time windows, calendar
  month boundaries, unknown completion and scoped JSON continuation.

## Offline workflow reports

- [2026-10-11 HTTP SDK replay](reports/offline-http-sdk-replay-2026-10-11.md): saved
  public payloads recreate 1,152 markets and ticker/book/trades with four consumed requests.
- [2026-10-11 custom aggregation](reports/offline-custom-aggregation-2026-10-11.md):
  recorded trades to bounded per-source buy/sell counts, windows and partial tail.
- [2026-10-11 trade OHLCV](reports/offline-ohlcv-2026-10-11.md): previously captured
  public trades to one-second bars, count/volume checks and an explicit partial tail.

## Live validation reports

These dated, sanitized reports record manual public-service checks. They are
historical evidence, not CI gates, throughput benchmarks, or proof of every
supported instrument/channel combination. Later code changes do not retroactively
change earlier observations.

- [2026-10-12 shared resource scopes](reports/live-smoke-resource-budget-2026-10-12.md):
  OKX excess-add rejection, same-ID replacement, slot reuse and shared REST group.
- [2026-10-11 public HTTP recording](reports/live-smoke-http-recording-2026-10-11.md):
  OKX directory/ticker/book/trades, four attempts/eight observations and offline validation.
- [2026-10-11 raw series](reports/live-smoke-raw-series-2026-10-11.md): 40 OKX
  observations across three files, clean runtime shutdown and 17 offline models.
- [2026-10-11 NBBO](reports/live-smoke-nbbo-2026-10-11.md): Binance/OKX Spot
  L1, twenty best changes and explicit final withdrawal before clean shutdown.
- [2026-10-11 JSONL sink](reports/live-smoke-sink-2026-10-11.md):
  20 public OKX events through the generic sink, shutdown and offline reader replay.
- [2026-10-10 funding history](reports/live-smoke-funding-history-2026-10-10.md):
  five public services, bounded first/resume batches and JSON cursor restoration.
- [2026-10-10 public REST](reports/live-smoke-rest-2026-10-10.md): twenty Spot/Perpetual
  ticker/book observations and the Gate Spot timestamp-unit correction.
- [2026-10-10 market metadata](reports/live-smoke-metadata-2026-10-10.md):
  ten Spot/Perpetual catalogs, typed BTC metadata and the earlier COIN-M request failure.
- [2026-10-10 sparse packing](reports/live-smoke-packing-2026-10-10.md):
  five Spot feeds, each one socket for Trade BTC/ETH plus L2 BTC, with exact recovery scope.
- [2026-10-10 authenticated proxy](reports/live-smoke-proxy-2026-10-10.md):
  actual Binance catalog, WS and REST-assisted L2 readiness through HTTP CONNECT.
- [2026-10-10 L2 recovery](reports/live-smoke-book-recovery-2026-10-10.md):
  real OKX anchored L2 updates, full local snapshot and retirement.
- [2026-10-10 directory reconciliation](reports/live-smoke-discovery-2026-10-10.md):
  real OKX periodic directory refresh without replacement, data and cleanup.
- [2026-10-10 readiness](reports/live-smoke-readiness-2026-10-10.md):
  public Trade/L2 confirmations and local book readiness on all five exchanges.
- [2026-10-10 managed runtime](reports/live-smoke-managed-2026-10-10.md):
  public OKX add, replacement, removal and clean shutdown with event identity.
- [2026-10-09 OKX endpoint migration](reports/live-smoke-2026-10-09.md):
  default TLS 443, recommended REST domain, and preopen catalog regression.
- [2026-10-08 final candidate checks](reports/live-smoke-pre-push-2026-10-08.md):
  aggregate five-exchange run and targeted Bybit/Binance/Bitget follow-ups.
- [2026-10-08 channel additions](reports/live-smoke-2026-10-08.md): Bitget L1 and
  derivative fields, OKX contract index, and Gate.io liquidation subscription.
- [2026-08-17 candidate baseline](reports/live-smoke-2026-08-17.md).
- [2026-08-04 protocol refresh](reports/live-smoke-2026-08-04.md).

Current milestones and remaining release work are tracked in
[PROGRESS.md](../PROGRESS.md). Internal review notes, retired implementation
plans, and presentation drafts stay in ignored `.local/docs/`.

[Route-first capacity planning](connection-planning.md#route-first-capacity-planning--2026-10-12)
now keeps small routes together when another endpoint needs capacity shards.

# Documentation

Start with the [project README](../README.md) for installation and examples,
and [PARITY.md](../PARITY.md) for the authoritative support and parity gates.

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

- [Python usage alignment](python-usage-alignment.md): source-backed gap inventory,
  implementation phases, and supported discovery/lifecycle conveniences.

- [Connection planning and budgets](connection-planning.md): native subscription
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

- [Raw WS files](raw-recording.md): bounded segments, privacy revalidation,
  lifecycle/context integrity and explicit prefix/footer semantics.

- [Public WS observation](raw-capture.md): pre-parser input/output capture,
  sanitization, bounded failure signaling, source context and remaining replay scope.

- [Normalized recording/replay](recording.md): versioned JSONL, source identity,
  bounded strict capture, offline callbacks, timing/cancellation and format scope.

- [Trade history](trade-history.md): explicit granularity, ID-aware paging,
  scoped continuation, native retention and bounded stop reasons.

- [Recent trades](recent-trades.md): bounded five-venue public queries, taker side,
  exact native IDs/quantity units, sorting and granularity differences.

- [Candle history](candle-history.md): bounded five-venue time windows, calendar
  month boundaries, unknown completion and scoped JSON continuation.

## Live validation reports

These dated, sanitized reports record manual public-service checks. They are
historical evidence, not CI gates, throughput benchmarks, or proof of every
supported instrument/channel combination. Later code changes do not retroactively
change earlier observations.

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

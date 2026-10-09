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

## Live validation reports

These dated, sanitized reports record manual public-service checks. They are
historical evidence, not CI gates, throughput benchmarks, or proof of every
supported instrument/channel combination. Later code changes do not retroactively
change earlier observations.

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

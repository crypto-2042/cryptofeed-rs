# Funding-history continuation smoke — 2026-10-10

`funding_history_public` requested the seven days ending at local observation
start for BTC-USDT-PERP, using page_size 5 and max_pages 2. Each returned cursor
was serialized to JSON, deserialized and used for a second bounded call.
No account/private/trading service or exchange credentials were used.

| Exchange | First records/scanned/pages | First stop | Resumed records/scanned/pages | Resumed stop | Duplicate identity/time |
| --- | --- | --- | --- | --- | --- |
| Binance | 10 / 10 / 2 | BudgetReached | 10 / 10 / 2 | BudgetReached | none |
| Bitget v3 | 10 / 10 / 2 | BudgetReached | 10 / 10 / 2 | BudgetReached | none |
| Bybit v5 | 10 / 10 / 2 | BudgetReached | 10 / 10 / 2 | BudgetReached | none |
| OKX v5 | 10 / 10 / 2 | BudgetReached | 10 / 10 / 2 | BudgetReached | none |
| Gate v4 | 10 / 10 / 2 | BudgetReached | 10 / 10 / 2 | BudgetReached | none |

All ten batches returned a next cursor, preserving the fact that the configured
budget had stopped the walk. In total 20 pages and 100 distinct per-exchange
settlement records were observed; process exit was 0. The example checked
normalized identity and no repeated timestamp across each exchange's two batches.
It did not print full funding data or claim the requested seven days were complete.

This proves live first/resume request shapes and cursor restoration for the
current public profiles. It is not a throughput benchmark, retention audit,
rate-limit/failure observation or certification of every historical instrument.
Deterministic offline tests independently verify raw-rate selection, precision,
unit conversion, scope/limits, malformed/conflicting/stalled pages and cancellation.

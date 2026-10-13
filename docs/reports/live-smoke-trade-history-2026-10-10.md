# Native trade-history smoke — 2026-10-10

Command: `cargo run -p cryptofeed-rs --example trade_history_public`.
Current unauthenticated endpoints, direct transport; final exit 0.

| Exchange/product | Granularity | First records/scanned/pages | JSON resume records/scanned/pages | Stop/next |
| --- | --- | --- | --- | --- |
| Binance Spot | Aggregate | 10 / 10 / 2 | 10 / 10 / 2 | BudgetReached / true |
| Binance USD-M perpetual | Aggregate | 10 / 10 / 2 | 10 / 10 / 2 | BudgetReached / true |
| OKX Spot | Individual | 10 / 10 / 2 | 10 / 10 / 2 | BudgetReached / true |
| OKX swap | Individual | 10 / 10 / 2 | 10 / 10 / 2 | BudgetReached / true |
| Gate Spot | Individual | 10 / 10 / 2 | 10 / 10 / 2 | BudgetReached / true |
| Gate USDT perpetual | Individual | 10 / 10 / 2 | 10 / 10 / 2 | BudgetReached / true |

Final run used a five-minute BTC-USDT/Perpetual range ending two minutes before
query construction, page size 5 and request budget 2. JSON cursors were serialized
and deserialized before resume. It checked normalized exchange/symbol identity,
requested time bounds and unique IDs across both batches. Total 24 pages / 120
per-product executions; all remained budget-limited with continuation. This is
not complete-range, full-retention, inverse/dated-futures or throughput evidence.

Retained earlier observations:

- A direct urllib OKX time-seed probe returned HTTP403. A separate curl request
  to the same official openapi history endpoint returned the documented payload;
  the SDK run succeeded without a private/obsolete-domain fallback.
- First SDK example used a one-minute Gate range with page size 100. Gate Spot
  returned 17 and perpetual 82 records, one page each, SourceExhausted/no cursor.
  The example then failed its requirement to exercise resume. This was valid
  source exhaustion, not evidence of a pager failure. Final example uses a wider
  five-minute range/page size 5 to exercise continuation on the same API paths.
- Gate range probes showed page 1/2 and offset 0/2 returning distinct IDs,
  including multiple perpetual executions at one millisecond. They also exposed
  the earlier recent-contract clock error, corrected in code/capture/tests and
  followed by a stronger ten-product recent-trades run (five-minute age bound).

Private APIs, archived-file import and deprecated last_id were not used.
Gate delivery range history is separately supported as one bounded native page
(SourceLimit if full); the live six-product table does not cover delivery. Native
retention/errors and future/delayed inserts can still limit traversal. Offline
same-time/empty/ceiling/scope/cancellation tests remain the deterministic gate.

# Candle-history public smoke — 2026-10-10

Command: `cargo run -p cryptofeed-rs --example candle_history_public`.
Unauthenticated current public endpoints, direct transport; exit 0.

| Exchange/product | First records/pages | JSON resume records/pages | Stop / next |
| --- | --- | --- | --- |
| Binance Spot | 10 / 2 | 10 / 2 | BudgetReached / true |
| Binance USD-M perpetual | 10 / 2 | 10 / 2 | BudgetReached / true |
| Bybit Spot | 10 / 2 | 10 / 2 | BudgetReached / true |
| Bybit linear perpetual | 10 / 2 | 10 / 2 | BudgetReached / true |

Each used BTC-USDT (or BTC-USDT-PERP), 1m interval, a last-hour millisecond range,
page size 5 and budget 2. Cursors were serialized/deserialized before resuming.
The example checked normalized identity, requested open-time bounds, unknown
finality and no duplicate open times across both batches. All four first calls
returned exactly 10 records/2 pages; printed resume results also had 10/2.
Total: 16 public pages, 80 per-product records. This is budget-limited evidence,
not a complete-hour, retention, inverse/dated-futures or monthly-live guarantee.
No first-attempt failures occurred. Other candle-history venues are not yet
implemented. Sparse windows, invalid data, scope edits, monthly/leap-year ends,
COIN-M routing/range limits and cancellation are covered by offline doubles.

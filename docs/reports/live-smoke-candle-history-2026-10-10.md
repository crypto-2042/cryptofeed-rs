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

## All-venue extension and retained failures

The first expanded run succeeded on Binance/Bybit/Bitget/OKX Spot and perpetual,
but both Gate products failed strict malformed/out-of-window validation. Public
range probes showed nonaligned from returned six bars for a five-minute request;
aligning from to the first legal open returned five on Spot and perpetual.
After that fix, both Gate products passed but Bitget's second run returned one
earlier bar on both products while the newest historical bar was unavailable.
This is consistent with its documented earlier-interval behavior; a direct
probe returned the same earlier row even with an aligned startTime.

The bounded Bitget exception counts one adjacent earlier row against the page
budget and filters it for this window. An offline JSON-resume regression proves
it is still emitted in the next window. More than one earlier interval, further
out-of-range/upper-bound rows and duplicates remain errors. A later successful
run does not erase the initial failures or claim arbitrary native retention.

Final command: `cargo run -p cryptofeed-rs --example candle_history_public`;
exit 0. The final example uses a completed minute boundary one minute before
now and a preceding one-hour range; finality is still taken only from native flags.

| Exchange/product | First records/pages | JSON resume records/pages | Stop / next |
| --- | --- | --- | --- |
| Binance Spot | 10 / 2 | 10 / 2 | BudgetReached / true |
| Binance USD-M perpetual | 10 / 2 | 10 / 2 | BudgetReached / true |
| Bybit Spot | 10 / 2 | 10 / 2 | BudgetReached / true |
| Bybit linear perpetual | 10 / 2 | 10 / 2 | BudgetReached / true |
| Bitget v3 Spot | 10 / 2 | 10 / 2 | BudgetReached / true |
| Bitget v3 USDT perpetual | 10 / 2 | 10 / 2 | BudgetReached / true |
| OKX Spot | 10 / 2 | 10 / 2 | BudgetReached / true |
| OKX swap | 10 / 2 | 10 / 2 | BudgetReached / true |
| Gate Spot | 10 / 2 | 10 / 2 | BudgetReached / true |
| Gate USDT perpetual | 10 / 2 | 10 / 2 | BudgetReached / true |

Final run total: 40 pages and 200 per-product records. Normalized identity,
requested open-time bounds and cross-batch duplicate checks passed. Binance,
Bybit and Bitget flags remain unknown; Gate perpetual also has no completion
field. OKX/Gate Spot use native flags. Inverse/dated/monthly live coverage is not
claimed; undocumented Gate delivery history remains rejected.

Additional unauthenticated Gate probes used September 2026 ranges: 30d returned
1788220800 on both Spot/perpetual (September 1 UTC); 1d opens had remainder zero
modulo 86400 seconds. Spot 7d returned 1788134400 and 1788739200, remainder
345600 modulo 604800 (Monday); perpetual 7d returned 1787788800 and 1788393600,
remainder zero. This caught and corrected the Spot weekly request-grid assumption
before commit. Regression assertions now bind the first legal open by product.

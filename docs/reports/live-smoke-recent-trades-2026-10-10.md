# Recent public trades smoke — 2026-10-10

Command: `cargo run -p cryptofeed-rs --example recent_trades_public`.
Current unauthenticated public endpoints, direct transport; exit 0.

| Exchange/product | Records | Unique IDs | Ascending timestamp |
| --- | --- | --- | --- |
| Binance Spot | 5 | 5 | true |
| Binance USD-M perpetual | 5 | 5 | true |
| Bitget v3 Spot | 5 | 5 | true |
| Bitget v3 USDT perpetual | 5 | 5 | true |
| Bybit Spot | 5 | 5 | true |
| Bybit linear perpetual | 5 | 5 | true |
| OKX Spot | 5 | 5 | true |
| OKX swap | 5 | 5 | true |
| Gate Spot | 5 | 5 | true |
| Gate USDT perpetual | 5 | 5 | true |

Each query requested BTC-USDT or BTC-USDT-PERP with limit 5. The example checked
exact exchange/normalized identity, mandatory unique IDs, finite positive native
times in ascending order and no timestamp more than 60 seconds after receipt.
Total ten queries / 50 per-product executions. No first-attempt failures occurred.
This is recent-only evidence; no pagination, retention/completeness, throughput,
archived import or inverse/dated-futures live result is claimed. Taker directions
and native units are asserted against official field tables by offline tests;
this smoke does not independently observe the matching engine's aggressor.

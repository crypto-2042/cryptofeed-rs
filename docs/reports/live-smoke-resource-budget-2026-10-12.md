# Shared resource scope smoke — 2026-10-12

Command: `cargo run -p cryptofeed-rs --example resource_budget_public`.
Unauthenticated current OKX Spot trade feeds and public REST through one shared
ResourceBudget/direct transport. HTTP capacity 1/start interval 2 seconds;
WS capacity 1 per exchange/start interval 2 seconds. Exit 0.

| Stage | Observed |
| --- | --- |
| Initial feed | BTC-USDT trade received |
| Excess add | ETH-USDT in the same scope rejected; existing feed retained |
| Same-ID replacement | ETH-USDT trade received after replacing BTC |
| Removal/reuse | Removed ETH, then added BTC and received trade on reused capacity |
| HTTP | Directory, ticker and five-level L2 queries completed using the same group |
| Shutdown | Managed runtime stopped and exited successfully |

Console: `resource budget shared=true rejected_add=true replaced_same_id=true live_slot_reused=true rest_queries=3`.
This proves one public lifecycle/admission/slot-reuse workflow. It is not sustained
load, exact server-arrival pacing, all-exchange/product validation, weighted/native
quota accounting or a distributed reservation service. Paused-clock tests separately
verify composite admission timing, cooldown delay behavior and permit cancellation.
No raw server payloads, credentials or private operations are committed.

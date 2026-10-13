# Public NBBO smoke — 2026-10-11

Command: `cargo run -p cryptofeed-rs --example nbbo_public`.
Unauthenticated Binance/OKX Spot BTC-USDT native L1 feeds, existing current
adapters and direct transport. Both sources declared Base quantity units.

| Check | Observed |
| --- | --- |
| Sources | Both exchanges supplied accepted complete L1 quotes |
| Changes | 20 best-quote changes in the bounded run |
| Freshness | Five-second age policy; local receive-to-consume age included |
| End | Both sources explicitly suspended; final quote=None withdrawal asserted |
| Runtime | Managed shutdown and process exit 0 |

Final console: `NBBO spot sources=2 changes=20 both_observed=true sources_suspended=true final_withdrawal=true`.
An earlier run also exited 0 with 20 changes; the example was then strengthened
to assert the final withdrawal explicitly and rerun successfully. This is a
bounded two-source Spot workflow, not sustained-load, all-product, reconnect,
loss/recovery, regulated consolidated feed or derivative unit-conversion evidence.
Synthetic offline tests cover those documented local scope/age/lifecycle bounds;
identical replay inputs/times preserve every emitted field. No live raw frames,
auth material or source payload files are committed.

# Native raw replay evidence — 2026-10-11

First, a pure offline command opened the prior OKX raw segment after its live
runtime had already stopped:
`cargo run -p cryptofeed-rs --features recording --example raw_parser_replay -- RAW_PATH`.
Exit 0: 24 observations, 17 normalized models, LimitReached, one open prefix session.
A read-only shape check found 17 data packets/17 trade rows plus one subscribe
reply carrying the trades arg. The earlier file report's 18 trade-channel packets
included that reply; it was not an 18-trade normalization claim. All 17 data rows
were normalized, and the control reply was not counted as a market model.

Then `cargo run -p cryptofeed-rs --features recording --example raw_replay_public`
captured current unauthenticated Spot feeds, stopped each runtime, and replayed
saved in-memory raw segments with native parsers. Exit 0, no first-attempt failures.

| Venue/profile | Raw observations | Offline models | Comparison |
| --- | --- | --- | --- |
| Binance Spot | 12 | 9 | Every full Trade exactly matched a live normalized model |
| Bitget v3 Spot | 12 | 56 | Same, including batched outputs |
| Bybit v5 Spot | 12 | 10 | Same |
| OKX v5 Spot | 12 | 9 | Same |
| Gate v4 Spot | 12 | 9 | Same |

Total: 60 observations / 93 models. Comparison included exact Decimal fields,
side/native ID/normalized symbol and exchange/receipt clocks, via model equality.
The live comparison ledger was independently drained and bounded; lag/overflow
would fail the example. Replay occurred after successful runtime shutdown, using
only bytes from the captured segment. Actual public payloads were not committed.

This is five Spot trade-family evidence, not a new all-category/product/live-L2,
HTTP bootstrap, readiness/timer simulation, sustained-load or native retention
certification. Offline regressions separately cover derivative/dated bindings,
Bybit ticker reset, three WS-native L2 families, sparse/candle policies, limits,
callback/cancellation behavior and refusal to fetch missing HTTP snapshots.

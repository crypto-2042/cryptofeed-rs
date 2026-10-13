# Directory reconciliation smoke — 2026-10-10

The public `discovery_public` example used an OKX v5 Spot Trade template and
`BTC-USDT` pattern, with a 60-second interval. It fetched the real instrument
directory initially and once more, using the managed runtime concurrently.
No credentials/private services were used.

| Observation | Result |
| --- | --- |
| Catalog cycles | 2, including initial refresh |
| Successful replacements | 0; selected symbol/native mapping unchanged |
| Identity | feed 1, generation 1 throughout |
| Discovery state after second cycle | Current, failures 0, no last error |
| Runtime state | Ready, connection 1/1, confirmation 1/1, epoch 1 |
| Normalized observations | 27 events, one channel/symbol pair |
| Explicit discovery stop | Stopped, next delay 0 |
| Managed removal | Remaining registry entries 0 |
| Shutdown | Process exit 0 |

This confirms real periodic directory refresh, unchanged-selection stability,
public readiness/data and explicit cleanup. It does not claim an actual listing,
delisting or native alias change occurred. Scripted offline tests independently
cover those transitions, refresh failures, ownership races and cancellation.
The event count is a short observation, not a throughput measurement or a
promise of lossless delivery. This manual example is not a CI gate.

# Sparse subscription packing smoke — 2026-10-10

`packed_public` configured each exchange with Trade ETH-USDT/BTC-USDT and L2
BTC-USDT, using explicit native mappings in first-seen ETH/BTC order. This tests
unequal channel sets sharing a Spot connection and Gate's filtered snapshot index
when the L2 symbol is second in the union. Observation deadline was 60 seconds.
No credentials/private services were used.

| Exchange | State | Connections | Native confirmations | Books | Normalized events | Observed pairs | Epoch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Binance | Ready | 1/1 | 3/3 | 1/1 | 200 | 3 | 1 |
| Bitget | Ready | 1/1 | 3/3 | 1/1 | 418 | 3 | 1 |
| Bybit | Ready | 1/1 | 3/3 | 1/1 | 134 | 3 | 1 |
| OKX | Ready | 1/1 | 3/3 | 1/1 | 86 | 3 | 1 |
| Gate.io | Ready | 1/1 | 3/3 | 1/1 | 145 | 3 | 1 |

Every recovery handle returned a BTC snapshot and no ETH snapshot. All connection
last-error fields were None, and shutdown exited with code 0. Explicit mappings
isolate subscription planning/readiness from HTTP directory hydration; Binance
and Gate still use their normal REST book bootstrap. Existing native bridging
and sequence checks were not relaxed.

The previous planner separated these unequal sets into two concrete Spot feeds;
the new observation uses one physical connection per exchange. This is not a
before/after throughput benchmark, all-market capacity test, or proof that every
combination can share one socket. Derivative product and public/business endpoint
separation are covered independently by offline tests. Event counts describe only
this short observation, and recovery still uses a bounded/lossy update stream.

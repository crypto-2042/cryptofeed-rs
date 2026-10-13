# Public Exchange Live Smoke — 2026-08-04

This report records the manual unauthenticated validation performed after the
2026-08-04 official-protocol refresh. Each successful configuration ran for
approximately 18–25 seconds against the real public REST and WebSocket services,
then received Ctrl-C. The temporary runner and diagnostic output were removed.

Counts are ordered as candle / ticker / trade / L2 events. They demonstrate
that active routes delivered normalized events; they are not throughput
benchmarks and are not deterministic expectations.

| Exchange/product | Symbol | Event counts | Result |
| --- | --- | --- | --- |
| Binance spot | `BTC-USDT` | 8 / 427 / 26 / 151 | pass; real book ticker without `e` parsed |
| Binance USD-M perpetual | `BTC-USDT-PERP` | 29 / 769 / 43 / 207 | pass; public and market connections both active |
| Bitget v3 spot | `BTC-USDT` | 500 / 3 / 50 / 10 | pass; books acknowledgement caused no retry |
| Bitget v3 USDT futures | `BTC-USDT-PERP` | 510 / 63 / 150 / 265 | pass; books acknowledgement caused no retry |
| Bybit v5 spot | `BTC-USDT` | 10 / 319 / 124 / 366 | pass; level-1 BBO Ticker and level-50 L2 both active |
| Bybit v5 linear | `BTC-USDT-PERP` | 8 / 24 / 139 / 237 | pass |
| Gate.io v4 spot | `BTC-USDT` | 5 / 488 / 8 / 190 | final pass; see bootstrap observation below |
| Gate.io v4 USDT perpetual | `BTC-USDT-PERP` | 4 / 100 / 62 / 65 | pass; catalog discovery accepted live zero multiplier record |
| OKX v5 spot | `BTC-USDT` | 8 / 74 / 19 / 109 | pass regression control |
| OKX v5 swap | `BTC-USDT-PERP` | 14 / 92 / 215 / 142 | pass regression control |

All final successful runs returned `Ok(())` after Ctrl-C. No credentials or
private channels were used. Sparse funding and liquidation channels were not
judged by event count during this short window.

## Gate.io spot bootstrap observation

Two early Gate.io spot attempts completed discovery and subscription but failed
during REST-snapshot plus buffered-delta bootstrap with:

```text
gateio initial delta does not bridge snapshot
```

The next diagnostic run and the final clean run both succeeded. A successful
trace discarded stale deltas, accepted the first bridging update, and continued
with a contiguous sequence. Existing deterministic coverage verifies stale
discard, snapshot bridging, post-bootstrap gap rejection, and resync.

The sequence rule was intentionally not weakened: accepting a non-bridging delta
would risk silently publishing a corrupt book. Treat this as an intermittent
bootstrap stability observation. A future fix requires a captured failing
snapshot ID and complete buffered `U/u` sequence, followed by a bounded
resnapshot test; live availability alone must not define the behavior.

## Regression conclusions

- Binance spot no longer depends on fabricated event metadata, and USD-M sends
  its public and market topics to the correct services.
- Bitget successful subscribe responses are control frames, not order-book
  payloads.
- Bybit spot BBO comes from the official level-1 order-book topic; its 24-hour
  ticker statistics are not coerced into the project Ticker model.
- Gate contract discovery no longer treats a non-identity zero multiplier as a
  malformed catalog.
- OKX spot and swap behavior remained stable during the targeted repairs.

# Managed readiness smoke — 2026-10-10

The `readiness_public` example requested public spot BTC-USDT Trade and L2Book
on Binance, Bitget v3, Bybit v5, OKX v5 and Gate.io v4 using explicit native pairs.
No credentials/private services were used. It queried the retained state API
without relying on lifecycle broadcast delivery. The observation window is at
most 60 seconds; successful observations can end earlier.

## First observation

Binance, Bitget, OKX and Gate reached Ready with one connected route, 2/2 native
subscriptions and 1/1 local book. Bybit stayed Reconnecting after TLS handshake
EOF errors (attempt epoch 6, zero observed events). The process returned exit 1
because the all-ready observation deadline was reached. This was a transport
failure before subscription, not a successful Bybit readiness observation; the
SDK did not label that connection Ready merely because other feeds were healthy.

## Follow-up after final reconnect/cache and publication-order changes

| Exchange | State | Connected routes | Confirmed native topics | Synchronized books | Normalized events | Observed pairs | Current epoch |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| Binance | Ready | 1/1 | 2/2 | 1/1 | 102 | 2 | 1 |
| Bitget v3 | Ready | 1/1 | 2/2 | 1/1 | 175 | 2 | 1 |
| Bybit v5 | Ready | 1/1 | 2/2 | 1/1 | 40 | 2 | 2 |
| OKX v5 | Ready | 1/1 | 2/2 | 1/1 | 52 | 2 | 1 |
| Gate.io v4 | Ready | 1/1 | 2/2 | 1/1 | 70 | 2 | 1 |

All current connection errors were None and the process exited with code 0 after
cleanup. Bybit succeeded on epoch 2 in this run. This does not assert that its
first epoch had previously been Ready; the earlier failed observation remains
separate above. Counts are cumulative within a configuration generation and
are not rate, losslessness or throughput benchmarks.

The successful run verifies selected current subscription confirmations and
local book initialization through real public services. Offline tests separately
verify stale/repeated replies, stop ordering, status lag, resync and rejection.
No live negative subscription, forced gap, all-product matrix, rare channel,
consumer recovery or long-duration health certification is claimed.

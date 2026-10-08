# Public Exchange Live Smoke — 2026-08-17

This report records the unauthenticated release-candidate smoke performed after
the protocol, feature-boundary, reconnect, and transport-hardening changes. The
aggregated runner connected spot and derivative feeds on all five active
exchanges for approximately 20 seconds, then received Ctrl-C and returned
`Ok(())`.

Counts are normalized events observed by `FeedHandler::subscribe()`. They are
acceptance evidence, not throughput benchmarks.

| Exchange/product | Symbol | Candle | Ticker | Trade | L2 | L1 | Funding | Liq. | OI | Index | Mark | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Binance spot | `BTC-USDT` | 15 | 10,245 | 1,526 | 293 | — | — | — | — | — | — | pass |
| Binance USD-M perpetual | `BTC-USDT-PERP` | — | 41,722 | 1,906 | 283 | 41,722 | 10 | — | — | 29 | 10 | pass |
| Bitget v3 spot | `BTC-USDT` | 517 | 98 | 916 | 534 | — | — | — | — | — | — | pass |
| Bitget v3 USDT perpetual | `BTC-USDT-PERP` | 525 | 272 | 2,957 | 598 | — | — | 7 | — | — | — | pass |
| Bybit v5 spot | `BTC-USDT` | 21 | 726 | 956 | 916 | — | — | — | — | — | — | pass |
| Bybit v5 linear | `BTC-USDT-PERP` | 30 | 163 | 6,036 | 1,211 | — | 1 | 25 | 4 | 64 | 25 | pass |
| Gate.io v4 spot | `BTC-USDT` | 11 | 415 | 240 | 265 | — | — | — | — | — | — | pass |
| Gate.io v4 USDT perpetual | `BTC-USDT-PERP` | 15 | 1,053 | 1,974 | 287 | 1,053 | 29 | — | 29 | 29 | 29 | pass |
| OKX v5 spot | `BTC-USDT` | 33 | 191 | 775 | 272 | — | — | — | — | — | — | pass |
| OKX v5 swap | `BTC-USDT-PERP` | 47 | 202 | 2,628 | 292 | — | 2 | — | 5 | — | 148 | pass |

The run also verified that multiplexed streams no longer publish unrequested
event categories: spot Binance and Gate.io feeds subscribed to Ticker but not
L1, and emitted no L1 events. Binance USD-M explicitly subscribed to L1 and did
emit it.

The market-wide Bitget liquidation stream was filtered to the requested BTC
symbol; unrelated liquidation rows were not published. Bybit's current
`T/s/S/v/p` liquidation shape delivered 25 normalized BTC events. Current
option-ticker fields remain covered by sourced offline fixtures, but all
Options and MARGIN products are outside the 0.1 runtime capability matrix.
Binance WebSocket open interest is likewise disabled.

No credentials, signatures, cookies, or private payloads were used.

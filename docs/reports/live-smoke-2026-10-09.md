# OKX endpoint migration smoke — 2026-10-09

This targeted manual check covers OKX spot after the endpoint/currency review.
It is not a repeat of the five-exchange matrix or a performance benchmark.

## Configuration and result

The existing `okx_public` example requested BTC-USDT Ticker, Trade, L2Book, and
Candles, using catalog hydration through `openapi.okx.com`. Public data used
`wss://ws.okx.com/ws/v5/public`; candles used the separate
`wss://ws.okx.com/ws/v5/business`. Both use default TLS port 443.

The final process ran for 35 seconds, received SIGINT, and exited with code 0.
Counts below are printed callback events, including book snapshots/deltas:

| Channel | Events |
| --- | ---: |
| Ticker | 198 |
| Trade | 65 |
| L2Book | 281 |
| Candles | 22 |

No credentials or private services were used. This verifies the final REST
hydration plus public/business WS paths for this spot configuration. It does
not certify every OKX product, all channels, regional domains, or future service
availability. Offline regressions separately verify URL planning and catalog
failure/filtering rules.

## Catalog regression found before the final run

An initial SDK run fetched the directory but panicked when a `state: preopen`
record had empty base/quote fields. A separate public response contained eight
such rows. Non-live OKX spot rows are now skipped, consistent with derivative
catalog handling; any remaining empty spot identity returns MalformedData.
The successful counts above are from the post-fix run only.

A standalone request to the recommended REST domain initially returned HTTP
403; subsequent public requests to both recommended and supported legacy domains
returned code 0 with 1160 directory rows. This transient observation is not a
reason to disable HTTP status validation or silently fall back to another host.

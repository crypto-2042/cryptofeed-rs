# Market metadata smoke — 2026-10-10

The public `metadata_public` example forced fresh Spot and Perpetual catalog
loads for all five active exchanges and queried BTC-USDT / BTC-USDT-PERP.
It printed typed metadata and build-specific WS capabilities, and checked native
identity consistency. No credentials/private service, trading or WebSocket was
used. Catalogs can contain multiple supported native settlement products.

| Exchange / product | Eligible markets | BTC price increment | BTC quantity increment | Minimum quantity | Minimum notional |
| --- | --- | --- | --- | --- | --- |
| Binance Spot | 1,375 | 0.01 | 0.00001 | 0.00001 | 5 |
| Binance Perpetual | 588 | 0.10 | 0.001 | 0.001 | 50 |
| Bitget Spot | 3,389 | None | None | None | 1 |
| Bitget Perpetual | 889 | 0.1 | 0.0001 | 0.0001 | 5 |
| Bybit Spot | 528 | 0.1 | 0.000001 | None | 5 |
| Bybit Perpetual | 881 | 0.10 | 0.001 | 0.001 | 5 |
| OKX Spot | 1,152 | 0.1 | 0.00000001 | 0.00001 | None |
| OKX Perpetual | 500 | 0.1 | 0.01 | 0.01 | None |
| Gate Spot | 2,201 | None | None | 0.000001 | 3 |
| Gate Perpetual | 1,028 | 0.1 | None | 1 | None |

Bitget Spot separately reported price/quantity decimal places 2/6; Gate Spot 1/6.
No increment was invented from those counts. Bybit Spot's deprecated minimum
quantity was intentionally absent. OKX BTC-USDT-SWAP reported face value 0.01 BTC,
multiplier 1 and settlement USDT; Gate BTC_USDT reported native multiplier 0.0001
and USDT settlement from its route. Values and counts describe this observation,
not permanent trading constraints or order-validation guarantees. None means
unreported/inapplicable, not zero.

An earlier run loaded Binance Spot successfully, then returned a request error
after two unsuccessful dapi.binance.com COIN-M directory attempts. A separate
curl probe also failed TLS establishment (SSL_ERROR_SYSCALL, HTTP 000). These
observations did not reach an HTTP status proving endpoint removal and did not
fail metadata assertions. The later aggregate run used unchanged directory
URLs/transport and loaded all ten catalogs successfully, exiting with code 0.
Both outcomes are retained; the first is consistent with transient connectivity,
not proof of an obsolete API.

The final example continues other catalog observations after a catalog request
failure and returns an aggregate failure if any remain. This is a manual public
service check, not a default test, a complete field audit of every returned market
or a live dated-futures/order-permission test. Deterministic fixtures separately
cover exact conversion and malformed/deprecated/conflicting inputs.

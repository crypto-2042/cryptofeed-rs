# Public HTTP frequency audit — 2026-10-12

This audit covers current SDK public spot/perpetual/dated-futures directory,
REST/history and L2 bootstrap requests. Python is a semantic reference, not the
source of current exchange quotas. No private trading limits are imported.

| Venue | Current SDK routes and verified rule | Official source |
| --- | --- | --- |
| Bybit v5 | `/v5/market/{instruments-info,tickers,orderbook,recent-trade,kline,funding/history}`; general public HTTP IP budget is 600 requests/5 seconds | [IP limits](https://bybit-exchange.github.io/docs/v5/rate-limit) |
| Bitget v3 | `/api/v3/market/{instruments,tickers,orderbook,fills,history-candles}`: 20 requests/second/IP | [Market data](https://www.bitget.com/docs/catalog/market/market-data) |
| Bitget v3 | `/api/v3/market/history-fund-rate`: 20 requests/second/IP | [Derivatives](https://www.bitget.com/docs/catalog/market/derivatives) |
| OKX v5 | `/api/v5/public/instruments`: 20/2 seconds, IP + instrument type; `/api/v5/public/funding-rate-history`: 10/2 seconds, IP + instrument ID | [Public data](https://app.okx.com/docs-v5/en/#public-data-rest-api-get-instruments) |
| OKX v5 | `/api/v5/market/ticker`, `history-candles`, `history-trades`: 20/2 seconds; `books`: 40/2 seconds; `trades`: 100/2 seconds; IP | [Market data](https://app.okx.com/docs-v5/en/#order-book-trading-market-data-get-ticker) |
| Gate v4 | `/api/v4/spot/{currency_pairs,tickers,order_book,trades,candlesticks}`, `/api/v4/futures/{usdt,btc}/{contracts,tickers,order_book,trades,candlesticks,funding_rate}`, `/api/v4/delivery/usdt/{contracts,tickers,order_book,trades,candlesticks}`; public endpoint rule: 200 requests/10 seconds/endpoint/IP | [Frequency rule](https://www.gate.com/docs/developers/apiv4/en/#frequency-limit-rule) |

These are request counts, not Binance REQUEST_WEIGHT tiers. The existing SDK
process-wide HTTP start interval is at least one second across **all** exchanges,
transports, resource groups and live HTTP call sites. A stricter ResourcePolicy
can only slow it further. It is below each published count budget above even
when every admitted operation targets one endpoint/instrument. No extra invented
weight ledger is needed to satisfy these verified limits at the current pacing.
Binance retains its separate [endpoint weight accounting](http-weights.md).

This closes the generic “other venue weights” audit for the listed routes and
current pacing; it does not certify server arrival frequency, throughput,
external clients or shared-IP usage. Adding routes or relaxing HTTP pacing must
reopen this audit and implement endpoint-scoped windows where needed. Native
quota errors and Retry-After cooldowns remain active. Bybit's documented ban for
“access too frequent” is not inferred from every HTTP 403, which has other causes.

## Evidence and remaining work

`runtime::snapshot::public_frequency_audit` executes 40 concurrent calls mixing
all four venues and independent local groups under the actual default policy.
It verifies the shared one-second admission spacing and permit release. Existing
snapshot tests cover cooldown/cancellation, scoped pacing, slot bounds and replay
bypass. `rest/{adapter,history,candles,trades,trade_history}.rs` and `markets.rs`
provide the route inventory; no request, parser, model or fixture changed.

There was no live traffic or saturation test. Binance minute used-weight feedback and [per-session WS send windows](ws-send-budgets.md)
are implemented. Cross-process/machine quota coordination
is explicitly deferred by the maintainer; it is not a current Python-parity gate.

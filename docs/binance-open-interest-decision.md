# Binance contract open interest — deferral decision

Decision date and official-document verification: 2026-10-08.
Status: deferred by explicit user decision; no implementation scheduled.
Scope: Binance USD-M and COIN-M perpetual and dated-futures OI.

## Decision

Do not implement Binance contract OI for now, including an HTTP polling
fallback. Keep `Channel::OpenInterest` absent from Binance's runtime capability
matrix so subscription requests fail preflight. Do not subscribe to guessed
`<symbol>@openInterest` or `!openInterest` topics, silently start polling, or
relabel generated HTTP observations as native WebSocket events.

This is a product/operational decision, not a claim that OI collection is
technically impossible. REST polling can deliver normalized OI, but the
project deliberately does not enable it at this stage.

## Reason

The SDK serves multi-symbol feeds. The documented current-OI endpoints require
one contract symbol per request and each request consumes weight 1. Polling
cost therefore grows with both symbol count and sampling frequency:

`request weight per minute = unique symbols × 60 / polling interval seconds`

For example, 20 contracts sampled every 5 seconds consume approximately 240
weight/minute; 200 contracts consume approximately 2400 weight/minute, before
retries or other REST requests. These are workload estimates, not fixed
Binance quota values. Actual limits must be read from current exchange/API
rate-limit information and are shared with other requests using the same IP.

Many symbols, concurrent feeds, bursts, and retries can exhaust that budget
and trigger rate limiting. Avoiding that operational exposure is the user's
reason for deferring OI. A safe poller would require shared request-budget
management, cross-feed deduplication, bounded concurrency, rate-limit backoff,
and an explicitly accepted sampling policy. Those changes are not authorized
by this decision.

## Official API state

| Surface | Verified state on 2026-10-08 | Project treatment |
| --- | --- | --- |
| USD-M public WebSocket market streams | No documented perpetual/dated-futures OI subscription found | Unsupported |
| COIN-M public WebSocket market streams | No documented perpetual/dated-futures OI subscription found | Unsupported |
| USD-M WebSocket request/response API | No documented OI query method found; this API is distinct from market-stream subscriptions | No WS fallback |
| USD-M current OI REST | `GET https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT`; required single symbol; weight 1; response includes `openInterest`, `symbol`, and `time` | Available upstream, deliberately not implemented |
| COIN-M current OI REST | `GET https://dapi.binance.com/dapi/v1/openInterest?symbol=BTCUSD_PERP`; required single symbol; weight 1; product/contract identity must be retained | Available upstream, deliberately not implemented |
| Historical OI REST | `/futures/data/openInterestHist` on the matching product REST service; minimum period `5m` | Historical statistics are not a substitute for second-scale current snapshots; no integration scheduled |
| Options OI WebSocket | `{underlying}@openInterest@{expirationDate}`, documented update speed 60s | Options-only; does not establish perpetual/dated-futures OI support; options remain outside 0.1 runtime scope |

The conclusion is that there is no currently verified official native contract
OI WS path, not that an undocumented endpoint can never exist or that Binance
can never add one. Earlier short live probes returned no OI messages; that
observation alone is not proof of permanent absence.

Official references:

- [USD-M market streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)
- [COIN-M market streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/ws-streams/~)
- [USD-M WebSocket API market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-api/market-data)
- [USD-M current and historical OI REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#open-interest)
- [COIN-M current and historical OI REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data#open-interest)
- [Options OI WebSocket](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/market#open-interest)

## GitHub cross-check

Repository source was inspected on 2026-10-08; these are implementation
references, not exchange protocol guarantees.

- [Official Binance Python SDK market-stream implementation](https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/websocket_streams/streams/market_api.py)
  does not expose a contract OI market-stream method.
- [Python cryptofeed BinanceBase](https://github.com/bmoscon/cryptofeed/blob/master/cryptofeed/exchanges/binance.py)
  collects OI via `_connect_rest()` / `HTTPPoll`; `_stream_names()` skips OI.
  Its OI capability does not demonstrate a native Binance OI WebSocket.
- [CCXT Binance](https://github.com/ccxt/ccxt/blob/master/ts/src/binance.ts)
  implements `fetchOpenInterest()` with the product-specific REST endpoints;
  no Binance `watchOpenInterest` implementation was found in the inspected
  [Pro adapter](https://github.com/ccxt/ccxt/blob/master/ts/src/pro/binance.ts).
- [Tardis Binance real-time feed](https://github.com/tardis-dev/tardis-node/blob/master/src/realtimefeeds/binance.ts)
  excludes OI from WS subscriptions and defaults to 5-second REST polling.
  It wraps observations in `stream: "<symbol>@openInterest"` with
  `generated: true`; this synthetic stream name must not be treated as a
  native exchange topic.

## Reconsideration

Reopen only through an explicit user-approved implementation plan. That plan
must establish either a documented and verified official contract OI stream,
or an accepted REST sampling/rate-budget design validated at the intended
multi-symbol scale. No automatic polling, implementation, or monitoring is
scheduled by this record.

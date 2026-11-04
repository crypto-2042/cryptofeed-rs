# Hyperliquid public perpetual and spot protocol baseline — 2026-11-04

Status: **implemented.** Public, unauthenticated market data only. This
baseline covers the Hyperliquid public WebSocket (`wss://api.hyperliquid.xyz/ws`)
and the public info endpoint (`POST https://api.hyperliquid.xyz/info`) for
perpetuals and spot. User/private channels, trading, vaults, HIP-3 perp dexs,
outcome markets and EVM JSON-RPC are out of scope. No new normalized category
is introduced.

The official documentation has no explicit numeric API version. The targeted
shape is the current GitBook documentation as read on 2026-11-04 (the
`subscriptions`, `timeouts-and-heartbeats`, `info-endpoint` and
`rate-limits-and-user-limits` pages) plus the read-only live verification
recorded below. The WebSocket and info shapes are additive; the runtime fails
closed on frames it cannot map.

## Endpoints

| Purpose | Endpoint |
| --- | --- |
| Public WebSocket (mainnet) | `wss://api.hyperliquid.xyz/ws` |
| Public WebSocket (testnet) | `wss://api.hyperliquid-testnet.xyz/ws` |
| Public info REST | `POST https://api.hyperliquid.xyz/info` (JSON body) |

Only the mainnet endpoints are wired into the runtime; the testnet host is
recorded for reference. The info endpoint is a `POST` with a JSON request body
(`{"type": ...}`); there is no GET catalog form.

## Catalog

### Perpetuals — `{"type":"meta"}`

Live response shape (2026-11-04, sanitized):

```json
{
  "universe": [
    {"szDecimals": 5, "name": "BTC", "maxLeverage": 40, "marginTableId": 56},
    {"szDecimals": 1, "name": "MATIC", "maxLeverage": 20, "marginTableId": 20,
     "isDelisted": true},
    {"szDecimals": 0, "name": "kPEPE", "maxLeverage": 10, "marginTableId": 7}
  ],
  "marginTables": [...],
  "collateralToken": 0
}
```

- The WebSocket `coin` is `universe[].name` (e.g. `BTC`, `kPEPE`).
- `isDelisted: true` rows are removed pairs and are **skipped**; a
  non-boolean `isDelisted` fails the catalog closed.
- Perpetuals are quoted in **USD** in this SDK (`BTC-USD-PERP`); the venue
  collateral is USDC but the contract quote unit is USD, and no USDC quote is
  invented.
- `szDecimals` is exposed as `MarketInfo.quantity_decimal_places`;
  `maxLeverage`/`marginTableId` have no normalized field and are not
  substituted into one.
- Live probe on 2026-11-04: 235 universe rows, 56 with `isDelisted: true`
  (`MATIC`, `RNDR`, `FTM`, `TON`, ...). The listed names include mixed-case
  `kPEPE`, `kSHIB`, `kBONK`, `kLUNC`, `kFLOKI`, `kNEIRO`; see coin identity.

### Spot — `{"type":"spotMeta"}`

Live response shape (2026-11-04, sanitized):

```json
{
  "tokens": [
    {"name": "USDC", "szDecimals": 8, "weiDecimals": 8, "index": 0,
     "tokenId": "0x...", "isCanonical": true, "evmContract": {...}, "fullName": null},
    {"name": "PURR", "szDecimals": 0, "weiDecimals": 5, "index": 1, ...},
    {"name": "HYPE", "szDecimals": 2, "weiDecimals": 8, "index": 150, ...},
    {"name": "UBTC", "szDecimals": 8, "weiDecimals": 8, "index": 234, ...}
  ],
  "universe": [
    {"tokens": [1, 0], "name": "PURR/USDC", "index": 0, "isCanonical": true},
    {"tokens": [150, 0], "name": "@107", "index": 107, "isCanonical": false},
    {"tokens": [234, 235], "name": "@234", "index": 234, "isCanonical": false}
  ]
}
```

Spot coin identity and normalization rules:

- The WebSocket `coin` is `universe[].name`: `PURR/USDC` for the canonical
  pair and `@{index}` (e.g. `@107`) for every other pair.
- The normalized symbol is built from the **token names** referenced by
  `universe[].tokens: [baseIndex, quoteIndex]`, e.g. `@107` -> `HYPE-USDC`,
  `@234` -> `UBTC-USDH`. The UI name is never used to guess identity (the
  official docs note that `BTC/USDC` on the app can be `UBTC/USDC` on
  HyperCore).
- Arbitrary quote tokens are preserved exactly. Live 2026-11-04: the universe
  contained 332 USDC pairs and 17 non-USDC pairs with quotes `USDT0`, `USDH`
  and `USDE` (e.g. `@234` = `UBTC/USDH`, `@244` = `USDXL/USDT0`).
- Removed spot pairs are absent from `universe` (indices have gaps: the live
  response had 349 universe rows while `@` names reached `@1187`). A
  documented `isDelisted: true` flag would be skipped the same way as perps.
- Consistency checks that fail the catalog closed:
  - every `tokens` index referenced by a pair must exist in `tokens[]`;
  - a non-`@` (canonical) `name` must equal `{base}/{quote}` from the token
    names;
  - an `@{index}` name must match the row's `index` field;
  - duplicate token indices and duplicate normalized identities are rejected
    by the registry;
  - token names containing `-`/`/` are rejected because they would make the
    normalized symbol ambiguous.

### Coin identity is case-sensitive on the wire

`universe[].name` is transmitted verbatim and subscription matching is
case-sensitive. Verified read-only on 2026-11-04:

- `{"type":"l2Book","coin":"kPEPE"}` -> `subscriptionResponse` then `l2Book`.
- `{"type":"l2Book","coin":"KPEPE"}` -> the server closed the connection
  without a close frame (no `error` channel reply was sent).

Therefore the catalog and `ExchangeFeed::exchange_symbols` preserve the native
spelling for Hyperliquid instead of upper-casing it, and the adapter never
re-cases a coin. `Symbol` itself upper-cases its normalized components, so
`kPEPE` is `KPEPE-USD-PERP` as a normalized symbol while the wire coin stays
`kPEPE`.

## WebSocket subscription framing

Subscribe/unsubscribe requests:

```json
{"method": "subscribe", "subscription": {"type": "trades", "coin": "BTC"}}
{"method": "unsubscribe", "subscription": {"type": "trades", "coin": "BTC"}}
```

Subscription objects generated by the runtime:

| Normalized channel | Subscription object |
| --- | --- |
| `Ticker`, `L1Book` | `{"type":"bbo","coin":"<coin>"}` |
| `Trade` | `{"type":"trades","coin":"<coin>"}` |
| `L2Book` | `{"type":"l2Book","coin":"<coin>"}` (plus `"fast":true` for the 5-level depth) |
| `Candles` | `{"type":"candle","coin":"<coin>","interval":"<1m..1M>"}` |
| `Funding`, `OpenInterest`, `Index`, `MarkPrice` (perpetual) | one shared `{"type":"activeAssetCtx","coin":"<coin>"}` |

One request is sent per channel/coin pair (the runtime paces them through the
shared send budget); `Ticker` and `L1Book` share one `bbo` request and the four
perpetual context channels share one `activeAssetCtx` request. The documented
optional `nSigFigs` and `mantissa` parameters are not used (they aggregate
price levels and would change the book semantics); only the documented
`fast` flag is used, and only when the caller requests depth 5.

### Acknowledgement and error channels

Live-verified 2026-11-04:

```json
{"channel":"subscriptionResponse","data":{"method":"subscribe",
 "subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}}}
```

- The ack echoes the original subscription and the server normalizes optional
  fields to `null`/`false`. Readiness correlates on `(type, coin, interval)`.
- Request/parse failures arrive on the `error` channel:
  `{"channel":"error","data":"Error parsing JSON into valid websocket request: ..."}`
  (live, produced by an unsupported subscription type). The runtime classifies
  every `error` channel frame as a permanent `Subscription` failure and does
  not retain the echoed request text.
- The documented heartbeat reply is `{"channel":"pong"}`.
- An invalid **coin** is not reported on the `error` channel in the observed
  build: the connection is closed instead (see open questions).

## Liveness and heartbeat

Official page
[timeouts-and-heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats):

> The server will close any connection if it hasn't sent a message to it in the
> last 60 seconds. If you are subscribing to a channel that doesn't receive
> messages every 60 seconds, you can send heartbeat messages to keep your
> connection alive. The format for these messages are: `{ "method": "ping" }`
> The server will respond with: `{ "channel": "pong" }`

Evidence (read-only, 2026-11-04):

- Live probe: sent `{"method":"ping"}` on an unauthenticated session and
  received `{"channel":"pong"}`.
- No unsolicited text heartbeat frames were observed during the probe; the
  runtime therefore sends the documented client ping proactively every 30
  seconds (half the documented 60-second close window) and treats
  `{"channel":"pong"}` as liveness, not market data. The shared session also
  answers WebSocket protocol-level ping frames with pong automatically.
- The session idle watchdog is 60 seconds, matching the documented close
  window.

## Channel-to-normalized mapping

| Hyperliquid channel | Data shape (live) | Normalized output |
| --- | --- | --- |
| `l2Book` | `{coin, time(ms), levels:[[WsLevel...],[WsLevel...]]}` | `L2Book` **snapshot only** |
| `trades` | `WsTrade[]` array | one `Trade` per row |
| `bbo` | `{coin, time(ms), bbo:[WsLevel|null, WsLevel|null]}` | `Ticker` + `L1Book`, only when both sides are present |
| `candle` | `{t,T,s,i,o,c,h,l,v,n}` object (docs say `Candle[]`) | `Candle` |
| `activeAssetCtx` (perpetual) | `{coin, ctx:{funding, openInterest, oraclePx, markPx, ...}}` | `Funding` + `OpenInterest` + `MarkPrice` + `IndexPrice` |
| `activeSpotAssetCtx` (spot) | `{coin, ctx:{markPx, midPx, circulatingSupply, ...}}` | `MarkPrice` only |
| `subscriptionResponse` | ack | control (readiness) |
| `error` | request failure text | permanent `Subscription` error |
| `pong` | heartbeat reply | liveness |

`WsLevel` is `{px: string, sz: string, n: number}`. `n` (order count) has no
normalized field and is not published.

### `l2Book` is snapshot-only

- `WsBook.levels` is `[bids, asks]`, best-first, with at most **20 levels per
  side by default** and **5 levels per side when the subscription sets
  `fast:true`** (documented). The runtime accepts `l2_book_depth(20)` (the
  default) and `l2_book_depth(5)` (which adds `fast:true`); every other
  requested depth fails preflight before connecting.
- The push is a **complete replacement snapshot** pushed on each block that is
  at least 0.5 s after the previous push. There is **no sequence number,
  checksum or delta form**. The runtime applies each push as an authoritative
  replacement, never as a delta, and does not invent a dedupe/staleness
  anchor. Reconnect recovery is a fresh snapshot on the new connection.
- `time` is milliseconds since epoch; it is the exchange timestamp. Empty sides
  are valid and published as empty.
- `nSigFigs`/`mantissa` are not requested; they are documented aggregation
  parameters and would not be a raw book.

### `trades`

- `side` is `"B"` or `"A"`; mapped to normalized `Buy`/`Sell` (B = buy,
  A = sell).
- `px`/`sz` are decimal strings; `time` is milliseconds.
- `tid` is a 50-bit hash of `(buyer_oid, seller_oid)` and is **not globally
  unique**; it is preserved verbatim as `Trade.id` and is not combined with
  block time or coin into an invented identifier.
- Every row in the documented array is processed; a malformed row for a
  configured coin fails the frame closed.

### `bbo`

- `bbo[0]` is the best bid and `bbo[1]` the best ask, each `WsLevel` **or
  `null`**. The runtime emits `Ticker` and `L1Book` only when **both** sides
  are present; a documented null side produces no quote at all and never a
  fabricated zero/one-sided quote.
- `Ticker` uses the best bid/ask prices; `L1Book` uses the documented sizes.

### `candle` intervals

Documented supported intervals: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`,
`4h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`. Hyperliquid spells them exactly
like the normalized vocabulary, so the wire interval is the normalized
interval; `6h`/`1s` are not served and fail preflight. A `candle` payload whose
documented `i` contradicts the subscribed interval fails the frame closed.
`t`/`T` are open/close milliseconds; the normalized `end` is derived as
`start + interval` (the venue transmits `T` as the last millisecond of the
bar). `o/c/h/l/v` are decimal strings, `n` is a number. No closed flag is
transmitted, so `Candle.closed` stays `None`.

Documentation/live discrepancy: the subscription page documents the `candle`
data format as `Candle[]`, while the live push verified on 2026-11-04 is a
single object. The parser accepts both shapes defensively and records the
discrepancy here.

### `activeAssetCtx`

Perpetual context (live 2026-11-04):

```json
{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{
  "funding":"0.0000125","openInterest":"39370.73328",
  "prevDayPx":"76611.0","dayNtlVlm":"3444111352.420372963",
  "premium":"-0.0004510106","oraclePx":"78490.4","markPx":"78464.8",
  "midPx":"78454.5","impactPxs":["78454.0","78455.0"],
  "dayBaseVlm":"44617.00231"}}}
```

- Split into `Funding` (`funding`, with `markPx` as `Funding.mark_price`),
  `OpenInterest` (`openInterest`), `MarkPrice` (`markPx`) and `IndexPrice`
  (`oraclePx`). The four values are documented as required for a perpetual
  context; a missing/unparsable one fails the frame closed.
- The payload carries **no exchange timestamp**; `exchange_ts` is the receive
  timestamp for all four normalized events.
- `next_funding_time` and `predicted_rate` are not transmitted by this channel
  and are not invented.

Spot context (live 2026-11-04) arrives on the distinct channel
`activeSpotAssetCtx` (the subscription type is still `activeAssetCtx`):

```json
{"channel":"activeSpotAssetCtx","data":{"coin":"PURR/USDC","ctx":{
  "prevDayPx":"0.11235","dayNtlVlm":"1869165.0426500002","markPx":"0.11552",
  "midPx":"0.115385","circulatingSupply":"594666965.8429700136",
  "coin":"PURR/USDC","totalSupply":"594666977.1494899988",
  "dayBaseVlm":"16536048.0"}}}
```

Only `MarkPrice` (`markPx`) is published; there is no spot funding, open
interest or oracle/index price in this payload. The docs also list
`activeAssetCtx` as a spot data format, so a spot feed accepts both channel
names; a perpetual feed accepts only `activeAssetCtx`.

## Capability rows

| Exchange | Product | Ticker | Trade | L2Book | L1Book | Candles | Funding | OpenInterest | Index | MarkPrice | Liquidations |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Hyperliquid | Spot | yes | yes | yes (20/5 snapshot) | yes | yes | — | — | — | yes | — |
| Hyperliquid | Perpetual | yes | yes | yes (20/5 snapshot) | yes | yes | yes | yes | yes | yes | — |

`Liquidations` is deliberately **not** advertised: public liquidation data
exists only inside private user event streams (`userEvents`), and this project
does not implement authenticated/user channels. Dated futures, options,
margin and HIP-3 perp-dex products are out of scope and fail preflight.

## Rate limits, connection and subscription caps

Official page
[rate-limits-and-user-limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits),
per IP address:

- REST: aggregated **1200 weight per minute**. `l2Book`, `allMids`,
  `clearinghouseState`, `orderStatus`, `spotClearinghouseState` and
  `exchangeStatus` cost 2; `userRole` costs 60; **all other documented info
  requests cost 20**, so `meta` and `spotMeta` cost 20 each.
- WebSocket: **10 connections** per IP; **30 new connections per minute**;
  **1000 subscriptions**; **10 unique users** across user-specific
  subscriptions; **2000 messages sent to Hyperliquid per minute** across all
  WebSocket connections; 100 simultaneous inflight POST messages.
- EVM JSON-RPC (`rpc.hyperliquid.xyz/evm`) is a separate 100 requests/minute
  limit and is out of scope.

Runtime behavior:

- The catalog is two info POSTs per refresh (`meta` and `spotMeta`, 20 weight
  each), served from the shared 24 h catalog cache when not refreshing.
- Planning budgets **1000 subscriptions per physical connection**; oversized
  feeds are sliced by symbol into additional connections.
- The runtime connection budget is tightened to **10 connections** with a
  **2 s start interval** for Hyperliquid (the documented 30/minute), instead
  of the shared 100-connection SDK ceiling.
- The shared session sends at most four text frames per second (250 ms
  subscription pacing), well inside the documented 2000 messages/minute, and
  sends the client ping every 30 s.
- HTTP catalog requests share the process-wide public-HTTP admission and
  retry/backoff policy; native `429` handling uses the shared HTTP status
  path (Hyperliquid documents the weight limit but no per-response quota
  header/code that this runtime recognizes).

## Verification log (read-only, 2026-11-04)

All probes were unauthenticated, read-only and against the public mainnet
hosts; no user data, credentials or trading calls were involved. Values below
are sanitized or shortened where they are only illustrative.

| Probe | Result |
| --- | --- |
| `POST /info {"type":"meta"}` | 235 universe rows, `isDelisted` present on 56; `szDecimals`/`maxLeverage`/`marginTableId` per row |
| `POST /info {"type":"spotMeta"}` | 511 tokens, 349 universe rows; `PURR/USDC` canonical, `@N` otherwise; 17 non-USDC quote pairs (`USDT0`, `USDH`, `USDE`); index gaps up to `@1187` |
| WS `{"method":"ping"}` | `{"channel":"pong"}` |
| WS subscribe `l2Book` BTC | `subscriptionResponse` echo, then `l2Book` snapshots with 20 levels per side and ms `time` |
| WS subscribe `l2Book` PURR/USDC | ack + whole-book snapshots for the spot coin |
| WS subscribe `trades` BTC | `WsTrade[]` with `side` `A`/`B`, string `px`/`sz`, ms `time`, numeric `tid`, `hash`, `users` |
| WS subscribe `bbo` BTC / PURR/USDC | `{coin,time,bbo:[WsLevel,WsLevel]}`; both sides present in the sample |
| WS subscribe `candle` BTC 1m | single object `{t,T,s,i,o,c,h,l,v,n}` (docs say `Candle[]`) |
| WS subscribe `activeAssetCtx` BTC | `activeAssetCtx` channel; `funding`/`openInterest`/`oraclePx`/`markPx` are strings |
| WS subscribe `activeAssetCtx` PURR/USDC | `activeSpotAssetCtx` channel; `ctx.markPx` present |
| WS unsupported subscription type | `{"channel":"error","data":"Error parsing JSON into valid websocket request: ..."}` |
| WS `l2Book` coin `KPEPE` vs `kPEPE` | `KPEPE` closed the connection without a close frame; `kPEPE` acknowledged and streamed |

## Open questions

- **Invalid coin behavior.** The observed build closed the WebSocket for an
  unknown/wrong-case coin instead of sending an `error` channel frame. The
  runtime relies on catalog-driven symbols and preflight validation; a catalog
  race that produces a rejected coin surfaces as a retryable close and the
  supervisor reconnects. Re-verify if the venue later reports this on `error`.
- **Spot `activeAssetCtx` channel name.** The docs list both
  `WsActiveAssetCtx`/`WsActiveSpotAssetCtx` as data formats while the live spot
  push uses `activeSpotAssetCtx`; both names are accepted for spot feeds.
- **Candle array vs object.** The docs say `Candle[]`, the live push is one
  object; both are accepted. If the venue moves to arrays permanently the
  object branch can be dropped.
- **No closed flag.** Candles carry `T` (close millis) but no final/closed
  flag; the runtime does not infer completion from `T`.
- **Book `time` reuse.** No sequence/checksum exists and `time` is not
  documented as strictly increasing; the runtime does not dedupe snapshots by
  `time`. Two identical consecutive pushes would both be delivered.
- **HIP-3 perp dexs / outcome markets.** The current catalog only targets the
  first perp dex (`meta` default) and standard spot; additional dexs are not
  requested.
- **`nSigFigs`/`mantissa`.** Documented book aggregation parameters are not
  supported because they would publish an aggregated book as if it were the
  raw book.

## References

- [WebSocket overview](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)
- [Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [Timeouts and heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)
- [Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
- [Rate limits and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)
- Implementation plan: [hyperliquid-plan-2026-11-04.md](hyperliquid-plan-2026-11-04.md)
- Scope note: [hyperliquid-research-2026-11-04.md](hyperliquid-research-2026-11-04.md)

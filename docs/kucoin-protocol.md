# KuCoin UTA WebSocket V2 protocol baseline

Status: **protocol reference for the phase-1 addition, implemented.** Facts
below were read from the official KuCoin documentation on 2026-11-03 and the
open questions were resolved against the public push hosts on the same day; the
verification section at the end records the observed shapes. Fixtures in the
test suite are sanitized documented examples plus sanitized live-shape
references (see `sample_data/SOURCES.md`), not recordings.

## Endpoints and handshake

- Public market data push, spot: `wss://x-push-spot.kucoin.com`
- Public market data push, futures: `wss://x-push-futures.kucoin.com`
- The public push hosts are documented for public data without a token; the
  Classic `/api/v1/bullet-public` token flow is a separate legacy path and is
  not required by UTA V2.
- Subscribe/unsubscribe:
  `{"id": "<client>", "action": "SUBSCRIBE" | "UNSUBSCRIBE", "channel":
  "<name>", "tradeType": "SPOT" | "FUTURES", "symbol": "<native>",
  "symbols": ["..."]}` plus channel-specific parameters (`interval`, `depth`).
- Every push carries `T` (topic with product suffix, e.g. `ticker.SPOT`), `P`
  (publish time, nanoseconds since epoch) and `d` (payload). `d.s` is the native
  symbol.

## Public channels and normalized mapping

| Channel | Products | Normalized category | Payload fields used |
| --- | --- | --- | --- |
| `ticker` | SPOT, FUTURES | `Ticker`; `L1Book` when both sides are present | `a`/`A` ask price/size, `b`/`B` bid price/size, `l` last price, `q` last size, `S` side, `M` matching-engine time (ns), `s` |
| `trade` | SPOT, FUTURES | `Trade` | `p` price, `q` quantity, `S` side (`buy`/`sell`), `ti` trade id, `M` matching-engine time (ns), `E` exchange sequence, `s` |
| `obu` | SPOT, FUTURES | `L2Book` | `t` `snapshot`/`delta`, `dp` `1`/`5`/`50`/`increment`/`increment@10ms`, `b`/`a` arrays of `[price, absolute size]`, `O` first sequence, `C` last sequence, `M` time (ns), `s` |
| `kline` | SPOT, FUTURES | `Candle` | `i` interval label, `O` open time (s), `C` close time (s), `o`/`h`/`l`/`c`, `v` base volume, `a` quote volume, `S` closed flag, `s` |
| `mark-price` | FUTURES | `MarkPrice`, `IndexPrice`, `OpenInterest` | `mp` mark price, `ip` index price, `oi` open interest, `ts` (ms), `s` |
| `funding-fee` | FUTURES | `Funding` | `fr` rate, `ft` funding (settlement) time (ms), `nt` next funding time (ms), `lfr` last rate, `gl` granularity, `fc`/`ff` caps, `s` |

Notes:

- `ticker` only pushes on trades; BBO-only changes do not trigger it. The L1
  projection is therefore only as fresh as the last trade-driven update, and the
  `obu` `depth=1` stream is the precise L1 source. Implementation must choose
  one and document the choice; the ticker projection must not be advertised as a
  standalone L1 subscription.
- `obu` `depth` of `1`, `5` or `50` is snapshot-only per push (`t: "snapshot"`),
  so no local book is required. `increment@10ms` first pushes a snapshot and
  then deltas. `O`/`C` are the sequence range of the push: a delta whose `O` is
  greater than the local anchor + 1 is a gap and must trigger the existing
  bounded resnapshot path; a delta whose `C` is not greater than the anchor is
  stale and dropped.
- `mark-price` carries three normalized categories in one payload. The runtime
  must emit them as independent normalized events under the same subscription,
  deduplicating the native topic exactly once.
- `kline` announces `6hour` as unavailable for futures. Interval labels are
  `1min`..`1month`; the SDK's interval mapping must reject `6hour` on futures
  during preflight rather than at subscription time.
- No public liquidation stream is documented; liquidation warnings are a private
  channel. `Liquidations` stays unsupported for KuCoin.

## Catalog and symbol identity

- UTA REST V2 base: `https://api.kucoin.com`, market data endpoints under
  `/api/ua/v2/market/...`:
  - `GET /api/ua/v2/market/instrument?tradeType=SPOT|FUTURES[&symbol=...]`
  - `GET /api/ua/v2/market/ticker?tradeType=...`
- Futures native symbols use a contract suffix (`XBTUSDTM`). The catalog mapping
  must not guess: `instrument` supplies base/quote/contract metadata, and the
  same alias table as other venues (`XBT` → `BTC`) applies before product
  qualification.
- Spot native symbols are `BASE-QUOTE` (`BTC-USDT`); futures contracts must
  normalize to `perpetual` or `futures` with an explicit expiry where the
  catalog provides one.

## Heartbeat and lifecycle

- The public push protocol documents application-level liveness through the
  documented ping/ack mechanism of the endpoint, not a market channel. The
  existing session supervisor needs an explicit heartbeat policy for KuCoin
  before live use; treat an unanswered heartbeat as an idle reconnect, not as a
  protocol error.
- Server-side load balancing may close public connections one or two times per
  week; normal reconnect/backoff covers it and must not be classified as a
  permanent failure.

## Open questions to resolve during implementation

All five questions were answered during implementation; see
[Implementation verification](#implementation-verification--2026-11-03).

1. Does the public push host require an initial handshake frame, or are channel
   frames accepted directly after the WebSocket upgrade?
2. Exact acknowledgement shape for `SUBSCRIBE` (success vs error) and whether
   one frame acknowledges several symbols.
3. Per-connection subscription/symbol limits for the public push hosts.
4. Whether `obu` `increment` (without `@10ms`) still starts with a snapshot or
   requires a REST bootstrap; the documentation marks the non-`@10ms` mode as
   deprecated in July 2026, so implementation should target `increment@10ms`
   and the snapshot-only depths.
5. `mark-price` push cadence is one second; confirm whether `ts` is always
   present and whether `oi` may be absent for some contracts.

## References

- [UTA WebSocket V2 introduction](https://www.kucoin.com/docs-new/websocket-api/introduction)
- [Ticker](https://www.kucoin.com/docs-new/3470355w0), [Trade](https://www.kucoin.com/docs-new/3470359w0),
  [Orderbook](https://www.kucoin.com/docs-new/3470354w0), [Kline](https://www.kucoin.com/docs-new/3470356w0),
  [Mark Price](https://www.kucoin.com/docs-new/3470358w0), [Funding Fee Rate](https://www.kucoin.com/docs-new/3470357w0)
- [UTA REST V2 market data](https://www.kucoin.com/docs-new/v2/rest/ua/instrument)

## Implementation verification — 2026-11-03

The public push hosts were probed without a token on 2026-11-03. Observations
are sanitized wire-shape references, not retained captures.

### Handshake and heartbeat

- The server sends a welcome frame immediately after the upgrade:
  `{"sessionId":"...","message":"welcome","pingInterval":18000}`. No token or
  authentication frame is required for public channels; `SUBSCRIBE` frames are
  accepted directly. The welcome frame is ignored by the runtime.
- Liveness is the documented JSON exchange, not a market channel: the client
  sends `{"id":"...","op":"ping"}` and receives `{"id":"...","op":"pong",
  "timestamp":...}`. The `pingInterval` recommendation is 18 000 ms; the SDK
  heartbeat policy uses an 18 s interval and a 45 s idle watchdog. An
  unanswered heartbeat is an idle reconnect, not a protocol error.
- The session also tolerates the server closing public connections during load
  balancing; the normal reconnect/backoff supervisor covers it.

### Subscription acknowledgement

- Request: `{"id":"<client>","action":"SUBSCRIBE","channel":"<name>",
  "tradeType":"SPOT"|"FUTURES","symbols":["<native>", ...]}` plus the
  channel-specific `depth`/`interval` parameter.
- Success: `{"id":"<client>","result":true}`. Rejection:
  `{"id":"<client>","result":false,"message":"invalid request data"}`; a
  rejected subscription is a permanent configuration error and fails fast
  instead of burning reconnect attempts.
- One acknowledgement covers one request frame, including a frame that carries
  several `symbols`. `UNSUBSCRIBE` uses the same envelope and is acknowledged
  the same way.
- Per-connection subscription limits are still not documented. The SDK uses a
  conservative 64 KiB generated-subscription budget and shards larger feeds;
  this is a local budget, not a claimed exchange limit.

### Order book (`obu`)

- `t` and `dp` are top-level frame fields, not payload fields; the payload is
  `d` with `s`, `M`, `b`, `a` and the sequence range.
- `depth=increment@10ms` first pushes `t: "snapshot"` with `O == C`, then
  `t: "delta"` pushes whose ranges are contiguous (`O == previous C + 1`).
  A gap triggers the bounded in-session resnapshot path (unsubscribe/subscribe
  for that symbol, at most three cumulative requests per connection) and then
  the existing reconnect supervisor. Deltas with `C <= anchor` are stale and
  dropped; buffered deltas bridge the replacement snapshot only when their
  range continues it.
- `depth=1/5/50` pushes are snapshot-only (`t: "snapshot"` on every push,
  `O == C`); they replace the local book and carry no incremental state. A
  delta on such a subscription is a protocol violation and fails closed.
- `dp=increment` (without `@10ms`) remains unimplemented as documented
  deprecated in July 2026.

### Channels and timestamps

- `ticker`/`trade`/`kline`/`obu` topics carry a product suffix (`ticker.SPOT`,
  `trade.FUTURES`, ...). `mark-price` and `funding-fee` topics carry no suffix;
  both are futures-only.
- `M` is the matching-engine time in nanoseconds and `P` the publish time in
  nanoseconds; both are converted to seconds. `mark-price` uses `ts`
  (milliseconds) with `P` as fallback.
- One `mark-price` payload is normalized into independent `MarkPrice`,
  `IndexPrice` and `OpenInterest` events; the native `mark-price` subscription
  is emitted once even when all three categories are requested.
- `funding-fee` `ft` is the current settlement time and becomes
  `next_funding_time`; `nt` is one period later and is not substituted.
- `kline` `S` is the documented closed flag. The endpoint accepts `6hour`
  subscriptions for futures without an immediate error, but the channel
  documentation marks it unavailable there; capability preflight rejects
  `6h` for futures and the runtime never relies on an empty subscription.

### Catalog

- `GET /api/ua/v2/market/instrument?tradeType=SPOT` returns
  `{"code":"200000","data":{"tradeType":"SPOT","list":[...]}}`; rows carry
  `symbol`, `baseCurrency`, `quoteCurrency`, `tradingStatus` and order-size
  fields. Only `TradingEnabled` rows are eligible.
- `GET /api/ua/v2/market/instrument?tradeType=FUTURES` rows add
  `contractType` (`"0"` perpetual, `"1"` dated), `expiryTime` (milliseconds for
  dated), `settlementCurrency`, `isInverse`, `marketStage` and `tickSize`.
  Only `tradingStatus: "Open"` and `marketStage: "NORMAL"` rows are eligible;
  pre-market stages are skipped instead of guessed.
- Perpetual native names are `{BASE}{QUOTE}M` (`XBTUSDTM`, `XBTUSDM`), and the
  `XBT` -> `BTC` alias is applied before product qualification. Dated contracts
  (`XBTMZ26`) normalize from the catalog expiry.

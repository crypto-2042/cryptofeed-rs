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

## Quantity units and contract metadata

Units are per field and per product, verified from the official pages listed in
[References](#references) on 2026-11-03. The parser never converts a native
quantity; it keeps each field in the unit the channel documents.

| Feed field | Spot | Futures |
| --- | --- | --- |
| `trade` `q` → `Trade.amount` | base-asset quantity | **contract count** (`UNIT`, lots) |
| `obu` level size → `L2Book` level amount | base-asset quantity | **contract count** (`UNIT`, lots) |
| `mark-price` `oi` → `OpenInterest.open_interest` | n/a | **contract count** |

Evidence:

- [UTA REST V2 Place Order](https://www.kucoin.com/docs-new/v2/rest/ua/place-order)
  documents FUTURES `size` as using `UNIT` (contracts) and the `sizeUnit` enum
  `UNIT` as "Contracts (only for Futures)". The minimum size refers to
  `unitSize` when `sizeUnit` is `BASECCY` and to `lotSize` when it is `UNIT`,
  tying one contract to the documented `unitSize` face value.
- [Classic Futures Add Order](https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order)
  states `size` is "Specified in lots and must be a positive integer" and that
  coin-swap contracts support only `size` (lot); `qty` (base currency) and
  `valueQty` (quote currency) are separate optional units there. UTA V2
  futures `size` remains `UNIT`.
- [UTA WebSocket V2 Trade](https://www.kucoin.com/docs-new/3470359w0): the spot
  example is fractional (`q: "0.00008036"` for BTC-USDT) while the futures
  example is an integer contract count (`q: "12"` for XBTUSDTM).
- [UTA WebSocket V2 Orderbook](https://www.kucoin.com/docs-new/3470354w0):
  spot level sizes are fractional base quantities; futures XBTUSDTM level
  sizes are integers (`"2236"`, `"301"`, `"891"`, `"1645"`).
- [UTA REST V2 Get Trades](https://www.kucoin.com/docs-new/v2/rest/ua/get-trades):
  the futures example fills are integers (`size: "1"`, `"3"`), the spot
  example is fractional.
- [UTA REST V2 Get Futures Open Interest](https://www.kucoin.com/docs-new/v2/rest/ua/get-futures-open-interest)
  labels `openInterest` "By No. of Contracts" and gives "1 contract =
  0.001 BTC" for the XBTUSDTM contract class.
- [Classic Futures Get Symbol](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol)
  defines the contract API's basic unit as lots and its `multiplier` as the
  coin quantity per lot: `XBTUSDTM` has `multiplier = 0.001` ("each XBTUSDTM
  contract being 0.001 BTC"), and "all coin-swap contracts, such as each
  XBTUSDM contract, correspond to 1 USD". A live shape check on 2026-11-03
  returned the classic `multiplier` as `-1.0` for the inverse XBTUSDM,
  ETHUSDM, SOLUSDM and XRPUSDM contracts (a sign/orientation convention) while
  UTA V2 `unitSize` is the positive magnitude `1.0`; the catalog therefore
  takes the positive UTA V2 face value and never reuses the classic signed
  multiplier.

The UTA WebSocket V2 field tables are rendered client-side and are not present
in the Markdown/llms index; the per-field unit conclusion above therefore rests
on the documented examples plus the UTA order/open-interest/Get Symbol field
descriptions and the classic contract-API definition. Trade, order-book and
open-interest quantities are not assumed to share a unit on other venues; on
KuCoin futures the three channels all document contract counts.

### Catalog contract metadata

`GET /api/ua/v2/market/instrument?tradeType=FUTURES` reports the face value as
`unitSize` ("Contract unit size", e.g. `0.001` for XBTUSDTM), the linear/inverse
flag as `isInverse`, and the order increment as `lotSize` ("Minimum contract
quantity increment"). UTA V2 has no separate multiplier field.

`MarketInfo` maps those rows as follows:

- `contract_value` = `unitSize`; it is never derived from the symbol or price.
- `contract_value_currency` is the **base** asset for a linear contract
  (`isInverse: false`, e.g. XBTUSDTM → 0.001 BTC) and the **quote** asset for an
  inverse coin-swap contract (`isInverse: true`, e.g. XBTUSDM → 1 USD). The
  reported `baseCurrency`/`quoteCurrency` pass through the same `XBT` → `BTC`
  alias as the normalized symbol, so the value currency is directly comparable
  with `Symbol`'s base/quote. `settlementCurrency` stays a separate raw source
  field: inverse contracts settle in the base asset even though their face
  value is quote-denominated.
- `contract_multiplier` stays `None`: `unitSize` already is the complete
  per-contract face value and UTA V2 reports no independent multiplier.
- A row with no `unitSize` keeps `contract_value`/`contract_value_currency`
  `None`; a row with `unitSize` but no `isInverse` keeps the currency
  undetermined instead of guessing. Zero is preserved; negative or non-boolean
  fields fail catalog loading.

`cryptofeed_rs::kucoin::units::contracts_to_base(&MarketInfo, contracts, price)`
applies this metadata explicitly: linear = `contracts * contract_value`, inverse
= `contracts * contract_value / price`. It fails closed for missing/zero/
non-positive metadata, an undetermined or foreign value currency, a non-KuCoin
or non-contract market, a non-positive inverse price, or overflow. Spot feeds
already carry base quantity and must not use it.

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
- [UTA REST V2 Get Symbol](https://www.kucoin.com/docs-new/v2/rest/ua/instrument),
  [Place Order](https://www.kucoin.com/docs-new/v2/rest/ua/place-order),
  [Get Trades](https://www.kucoin.com/docs-new/v2/rest/ua/get-trades),
  [Get Futures Open Interest](https://www.kucoin.com/docs-new/v2/rest/ua/get-futures-open-interest)
- [Classic futures Get Symbol](https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol)
  (`multiplier`/lots definition), [classic Add Order](https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order)

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
  channel-specific `depth`/`interval` parameter. **Verified live 2026-11-04:**
  `trade` and `mark-price` reject the batched `symbols` array
  (`symbols are not supported for topic ...`) and require one frame per
  instrument with a singular `symbol`; `ticker`, `obu`, `kline` and
  `funding-fee` accept the array. The adapter follows that per-topic rule.
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
  dated), `settlementCurrency`, `isInverse`, `unitSize`, `lotSize`,
  `marketStage` and `tickSize`. `unitSize`/`isInverse` feed the contract face
  value metadata described above. Only `tradingStatus: "Open"` and
  `marketStage: "NORMAL"` rows are eligible; pre-market stages are skipped
  instead of guessed.
- Perpetual native names are `{BASE}{QUOTE}M` (`XBTUSDTM`, `XBTUSDM`), and the
  `XBT` -> `BTC` alias is applied before product qualification. Dated contracts
  (`XBTMZ26`) normalize from the catalog expiry.

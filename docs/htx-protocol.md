# HTX (Huobi) public WebSocket protocol baseline

Status: **protocol reference for the monitoring-venue addition, implemented.**
Facts below were read from the current official HTX documentation on 2026-11-04
(spot v1 and USDT-margined swap v1) and cross-checked against the public
production WebSockets the same day (read-only probes; shapes only, no live
capture is committed). Two live checks changed the original assumptions and are
recorded explicitly below: the spot market host/framing and the swap
funding/liquidation host. Fixtures in the test suite are sanitized documented
examples, not recordings.

## Endpoints and API version

| Surface | Endpoint | Framing |
| --- | --- | --- |
| Spot market data | `wss://api.huobi.pro/ws` (AWS: `wss://api-aws.huobi.pro/ws`) | `sub`/`id`, GZIP binary pushes |
| Spot MBP incremental / REQ (out of scope) | `wss://api.huobi.pro/feed` | `req`-based |
| USDT-M market data | `wss://api.hbdm.com/linear-swap-ws` | `sub`/`id`, GZIP binary pushes |
| USDT-M notification topics (funding, liquidation) | `wss://api.hbdm.com/linear-swap-notification` | `op`/`topic`, GZIP binary pushes |
| Spot catalog | `GET https://api.huobi.pro/v1/common/symbols` | REST JSON |
| USDT-M catalog | `GET https://api.hbdm.com/linear-swap-api/v1/swap_contract_info` | REST JSON |

Sources (verified 2026-11-04):

- Spot API v1 (market WebSocket, topic field tables, catalog):
  <https://huobiapi.github.io/docs/spot/v1/en/>
- USDT-M swap API v1 (market WebSocket and notification WebSocket framing,
  topic field tables, catalog, heartbeat):
  <https://huobiapi.github.io/docs/usdt_swap/v1/en/>

### Live host/framing verification (2026-11-04)

- `wss://api.huobi.pro/ws/v2` is the **account/order** endpoint in the current
  spot docs, and it is not GZIP-compressed. A live market subscription sent to
  `/ws/v2` with the account framing (`{"action":"sub","ch":
  "market.btcusdt.ticker"}`) was rejected with
  `{"action":"sub","code":2001,"ch":"market.btcusdt.ticker","message":"invalid.ch"}`.
  The same subscription on `wss://api.huobi.pro/ws` with
  `{"sub":"market.btcusdt.ticker","id":"probe"}` was acknowledged
  (`{"id":"probe","status":"ok","subbed":"market.btcusdt.ticker","ts":...}`) and
  produced GZIP-compressed ticker pushes. The implementation therefore uses
  `wss://api.huobi.pro/ws` with `sub`/`id`; the brief's `/ws/v2` assumption is
  recorded as **not current**.
- `wss://api.hbdm.com/linear-swap-ws` rejected the documented notification
  preambles (`{"op":"sub","topic":...}` → `invalid command`) and accepted
  `{"sub":"market.BTC-USDT.detail","id":"..."}`. The notification topics
  (`public.<contract>.funding_rate`, `public.<contract>.liquidation_orders`)
  **are** served by `wss://api.hbdm.com/linear-swap-notification` with the
  documented `op`/`topic` framing (`err-code: 0` ack, immediate `op:"notify"`
  funding snapshot). The feed therefore uses two swap hosts: market topics on
  `linear-swap-ws`, notification topics on `linear-swap-notification`.
- Market host heartbeat is the integer form `{"ping":<ts>}` /
  `{"pong":<ts>}` on both spot and USDT-M market sockets (the documentation's
  `op`/`ping` form applies to the notification socket). Every frame observed on
  the USDT-M market socket, including subscription acks, was GZIP binary.
- `market.BTC-USDT.mark_price.1min` and `market.BTC-USDT.index.1min` were
  rejected as `invalid topic` on the current USDT-M market host. Mark price,
  index price and open interest are therefore **not advertised** (see gaps).

## Subscription framing, acknowledgements and errors

### Spot market (`sub`/`id`)

```json
{"sub":"market.btcusdt.ticker","id":"1"}
{"sub":"market.btcusdt.depth.step0","id":"2"}
{"sub":"market.btcusdt.trade.detail","id":"3"}
{"sub":"market.btcusdt.kline.1min","id":"4"}
{"sub":"market.btcusdt.bbo","id":"5"}
```

- Ack: `{"id":"1","status":"ok","subbed":"market.btcusdt.ticker","ts":...}`.
- Unsubscribe: `{"unsub":"market.btcusdt.ticker","id":"6"}`.
- Error: `{"id":"...","status":"error","err-code":"bad-request","err-msg":"..."}`.
- Topic names are lowercase (`btcusdt`). Pushes are `{"ch":"<topic>","ts":ms,
  "tick":{...}}`.
- Server heartbeat every ~5 s: `{"ping":<integer>}`; the client must send
  `{"pong":<same integer>}` or the server disconnects after missed beats.

### USDT-M market (`sub`/`id`, host `linear-swap-ws`)

Same shape, with uppercase contract codes:

```json
{"sub":"market.BTC-USDT.detail","id":"1"}
{"sub":"market.BTC-USDT.depth.size_20.high_freq","data_type":"incremental","id":"2"}
{"sub":"market.BTC-USDT.trade.detail","id":"3"}
{"sub":"market.BTC-USDT.kline.1min","id":"4"}
{"sub":"market.BTC-USDT.bbo","id":"5"}
```

- Ack: `{"id":"2","status":"ok","subbed":"market.BTC-USDT.depth.size_20.high_freq","ts":...}`.
- Error: `{"id":"...","status":"error","err-code":"bad-request","err-msg":"invalid topic ..."}`.
- Pushes: `{"ch":"<topic>","ts":ms,"tick":{...}}` (market topics) or
  `{"op":"notify","topic":"...","ts":ms,"data":[...]}` (notification topics).
- Heartbeat: `{"ping":<integer>}` / `{"pong":<integer>}`.

### USDT-M notification (`op`/`topic`, host `linear-swap-notification`)

```json
{"op":"sub","topic":"public.BTC-USDT.funding_rate","cid":"1"}
{"op":"sub","topic":"public.BTC-USDT.liquidation_orders","cid":"2"}
```

- Ack: `{"op":"sub","cid":"1","topic":"public.BTC-USDT.funding_rate","ts":...,
  "err-code":0}`.
- Error: non-zero `err-code` + `err-msg`.
- Notify: `{"op":"notify","topic":"...","ts":ms,"data":[...]}`.
- Heartbeat: `{"op":"ping","ts":"..."}` → `{"op":"pong","ts":"<same>"}`.
- Funding pushes are sent immediately on subscribe and then at the documented
  cadence (the current live host sends an initial notification right away).
- Connection planning follows the channels actually configured: a perpetual
  feed with only funding/liquidations plans a single notification connection
  and no market connection. Planning an empty market connection left it with
  `subscriptions_expected=0` and kept the feed permanently unready while
  funding events were already arriving (issue #25).

All market/notification frames are GZIP-compressed binary WebSocket messages.
The runtime decompresses them with the pinned `flate2` dependency (`default-
features = false`, `rust_backend`) before control/market classification; no
async compression stack is added. `flate2` is Apache-2.0/MIT dual-licensed and
supports the workspace MSRV (1.85). GZIP decompression is only enabled for
`ExchangeId::Htx` frames; every other exchange keeps treating binary frames as
non-data.

## Public topics and normalized mapping

### Spot (`wss://api.huobi.pro/ws`)

| Topic | Normalized | Fields used |
| --- | --- | --- |
| `market.<symbol>.ticker` | `Ticker` | `tick.bid`, `tick.ask`, `tick.lastPrice`, `ts` |
| `market.<symbol>.bbo` | `L1Book` | `tick.bid`, `tick.bidSize`, `tick.ask`, `tick.askSize`, `tick.quoteTime` |
| `market.<symbol>.trade.detail` | `Trade` | `tick.data[].tradeId` (`trade-id` fallback), `price`, `amount`, `direction`, `ts` |
| `market.<symbol>.depth.<type>` | `L2Book` snapshot | `tick.bids`/`asks` `[price, size]`, `tick.version`, `tick.ts` |
| `market.<symbol>.kline.<period>` | `Candle` | `tick.id` (start s), `open`, `high`, `low`, `close`, `amount`, `count` |

- Spot depth is **snapshot-only**: one complete book per push ("sends the latest
  market by price order book in snapshot mode at 1-second interval"). The
  documented `version` field is used to drop stale snapshots (`version` not
  greater than the local anchor); it is not a delta sequence.
- Depth type `step0` has up to 150 unmerged levels; `step1..step5` aggregate up
  to 20. The feed uses `step0` (no aggregation) and rejects a requested depth.
- `bbo` `seqId` is documented as a sequence number; it is used only to drop
  stale BBO pushes (monotonic check), never to resync a book.
- Spot kline periods: `1min`, `5min`, `15min`, `30min`, `60min`, `4hour`,
  `1day`, `1mon`, `1week`, `1year`.

### USDT-M perpetual market (`wss://api.hbdm.com/linear-swap-ws`)

| Topic | Normalized | Fields used |
| --- | --- | --- |
| `market.<contract>.detail` | `Ticker` | `tick.bid[0]`, `tick.ask[0]`, `tick.close`, `ts` |
| `market.<contract>.bbo` | `L1Book` | `tick.bid`/`ask` `[price, volume(cont.)]`, `tick.ts` |
| `market.<contract>.trade.detail` | `Trade` | `tick.data[].id`, `price`, `quantity` (coin), `direction`, `ts` |
| `market.<contract>.depth.size_20.high_freq` (`data_type=incremental`) | `L2Book` | `tick.event` (`snapshot`/`update`), `tick.version`, `bids`/`asks` `[price, volume(cont.)]`, `ts` |
| `market.<contract>.kline.<period>` | `Candle` | `tick.id` (start s), `open`, `high`, `low`, `close`, `amount`, `vol`, `count` |

- The swap market host has **no standalone ticker topic**: `market.<contract>.detail`
  is the documented market-detail push carrying 24h stats plus best bid/ask
  arrays (the docs note ~500 ms staleness on those bid/ask prices). It is the
  ticker source.
- Swap kline periods: `1min`, `5min`, `15min`, `30min`, `60min`, `1hour`,
  `4hour`, `1day`, `1mon`.
- Swap depth: `depth.stepX` is full-push depth; the incremental
  `depth.size_20.high_freq` topic first pushes `event:"snapshot"` and then
  `event:"update"` rows. `version` is documented as auto-incrementing within a
  single WebSocket connection and a snapshot is pushed on the first message and
  after reconnect. The sync state machine requires `version == anchor + 1` for
  updates; a gap enters a bounded in-session unsubscribe/resubscribe (same
  `data_type=incremental` subscription) with at most three requests per
  connection. Rows are absolute sizes at the price level (`0` removes).
- Swap trade quantity: the documented fields are `amount` (contracts) and
  `quantity` (coin, added 2021-02-03). `Trade.amount` uses the documented
  base-coin `quantity`; a row without it fails closed rather than silently
  publishing contracts as coin.
- Swap level sizes are **contract counts**, preserved verbatim in
  `L2Book`/`L1Book.amount` (native unit, documented per topic).

### USDT-M notification (`wss://api.hbdm.com/linear-swap-notification`)

| Topic | Normalized | Fields used |
| --- | --- | --- |
| `public.<contract>.funding_rate` | `Funding` | `data[].funding_rate`, `estimated_rate`, `settlement_time`, `ts` |
| `public.<contract>.liquidation_orders` | `Liquidation` | `data[].direction`, `amount` (token), `price`, `created_at`, `contract_code` |

- Funding: `rate = funding_rate`, `predicted_rate = estimated_rate`
  (nullable → `None`), `next_funding_time = settlement_time` (documented
  settlement timestamp, ms → seconds), `exchange_ts = ts`. No mark price is
  carried by this topic.
- Liquidation: `side` is the documented liquidation direction (`buy`/`sell`),
  `quantity` is the documented base-token `amount`, `price` is the documented
  bankruptcy price, `status = Filled` (each push is an executed liquidation
  order), `id = None` (no order id is documented), `exchange_ts = created_at`.
- The notification host is a **second connection** for a perpetual feed that
  subscribes funding/liquidations; the runtime plans it separately from the
  market connection.

## JSON number formatting

Market rows may serialize very small prices as JSON numbers in scientific
notation (live 2026-11-04: PEPE spot `price=5.213462e-06`, USDT-M
`price=5.1935e-06`). Parsers accept both plain decimals and exponent form
(`Decimal::from_scientific` fallback); `from_str_exact` alone rejects the
exponent and previously produced `malformed market data` reconnect loops
(issue #21). Trade `amount` on USDT-M remains contracts; the normalized amount
uses the documented base-coin `quantity` field.

## Candle intervals

| Normalized | Spot wire | Swap wire |
| --- | --- | --- |
| `1m` | `1min` | `1min` |
| `5m` | `5min` | `5min` |
| `15m` | `15min` | `15min` |
| `30m` | `30min` | `30min` |
| `1h` | `60min` | `60min` (docs also list `1hour`) |
| `4h` | `4hour` | `4hour` |
| `1d` | `1day` | `1day` |
| `1w` | `1week` | unsupported |
| `1M` | `1mon` | `1mon` |

No closed flag is transmitted; `Candle.closed` stays `None` and completion is
never inferred from wall-clock time. `end = start + normalized interval`.

## Catalog and product identity

### Spot: `GET https://api.huobi.pro/v1/common/symbols`

Response `{"status":"ok","ts":...,"data":[{...}]}`. Fields used:

| Native field | Use |
| --- | --- |
| `symbol` | native symbol (lowercase `btcusdt`); normalized via `base-currency` + `quote-currency` and cross-checked against the native form |
| `base-currency`, `quote-currency` | `Symbol::spot(base, quote)` |
| `state` | only `online` rows are accepted; other states skip (never guessed) |
| `api-trading` | surfaced when present; not a subscription gate |
| `price-precision`, `amount-precision` | `MarketInfo.price_decimal_places` / `quantity_decimal_places` |
| `min-order-amt`, `min-order-value` | `minimum_quantity` / `minimum_notional` |

### USDT-M: `GET https://api.hbdm.com/linear-swap-api/v1/swap_contract_info`

Response `{"status":"ok","data":[{...}]}`. Fields used:

| Native field | Use |
| --- | --- |
| `contract_code` | native symbol (`BTC-USDT`); normalized as `BASE-QUOTE-PERP` via `symbol`/`pair` |
| `symbol`, `pair` | base; cross-checked against `contract_code` |
| `business_type` / `contract_type` | must be `swap` (delivery/futures rows are skipped; the feed is perpetual-only) |
| `contract_status` | only `1` (settlement complete / trading resumed) is accepted |
| `contract_size`, `price_tick` | `MarketInfo.contract_value` (base coin per contract), `price_increment` |
| `settlement_period` | surfaced as native settlement cadence context |

Product identity is product-qualified: spot `BTC-USDT` and perpetual
`BTC-USDT-PERP` are distinct normalized symbols; a feed cannot mix them.

## Order-book confusion/sequence/checksum summary

- **Spot depth** (`depth.stepX`): replacement snapshot per push; `version`
  monotonic dedupe; no checksum, no deltas.
- **Spot BBO**: tick-by-tick L1; `seqId` monotonic dedupe; complete replacement
  of the top of book.
- **Swap depth** (`depth.size_20.high_freq`, `data_type=incremental`): first
  message snapshot, later updates with absolute sizes; `version` per-connection
  auto-increment with a gap → bounded in-session resubscribe; no checksum.
- No venue-documented checksum exists for either product; none is invented.
- Malformed/out-of-order frames fail the session; reconnect rebuilds from the
  next documented snapshot.

## Heartbeat, liveness and idle policy

- Market sockets (spot and swap): server sends `{"ping":<integer>}` every ~5 s;
  the session answers `{"pong":<integer>}`. The SDK does not send proactive
  heartbeats; the server push feeds the idle watchdog (60 s).
- Notification socket: server sends `{"op":"ping","ts":"<string>"}`; the
  session answers `{"op":"pong","ts":"<same>"}`. Idle watchdog 60 s.
- GZIP decoding must happen before heartbeat detection; every frame is binary.

### Close codes, ping expiry and recovery contract

Live probe 2026-11-05 on the spot market socket: when pings are not answered,
the server closes with **1003 `ping check expired` after ~15 s** (pings every
~1 s in that state, 5.5 s spaced normally). With the documented
`{"pong":<ping>}` reply the same 13-market subscription ran for the probe
window without a close. The SDK therefore:

- sends pongs immediately, outside the paced subscription queue, so a pending
  resubscription cannot delay the heartbeat reply;
- classifies HTX `1003` as retryable **only** when the close reason contains
  `ping` (case-insensitive). Any other `1003`, or a `1003` without a reason,
  keeps the protocol-failure terminal semantics; retries remain bounded by the
  configured retry limit and backoff, so a repeating protocol fault still ends
  the connection plan;
- keeps the close reason out of logs and status; only the code and the derived
  retryable flag are retained.

A production 76-minute HTX spot trade feed terminated on `1003` with no tape
before the fault (issue #28). Because the read loop awaited handlers inline, a
slow callback could delay pong handling past the 15 s window; the opt-in
`HandlerDispatch::Async` policy plus the immediate-pong path remove that cause.
Session-end diagnostics record bounded frame kinds/counts/bytes, the last-frame
age and ping/pong/pong-failure counts; no payloads or raw reasons are logged.
`crates/runtime/examples/htx_spot_soak.rs` runs the same 13-market subscription
for 90 minutes through the SDK for post-fix observation.

## Rate limits

- Spot market WebSocket: the official v1 page documents pull-style `req` as
  limited to 100 ms per request per connection; subscription pushes are
  server-paced. The account socket's documented 50 valid requests/second per
  connection is not the market feed's limit and is not claimed here.
- USDT-M: the official page notes "the WS request connection should not go over
  30 normally" and documents REST public-market limits of 240 requests per 3 s
  per IP. The SDK sends one subscription batch per connection plus a single
  catalog read at hydration; it adds no polling.

## Open questions / explicit gaps

- **Mark price, index price and open interest are not advertised for HTX.**
  The documented USDT-M topics `market.<contract>.mark_price.<period>` and
  `market.<contract>.index.<period>` were rejected as `invalid topic` on the
  current public market host, and no current public WebSocket topic carries a
  standalone mark price, index price or open interest. The REST endpoints exist
  (`/index/market/history/linear_swap_mark_price_kline`,
  `/linear-swap-api/v1/swap_his_open_interest`) but the runtime has no
  derivative REST-polling path; a hidden poll would be a new delivery contract,
  so the cells stay unsupported and fail preflight.
- **Spot `/ws/v2` is not the market endpoint and is not used.** The account
  socket remains out of scope.
- HTX's spot and swap documentation layout is older than the other venues; the
  field tables above were cross-checked against the live productions sockets
  because the docs are not always rendered with their field tables. Where live
  behaviour contradicted the docs (spot host, swap notification host, mark/index
  topics), the live-verified behaviour is recorded and implemented.
- USDT-M delivery contracts (`BTC-USDT-CW`, `...-210625`, ...) are out of scope;
  only `business_type=swap` rows are accepted.

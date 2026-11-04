# Upbit public WebSocket/REST protocol baseline

Status: **protocol reference for the phase-A addition, implemented.** Facts were
read from the official Upbit documentation on 2026-11-03 — the global index
(`https://global-docs.upbit.com/llms.txt`) and the Korean index
(`https://docs.upbit.com/llms.txt`) — and checked against the public REST
endpoints from this host on the same day. Fixtures in the test suite are
sanitized documented examples (see `sample_data/SOURCES.md`), not recordings.

Upbit separates its quotation API by region. This SDK implements the **Korean
(KRW) quotation service**, whose host is the unsuffixed one; the regional hosts
(Singapore/Indonesia/Thailand) are documented but not routed.

## Endpoints

| Purpose | Endpoint |
| --- | --- |
| Public WebSocket (Korea) | `wss://api.upbit.com/websocket/v1` |
| Public WebSocket (private) | `wss://api.upbit.com/websocket/v1/private` (out of scope) |
| Public WebSocket (info) | `wss://api.upbit.com/websocket/v1/info` (out of scope) |
| Regional WebSocket | `wss://sg-api.upbit.com/websocket/v1`, `wss://id-api.upbit.com/...`, `wss://th-api.upbit.com/...` |
| Quotation REST (Korea) | `https://api.upbit.com` |
| Regional REST | `https://sg-api.upbit.com`, `https://id-api.upbit.com`, `https://th-api.upbit.com` |

The global documentation lists only `wss://{region}-api.upbit.com/websocket/v1`
and the Korean documentation lists the concrete `wss://api.upbit.com/websocket/v1`
as the public host for `ticker`, `trade`, `orderbook` and `candle`. No
authentication is required for those four types.

## Subscription framing

One request message is a JSON **array** sent as one WebSocket text frame:

```json
[
  {"ticket": "cryptofeed-rs"},
  {"type": "trade", "codes": ["KRW-BTC", "KRW-ETH"]},
  {"type": "orderbook", "codes": ["KRW-BTC.5"]},
  {"type": "candle.1m", "codes": ["KRW-BTC"]},
  {"format": "DEFAULT"}
]
```

- `ticket` (object, first element): unique requester identifier. Required.
- Data-type objects (`type`, `codes`, optional `is_only_snapshot` /
  `is_only_realtime`, and `level` for orderbook aggregation): at least one.
  Several types may share one message and one connection.
- `format` (object, last element): `DEFAULT`, `SIMPLE`, `JSON_LIST`,
  `SIMPLE_LIST`. The SDK requests `DEFAULT` and only parses the documented
  `DEFAULT` field names.
- `codes` must be uppercase market codes (`KRW-BTC`, `BTC-ETH`, `USDT-BTC`).
  The quote currency is the **prefix**; `KRW-BTC` means base `BTC`, quote `KRW`.
- Orderbook unit-count selection is part of the code string: `{code}.{count}`
  with `count` in `1`, `5`, `15`, `30`. An unsupported count falls back to the
  server default `30`; the SDK therefore never sends an unsupported count and
  simply omits the suffix for the documented default of 30 units.
- There is no documented subscription acknowledgement. The server answers a
  rejected request with an error envelope and otherwise starts streaming; the
  first data frame of a type is the only positive confirmation. Error envelope:

  ```json
  {"error": {"name": "WRONG_FORMAT", "message": "..."}}
  ```

  Documented names: `INVALID_AUTH`, `WRONG_FORMAT`, `NO_TICKET`, `NO_TYPE`,
  `NO_CODES`, `INVALID_PARAM`, `Too Many Requests`, `I'm a teapot`.
- Upbit also documents `LIST_SUBSCRIPTIONS` (`[{"ticket": "..."},
  {"method": "LIST_SUBSCRIPTIONS"}]`, answered with
  `{"method": "LIST_SUBSCRIPTIONS", "result": [{"type": ..., "codes": [...],
  "level": ...}], "ticket": ...}`). The SDK does not send it: it costs one
  rate-limited request and the data-frame confirmation used here is equivalent
  for readiness purposes.

## Public channels and normalized mapping

| `type` | Normalized category | Payload fields used |
| --- | --- | --- |
| `trade` | `Trade` | `code`, `trade_price`, `trade_volume`, `ask_bid` (`ASK` sell / `BID` buy), `trade_timestamp` (ms), `sequential_id`, `timestamp` (ms) |
| `orderbook` | `L2Book` (snapshot only) | `code`, `orderbook_units[]` with `ask_price`/`bid_price`/`ask_size`/`bid_size`, `total_ask_size`, `total_bid_size`, `timestamp` (ms) |
| `candle.{unit}` | `Candle` | `code`, `type` (unit), `candle_date_time_utc`, `opening_price`, `high_price`, `low_price`, `trade_price` (close), `candle_acc_trade_volume`, `candle_acc_trade_price`, `timestamp` (ms) |
| `ticker` | **unsupported** | `trade_price`, OHLC/24h statistics; no bid/ask, so it cannot satisfy the normalized `Ticker` BBO contract |

`ticker` is deliberately not a capability: its frame is a last-price and 24-hour
statistics snapshot without a best bid or best ask, and the normalized `Ticker`
model is a BBO contract. Substituting `trade_price` for both sides would publish
a fabricated zero-spread quote; the venue is therefore `Trade`, `L2Book` and
`Candles` only. **No `Ticker` and no `L1Book` cell is claimed**: there is no
native top-of-book channel, so the orderbook stream is the book source.

Notes:

- `orderbook_units[i]` pairs the `i`-th best ask (`ask_price`, `ask_size`) with
  the `i`-th best bid (`bid_price`, `bid_size`). The normalized book is built as
  `asks = [ask_price...]` and `bids = [bid_price...]` from the same array, never
  by assuming equal prices or a spread.
- Every orderbook push is a **complete snapshot** for the selected unit count;
  Upbit documents no delta, sequence or checksum for this stream. The SDK treats
  each push as a full replacement (snapshot-only L2), and a **delta-shaped frame
  fails closed** instead of being applied. There is no gap/sequence recovery path
  because there is nothing to sequence.
- `orderbook` also documents an aggregation `level` field. It merges price
  levels and is therefore not used: the SDK neither aggregates nor requests it,
  and `l2_book_depth` maps only to the documented unit counts.
- Candle unit coverage: `1s`, `1m`, `3m`, `5m`, `10m`, `15m`, `30m`, `60m`,
  `240m`. Normalized intervals: `1s` → `1s`, `1m`..`30m` unchanged, `60m` → `1h`,
  `240m` → `4h`. Any other unit is rejected during preflight instead of being
  relabeled. Upbit's REST catalog exposes the same minute units
  (`/v1/candles/minutes/{1,3,5,10,15,30,60,240}`), one-second candles
  (`/v1/candles/seconds`), and day/week/month/year candles.
- Candle completion is **not** transmitted by the WebSocket stream. `stream_type`
  distinguishes the initial `SNAPSHOT` from later `REALTIME` pushes, and the same
  `candle_date_time_utc` bar may be pushed repeatedly while it updates; neither
  means the bar is final. The parser therefore sets `closed: None` and never
  infers completion from elapsed time.
- `candle_date_time_utc` uses `yyyy-MM-dd'T'HH:mm:ss` with no zone suffix and is
  UTC; it is the bar open time. `end` is derived as `start + unit`.
- A requested candle interval that has not produced a bar yet is served from the
  previous interval on subscription (documented), so the first candle frame of a
  subscription may be an older bar. Bars are keyed by their own
  `candle_date_time_utc`, not by arrival order.
- Trade frames carry `best_ask_price`/`best_ask_size`/`best_bid_price`/
  `best_bid_size`. That is a trade-time BBO sample, not a subscription; the SDK
  does not derive an `L1Book` contract from it.

## Timestamps

- All Upbit wire timestamps are integer **milliseconds** since the epoch and are
  converted to seconds: `timestamp` is the frame/event publish time,
  `trade_timestamp` is the trade execution time (used for `Trade.exchange_ts`),
  and candle `timestamp` is the publish time (used for `Candle.exchange_ts`).
- `trade_date`/`trade_time` are UTC strings for display; the SDK uses the
  millisecond fields only.
- A missing or malformed timestamp falls back to the local receive clock, as for
  the other venues.

## Heartbeat and lifecycle

- The server closes a connection after ~120 s without data or client PING.
- Liveness: send the text message `PING`; while the connection is healthy the
  server pushes `{"status": "UP"}` every 10 s. WebSocket protocol-level PING
  frames are also supported by the server.
- The SDK heartbeat policy sends `PING` every 30 s with a 90 s idle watchdog and
  filters `{"status": "UP"}` frames before parsing. An unanswered heartbeat is an
  idle reconnect, not a protocol error.
- No session/token handshake is needed for public channels.

## Catalog and symbol identity

- `GET https://api.upbit.com/v1/market/all?is_details=true` returns a JSON array:
  `[{"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin",
  "market_event": {"warning": false, "caution": {"PRICE_FLUCTUATIONS": false,
  "TRADING_VOLUME_SOARING": false, "DEPOSIT_AMOUNT_SOARING": false,
  "GLOBAL_PRICE_DIFFERENCES": false, "CONCENTRATION_OF_SMALL_ACCOUNTS": false}}}]`.
- `market` is `{QUOTE}-{BASE}`. Normalization preserves the quote exactly:
  `KRW-BTC` → `BTC-KRW`, `BTC-ETH` → `ETH-BTC`, `USDT-BTC` → `BTC-USDT`. KRW is
  a fiat quote and is never re-mapped to USDT or guessed from the base symbol.
- Live 2026-11-03: 875 markets (`299` KRW, `331` BTC, `245` USDT). All rows are
  listed markets; `market_event` is metadata, not a tradability gate, and the
  SDK never drops a pair because alert flags are set. The flags are surfaced as
  the catalog `native_status` (`WARNING` if `warning`, else `CAUTION` if any
  caution flag, else `NONE`).
- `is_details=false` omits `market_event`; the SDK always requests details so the
  status is read from the official field rather than inferred.
- No REST endpoint on this surface publishes trading suspension/delisting state.

## Rate limits

- Quotation REST is measured per IP and shared inside the market group: up to
  **10 requests/s** (live check used only the catalog endpoint).
- WebSocket connection requests: up to **5 requests/s** for unauthenticated
  connections (per IP), measured separately from data requests.
- WebSocket data requests: up to **5/s and 100/min per connection**. A request
  message carrying several type objects counts once. The SDK sends one
  subscription message (plus its periodic `PING`, which is a frame, not a data
  request) and therefore stays far below the documented budget.
- Requests carrying an `Origin` header have a separate, much stricter policy;
  this SDK does not send one.
- Native error names `Too Many Requests` / `I'm a teapot` are mapped to the
  shared rate-limited error class rather than a permanent subscription failure.

## Open questions

1. Whether the `LIST_SUBSCRIPTIONS` echo reports orderbook unit counts verbatim
   after a `{code}.{count}` subscription is not documented; that is the reason
   the SDK does not use it as its readiness confirmation.
2. Upbit does not document a per-connection subscription/symbol ceiling. The
   SDK uses a conservative generated-message byte budget and shards larger
   feeds, as for the other venues; this is a local budget, not a claimed limit.
3. Regional hosts are documented but not routed by this SDK; routing them would
   need a product decision about non-Korean quotation service scope.
4. Upbit's WebSocket `trade` frame carries trade-time BBO prices/sizes. Whether
   a future increment advertises an `L1Book` projection from it is a contract
   question; the current increment does not.

## References

- [Global documentation index](https://global-docs.upbit.com/llms.txt)
- [Korean documentation index](https://docs.upbit.com/llms.txt)
- [WebSocket usage and errors](https://docs.upbit.com/kr/reference/websocket-guide)
- [Ticker](https://docs.upbit.com/kr/reference/websocket-ticker),
  [Trade](https://docs.upbit.com/kr/reference/websocket-trade),
  [Orderbook](https://docs.upbit.com/kr/reference/websocket-orderbook),
  [Candle](https://docs.upbit.com/kr/reference/websocket-candle)
- [Subscriptions](https://docs.upbit.com/kr/reference/list-subscriptions)
- [Trading pairs](https://docs.upbit.com/kr/reference/list-trading-pairs),
  [Rate limits](https://docs.upbit.com/kr/reference/rate-limits)
- [WebSocket best practice](https://docs.upbit.com/kr/docs/websocket-best-practice)

## Implementation verification — 2026-11-03

- The Korean quotation catalog was fetched live:
  `GET https://api.upbit.com/v1/market/all?is_details=true` returned 875 rows
  with the documented `market`/`korean_name`/`english_name`/`market_event`
  shape and KRW/BTC/USDT quote prefixes only.
- No WebSocket capture is committed. Channel fixtures are the documented
  example frames (trade, orderbook units, candle) sanitized into the test
  suite; they are not live recordings.
- The implemented capability row for Upbit Spot is `Trade`, `L2Book` and
  `Candles`. `Ticker`, `L1Book`, funding, open interest, index price, mark price
  and liquidations are rejected during preflight.

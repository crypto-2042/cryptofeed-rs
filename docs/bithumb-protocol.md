# Bithumb public WebSocket/REST protocol baseline

Status: **protocol reference for the phase-B addition, implemented.** Facts were
read from the official Bithumb documentation on 2026-11-03
(`https://apidocs.bithumb.com/llms.txt`, API 2.0) and checked against the public
REST catalog from this host on the same day. Fixtures in the test suite are
sanitized documented examples (see `sample_data/SOURCES.md`), not recordings.

## Endpoints

| Purpose | Endpoint |
| --- | --- |
| Public WebSocket v1 | `wss://ws-api.bithumb.com/websocket/v1` |
| Private WebSocket v2 | `wss://ws-api.bithumb.com/websocket/v2/private` (out of scope) |
| Public REST | `https://api.bithumb.com/v1` |

Public v1 streams are `ticker`, `trade` and `orderbook`. They need no
authentication. Private v1 (`myOrder`, `myAsset`) is retired on 2026-11-13; the
public v1 surface is explicitly not part of that retirement, and this SDK only
implements public v1.

## Subscription framing

The request format is the same ticket/type/format array model Upbit uses; one
JSON **array** is sent as one WebSocket **text** frame; the server's market pushes arrive as **binary** frames carrying UTF-8 JSON (verified live 2026-11-04), decoded by the session before parsing:

```json
[
  {"ticket": "cryptofeed-rs"},
  {"type": "trade", "codes": ["KRW-BTC", "KRW-ETH"]},
  {"type": "orderbook", "codes": ["KRW-BTC"]},
  {"format": "DEFAULT"}
]
```

- `ticket` (object, first element): requester identifier. Required.
- Data-type objects: `type` plus `codes` (uppercase market codes such as
  `KRW-BTC`), optional `is_only_snapshot` / `is_only_realtime`. Several types may
  share one message and one connection. The documented aliases `isOnlySnapshot`
  / `isOnlyRealtime` appear in some tables; the SDK omits both fields entirely
  and relies on the documented default (snapshot **and** realtime).
- `orderbook` additionally accepts `level`, a price-aggregation unit (default
  `1`, i.e. the native price steps). It is not a depth count and is not
  requested by the SDK.
- `format` (object, last element): `DEFAULT` or `SIMPLE`. The SDK requests
  `DEFAULT` and parses only the documented `DEFAULT` field names.
- No subscription acknowledgement is documented. The only positive confirmation
  is the first data frame of a type; a rejected request returns an error
  envelope:

  ```json
  {"error": {"name": "WRONG_FORMAT", "message": "..."}}
  ```

  Documented names: `WRONG_FORMAT`, `NO_TICKET`, `NO_TYPE`, `NO_CODES`,
  `INVALID_PARAM`, `INVALID_PARAM_FORMAT`. They are treated as permanent
  configuration errors.

## Public channels and normalized mapping

| `type` | Normalized category | Payload fields used |
| --- | --- | --- |
| `trade` | `Trade` | `code`, `trade_price`, `trade_volume`, `ask_bid` (`ASK` sell / `BID` buy), `trade_timestamp` (ms), `sequential_id`, `timestamp` (ms) |
| `orderbook` | `L2Book` (snapshot only) | `code`, `orderbook_units[]` (max 15) with `ask_price`/`bid_price`/`ask_size`/`bid_size`, `total_ask_size`, `total_bid_size`, `timestamp` (ms) |
| `ticker` | **unsupported** | `trade_price`, OHLC/24h statistics; no bid/ask, so it cannot satisfy the normalized `Ticker` BBO contract |
| candle | **unsupported over WebSocket** | no public WS candle stream is documented; candles exist only as REST history |

`ticker` is deliberately not a capability for the same reason as Upbit: the
frame is a last-price/24-hour-statistics snapshot without a best bid or best
ask, and fabricating a zero-spread quote would violate the normalized `Ticker`
BBO contract. **No `Ticker` and no `L1Book` cell is claimed**: the capability row
is `Trade` and `L2Book` only, and there is no native top-of-book channel to
advertise.

Notes:

- `orderbook_units[i]` pairs the `i`-th best ask (`ask_price`, `ask_size`) with
  the `i`-th best bid (`bid_price`, `bid_size`). The normalized book takes asks
  and bids from the same array, never by matching prices or assuming a spread.
- The stream returns at most 15 units per side and documents no unit-count
  request parameter. The SDK therefore accepts no `l2_book_depth` value for
  Bithumb (fail closed instead of silently ignoring a depth request); the
  delivered book is whatever the venue sends, and `l2_book_max_depth` remains the
  separate delivery-side projection.
- Every push is a **complete snapshot**. Bithumb documents no delta, sequence or
  checksum for public orderbook frames. The SDK replaces the local book on every
  push (snapshot-only L2), and a delta-shaped frame — no documented
  `orderbook_units` array, or an explicit delta model — **fails closed** instead
  of being applied to the local book.
- The `level` aggregation field merges price levels. Supporting it would change
  the meaning of a level, so it is neither requested nor accepted through
  `l2_book_depth`.
- Candles: the public WebSocket has no candle type. REST history is documented as
  `/v1/candles/minutes/{unit}` (`unit` in `1`, `3`, `5`, `10`, `15`, `30`, `60`,
  `240`), `/v1/candles/days`, `/v1/candles/weeks` and `/v1/candles/months`, with
  `market`, `count` (max 200) and an optional KST `to` cursor; responses are
  sorted newest-first and carry `candle_date_time_utc`/`_kst`, OHLC, accumulated
  price/volume and no completion flag. The normalized **live** candle channel is
  not offered for Bithumb in this increment: a polling feed would add a new
  delivery model rather than use the documented push protocol, and the bounded
  REST candle-history surface (`PublicRestClient::candle_history`) remains an
  **explicitly deferred follow-up** (the same scope as Kraken and KuCoin before
  it). The venue has no WS candle channel; that gap is recorded rather than
  hidden. See the plan document for the explicit gap.

## Timestamps

- Wire timestamps are integer **milliseconds** since the epoch; they are
  converted to seconds. `trade_timestamp` is the trade execution time (used for
  `Trade.exchange_ts`), `timestamp` is the frame publish time (used for book and
  event times).
- `trade_date`/`trade_time` are **KST** display strings (`yyyy-MM-dd`,
  `HH:mm:ss`) — unlike Upbit, whose equivalents are UTC — and are not used for
  event times.
- `stream_type` is `SNAPSHOT` for the first push of a subscription and
  `REALTIME` afterwards; it distinguishes data-type delivery, not candle
  completion or book incrementality.
- A missing or malformed timestamp falls back to the local receive clock.

## Heartbeat and lifecycle

- The server closes a connection after ~120 s without traffic.
- Liveness: send the text message `PING` (WebSocket protocol PING frames are also
  accepted); while the connection is healthy the server pushes
  `{"status": "UP"}` every 10 s. The SDK sends `PING` every 30 s with a 90 s idle
  watchdog and filters `{"status": "UP"}` frames before parsing. An unanswered
  heartbeat is an idle reconnect, not a protocol error.
- Message compression: since 2026-08-12 the server supports RFC 7692
  `permessage-deflate`. Clients that do not request it get unmodified plain JSON
  frames; the SDK does not request it, so the framing is unchanged. The
  subscription and response JSON schema is identical either way.
- Maintenance history to track: the public WebSocket endpoint had a scheduled
  connection interruption on 2026-08-20 (existing sessions may be closed). That
  is a normal reconnect event, not a protocol change. Private v1 retirement
  (2026-11-13) does not affect the public streams.

## Catalog and symbol identity

- `GET https://api.bithumb.com/v1/market/all?isDetails=true` returns
  `[{"market": "KRW-BTC", "korean_name": "비트코인", "english_name": "Bitcoin",
  "market_warning": "NONE"}]`. With `isDetails=false` the `market_warning` field
  is omitted; the SDK always requests details.
- `market` is `{QUOTE}-{BASE}`. Normalization preserves the quote exactly:
  `KRW-BTC` → `BTC-KRW`, `BTC-ETH` → `ETH-BTC`.
- Live 2026-11-03: 500 markets with `KRW` and `BTC` quote prefixes, and
  `market_warning` values `NONE` or `CAUTION`. `CAUTION` is metadata, not a
  tradability gate: the SDK keeps those pairs and surfaces the official string as
  the catalog `native_status` instead of dropping or reinterpreting them.
- Investment-caution levels (`경보제`) are published by a separate documented REST
  endpoint, not by the catalog; they are not inferred here.
- The catalog does not publish trading-suspension or delisting state beyond the
  warning field; the SDK does not infer suspension from missing data.

## Rate limits

- Public REST: 150 requests/s per IP, measured per API category (candles,
  orderbook, ticker, trades, other). The catalog fetch is a single request and
  stays far below it.
- WebSocket connection requests: 10 requests/s per IP. Exceeding it returns
  HTTP 429, and repeated excess can block the IP for 10 minutes. The SDK opens
  one connection per planned feed and reconnects through the shared supervisor.
- No per-connection message-rate limit is documented for public data requests;
  the SDK still paces subscription frames (one frame per feed) as for every other
  venue.

## Open questions

1. The orderbook request documentation contains an example with a per-code
   `.{n}` suffix (`KRW-ETH.3`) copied from the Upbit framing, while the field
   table documents only `level` as a request field. The SDK sends plain codes and
   never a suffix; the feedback effect on the returned unit count is undocumented.
2. Whether `market_state` / `is_trading_suspended` in `ticker` frames are
   authoritative for all pairs (the catalog exposes only `market_warning`) is not
   documented; the SDK does not gate subscriptions on them.
3. No public WS candle channel is documented; a future increment would have to
   decide between bounded REST history and a polling feed, which is a delivery
   contract question rather than a protocol detail.
4. Per-connection subscription/symbol ceiling is not documented. The SDK uses a
   conservative generated-message byte budget and shards larger feeds; this is a
   local budget, not a claimed exchange limit.

## References

- [Bithumb documentation index](https://apidocs.bithumb.com/llms.txt)
- [WebSocket basic information](https://apidocs.bithumb.com/reference/기본-정보)
- [Connection management](https://apidocs.bithumb.com/reference/연결-관리)
- [Request format](https://apidocs.bithumb.com/reference/요청-포맷)
- [Ticker](https://apidocs.bithumb.com/reference/현재가-ticker),
  [Trade](https://apidocs.bithumb.com/reference/체결-trade),
  [Orderbook](https://apidocs.bithumb.com/reference/호가-orderbook)
- [WebSocket errors](https://apidocs.bithumb.com/reference/웹소켓-에러)
- [Trading pairs](https://apidocs.bithumb.com/reference/거래-대상-목록-조회),
  [Minute candles](https://apidocs.bithumb.com/reference/분minute-캔들-조회)
- [Rate limits](https://apidocs.bithumb.com/docs/api-요청-수-제한-안내),
  [compression notice](https://apidocs.bithumb.com/changelog/websocket-메시지-경량화압축-기능-적용-안내),
  [public WS maintenance notice](https://apidocs.bithumb.com/changelog/공지-public-websocket-연결-일시-중단-안내-1)

## Implementation verification — 2026-11-03

- Current public endpoint confirmed from the official "basic information" page:
  `wss://ws-api.bithumb.com/websocket/v1`, public v1 streams `ticker`, `trade`,
  `orderbook`.
- The catalog was fetched live: `GET https://api.bithumb.com/v1/market/all?isDetails=true`
  returned 500 rows with `market`/`korean_name`/`english_name`/`market_warning`,
  KRW and BTC quote prefixes. The host needed HTTP/1.1 for this environment's
  curl; that is a local TLS-stack detail, not a venue requirement.
- Frame encoding: plain JSON text frames (no compression requested by this SDK);
  compression is an optional RFC 7692 extension and the JSON schema is unchanged.
- No WebSocket capture is committed. Channel fixtures are documented example
  frames sanitized into the test suite, not live recordings.
- The implemented capability row for Bithumb Spot is `Trade` and `L2Book`.
  `Ticker`, `Candles`, `L1Book`, funding, open interest, index price, mark price
  and liquidations are rejected during preflight.

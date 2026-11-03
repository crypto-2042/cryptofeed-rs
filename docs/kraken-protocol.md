# Kraken Spot WebSocket v2 protocol baseline

Status: **protocol reference for the phase-2 addition, implemented.** Facts were
read from the official Kraken documentation on 2026-11-03 and the open questions
were resolved against the public WebSocket v2 endpoint on the same day; the
verification section at the end records the observed shapes. Fixtures in the
test suite are sanitized documented examples plus sanitized live-shape
references (see `sample_data/SOURCES.md`), not recordings.

## Endpoints and framing

- Public market data WebSocket: `wss://ws.kraken.com/v2`
- One JSON object per frame, `method`-based:
  - subscribe: `{"method":"subscribe","params":{"channel":"ticker","symbol":["ALGO/USD"],"depth":10}}`
  - unsubscribe: `{"method":"unsubscribe","params":{...}}`
  - liveness: `{"method":"ping"}` with a `pong` reply; the session supervisor
    uses this as the application heartbeat.
- Acknowledgement: `{"method":"subscribe","result":{"channel":"ticker","snapshot":true,"symbol":"ALGO/USD"},"success":true,"time_in":...,"time_out":...}`.
  A failed request carries `success: false` with an `error` string; it is a
  subscription rejection, not a transport failure.
- Market pushes: `{"channel":"ticker","type":"snapshot"|"update","data":[{...}]}`.

## Public channels and normalized mapping

| Channel | Normalized category | Payload fields used |
| --- | --- | --- |
| `ticker` | `Ticker` (BBO + last) | `bid`, `bid_qty`, `ask`, `ask_qty`, `last`, `volume`, `vwap`, `low`, `high`, `change`, `change_pct`, ISO-8601 `timestamp` |
| `trade` | `Trade` | `price`, `qty`, `side` (`buy`/`sell`), `ord_type`, `trade_id`, `timestamp` |
| `book` | `L2Book` | `bids`/`asks` arrays of `[price, qty, timestamp]`, `checksum` (CRC32 over the top ten levels), `type` snapshot/update |
| `ohlc` | `Candle` | `interval_begin` (ISO-8601), `interval` (minutes), `open`, `high`, `low`, `close`, `vwap`, `volume`, `trades`; the deprecated `timestamp` is ignored |
| `instrument` | catalog/status helper | channel/instrument metadata; the REST catalog remains authoritative |

Details that shape the implementation:

- `book` depth is one of `10`, `25`, `100`, `500`, `1000`. Every push includes a
  CRC32 checksum of the top ten bids and asks; a mismatch must invalidate the
  local book and trigger the bounded resnapshot path. The checksum algorithm is
  the documented Kraken concatenation of price/size strings without separators.
- `ohlc` intervals are minutes (`1`, `5`, `15`, `30`, `60`, `240`, `1440`,
  `10080`, `21600`); the SDK's normalized interval mapping must reject anything
  outside that set during preflight.
- `ticker` publishes both sides with sizes, so it can serve the `Ticker`
  category. It is not a `L1Book` channel: Kraken's L1 equivalent is `book` with
  `depth=10`, and the SDK should not silently relabel a ticker event as an L1
  snapshot.
- Symbol form on the wire is `wsname` from `AssetPairs`, e.g. `XBT/USD`,
  `ETH/USDT`. Normalization needs an alias table (`XBT` → `BTC`) and must
  preserve the quote (`USD`, `USDT`, `EUR`, ...) rather than assuming USDT.
- Timestamps are ISO-8601 with nanoseconds; the normalized model uses Unix
  seconds as `f64`.

## Catalog

- `GET https://api.kraken.com/0/public/AssetPairs` returns, per pair,
  `altname` (e.g. `XBTUSD`), `wsname` (`XBT/USD`), `base`, `quote`, `status`,
  tick/lot size and precision fields.
- Discovery must be product-qualified and reject delisted/disabled pairs using
  the documented status field instead of assuming every returned pair is
  tradable.

## Rate limits and lifecycle

- Kraken documents a call counter for Spot REST and message limits for
  WebSocket; the SDK must count subscriptions per connection and honor the
  documented reconnect/backoff rules.
- Normal server close is reconnectable; an explicit shutdown stays clean.

## Open questions to resolve during implementation

All four questions were answered during implementation; see
[Implementation verification](#implementation-verification--2026-11-03).

1. Exact per-connection subscription limits and whether one `subscribe` frame
   may carry many symbols for the same channel (the examples show arrays).
2. Whether `book` updates can arrive before the snapshot when several symbols
   share a connection, and how the `snapshot` flag correlates with `type`.
3. The precise CRC32 input format (separator-free price/size concatenation) —
   it must be implemented from the official guide and covered by a fixture test.
4. `instrument` channel payload fields and whether the channel is stable enough
   to serve status changes for managed readiness.

## References

- [Ticker](https://docs.kraken.com/api/docs/websocket-v2/ticker),
  [Book](https://docs.kraken.com/api/docs/websocket-v2/book),
  [Trades](https://docs.kraken.com/api/docs/websocket-v2/trade),
  [Candles](https://docs.kraken.com/api/docs/websocket-v2/ohlc)
- [AssetPairs REST](https://docs.kraken.com/api/docs/rest-api/get-tradable-asset-pairs)
- [Kraken developer index](https://docs.kraken.com/llms.txt)

## Implementation verification — 2026-11-03

The public WebSocket v2 endpoint was probed on 2026-11-03. Observations are
sanitized wire-shape references, not retained captures.

### Framing, acknowledgements and lifecycle

- The server sends a `status` channel frame on connect
  (`{"channel":"status","type":"update","data":[{"system":"online",...}]}`) and
  periodic `{"channel":"heartbeat"}` frames. Both are control frames and are
  ignored by the runtime; neither is treated as a market event or a failure.
- Subscribe/unsubscribe are `method`-based JSON objects and accept a `symbol`
  array plus channel-specific `depth`/`interval`/`event_trigger`. The optional
  `req_id` is echoed in every acknowledgement, so the SDK sets it to correlate
  managed readiness.
- Success is one acknowledgement per requested symbol:
  `{"method":"subscribe","req_id":1,"result":{"channel":"ticker","symbol":
  "BTC/USD",...},"success":true,...}`. A rejection is
  `{"method":"subscribe","req_id":1,"success":false,"error":"..."}` and is a
  permanent configuration failure.
- Liveness is `{"method":"ping"}` / `{"method":"pong"}`. The SDK heartbeat
  policy pings every 30 s with a 60 s idle watchdog; a missing pong is an idle
  reconnect, not a protocol error.

### Symbols

- REST `AssetPairs` `wsname` uses legacy asset codes (`XBT/USD`, `XDG/USD`,
  `AAVE/XBT`), while WebSocket v2 accepts only canonical names. Live probes on
  2026-11-03 rejected `XBT/USD` and `XDG/USD` with
  `Currency pair not supported` and accepted `BTC/USD`, `DOGE/USD` and
  `AAVE/BTC`. The SDK therefore applies `XBT` -> `BTC` and `XDG` -> `DOGE` to
  both pair sides when building the wire symbol and the normalized symbol.
- Quote currencies are preserved (`USD`, `USDT`, `EUR`, ...); nothing is
  flattened to USDT.

### Channels

- `ticker` publishes both sides with sizes plus last trade statistics.
  Subscriptions use `event_trigger: "bbo"` so updates are driven by best-bid/ask
  changes rather than only by trades; the normalized model remains `Ticker`.
  It is not an `L1Book` channel.
- `trade` rows carry `side` (`buy`/`sell`), `price`, `qty`, `ord_type`,
  `trade_id` and an ISO-8601 `timestamp`.
- `ohlc` rows carry `interval_begin` (RFC 3339 with nanoseconds) and `interval`
  in minutes; the deprecated `timestamp` field is ignored. Intervals are
  `1/5/15/30/60/240/1440/10080/21600` minutes; the normalized vocabulary cannot
  express `21600` (15 days), so it is not enabled instead of being relabeled.
  Kraken does not mark bars final, so `closed` stays `None`.
- `book` rows carry object levels `{"price": ..., "qty": ...}`, a CRC32
  `checksum` and an ISO-8601 `timestamp`. `qty: 0` removes a level. Depth is
  `10/25/100/500/1000`; every checksum covers the top ten levels regardless of
  subscription depth.
- Timestamps are RFC 3339 UTC (`2026-11-03T07:02:36.699826Z`) and convert to
  Unix seconds.

### Book checksum and recovery

- The documented checksum algorithm was reproduced exactly against the official
  example (`3310070434`): top ten asks low-to-high then top ten bids
  high-to-low, each level formatted as price+qty with `.` removed and leading
  zeros stripped, concatenated and hashed with IEEE CRC-32.
- Live verification on 2026-11-03 confirmed the algorithm on every snapshot and
  update at `depth=25`/`depth=100`. At `depth=10`, updates only matched after
  truncating the local book to the subscribed depth: out-of-scope levels never
  receive a `qty: 0` removal, so keeping them changes the local top ten. The
  runtime therefore truncates to the subscribed depth after every push.
- A checksum mismatch clears the local book and enters the bounded in-session
  resnapshot path (unsubscribe/subscribe with `snapshot: true`, at most three
  cumulative requests per connection); a corrupt book is never published.
  Updates received while a replacement snapshot is pending are dropped, since
  the snapshot that follows subscription covers the full book.

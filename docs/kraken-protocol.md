# Kraken Spot WebSocket v2 protocol baseline

Status: **protocol reference for the phase-2 addition.** Facts were read from the
official Kraken documentation on 2026-11-03. No live capture was taken.

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

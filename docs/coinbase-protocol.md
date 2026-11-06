# Coinbase Advanced Trade public WebSocket protocol baseline

Status: **protocol reference for the monitoring-venue addition, implemented.**
Facts below were read from the current official Coinbase Developer
Documentation (CDP) on 2026-11-04 and cross-checked against the public
production WebSocket the same day (read-only probe; shapes only, no live capture
is committed). The machine-readable contract used for every field is the
official AsyncAPI document linked below. Fixtures in the test suite are
sanitized documented examples, not recordings.

## Endpoints and API version

| Surface | Endpoint | Authentication |
| --- | --- | --- |
| Market-data WebSocket | `wss://advanced-trade-ws.coinbase.com` | none for public channels |
| User-order WebSocket (out of scope) | `wss://advanced-trade-ws-user.coinbase.com` | JWT required |
| REST catalog | `https://api.coinbase.com/api/v3/brokerage/market/products` | none (public) |
| REST candles (reference only; not used by the runtime) | `https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/candles` | none (public) |

Sources (verified 2026-11-04):

- Advanced Trade WebSocket overview:
  <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview>
- WebSocket endpoint page:
  <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints>
- Advanced Trade WebSocket rate limits:
  <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-rate-limits>
- Official AsyncAPI 3.0 contract (channels, payload schemas, subscription
  frames): <https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json>
- Official OpenAPI contract (public products, public candles):
  <https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/advanced-trade-spec.yaml>
  (`GET /api/v3/brokerage/market/products`,
  `GET /api/v3/brokerage/market/products/{product_id}/candles`)

The feed targets the public **Advanced Trade** market-data WebSocket, not the
legacy Coinbase Exchange (Exchange API) feed. Coinbase's own documentation states
that most Advanced Trade market-data channels are available without
authentication; the SDK sends no `jwt` field at all.

## Subscription framing, acknowledgements and errors

One channel per subscription message; `product_ids` is required for every
market-data channel except `heartbeats`:

```json
{"type":"subscribe","channel":"ticker","product_ids":["BTC-USD"]}
{"type":"subscribe","channel":"level2","product_ids":["BTC-USD"]}
{"type":"subscribe","channel":"market_trades","product_ids":["BTC-USD"]}
{"type":"subscribe","channel":"candles","product_ids":["BTC-USD"]}
{"type":"subscribe","channel":"heartbeats"}
```

- The connection must receive a `subscribe` frame within five seconds; the
  server disconnects otherwise.
- Unsubscribe uses `{"type":"unsubscribe", ...}` with the same shape and receives
  a `subscriptions` message in response.
- Acknowledgement is a `subscriptions` message and it is **cumulative, not
  per-request**: `{"channel":"subscriptions","timestamp":"...","sequence_num":N,
  "events":[{"subscriptions":{"ticker":["BTC-USD"],"heartbeats":["heartbeats"]}}]}`.
  A successful subscription can merge several channels in one map; an empty
  `subscriptions` map means nothing was subscribed.
- Errors are a top-level envelope with no channel:
  `{"type":"error","message":"...\"}`. The documented/first-seen error text is
  free-form, so the adapter maps every error frame to a permanent
  `Error::Subscription` failure instead of burning retries.
- Unknown public channel names and malformed frames are rejected. An unknown
  `product_id` is *not* an error: the ack simply omits it. The catalog-driven
  preflight is therefore the only product-existence gate.
- All server frames carry `sequence_num`, a **per-connection** counter across
  every channel on that socket. It is not a per-product book sequence and is not
  used as one.

`channel` in the subscription request is not always the `channel` of the pushed
envelope: `level2` is subscribed by name but pushes `channel:"l2_data"`.

## Public channels and normalized mapping

| Subscribe channel | Push channel | Normalized category | Fields used |
| --- | --- | --- | --- |
| `ticker` | `ticker` | `Ticker` and `L1Book` | `events[].type` (`snapshot`/`update`), `product_id`, `best_bid`, `best_ask`, `best_bid_quantity`, `best_ask_quantity`, plus `price`/24h stats for reference |
| `market_trades` | `market_trades` | `Trade` | `trade_id`, `product_id`, `price`, `size`, `side`, `time` (RFC 3339) |
| `level2` | `l2_data` | `L2Book` | `events[].type`, `product_id`, `updates[].side` (`bid`/`offer`), `price_level`, `new_quantity`, `event_time` |
| `candles` | `candles` | `Candle` | `start` (Unix seconds as string), `open`, `high`, `low`, `close`, `volume`, `product_id` |
| `heartbeats` | `heartbeats` | control only | `heartbeat_counter`, `current_time` |

Notes:

- **`Ticker` / `L1Book` decision.** The AsyncAPI `Ticker` schema and the live
  frame both carry `best_bid_quantity` and `best_ask_quantity` alongside
  `best_bid`/`best_ask`, so the same `ticker` push is normalized twice: a
  `Ticker` (prices) and an `L1Book` (prices **with sizes**). There is no
  separate L1 endpoint; the adapter documents that `L1Book` is projected from
  the documented ticker BBO fields and is advertised only because those sizes
  are present.
- **`Trade.side`** is the aggressor/taker side, matching the SDK convention.
  Coinbase documents `MarketTrade.side` as "the maker's side of the trade", so
  the parser inverts it (`BUY` maker → sell aggressor, `SELL` maker → buy
  aggressor).
- `MarketTrade.time` is RFC 3339 with microseconds; it is parsed to Unix
  seconds (fraction preserved).
- `ticker` and `market_trades` also send a `snapshot` event first; the normalized
  adapter emits every row of both event kinds (last-write wins downstream).
- `candles` pushes `snapshot` then `update` events for the same five-minute
  bucket. There is no completion flag in the payload, so `Candle.closed` is
  `None` and the SDK never infers completion from wall-clock time.

### `market_trades` intra-frame ordering contract

Coinbase aggregates public trades into ~250 ms pushes but does not guarantee
that a `market_trades` batch is ordered. Live frames on 2026-11-06 contained
reversed adjacent trades, including the same-millisecond pair
`1116181183 @ 07:08:17.218455Z` followed by
`1116181182 @ 07:08:17.185577Z`.

The adapter normalizes **only a fully received frame**:

- every trade in the frame is kept, with its exact `time` (microseconds
  preserved as `exchange_ts`), `trade_id`, price, size and side;
- trades are emitted by exact exchange time ascending, then numeric
  `trade_id` ascending for equal timestamps, then symbol;
- sorting never crosses frames, sockets or channels. A genuinely late trade
  arriving in a later frame is emitted in that frame's order; there is no
  global time ordering, cross-frame buffer, deduplication or latest-only
  collapse;
- handler callbacks and the broadcast/anchored event streams receive the same
  normalized order, because both consume the adapter's ordered `Vec`.

Consumers that need to reject cross-frame stale prices must keep their own
window; this contract only removes intra-frame reversals. An application that
coarsens microsecond timestamps to milliseconds still owns equality/precision
handling at that boundary.

### Candles: five-minute-only cell

The public `candles` channel is **documented as five-minute buckets only**
("grouped into five-minute buckets, with updates every second"), and the payload
carries no interval field. The capability is advertised as a `Candles` cell whose
only accepted normalized interval is `5m`:

- `candles_interval("5m")` is accepted and emits `interval:"5m"`, `start` from
  the wire and `end = start + 300`.
- The default `1m` and every other interval fail capability preflight with
  `UnsupportedCapability`; no silent substitution to 5m is allowed.
- The REST candles endpoint (`granularity=ONE_MINUTE|FIVE_MINUTE|...`, max 350
  buckets) is documented and can backfill 1m history, but the runtime's candle
  path is stream-only; the 1m gap is recorded here rather than implemented by a
  hidden REST fallback.

## Order-book snapshot / sequence / checksum semantics

- `level2` is a **snapshot + absolute-update** stream: the first event for a
  product is `type:"snapshot"` with every price level; subsequent events are
  `type:"update"` rows whose `new_quantity` is the **updated absolute size** at
  `price_level` (not a delta), and `new_quantity == "0"` removes the level.
- There is no published checksum and no per-product sequence number. The
  documented guarantee is delivery of all updates ("the easiest way to keep an
  order book snapshot in sync"). `sequence_num` is connection-wide and is only
  exposed for recording/diagnostics, never used to gap-detect a book.
- Fail-closed rules implemented by the sync state machine:
  - an `l2_data` event for a configured product whose `events[].type` is
    missing/unknown is rejected;
  - an `update` event received before that product's snapshot is rejected as an
    out-of-order frame;
  - a row with unknown `side`, missing/!finite `price_level`, or
    missing/non-`Decimal` `new_quantity` is rejected;
  - a snapshot for an unsubscribed `product_id` is ignored; an update for an
    unsubscribed product is ignored;
  - reconnect clears the book state before the next session, and recovery is the
    documented reconnect (there is no in-session resnapshot contract).
- Snapshot size is large (the BTC-USD snapshot observed on 2026-11-04 had
  ~40,000 levels); the shared WebSocket frame limit is 8 MiB and covers it.

## Catalog and product identity

`GET /api/v3/brokerage/market/products` with `product_type=SPOT`, `limit`, and
`cursor` (base64 cursor; response `pagination.next_cursor`) returns
`{"products":[...],"num_products":N,"pagination":{...}}`. Fields used:

| Native field | Use |
| --- | --- |
| `product_id` | native symbol (`BTC-USD`); normalized as `BASE-QUOTE` through `base_currency_id` / `quote_currency_id` cross-checked against the native form |
| `base_currency_id`, `quote_currency_id` | normalized `Symbol::spot(base, quote)`; a contradiction with `product_id` fails the catalog instead of guessing |
| `status` | surfaced as `MarketInfo.native_status`; not a subscription gate by itself |
| `trading_disabled`, `is_disabled`, `cancel_only`, `auction_mode` | surfaced as native status context only |
| `base_increment` | `MarketInfo.quantity_increment` |
| `quote_increment` / `price_increment` | `MarketInfo.price_increment` (`price_increment` is preferred when present; `quote_increment` is the documented minimum price increment) |
| `base_min_size` / `quote_min_size` | `minimum_quantity` / `minimum_notional` |

Only products with `product_type == "SPOT"` are accepted (the venue also lists
derivatives under other product types; those remain out of scope and are
skipped). Duplicate normalized symbols or a base/quote contradiction fail the
catalog rather than producing an ambiguous registry.

## Heartbeat, liveness and idle policy

- `heartbeats` is a documented public channel that pushes every second with a
  monotonically increasing `heartbeat_counter`. The adapter subscribes it on
  every market-data connection; it is a control frame, not a normalized event.
- The feed sends no client heartbeat: Coinbase's server pushes keep the
  connection and the SDK's idle watchdog alive. `HeartbeatPolicy` uses no client
  text payload and a 90 s idle timeout (server heartbeat cadence is 1 s).

## Rate limits

- Advanced Trade WebSocket: connections and unauthenticated messages are each
  limited to **8 per second per IP** (official page above). The SDK paces
  subscription sends through the shared send budget, so a multi-channel feed
  never bursts past the documented unauthenticated-message rate.
- REST public products/candles are documented under the Advanced Trade REST
  rate limits; the catalog fetch is a single paginated read at hydration and the
  runtime never polls it in a loop.

## Open questions / explicit gaps

- The five-minute-only candle cell is a deliberate capability limitation, not a
  conversion: 1m candles would require a REST backfill path that does not exist
  in the normalized stream runtime today.
- The AsyncAPI does not define a per-product book sequence or a checksum; the
  implementation therefore never synthesizes one. A dropped connection is
  recovered by resubscribing and rebuilding from the next snapshot.
- Public channel errors are free-form text; the adapter fails the session on any
  `type:"error"` frame and lets the supervisor reconnect rather than pattern
  matching individual messages.
- The `status` channel is documented but has no normalized category today; it is
  not subscribed.

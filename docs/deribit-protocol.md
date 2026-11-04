# Deribit JSON-RPC v2 public market-data protocol baseline

Status: **protocol reference for the monitoring-venue addition, implemented.**
Facts below were read from the current official Deribit documentation on
2026-11-04 and cross-checked against the public production JSON-RPC WebSocket the
same day (read-only probe; shapes only, no live capture is committed). Fixtures
in the test suite are sanitized documented examples, not recordings.

## Endpoints and API version

| Surface | Endpoint |
| --- | --- |
| Production JSON-RPC 2.0 WebSocket | `wss://www.deribit.com/ws/api/v2` |
| Testnet WebSocket (not used by the runtime) | `wss://test.deribit.com/ws/api/v2` |
| Production REST (`public/get_instruments` and the same JSON-RPC methods) | `https://www.deribit.com/api/v2/...` |

Sources (verified 2026-11-04):

- API home / server list: <https://docs.deribit.com/>
- JSON-RPC overview: <https://docs.deribit.com/articles/json-rpc-overview>
- Rate limits (credit system): <https://docs.deribit.com/articles/rate-limits>
- `public/get_instruments`:
  <https://docs.deribit.com/api-reference/market-data/public-get_instruments>
- `public/subscribe`:
  <https://docs.deribit.com/api-reference/subscription-management/public-subscribe>
- `public/set_heartbeat`:
  <https://docs.deribit.com/api-reference/session-management/public-set_heartbeat>
- `book.{instrument_name}.{interval}`:
  <https://docs.deribit.com/subscriptions/orderbook/bookinstrument_nameinterval>
- `trades.{instrument_name}.{interval}`:
  <https://docs.deribit.com/subscriptions/trades/tradesinstrument_nameinterval>
- `ticker.{instrument_name}.{interval}`:
  <https://docs.deribit.com/subscriptions/market-data/tickerinstrument_nameinterval>
- `chart.trades.{instrument_name}.{resolution}`:
  <https://docs.deribit.com/subscriptions/market-data/charttradesinstrument_nameresolution>

The protocol is JSON-RPC 2.0 over one WebSocket. Public methods need no
authentication. Normalized scope is **perpetual and dated futures only**;
options (and option/future combos) stay unsupported and are skipped by the
catalog plus rejected by capability preflight.

## Subscription framing, acknowledgements and errors

Requests are JSON-RPC 2.0 method calls with client ids:

```json
{"jsonrpc":"2.0","id":1,"method":"public/subscribe","params":{"channels":["book.BTC-PERPETUAL.100ms","ticker.BTC-PERPETUAL.100ms"]}}
{"jsonrpc":"2.0","id":2,"method":"public/unsubscribe","params":{"channels":["book.BTC-PERPETUAL.100ms"]}}
{"jsonrpc":"2.0","id":3,"method":"public/set_heartbeat","params":{"interval":30}}
```

- Success: `{"jsonrpc":"2.0","id":<id>,"result":["channel",...]}`, where `result`
  is the list of channels actually subscribed (observed order can differ from the
  request order; the adapter does not assume it matches).
- Error: JSON-RPC error object `{"jsonrpc":"2.0","id":<id>,"error":{"code":N,
  "message":"..."}}`; rate-limit errors carry the documented `too_many_requests`
  code family and are surfaced as permanent subscription failures for that
  session (the supervisor reconnects with backoff).
- Notifications have no `id` and carry `method:"subscription"`:
  `{"jsonrpc":"2.0","method":"subscription","params":{"channel":"...","data":...}}`.
- Heartbeats: the client enables them with
  `public/set_heartbeat {"interval": <10..300>}`. The server then sends
  `{"method":"subscription","params":{"type":"heartbeat"}}` for idle periods and
  `...{"type":"test_request"}` when it wants proof of life; the client must
  answer with a JSON-RPC `public/test` request. Any client text message counts as
  activity. The SDK sends a periodic `public/test` (30 s) through the shared
  heartbeat policy and filters the `{"result":"ok"}` response; a documented close
  code 4000 follows prolonged silence.
- `public/subscribe` is rate limited to roughly 3.3 requests/second (burst 10);
  `public/get_instruments` to 1 request/second (burst 50). The runtime sends one
  subscription call per connection at hydration time and never polls.

## Public channels and normalized mapping

| Channel | Normalized category | Payload fields used |
| --- | --- | --- |
| `ticker.{instrument}.{interval}` | `Ticker`, `MarkPrice`, `IndexPrice`, `OpenInterest`, `Funding` | `best_bid_price`, `best_ask_price`, `mark_price`, `index_price`, `open_interest`, `current_funding`, `funding_8h`, `timestamp` (ms) |
| `trades.{instrument}.{interval}` | `Trade` | `trade_id`, `trade_seq`, `instrument_name`, `timestamp` (ms), `direction` (taker), `price`, `amount` (USD for perps/futures), `contracts`, `index_price`, `mark_price` |
| `book.{instrument}.{interval}` | `L2Book` | `type` (`snapshot`/`change`), `change_id`, `prev_change_id`, `instrument_name`, `timestamp` (ms), `bids`/`asks` rows `[action, price, amount]` |
| `chart.trades.{instrument}.{resolution}` | `Candle` | `tick` (ms), `open`, `high`, `low`, `close`, `volume`, `cost` |

Notes:

- One public `ticker` payload is split into up to five normalized categories:
  - `Ticker` from `best_bid_price`/`best_ask_price`;
  - `MarkPrice` from `mark_price`;
  - `IndexPrice` from `index_price`;
  - `OpenInterest` from `open_interest`;
  - `Funding` with `rate = current_funding` (and `funding_8h` recorded in the
    field table below; `Funding.mark_price` carries the same payload's
    `mark_price`).
  Each category is published independently and only when the feed subscribed it;
  the native topic is subscribed exactly once.
- `trades.data` is an **array** of trades per notification; the adapter
  normalizes every element.
- `Trade.side` is the aggressor/taker direction (`buy`/`sell` as documented).
- `interval` accepts `100ms`, `agg2` and `raw`; **`raw` is only available to
  authorized users**, so unauthenticated feeds always request `100ms`. The
  normalized L2 subscription therefore has no user-selectable interval.
- `Funding`: `current_funding` is the current funding rate; `funding_8h` is the
  8-hour funding rate. No next-funding timestamp is transmitted by this channel,
  so `Funding.next_funding_time` stays `None`. Nothing is synthesized.
- `OpenInterest.open_interest` keeps the venue-native unit. Deribit documents
  book/order amounts for perpetuals and futures in **USD units**, and
  `open_interest` follows the same convention; the parser preserves it and does
  not invent a base-coin conversion.
- Book amounts are likewise native USD notional for perpetuals and futures
  (`amount` unit documented by the book channel). `L2Book` level `amount` is
  preserved verbatim; this matches the existing inverse-contract policy of the
  SDK (native units are documented, not converted).
- Option instruments are never subscribed. If a ticker payload contains option
  fields (`bid_iv`, `greeks`, ...), they are ignored; the instrument-kind
  gate is the catalog plus capability preflight.

### Candles

`chart.trades.{instrument}.{resolution}` maps normalized intervals to the
documented resolution enum:

| Normalized | Wire resolution |
| --- | --- |
| `1m` | `1` |
| `3m` | `3` |
| `5m` | `5` |
| `10m` | `10` |
| `15m` | `15` |
| `30m` | `30` |
| `1h` | `60` |
| `2h` | `120` |
| `3h` | `180` |
| `6h` | `360` |
| `12h` | `720` |
| `1d` | `1D` (or `D`) |

- `tick` is the bucket start in milliseconds → `start` seconds; `end` is derived
  from the normalized interval (`start + interval_seconds`). There is no closed
  flag, so `Candle.closed` is `None` and completion is not inferred.
- The docs note a filling sample is emitted for a resolution period without
  trades; it reuses the previous candle's open/close values. The adapter
  normalizes the documented shape regardless; consumers should treat volume as
  authoritative.
- `volume` is the documented candle volume; `cost` is not mapped (no normalized
  field).

## Order-book snapshot / delta and resnapshot semantics

- The first notification on `book.{instrument}.{interval}` is a full snapshot:
  `type:"snapshot"` with every price level and no `prev_change_id`.
- Every later notification is `type:"change"` with `change_id` and
  `prev_change_id`. `prev_change_id == previous change_id` proves no messages
  were missed.
- Delta rows are `[action, price, amount]` where `action` is `new`, `change` or
  `delete`; amount is the new absolute size at the price level.
- The sync state machine:
  - buffers/ignores a change before the snapshot (pre-snapshot change is
    out-of-order and fails closed);
  - drops a stale change whose `change_id` is not greater than the local
    anchor;
  - on `prev_change_id != local change_id` reports a gap and enters a **bounded
    in-session resubscribe**: `public/unsubscribe` then `public/subscribe` for
    the same channel, at most three requests per connection; changes arriving
    while the replacement snapshot is pending are dropped, and the replacement
    snapshot resets the book;
  - a malformed row (unknown action, missing price/amount, non-finite values)
    fails the session;
  - reconnects clear the book before the next session.

## Catalog and product identity

`public/get_instruments` is called over REST:
`GET https://www.deribit.com/api/v2/public/get_instruments?currency=any&kind=future&expired=false`
(public, no auth; response `{"jsonrpc":"2.0","id":N,"result":[...]}`).

| Native field | Use |
| --- | --- |
| `kind` | must be `future`; options/combos are skipped |
| `instrument_name` | native symbol (`BTC-PERPETUAL`, `BTC-25DEC26`) |
| `base_currency`, `quote_currency` | normalized `Symbol` base/quote |
| `settlement_period` | `perpetual` → `InstrumentKind::Perpetual`; calendar periods → dated `InstrumentKind::Futures` |
| `expiration_timestamp` | dated expiry (ms) → normalized `YYMMDD`; perpetual instruments also carry a far-future value, which is never treated as a dated expiry |
| `state` | only `open` rows are accepted (the only state that accepts orders) |
| `is_active` | must be `true` |
| `instrument_type` | `linear`/`reversed`, surfaced as native contract type |
| `contract_size`, `min_trade_amount`, `tick_size` | `MarketInfo.contract_value`, `quantity_increment`/`minimum_quantity`, `price_increment` |
| `settlement_currency` | `MarketInfo.settlement_currency` |

Product-qualified identity: the same base can have both `BTC-PERPETUAL`
(quote `USD`, inverse) and `BTC_USDC-PERPETUAL` (quote `USDC`, linear); the
normalized symbols differ (`BTC-USD-PERP` vs `BTC-USDC-PERP`) and the dated form
adds the expiry suffix (`BTC-USD-261225`). A base/quote contradiction between the
instrument name and the catalog fields fails the catalog instead of guessing.

## Heartbeat, liveness and idle policy

- Client heartbeat: `public/set_heartbeat` with `interval` in 10..300 s. The
  adapter enables 30 s and also sends a `public/test` every 30 s through the
  shared text heartbeat policy; JSON-RPC `{"result":"ok"}` responses are filtered
  as control frames.
- Idle watchdog: 90 s (documented close happens after `2*interval + 500 ms` of
  client silence; the SDK stays well inside that).
- Heartbeat/test responses are never normalized; `public/test` has no market
  data.

## Rate limits

- Credit-based per sub-account; `public/subscribe` sustained ~3.3 req/s (burst
  10) and `public/get_instruments` 1 req/s (burst 50) per the official table.
- The SDK sends one subscribe call per connection and fetches the catalog once
  per hydration; no per-tick REST traffic is added.

## Open questions / explicit gaps

- Spot instruments are outside the selected scope; the catalog filters
  `kind=future` and capability preflight rejects spot.
- `funder` fields (`interest_value`, `estimated_delivery_price`, option Greeks)
  have no normalized equivalent and are not published.
- The `book.{instrument}.{group}.{depth}.{interval}` grouped channel is
  documented but changes the level aggregation/depth semantics; the SDK targets
  the full book channel and rejects a requested L2 depth at preflight rather
  than silently subscribing a grouped stream.
- No public liquidation stream is documented for derivatives on this protocol;
  `Liquidations` stays unsupported for Deribit.

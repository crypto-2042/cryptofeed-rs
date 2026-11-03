# KuCoin UTA WebSocket V2 protocol baseline

Status: **protocol reference for the phase-1 addition.** Facts below were read
from the official KuCoin documentation on 2026-11-03. No live capture was taken
for this note; implementation must add sanitized fixtures from the documented
examples and confirm the open questions during verification.

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

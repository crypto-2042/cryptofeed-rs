# Korean CEX design and implementation plan — 2026-11-03

Status: **plan; both phases implemented on 2026-11-03** (Upbit phase A,
Bithumb phase B). Selection and live checks:
[research](korean-cex-research-2026-11-03.md). Protocol baselines:
[Upbit](upbit-protocol.md), [Bithumb](bithumb-protocol.md).

## Scope

In scope: public, unauthenticated spot market data for Upbit (phase A) and
Bithumb (phase B), mapped onto the existing normalized model and capability
preflight. Out of scope: authenticated streams, trading, new normalized
categories, Korean derivatives, regional Upbit hosts, and any change to delivery,
retention, readiness or lossless semantics.

## Capability rows

| Venue | Product | Channels | Notes |
| --- | --- | --- | --- |
| Upbit | Spot | `Trade`, `L2Book`, `Candles` | `orderbook` is snapshot-only L2 with documented 1/5/15/30 units (default 30); `candle.{unit}` covers 1s/1m/3m/5m/10m/15m/30m/60m/240m |
| Bithumb | Spot | `Trade`, `L2Book` | `orderbook` is a snapshot with up to 15 units and no documented depth parameter; candles exist only as REST history |

Deliberate gaps, kept visible instead of hidden:

- **`Ticker` is not advertised for either venue.** Both public `ticker` frames are
  last-price/24-hour-statistics snapshots with no bid/ask, while the normalized
  `Ticker` model is a BBO contract. Mapping `trade_price` to both sides would
  publish a fabricated zero-spread quote; the capability matrix therefore rejects
  `Ticker` at preflight for Upbit and Bithumb.
- **`L1Book` is not advertised.** Neither venue documents a native top-of-book
  channel, and deriving one from the orderbook stream would be a new contract.
- **No Korean derivatives row.** Neither venue offers public derivative market
  data under this SDK scope.
- **Bithumb live candles are not offered.** The venue documents no public WS
  candle stream. A polling feed would add a new delivery model; the bounded REST
  candle-history surface is not extended in this increment (same scope as Kraken
  and KuCoin). The documented REST endpoints and their shape are recorded in the
  protocol baseline so a later increment can pick them up.

## Layer-by-layer changes

| Layer | Change |
| --- | --- |
| `crates/core` | Add `ExchangeId::Upbit` and `ExchangeId::Bithumb` (the enum is `#[non_exhaustive]`, so this is additive). No model changes: both venues reuse `Trade`, `L2Book` and (Upbit) `Candle`. |
| `crates/runtime/src/exchange/{upbit,bithumb}/` | New `adapter.rs` (endpoints, ticket/type/codes subscription frames, error envelopes, capability helpers), `parser.rs` (documented payload → normalized models), `book_sync.rs` (snapshot-only L2 replacement; a non-snapshot shape fails closed). |
| `markets.rs` | Capability rows per exchange/product (see above); catalog fetch for `GET https://api.upbit.com/v1/market/all?is_details=true` and `GET https://api.bithumb.com/v1/market/all?isDetails=true`; product-qualified normalization that preserves the quote prefix exactly (`KRW-BTC` → `BTC-KRW`). |
| `planning.rs` / `runtime.rs` / `runtime/connection.rs` | Route planning (`wss://api.upbit.com/websocket/v1`, `wss://ws-api.bithumb.com/websocket/v1`), `planned_url`, heartbeat policy (text `PING` / `{"status":"UP"}`), `consume_*_feed` dispatch arms, orderbook snapshot path. |
| `runtime/readiness.rs` | The two venues document no subscription acknowledgement. Their subscription frames are confirmed when sent and a documented error envelope still fails the session; other venues keep their acknowledgement rules unchanged. |
| validation | Per-exchange channel sets, Upbit candle interval set, Upbit L2 unit counts (`1/5/15/30`, default 30), no Bithumb depth parameter; unsupported products/channels/units/depths rejected in preflight. |
| docs | `PARITY.md` matrix rows and protocol gates, `README` exchange list and capability tables, `docs/feature-status.html`, `docs/exchange-protocol-baseline.md`, `docs/README.md`, `sample_data/SOURCES.md`, `PROGRESS.md`, `MEMORY.md`. |

## Book rules

Upbit:

- Each `orderbook` push is a complete snapshot for the selected unit count. The
  local book is replaced, never merged; there is no sequence, checksum or delta
  rule to implement and none is invented.
- `l2_book_depth` selects the documented unit count through the `{code}.{count}`
  subscription form. The documented default (30) is sent as a plain code. A push
  whose unit pairing is inconsistent (different bid/ask level counts), empty, or
  larger than the requested count is malformed and fails closed.

Bithumb:

- Each `orderbook` push is a complete snapshot of at most 15 units. It replaces
  the local book. No depth parameter is accepted, because the documented `level`
  field is a price-aggregation unit rather than a level count.
- No delta, sequence or checksum shape is accepted; anything else fails closed.

## Ordered work

1. Core enum, module skeletons, capability rows, catalog fetch and symbol
   normalization; reject every unsupported Upbit/Bithumb product, channel, candle
   unit and depth during preflight.
2. Upbit parsers (`trade`, `orderbook`, `candle.{unit}`) with documented-frame
   fixtures; adapter (subscription frame, error envelope, heartbeat); snapshot
   book sync; runtime consume/dispatch; full-session WebSocket double.
3. Bithumb parsers (`trade`, `orderbook`), adapter for the verified endpoint and
   framing, snapshot book sync, runtime consume/dispatch, full-session double.
4. Readiness confirmation policy for the two ack-less venues.
5. Capability/docs updates: PARITY rows, feature status, README, baseline notes,
   sample-data provenance, progress/memory notes.
6. Verification: workspace tests, strict Clippy, isolated features, MSRV 1.85 and
   formatting. Live long-run qualification stays separate, as for every other
   venue.

## Verification and acceptance

- Deterministic gates must pass for every feature combination touched by the new
  exchange modules; the capability matrix stays authoritative.
- Parser fixtures come from the documented examples in the protocol baselines.
  No live-network test is added.
- A new venue is "implemented" only when preflight, catalog discovery, parsers,
  book sync, adapter and full-session double exist and its PARITY row is filled.

## Risks

- Both venues share one ticket/type/codes framing family, and their documents
  mix snake-case and camelCase spellings of the optional snapshot flags and copy
  examples between venues. The SDK omits the optional flags entirely (documented
  default: snapshot and realtime) and parses only the documented `DEFAULT` field
  names, so an undocumented alias cannot silently change behavior.
- Bithumb documents recent public-WebSocket maintenance and optional message
  compression. The SDK does not request compression and treats scheduled
  connection drops as ordinary reconnects.
- KRW is a fiat quote. Any future symbol mapping must keep it explicit; the
  implementation never derives a quote currency from an asset name.
- Snapshot-only books have no recovery signal: a missed push is invisible until
  the next push. That is a documented venue property, not a defect the SDK may
  paper over with invented sequencing.

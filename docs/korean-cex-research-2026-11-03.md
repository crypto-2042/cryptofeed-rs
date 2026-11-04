# Korean CEX public-API research and selection — 2026-11-03

Status: **research and selection only.** Facts were read from each venue's
current official documentation on 2026-11-03 and checked against the live public
REST endpoints from this host. No venue WebSocket capture is committed.

## Evaluation criteria

Same criteria as the previous expansion: unauthenticated public WebSocket market
data covering several normalized categories, an official REST catalog that
yields product-qualified symbols, current official documentation that can be
archived, and no new normalized category or delivery-semantics change.

## Candidate matrix

| Venue | Products | Public WS channels | Gaps | Verdict |
| --- | --- | --- | --- | --- |
| [Upbit](https://global-docs.upbit.com/llms.txt) | spot (KRW/BTC/USDT) | `ticker`, `trade`, `orderbook` (full snapshot, 1/5/15/30 units), `candle.{1s,1m,3m,5m,10m,15m,30m,60m,240m}` | spot only; no derivatives, no L1 channel (orderbook is the book source) | **Selected, phase A** |
| [Bithumb](https://apidocs.bithumb.com/llms.txt) | spot (KRW) | public WebSocket `ticker`, `trade`, `orderbook`; REST candles | spot only; public WS maintenance/compression notices to track | **Selected, phase B** |
| [Coinone](https://docs.coinone.co.kr/llms.txt) | spot (KRW) | public WebSocket `ORDERBOOK`, `TICKER`, `TRADE`, `CHART` | small venue; chart frame is not the normalized candle shape | Deferred |
| [Korbit](https://apidocs.korbit.co.kr/) | spot (KRW) | legacy WS ticker/orderbook/trade | API migrated to Mirae Asset `digitalx.miraeasset.com`; the documented host is changing | Deferred until the migration settles |
| Bithumb/Upbit derivatives | — | — | neither offers public derivative market data under this SDK scope | Not applicable |

Live checks on 2026-11-03:

- `GET https://api.upbit.com/v1/market/all` returned 875 markets: 299 KRW, 331
  BTC, 245 USDT. KRW quotes are reachable from this host.
- `GET https://api.korbit.co.kr/v1/ticker?symbol=btc_krw` still answers, but the
  documentation points at the Mirae Asset host.
- `GET https://api.coinone.co.kr/public/v2/markets/KRW` answered normally.

## Why Upbit first

- Largest Korean venue with an official, machine-readable documentation index
  (`llms.txt`) in both English (global) and Korean (domestic) form.
- One subscription object per data type with `codes` arrays; ticker/trade/
  orderbook/candle all run on the same public quotation endpoint.
- `orderbook` frames are complete snapshots with a configurable unit count
  (1/5/15/30, default 30), which maps onto the existing snapshot-only L2 path
  without inventing delta rules.
- `candle.<unit>` frames carry `candle_date_time_utc` and a closed-state
  convention that the existing candle model can represent.

## Why Bithumb second

- Second-largest Korean venue with current REST v2 and public WebSocket
  documentation (`ticker`/`trade`/`orderbook`).
- Same normalized shape as Upbit (snapshot orderbook, KRW quotes), so the
  adapter pattern is shared.
- Documentation lists recent public-WebSocket maintenance and message
  compression changes; the protocol baseline must verify the current endpoint
  and frame encoding before implementation.

## Normalization consequences

- New `ExchangeId` variants `Upbit` and `Bithumb`; both are spot-only rows in the
  capability matrix with `Ticker`, `Trade`, `L2Book` and `Candles`.
- KRW is a fiat quote. `Symbol::spot(base, quote)` already preserves arbitrary
  quotes, so `KRW-BTC` maps to `BTC-KRW`; no quote-currency guessing is added.
- Upbit's orderbook packets are full snapshots: each push replaces the local
  book (snapshot-only mode), and there is no checksum/sequence rule to invent.
- No L1 cell is advertised: the orderbook stream is the book source, and
  deriving L1 would be a new contract.
- Korean market states (investment caution, trading suspension, delisting) are
  catalog-status concerns; they must be read from the official fields rather
  than inferred from missing data.

## Deferred / rejected

- **Coinone**: documentation is adequate, but the venue's public WS `CHART`
  frame is not the normalized candle shape and its liquidity is well below the
  two selected venues; revisit after the first Korean increment.
- **Korbit**: the API domain and documentation are migrating to Mirae Asset
  Digital X (`api.digitalx.miraeasset.com`); pinning a protocol baseline now
  would capture a moving target.
- **Korean derivatives**: not publicly offered under the venues' public APIs.

## Next documents

Per-venue protocol baselines and the implementation plan accompany this note:
`upbit-protocol.md`, `bithumb-protocol.md`, `korean-cex-plan-2026-11-03.md`.

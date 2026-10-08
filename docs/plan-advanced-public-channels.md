# Advanced Public Channels Plan

Status: planning baseline verified 2026-08-04; implementation order follows
Section 4. This plan covers the two in-progress workstreams moved out of the
deferred scope:

- Task A — funding and liquidations channels for the four non-Binance
  exchanges (Bitget v3, Bybit v5, OKX v5, Gate.io v4).
- Task B — options, L1/L3 books, open interest, and index price data.

This plan follows the harness contract in `docs/harness.md`: every supported
cell requires current official documentation, a sanitized sourced fixture, an
inline parity assertion, and (where live evidence is meaningful) a manual smoke
run. The authority order from `docs/exchange-protocol-baseline.md` applies:
official docs first, sanitized captures second, the normalized contract third,
Python `cryptofeed` last.

Release-scope note (2026-10-08): Task B option and MARGIN product work is
retained as implementation research only. The 0.1 capability matrix supports
spot, perpetual/swap, and dated futures; Options and MARGIN fail preflight and
require a new explicit plan before being reopened.

## 1. Task A — Funding and Liquidations outside Binance

### 1.1 Verified target matrix (official docs checked 2026-08-04)

| Exchange | Funding | Liquidations |
| --- | --- | --- |
| Binance | supported (existing) | supported (existing) |
| Bitget v3 | supported — fundingRate / nextFundingTime embedded in derivative `ticker` (2026-10-08 verification) | `liquidation` (public, instType-scoped) |
| Bybit v5 | `funding.{symbol}` (linear/inverse) | `allLiquidation.{symbol}` (linear/inverse) |
| OKX v5 | `funding-rate` (SWAP only) | `liquidation-orders` (SWAP/FUTURES/MARGIN) |
| Gate.io v4 | supported (embedded in `futures.tickers`; no funding-time field, so the normalized funding carries rate + mark price with the applicable time unset — mirrors Python `cryptofeed`) | supported for perpetuals — public `futures.public_liquidates`; authenticated `futures.liquidates` is a separate user channel; delivery remains unverified |

A cell marked unsupported must fail explicitly in the capability matrix, never
subscribe silently to nothing.

### 1.2 Protocol facts per exchange

#### Bybit v5 (linear and inverse endpoints only)

- Funding is carried by derivative `tickers.{symbol}`; the retired
  `funding.{symbol}` topic is not subscribed.
- Channel `allLiquidation.{symbol}` (replaces the deprecated
  `liquidation.{symbol}`) pushes at ~500 ms: `T`, `s`, `S`, `v`, `p`; `S` is
  the liquidated position side and is inverted to the SDK aggressor side.
  Available on `/v5/public/linear` and `/v5/public/inverse`.
- No funding or liquidation channel exists on the spot endpoint; spot
  combinations must stay unsupported.

Official references:
- [Bybit v5 WebSocket guide](https://bybit-exchange.github.io/docs/v5/ws/connect)
- [Bybit v5 liquidation channel](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation)
- [Bybit v5 funding channel](https://bybit-exchange.github.io/docs/v5/websocket/public/funding)

#### OKX v5

- Channel `funding-rate` applies to SWAP instruments only: `instId`,
  `fundingRate`, `fundingTime`, `nextFundingTime`.
- Channel `liquidation-orders` covers SWAP, FUTURES, OPTION, and MARGIN
  instruments: `instId`, `px`, `sz`, `notionalUsd`, `side`, `ts`, `bkPx`,
  `uly`, `clOrdId`, `ordId`.
- Both are public channels on `/ws/v5/public`; they keep the standard
  `arg.channel`/`arg.instId` subscription envelope.

Official references:
- [OKX WebSocket overview](https://www.okx.com/docs-v5/en/#overview-websocket)

#### Bitget v3

- Channel `liquidation` is public and scoped by `instType` (`usdt-futures`,
  `usdc-futures`, `coin-futures`), not by symbol: the server pushes the
  platform-wide stream. Since 2026-06-23 it pushes once per second, containing
  at most the largest long and largest short liquidation per trading pair.
  Payload rows: `symbol`, `side`, `price`, `amount`, `ts`.
- The v3 API has no separate funding-rate topic, but derivative `ticker`
  carries `fundingRate` and `nextFundingTime`. Funding/OI/index/mark price
  extraction and spot/contract `books1` L1 were completed on 2026-10-08.

Official references:
- [Bitget v3 liquidation channel](https://www.bitgetapp.com/api-doc/uta/websocket/public/Liquidation-Channel)
- [Bitget v3 change log](https://www.bitget.com/api-doc/uta/changelog)

#### Gate.io v4

- No independent funding channel: `futures.tickers` carries `funding_rate`
  and `mark_price` but no funding-time field. The normalized funding model
  keeps the applicable time unset (`next_funding_time: None`), carrying rate
  plus mark price; the Python `cryptofeed` baseline follows the same shape.
  Delivery contracts do not have funding and their ticker rows carry no
  `funding_rate`, so non-funding rows never emit a funding event.
- Current contract size/open interest (`total_size`), index price
  (`index_price`), and mark price (`mark_price`) ride the same
  `futures.tickers` stream; no USD open-interest value is carried there.
- The `book_ticker` stream (spot and futures) doubles as the L1 top-of-book
  channel: best bid/ask with sizes (`b`/`B`/`a`/`A`).
- The delivery WebSocket serves `futures.*` channel names, not `delivery.*`
  (verified live 2026-08-07).
- `futures.liquidates` requires authentication (user's own liquidations).
  The separate `futures.public_liquidates` channel is public and now supports
  perpetual liquidation snapshots (2026-10-08 correction); delivery remains
  unverified.

### 1.3 Implementation phases

Each phase completes the full harness loop and updates `PARITY.md` and the
capability matrix in the same change.

1. **Bybit v5 funding + liquidations** — DONE (2026-08-04): `funding.{symbol}`
   and `allLiquidation.{symbol}` on linear/inverse, capability matrix updated,
   fixtures and parity assertions added. Establishes the shared
   parser/dispatch pattern for the other exchanges.
2. **OKX v5 funding-rate + liquidation-orders** — DONE (2026-08-05):
   SWAP-only funding gating (futures funding fails preflight while futures
   liquidations stay supported), fixtures and parity assertions added.
3. **Bitget v3 liquidation** — DONE (2026-08-05): instType-scoped subscription
   without `symbol`, per-row symbol resolution against the product catalog,
   fixtures and parity assertions added.
4. **Gate.io v4** — DONE (2026-08-06/07): funding, open interest, index
   price, and mark price ride the derivative `futures.tickers` stream (rate +
   mark price without a funding time; OI plus USD value; index and mark
   price), L1 top-of-book comes from the `book_ticker` stream, capability
   matrix opened accordingly, fixtures and parity assertions added.
   Public perpetual liquidations were added on 2026-10-08 through
   `futures.public_liquidates`; delivery liquidation support stays rejected.
   The delivery endpoint prefix was verified live as `futures.*` (2026-08-07).

Phase gate: each supported cell has (a) a baseline entry with the verification
date, (b) a sanitized sourced fixture in `sample_data/`, (c) inline parity
assertions covering normalization, decimal/timestamp precision, and a control
or malformed case, and (d) a capability-matrix cell that permits exactly the
verified combination.

## 2. Task B — Options, L1/L3 Books, Open Interest, and Index Price

### 2.1 Scope decisions (to be re-verified per item during implementation)

| Item | Initial assessment | Open questions for implementation |
| --- | --- | --- |
| Open interest | Bitget/Bybit/Gate.io expose OI inside derivative ticker streams; OKX uses `open-interest`. No verified official Binance perpetual/dated-futures OI WS path was found; its current OI is single-symbol REST (weight 1). | Binance REST polling explicitly deferred on 2026-10-08 due to multi-symbol rate-limit exposure; reopening requires an explicit plan. See [decision record](binance-open-interest-decision.md). |
| Index price | Public index streams exist (Bybit `index-price.{symbol}`, OKX `index-tickers`, Binance index price streams). Requires a new model crate and `Channel::Index`. | Which products expose an index; overlap with mark price. |
| L1 books | Some exchanges expose top-of-book streams (Bybit `orderbook.1`, OKX `bbo-tbt`, Binance spot `bookTicker` is already consumed as Ticker). Requires `Channel::L1Book` plus a distinct model. | Whether L1 should reuse the existing Ticker mapping or become an independent channel. |
| L3 books | No current public L3 stream across the five active exchanges (L3 depth is typically private or absent). Likely remains unsupported unless a verified official public channel is found. | Confirm against current official docs before declaring. |
| Options | Binance/Bybit/OKX expose options products (e.g. Bybit `publicOption` endpoint, OKX `OPTION` instType, Binance European options). Requires a new `InstrumentKind::Option` (strike, expiry, call/put), a new model crate for options data, and product-aware routing. Largest item. | Strike/expiry symbol form; which channels (trades, ticker, books) apply; settlement identity. |

### 2.2 Implementation phases

1. **B1 — Open interest and index price**.
   - Open interest — DONE (2026-08-05): new `cryptofeed-openinterest` crate,
     `Channel::OpenInterest`, and runtime `openinterest` feature. OKX uses its
     standalone `open-interest` channel (SWAP/FUTURES); Bybit has no standalone
     channel, so open interest is extracted from the derivative
     `tickers.{symbol}` stream. Gate.io uses `futures.tickers`; Bitget v3
     derivative `ticker` was added on 2026-10-08. Binance contract OI is
     explicitly deferred by user decision: no verified official native
     contract OI WS path was found, and multi-symbol REST polling can exhaust
     the shared request budget. Do not implement a fallback without a new
     explicit plan; see [the decision record](binance-open-interest-decision.md).
   - Index price — DONE for OKX (2026-08-05): `cryptofeed-index` crate,
     `Channel::Index`, and runtime `index` feature. Symbol convention: an
     index instId (e.g. `BTC-USD`) is represented by `Symbol::spot(base,
     quote)` for explicit index feeds. Since 2026-10-08, SWAP/FUTURES also
     support Index: derive the index ID from the hydrated native base/quote,
     deduplicate the subscription, and fan out to configured contract symbols.
     The index is a base-market reference, not a tradable spot instrument.
      Bybit's ticker-embedded `indexPrice` extraction remains a follow-up
      (implemented 2026-08-06: the derivative `tickers.{symbol}` stream
      carries `indexPrice` on every push — current official docs expose no
      standalone index-price channel; the Index event reuses the derivative
      symbol, mirroring the open-interest convention).
2. **B2 — Options** — DEFERRED FROM 0.1: `InstrumentKind::Option` and
   `Symbol::option(base, quote, expiry, strike, C|P)`; Bybit active-option
   discovery via the tickers endpoint with settlement derived from the
   documented `-USDT` suffix and base-coin `publicTrade.{BASE}` trades; OKX
   `OPTION` discovery with ticker/trade/L2. Binance parser fixtures cover the
   historical eapi/nbstream contract, but live capability is disabled because
   no current endpoint has passed verification. Implied volatility is
   normalized on the Ticker and Trade models (`Option<Decimal>`) wherever the
   exchange sends it (Bybit ticker `markPriceIv`, Bybit trade `iv`).
3. **B3 — L1/L3**:
   - L1 — DONE (2026-08-05): `L1Book` (best bid/ask with sizes) as an
     independent channel on Bybit (`orderbook.1`, doubling the existing BBO
     Ticker source) and OKX (`bbo-tbt`, routed out of the L2 sync path). The
     Ticker model stays price-only; L1Book carries sizes, so the two remain
     distinct.
   - L3 — UNSUPPORTED (verified 2026-08-05): none of the five active exchanges
     exposes a public order-by-order (L3) depth stream. Public depth is
     price-level only (`@depth`/`@depth@100ms` on Binance, `orderbook.{1,50,200}`
     on Bybit, `books*` on OKX and Bitget, `order_book_update` on Gate.io).
     Known L3-class streams (Coinbase full channel, Deribit) belong to
     exchanges outside the active scope. Moved to the deferred scope with this
     analysis; reopening requires a new verified public L3 source.

Phase gate: identical to Section 1.3, plus explicit negative assertions for
any cell declared unsupported (L3 unless verified, spot funding/liquidations,
etc.).

## 3. Cross-cutting constraints

- Capability preflight remains authoritative: absent cells fail before
  connecting; no silent no-op subscriptions.
- New model crates follow the thin-category pattern (`model.rs` + `handler.rs`
  + `prelude.rs`), gated behind runtime features with cfg-consistent event
  variants.
- Funding/liquidation rows normalize with `Decimal` and distinct
  exchange/receive timestamps like every other channel.
- Product identity must survive for liquidations pushed platform-wide
  (Bitget): resolve each row's symbol against the product-qualified catalog.
- The runtime prelude, `ExchangeFeedBuilder`, `FeedHandler` dispatch, and the
  examples stay in sync with every new channel in the same change.

## 4. Deferred conditions

An item returns to the deferred scope when its official protocol cannot be
represented faithfully, when no current official public channel exists
(L3 — see Section 2.2 B3 for the verification), or when the normalized contract would require ambiguity (spot
funding/liquidation on exchanges that do not expose them). Deferred items stay
documented with their analysis so they can be reopened when the protocol or
scope changes.

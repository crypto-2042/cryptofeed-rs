# Additional CEX research and selection — 2026-11-03

Status: **research and selection only.** This note records which public
market-data venues fit the 0.1 normalized contract, which do not, and why
KuCoin and Kraken are the first two additions. Protocol facts were read from
each venue's current official documentation on 2026-11-03; no live capture was
taken for this note.

## Evaluation criteria

A candidate must provide, without authentication:

1. a current official WebSocket public market-data API covering several of the
   normalized categories (`Ticker`, `Trade`, `L1Book`, `L2Book`, `Candles`,
   `Funding`, `OpenInterest`, `IndexPrice`, `MarkPrice`, `Liquidations`);
2. spot plus at least one derivative product, or a spot market large enough to
   justify a spot-only addition;
3. an official REST catalog that maps native symbols to product identity, so
   preflight and reverse mapping stay product-qualified;
4. a documented, current protocol with stable channel/topic names and sequence
   or checksum rules that can be verified offline;
5. documentation that can be fetched and archived as evidence (`llms.txt`,
   OpenAPI/AsyncAPI or static HTML).

Excluded by the 0.1 scope: authenticated/private streams, trading, options and
MARGIN products (existing decision in [`PARITY.md`](../PARITY.md)).

## Candidate matrix

| Venue | Products | Public WS channels mapped to the normalized model | Gaps | Verdict |
| --- | --- | --- | --- | --- |
| [KuCoin UTA WS V2](https://www.kucoin.com/docs-new/websocket-api/introduction) | SPOT, FUTURES | `ticker`, `trade`, `obu` (BBO/5/50/increment), `kline` (1m–1M), `mark-price` (mark + index + open interest), `funding-fee` | no public liquidation stream; one `mark-price` payload carries three categories | **Selected, phase 1** |
| [Kraken Spot WS v2](https://docs.kraken.com/api/docs/websocket-v2/ticker) | SPOT | `ticker` (BBO + last, sizes), `trade`, `book` (10/25/100/500/1000 with CRC32), `ohlc` (minutes), `instrument` | spot only; funding/OI/mark/liquidations live on the separate Kraken Derivatives platform | **Selected, phase 2** |
| [Coinbase Advanced Trade](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints) | SPOT, US futures | `ticker`, `market_trades`, `level2`, `candles` (5-minute buckets only), `status` | no 1m candles, no funding/OI/mark public stream | Candidate 3, not selected now |
| [HTX](https://huobiapi.github.io/docs/spot/v1/en/) / [USDT-M](https://huobiapi.github.io/docs/usdt_swap/v1/en/) | SPOT, USDT-M | `market.ticker/depth/trade/detail/kline`; swap funding, OI, mark, liquidation | older documentation layout; several channels overlap | Candidate 4 |
| [MEXC](https://mexcdevelop.github.io/apidocs/contract_v1_en/) | SPOT, contract | `sub.ticker/trade/depth/kline`, funding, mark, liquidation | documentation and channel naming less consistent | Candidate 5 |
| [Deribit](https://docs.deribit.com/) | perp/futures/options (JSON-RPC) | ticker, book, trades, funding, mark, OI | no spot product of its own; options are out of scope | Derivatives-only alternative |
| Bitstamp / Gemini / Upbit | SPOT | ticker/trades/book/ohlc subsets | no derivatives, limited channels, small normalized gain | Not selected |
| [Crypto.com Exchange](https://exchange-docs.crypto.com/exchange/v1/ws_public) | SPOT, perp | ticker/trade/book/candles/funding/mark/OI | documentation endpoint returned HTTP 403 to this review; unverified | Not selected |
| Hyperliquid | Perps (DEX) | trades, book, funding, OI | not a centralized exchange | Out of scope |

## Why KuCoin first

- One documented channel set covers both SPOT and FUTURES (`tradeType`), so a
  single adapter shape serves two products.
- The `mark-price` channel publishes mark price, index price **and** open
  interest together, and `funding-fee` publishes rate, funding time and next
  funding time. Together with `ticker`/`trade`/`obu`/`kline` that is nine of
  the ten normalized categories, the highest coverage of any candidate.
- The public push hosts (`wss://x-push-spot.kucoin.com`,
  `wss://x-push-futures.kucoin.com`) are documented for public data without a
  token; the older token-based Classic endpoints are not required.
- `obu` documents explicit sequence numbers (`O`/`C`) for incremental pushes and
  snapshot-only modes for BBO/5/50, which maps onto the existing L2 bootstrap
  and gap handling instead of inventing a new rule.

Known risks to verify during implementation: whether the public push hosts
require the `id`/`action` handshake only (no token), the exact `obu`
`increment` versus `increment@10ms` snapshot semantics, futures symbol suffixes
(`*USDTM`), and per-connection subscription limits.

## Why Kraken second

- Kraken Spot WS v2 is a small, explicit protocol: `subscribe`/`unsubscribe`
  with `channel` + `symbol`, snapshot/update `type`, ISO-8601 timestamps, and a
  CRC32 checksum over the top ten book levels.
- `ticker` already carries bid/ask with sizes, so L1 needs no extra channel.
- It adds a major venue at low protocol risk; the cost is spot-only coverage
  until the separate Kraken Derivatives API is planned.
- Symbol handling needs an alias table (`XBT` → `BTC`) and `wsname`
  (`XBT/USD`) from `AssetPairs`.

## Baseline documents to produce before code

Each selected venue gets a protocol baseline in this directory family
([`kucoin-protocol.md`](kucoin-protocol.md),
[`kraken-protocol.md`](kraken-protocol.md)) recording endpoints, subscription
framing, field mapping to normalized models, sequence/checksum rules, catalog
fields, rate limits and open questions. Implementation is tracked in
[the expansion plan](cex-expansion-plan-2026-11-03.md).

## Rejected or deferred, with reasons

- **Coinbase**: public `level2` no longer needs a JWT, but WebSocket candles are
  five-minute buckets only, and no public funding/OI/mark stream exists. Worth a
  later increment if spot breadth matters more than channel depth.
- **HTX / MEXC**: full derivative coverage, but current documentation is harder
  to pin to versioned channel semantics; keep as phase-3 candidates.
- **Deribit**: best documented derivatives API (OpenAPI + AsyncAPI), but no spot
  product and options are out of scope; a derivatives-only increment would not
  extend the product matrix.
- **Bitstamp / Gemini / Upbit**: spot-only with fewer channels than the
  exchanges already integrated.
- **Crypto.com**: documentation was not retrievable (HTTP 403) during this
  review; cannot be assessed as current and official.
- **Hyperliquid**: a DEX, outside the CEX scope of this SDK.

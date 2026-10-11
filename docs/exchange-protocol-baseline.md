# Current CEX Public Protocol Baseline

Verified against the current official documentation and unauthenticated public
services on 2026-08-09.

This document is the protocol input to the offline parity harness. It records
wire facts separately from SDK normalization decisions so that a fixture cannot
silently turn a project assumption into an exchange guarantee. The supported
scope remains public market data for Binance, Bitget, Bybit, OKX, and Gate.io.
For the 0.1 release, only spot, perpetual/swap, and dated-futures products are
runtime capabilities. Option and MARGIN sections below document retained
parser/catalog evidence, not enabled capability-matrix cells.

Incremental official-document verification on 2026-10-08 covers Bitget v3
L1 and derivative ticker fields, Gate.io public perpetual liquidations, and
OKX contract-to-index subscription mapping. Earlier live observations retain
their original dates; documentation verification is not a live smoke result.

## API currency review — 2026-10-09

The enabled REST/JSON WebSocket surfaces were compared with current official
references. API version numbers are product-specific: Binance `/fapi/v1` and
`/dapi/v1` are not obsolete merely because Spot uses `/api/v3`. New optional
SBE/RPI services do not invalidate supported stable JSON channels. This review
covers selected enabled endpoints and subscription contracts, not every optional
field or all products offered by each exchange. No Python protocol constants
were introduced by the usage-alignment work; MarketCatalog reuses Rust's catalog
fetchers.

| Exchange | Enabled API selection | Official references / result |
| --- | --- | --- |
| Binance | Spot REST v3/combined streams; USD-M fapi plus public/market split; COIN-M dapi/dstream | [Spot streams](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~), [USD-M public](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public), [USD-M market](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market), [COIN-M connection guide](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect). Existing product routes remain documented; no guessed OI stream or polling fallback. |
| Bitget | REST `/api/v3/market/instruments`; `/v3/ws/public`; v3 `instType/topic/symbol` envelopes | [Guide](https://www.bitget.com/docs/uta/quick-start), [instruments](https://www.bitget.com/docs/catalog/market-market-data/market-instruments), [depth](https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel), [ticker](https://www.bitget.com/docs/uta/websocket/public/Tickers-Channel), [candles](https://www.bitget.com/docs/uta/websocket/public/Candlesticks-Channel). Current v3 surfaces, not v1/v2 subscriptions. |
| Bybit | REST v5 instruments; v5 spot/linear/inverse WS; allLiquidation and derivative tickers | [Connect](https://bybit-exchange.github.io/docs/v5/ws/connect), [instruments](https://bybit-exchange.github.io/docs/v5/market/instrument), [all liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation), [ticker](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker). Retired funding topics remain parser references only. |
| OKX | REST v5 instruments; WS v5 public/business | [Changelog](https://www.okx.com/docs-v5/log_en/): use recommended `openapi.okx.com` REST domain and default WS TLS port 443; migration details below. |
| Gate.io | REST v4; spot/perpetual/delivery WS v4; public liquidation topic | [Spot REST](https://www.gate.com/docs/developers/apiv4/en/spot/), [spot WS](https://www.gate.com/docs/developers/apiv4/ws/), [futures WS](https://www.gate.com/docs/developers/futures/ws/), [delivery WS](https://www.gate.com/docs/developers/delivery/ws/en/). Current v4 service paths; public liquidations are distinct from private liquidates. |

### OKX endpoint migration and catalog boundary

- The 2026-09-30 [official port announcement](https://www.okx.com/en-us/help/okx-websocket-port-8443-discontinuation-announcement)
  says port 8443 stops accepting WS connections on 2026-10-31. Port 443 already
  works. Rust public/business URLs now omit `:8443`; host and v5 paths are
  unchanged. An offline parity regression covers both mixed-feed connections.
- The 2026-05-20 official changelog recommends `https://openapi.okx.com` for
  Global REST. `www.okx.com` remains supported and is explicitly not deprecated.
  Rust instrument discovery now uses the recommended domain; this is a host
  migration, not a new REST protocol version.
- A current SPOT directory response contained eight `state: preopen` rows with
  empty base/quote fields. This is a dated service observation, not a claim that
  all preopen records omit identity. Rust now filters non-live OKX spot rows as
  it already did for derivatives. Any remaining empty spot identity fails with
  MalformedData before constructing Symbol, instead of panicking or guessing
  currencies from instId. Inline unit regressions and the sanitized REST
  reference preserve both the preopen and malformed-live cases.

Older option endpoints and legacy standalone funding/index parser references
are not enabled subscriptions. Capability preflight remains authoritative;
Coinbase/Kraken builders do not imply active runtimes. The Python sibling itself
has local protocol updates, so its entire checkout should not be labeled old.

## Evidence and change rule

For protocol work, use this order of authority:

1. current stable official exchange documentation;
2. a sanitized observation from the current public service;
3. the normalized contract of this project; and
4. Python `cryptofeed` as a semantic migration reference only.

If documentation and a live response differ, preserve both observations, avoid
weakening parsing without a bounded reason, and add an offline regression for
the accepted wire variants. Live services remain a manual smoke target and must
never become a CI dependency.

## Verified endpoint and channel matrix

| Exchange/product | Discovery and bootstrap | WebSocket routing | Harness-critical notes |
| --- | --- | --- | --- |
| Binance spot | Spot REST exchange information and depth snapshot | `wss://stream.binance.com:9443/stream` | `bookTicker` is `u,s,b,B,a,A`; it does not carry `e` or `E`. |
| Binance USD-M | USD-M exchange information and depth snapshot | book ticker/depth use `/public/stream`; aggregate trade, kline, mark price, and liquidation use `/market/stream` | A mixed feed needs separate physical connections. Do not send all derivative topics to one URL. |
| Binance coin-margined | Current COIN-M exchange information and depth surfaces | Current COIN-M WebSocket market-stream service | Keep perpetual and dated identity; do not infer product solely from the native symbol suffix. Recheck separately from USD-M because the catalog still exposes it as a distinct API product. |
| Bitget v3 spot/futures | `GET /api/v3/market/instruments` and v3 order book | `wss://ws.bitget.com/v3/ws/public` | `event: subscribe` is a control acknowledgement and has no `data`; `books` data uses `action` plus `seq`/`pseq`. |
| Bybit v5 spot | v5 spot instruments | `wss://stream.bybit.com/v5/public/spot` | Spot `tickers.{symbol}` has 24-hour statistics but no best bid/ask. `orderbook.1.{symbol}` is the official BBO source. |
| Bybit v5 linear/inverse | v5 linear/inverse instruments | product-specific `/v5/public/linear` or `/v5/public/inverse` | Derivative ticker payloads include `bid1Price` and `ask1Price`. A new order-book snapshot replaces local state. |
| OKX v5 spot/swap/futures | `GET /api/v5/public/instruments` and product-aware book bootstrap | public market-data channels use `/ws/v5/public`; candle channels that OKX classifies as business data use `/ws/v5/business` | Keep `instType` through discovery, routing, parsing, and callbacks. Text `ping` expects text `pong`. |
| Gate.io v4 spot | spot currency-pair discovery and spot book bootstrap | `wss://api.gateio.ws/ws/v4/` | Subscription timestamps are seconds and must be fresh. Subscription acknowledgements are control messages. |
| Gate.io v4 futures/delivery | settlement-specific contract catalogs and matching book endpoints | settlement/product-specific futures or delivery WebSocket URL | Catalog identity is derived from contract name/type/settlement. `quanto_multiplier` has no documented positive-value restriction and is not a symbol-identity gate. Funding, mark price, open interest, and index price ride the `futures.tickers` stream; `book_ticker` doubles as L1. The delivery service serves `futures.*` channel names. |

## Binance

### Current protocol facts

- Spot combined streams use
  `wss://stream.binance.com:9443/stream?streams=...`.
- The spot individual book-ticker response contains only update ID, symbol, bid
  price/size, and ask price/size (`u,s,b,B,a,A`). It has no event-type or event-
  time field.
- The same book-ticker shape (`u,s,b,B,a,A`) is the USD-M and COIN-M individual
  book-ticker stream; it doubles as the L1 top-of-book source on spot and
  derivatives.
- The current contract Index source is documented `markPriceUpdate.i`, with
  exchange timestamp `E` and contract identity `s`. `T` on this payload is
  the next funding time and must not become the Index timestamp. When Index
  is requested, one `@markPrice@1s` topic is shared with Funding/MarkPrice;
  otherwise their default cadence is unchanged. This covers native USD-M
  and COIN-M perpetual/dated contract names without guessing an index topic.
  Older `IndexUpdate` / `indexPriceUpdate` captures and `p/T` parsers remain
  implementation references; their 2026-08-07 observations are historical.
- Contract OI rechecked against official documentation and GitHub SDK/feed
  implementations on 2026-10-08: no documented USD-M/COIN-M perpetual or
  dated-futures OI market stream was found, nor a USD-M WS API OI query.
  Current OI is available through `/fapi/v1/openInterest` and
  `/dapi/v1/openInterest` (single required symbol, weight 1 each); historical
  OI REST has a minimum `5m` period. The options-only OI WS stream is separate.
- The user explicitly deferred Binance contract OI on 2026-10-08 because
  multi-symbol polling can exhaust shared request budgets. Both native WS
  OI and REST polling remain disabled; see
  [the decision and official sources](binance-open-interest-decision.md).
  Earlier `<symbol>@openInterest` probes returned no messages over three
  windows while REST returned data; that observation alone does not prove
  permanent absence.
- The current USD-M catalog separates book ticker and depth under
  `/public/ws` or `/public/stream` from aggregate trades, klines, mark price,
  index price and liquidation under `/market/ws` or
  `/market/stream`.
- Candle intervals follow the stable `kline_{interval}` naming
  (`1m`..`1M`, including `8h`); partial depth streams are
  `@depth{5,10,20}@100ms` and must be bootstrapped from a REST snapshot of the
  same width. Depth update intervals are `100ms`/`1000ms` on spot and
  `100ms`/`250ms`/`500ms` on USD-M/COIN-M, applied to both partial and
  full-depth streams.
- Mark-price `P` is an estimated settlement price, not a predicted funding
  rate. Funding and MarkPrice both leave `predicted_rate` unset. This was
  rechecked against the official USD-M field table on 2026-10-08; the earlier
  fixture containing a rate-like `P` was synthetic and has been corrected.
- Funding applies only to perpetuals. All dated-futures Funding capability
  cells are disabled (2026-10-08 correction), independently of shared price
  transport. Binance delivery rate fields are empty and funding time is zero;
  see the official [COIN-M field table](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data).
- Mark-price events provide the funding rate and next funding time. Liquidation
  events use the symbol-specific `@forceOrder` market stream.

note

The historical index-price/L1/depth observations above follow the long-stable
USD-M/COIN-M market-stream contract and the 2026-08-04 baseline. The current
Binance developer docs site is client-rendered and was unreachable for
re-verification on 2026-08-06; a live smoke run is the acceptance evidence for
these cells (see `docs/reports/live-smoke-2026-08-04.md` for the previous run).
- Historical Binance option fixtures remain parser migration references, but
  the legacy `nbstream` endpoint returned 404 and no current endpoint has
  passed live validation. Capability preflight therefore rejects Binance
  options instead of opening a known-unverified connection.

Official references:

- [Spot WebSocket streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams)
- [USD-M public streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)
- [USD-M market streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)
- [COIN-M market streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/ws-streams)

### SDK and harness consequences

- Route a combined normalized feed into as many physical connections as the
  official endpoint split requires while preserving one user-facing
  `FeedHandler` configuration.
- Dispatch Binance payloads from the subscribed stream name and payload shape;
  never require `data.e` for spot book ticker.
- The spot fixture must omit the fabricated `e` and `E` fields. Derivative
  fixtures must record public and market URLs separately and prove that
  book/depth and trade/kline/funding/liquidation are assigned correctly.
- A spot example must not request funding or liquidation, because those are
  derivative-only capabilities.

## Bitget v3

### Current protocol facts

- Instrument discovery is the unified v3 market-instruments API.
- Public WebSocket subscriptions use objects containing `instType`, `topic`,
  `symbol`, and, for kline, a separate `interval`.
- A successful subscription response is a control object such as
  `{"event":"subscribe","arg":{...},"connId":"..."}`. It does not contain
  market `data` and must not be passed to a topic parser.
- The `books` channel sends `action: snapshot` or `action: update`. For
  incremental continuity, the previous update `seq` must equal the following
  update `pseq`; a snapshot sequence must fall inside the first update's
  documented range.
- The application heartbeat is the text string `ping` every 30 seconds with
  text `pong` expected; absence of pong requires reconnect, and the server may
  close a connection that receives no ping for two minutes.

Official references:

- [v3 instruments](https://www.bitget.com/api-doc/uta/public/Instruments)
- [v3 WebSocket guide](https://www.bitget.com/api-doc/uta/guide)
- [v3 order-book channel](https://www.bitget.com/api-doc/uta/websocket/public/Order-Book-Channel)
- [v3 candlestick channel](https://www.bitget.com/api-doc/uta/websocket/public/Candlesticks-Channel)

### Current protocol facts (liquidations)

- The public `liquidation` channel is scoped by `instType`
  (`usdt-futures`, `usdc-futures`, `coin-futures`), not by symbol: the server
  pushes the platform-wide stream. Since 2026-06-23 it pushes once per second,
  containing at most the largest long and largest short liquidation per
  trading pair. Rows carry `symbol`, `side`, `price`, `amount`, and `ts`.
- There is no separate funding-rate channel on v3, but the public derivative
  `ticker` sends `fundingRate`, `nextFundingTime`, `openInterest`, `indexPrice`,
  and `markPrice`. These fields back separate normalized events; an absent or
  empty field is not invented. OI denomination/USD value are not transmitted
  by this schema, so both stay unset.
- `books1` sends one-level snapshots on spot and all three futures product
  lines, with a documented 1ms cadence. L1 does not enter the L2 state machine
  unless the configured L2 depth is also 1; a shared subscription emits one
  L1 event and one L2 event per valid push.
- Ticker-based normalized events bind to the configured native symbol mapping
  and `instType` to retain dated-futures identity and avoid binding spot rows
  to a contract sharing the same native name. Unrequested liquidation rows
  are filtered without being classified as malformed payloads.
- Current v3 candle intervals rechecked on 2026-10-08 are `1m/3m/5m/15m/30m`
  and `1H/4H/6H/12H/1D`. Normalized lowercase inputs map to uppercase wire
  hours/days. Longer periods in earlier raw-parser references are not in the
  current official table and fail runtime preflight.
  [Current candlestick channel](https://www.bitget.com/docs/uta/websocket/public/Candlesticks-Channel).

Official references rechecked 2026-10-08:

- [Bitget v3 depth](https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel)
- [Bitget v3 ticker](https://www.bitget.com/docs/uta/websocket/public/Tickers-Channel)

### SDK and harness consequences

- Classify `event` responses before topic dispatch. Successful acknowledgements
  produce no normalized event; non-success responses produce a visible
  subscription/protocol error.
- Add a successful `books` acknowledgement with no `data` to the fixture and a
  regression proving it neither fails the connection nor emits an L2 event.
- Retain separate tests for snapshot, continuous update, stale update, gap, and
  replacement snapshot.
- The liquidation subscription arg has no `symbol`; product identity comes from
  `instType` and the per-row native symbol resolves against the product
  catalog. `side` is the liquidated position side and `amount` is quote
  currency; the SDK inverts the side to the liquidation aggressor and divides
  amount by price to expose base quantity. Rows normalize with
  `status: Filled` and no order id.
- Liquidations are a derivative-only capability; spot combinations must fail
  preflight.

## Bybit v5

### Current protocol facts

- Public spot, linear, and inverse feeds use distinct v5 WebSocket paths.
- Linear/inverse `tickers.{symbol}` uses an initial snapshot followed by
  deltas containing only changed fields. The session reconstructs the latest
  known supported fields, retaining unchanged values and using the current
  frame timestamp; each accepted update emits the requested latest models.
  A new snapshot replaces prior values and reconnect starts a fresh cache.
  The cache only accepts configured native symbols and normalized-model fields.
  [Official ticker contract](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker).
- Linear/inverse `tickers.{symbol}` includes `bid1Price`, `bid1Size`,
  `ask1Price`, and `ask1Size`.
- Spot `tickers.{symbol}` is snapshot-only and documents last price, high/low,
  previous price, volume, turnover, percentage change, and USD index price. It
  does not document or send BBO fields.
- Spot and derivatives both expose `orderbook.1.{symbol}`. Level 1 is
  snapshot-only, is pushed at the documented 10 ms cadence when changing, and
  may repeat a snapshot after three seconds without change.
- Any new order-book snapshot replaces local state; update ID `u = 1` is also a
  service-restart snapshot and must overwrite prior state.

Official references:

- [v5 ticker channel](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker)
- [v5 order-book channel](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
- [v5 connection guide](https://bybit-exchange.github.io/docs/v5/ws/connect)

### Current protocol facts (funding and liquidations)

- Funding is embedded in derivative `tickers.{symbol}`; the retired
  `funding.{symbol}` topic is not subscribed.
- `allLiquidation.{symbol}` (which replaced the deprecated
  `liquidation.{symbol}`) pushes liquidation events at ~500 ms on linear and
  inverse: `T`, `s`, `S`, `v`, `p`. `S` is the liquidated position side.
- Spot has no funding or liquidation channel.

### SDK and harness consequences

- The project Ticker model is BBO, not a 24-hour statistics object. Therefore
  Bybit spot Ticker must be sourced from `orderbook.1`, or be rejected as
  unsupported; inventing bid/ask from spot `tickers` is forbidden.
- The selected mapping must be explicit in the capability/connection plan. A
  fixture for the real spot ticker shape must prove that it is not parsed as a
  BBO Ticker, and an `orderbook.1` fixture must cover the chosen BBO mapping.
- If both Ticker and L2 are requested, keep their topics and normalized event
  types independent even though both originate from order-book channels.
- Funding normalizes to the project `Funding` model with `next_funding_time`
  and `predicted_rate` as `None` (Bybit does not push them); the applicable
  funding time becomes `exchange_ts`.
- Liquidations normalize with `status: Filled` and no order id; the position
  side in `S` is inverted to the liquidation order/aggressor side.
- Funding and liquidations are derivative-only capabilities; spot combinations
  must fail preflight.
- Bybit has no standalone open-interest channel: the linear/inverse
  `tickers.{symbol}` stream carries `openInterest` and `openInterestValue`.
  Open interest is therefore sourced from the same stream as the derivative
  ticker and is a derivative-only capability.
- Bybit has no standalone index-price channel in the current official docs:
  the linear/inverse `tickers.{symbol}` stream carries `indexPrice` (and
  `markPrice`) on every push (100 ms cadence). The normalized Index event is
  therefore sourced from the derivative ticker stream with the derivative
  symbol, mirroring the open-interest convention (verified 2026-08-06).
- Kline intervals use the `kline.{interval}.{symbol}` topic with the wire
  values `1` `3` `5` `15` `30` `60` `120` `240` `360` `720` (min), `D`, `W`,
  `M`; L2 depth levels are `orderbook.{50,200,1000}.{symbol}` on
  spot/linear/inverse and `orderbook.{25,100}` on options (verified
  2026-08-06).
- The kline contract covers spot/linear/inverse only — options klines are not
  exposed (verified 2026-08-06); option candles must stay rejected. The
  derivative `tickers.{symbol}` stream additionally carries `markPrice` on
  every push, which backs the normalized MarkPrice event.
- The standalone `funding.{symbol}` channel is no longer served (live
  rejection `error:handler not found` on 2026-08-06): funding data
  (`fundingRate`, `nextFundingTime`, `markPrice`) rides the derivative
  `tickers.{symbol}` stream. The `markPrice.{symbol}` channel is likewise not
  served; mark price comes from the tickers stream.
- Option discovery uses `GET /v5/market/tickers?category=option`, which
  returned `PARAMS_ERROR` on 2026-08-06 because the tickers contract requires
  a `baseCoin`; the full option catalog is served by
  `GET /v5/market/instruments-info?category=option`, which pages via
  `nextPageCursor` without a base coin and carries `optionsType`
  (`Call`/`Put`) for cross-checking the `{C|P}` suffix (verified live
  2026-08-07).
- The level-1 (top-of-book) channel is `orderbook.1.{symbol}` on spot, linear,
  and inverse endpoints. It is already consumed as the BBO Ticker source; as
  an independent L1 channel it also carries the best bid/ask sizes, which the
  Ticker model does not.
- Options use the `publicOption` endpoint and the same ticker/order-book
  topics as derivatives. Option instIds follow `{BASE}-{EXPIRY}-{STRIKE}-{C|P}`
  (USDT-settled options append `-USDT`), and the normalized form is
  `{BASE}-{QUOTE}-{EXPIRY}-{STRIKE}-{C|P}`. Settlement is derived from the
  documented `-USDT` suffix (otherwise USDC); active-option discovery pages
  the `instruments-info?category=option` catalog.
- Option ticker BBO fields are `bidPrice`/`bidSize` and
  `askPrice`/`askSize`; implied volatility is exposed from `markPriceIv`
  (`bidIv` and `askIv` describe the individual quotes).
- Option trades arrive on the base-coin stream `publicTrade.{BASE}` (one
  subscription covers all options of a base coin); the full option symbol is
  per row in `data.s` and each row resolves its own instrument (batches can
  span several series), with option-specific fields `mP`, `iP`, `mIv`, and
  `iv` (implied volatility). `iv` is normalized into `Trade::implied_volatility`;
  `mIv`/`mP`/`iP` are not yet normalized.

## OKX v5

### Current protocol facts

- Public instruments come from `GET /api/v5/public/instruments`; product kind is
  selected with `instType` and cannot be discarded after discovery.
- Public ticker, trade, funding, liquidation-related, and book channels use the
  applicable current v5 public service. Candle channels use the endpoint class
  specified by the current OKX channel documentation.
- The documented heartbeat is text `ping` and the expected response is text
  `pong`. Snapshot messages replace book state; update sequencing and checksum
  validation remain mandatory.

Official references:

- [OKX v5 API guide](https://www.okx.com/docs-v5/en/)
- [OKX WebSocket overview](https://www.okx.com/docs-v5/en/#overview-websocket)
- [OKX instruments](https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments)

### Current protocol facts (funding and liquidations)

- `funding-rate` applies to SWAP instruments only and pushes
  `fundingRate` (current period), `fundingTime`, `nextFundingRate` (may be
  absent or empty until the next period), and `nextFundingTime`. No mark price
  is sent on this channel.
- Monthly `1M` and quarterly `3M` candle ends follow calendar months at
  their documented UTC+8 opening boundary, rather than fixed 30/90-day spans.
  Leap-year and month-length boundary regressions cover this derivation.
  [Official candle periods](https://app.okx.com/docs-v5/en/#order-book-trading-market-data-ws-candlesticks-channel).
- The standalone `mark-price` channel pushes `instType`, `instId`, `markPx`,
  and `ts` (data return time) and is used for the normalized MarkPrice event
  on SWAP/FUTURES (verified 2026-08-06). The Get mark price REST surface also
  covers MARGIN/OPTION/EVENTS instrument types.
- `liquidation-orders` is scoped by `instType`, not `instId` (live rejection
  60018 for instId arguments on 2026-08-06); subscribe once per feed product
  with `{"channel":"liquidation-orders","instType":"<T>"}`.
- The `books` channel transmits `checksum: 0` on every snapshot and update
  (measured live 2026-08-06, 198/198 messages), meaning "no checksum
  provided"; a zero checksum is skipped, nonzero checksums stay strictly
  validated. The checksum string interleaves the top 25 levels per index
  (`bid1:size1:ask1:size1:bid2:...`, truncated when a side runs short), not
  all bids followed by all asks.
- Candle channels cover SPOT, MARGIN, FUTURES, and SWAP at every bar, and
  OPTION at every bar except `1s` ("1s candle is not supported by OPTION",
  verified 2026-08-06). The `liquidation-orders` channel additionally covers
  MARGIN.
- Order book channels are `books` (400, incremental), `books5` (5, snapshot
  replacement), `bbo-tbt` (1), and the tick-by-tick `books-l2-tbt` (400) and
  `books50-l2-tbt` (50) which require VIP4+ and return error 64003 otherwise
  (verified 2026-08-06). There are no `books50-l2`/`books400-l2` channels.
- The spot `order_book` REST response no longer carries a book `id`
  (measured 2026-08-06, A/B rolled): only `current`/`update` timestamps plus
  `bids`/`asks` are returned. Bootstrap therefore anchors on the buffered
  delta timestamps (`update_ts` must cover the first buffered delta's `t`,
  sequence anchored at `first_update_id - 1`); the legacy `id`-carrying
  response keeps the strict id bridge.
- MARGIN instruments reuse the spot instId form (e.g. `BTC-USDT` under
  `instType=MARGIN`); officially verified MARGIN surfaces are the candlestick
  channels (every bar; the 1s exclusion is OPTION-only), the
  `liquidation-orders` channel, and the mark-price surface (Get mark price
  covers `MARGIN` instrument type). MARGIN ticker/trades/order-book coverage
  is not in the supported set. Because the instId form is ambiguous with
  spot, MARGIN symbols require explicit `exchange_symbol` pairs and the
  runtime rebinds payload-parsed symbols to the configured feed instrument.
- `liquidation-orders` covers SWAP, FUTURES, and MARGIN instruments with rows
  `instId`, `px`, `sz`, `side`, `ts`, `ordId`, and `notionalUsd`/`bkPx`
  extras. `side` is lowercase `buy`/`sell`.

### SDK and harness consequences

- Current spot and swap live smokes are regression controls: do not change their
  routing or payload expectations while fixing another exchange.
- Preserve batched entries, snapshot replacement, sequence/checksum failures,
  heartbeat, and subscription errors in deterministic tests.
- Funding normalizes with `mark_price: None`, `next_funding_time` from
  `nextFundingTime`, and `predicted_rate` from `nextFundingRate` when present.
- Liquidations keep `ordId` as the order id and `status: Filled` (OKX pushes no
  fill status). Funding is a SWAP-only capability; futures funding must fail
  preflight while futures liquidations stay supported.
- `open-interest` pushes `oi` (contract quantity), `oiCcy` (coin-denominated
  quantity, not a currency code), and `oiUsd` (USD value) every 3 seconds for SWAP, FUTURES, and OPTION
  instruments. The normalized fields are Decimal `open_interest`,
  `coin_quantity`, and `value_usd`; official numeric `oiCcy` replaces the
  old synthetic `"BTC"` fixture (rechecked 2026-10-08).
- `index-tickers` pushes the underlying index: `instId` (e.g. `BTC-USD`),
  `idxPx`, `open24h`, `high24h`, `low24h`, and `ts`. Index instIds are not
  product symbols. Explicit spot index feeds retain `Symbol::spot(base, quote)`.
  SWAP/FUTURES subscriptions derive the index ID from the first two components
  of the hydrated native contract ID (`BTC-USD-SWAP` → `BTC-USD`). Identical
  index subscriptions are deduplicated; a push fans out to every matching
  configured contract, retaining each contract's product/expiry identity.
  No new quote currency is guessed. See the
  [official index channel](https://app.okx.com/docs-v5/en/#public-data-websocket-index-tickers-channel)
  (rechecked 2026-10-08).
- `bbo-tbt` is the level-1 (top-of-book) channel: one best bid and one best
  ask per row (with sizes). It is routed as `L1Book` events and is distinct
  from the `books`/`books5` L2 sync path.
- Options (`instType=OPTION`) are discovered from the public instruments
  endpoint; option instIds are `{BASE}-{QUOTE}-{EXPIRY}-{STRIKE}-{C|P}` and
  their ticker/trade/book channels use the standard public endpoint and
  envelope.

## Gate.io v4

### Current protocol facts

- Spot uses the v4 spot WebSocket and spot channel names. Futures and delivery
  use product/settlement-specific services and `futures.*` channels.
- A subscription response has `event: subscribe`, `error`, and a result status;
  market updates use `event: update`. A response is successful only when
  `error` is null. Subscription request `time` must be regenerated for every
  connection attempt and remain within 60 seconds of server time.
- Futures contract schema describes `quanto_multiplier` as a string contract
  multiplier and declares no restriction requiring a positive value.
- Public contract catalogs can contain instruments unrelated to the requested
  normalized symbol. A field not required for product identity must not make
  the entire catalog unusable.
- The derivative `futures.tickers` stream carries funding rate, mark price,
  index price, and current contract `total_size` (normalized as open interest)
  in a single per-contract
  push; there is no standalone channel for any of them. Delivery contracts
  have no funding and their ticker rows carry no `funding_rate` field.
- The public perpetual `futures.public_liquidates` channel needs no login.
  Subscribe with a contract list (or `!all`). It emits at most one liquidation
  order per contract per second: `contract`, signed `size`, `price`, `time_ms`.
  The SDK uses the signed order direction (negative = sell), absolute native
  quantity, row timestamp, no order ID, and the existing Filled convention.
  Resolve every batch row against the plan; ignore contracts not configured.
  The authenticated `futures.liquidates` user channel is distinct. Delivery
  capability stays rejected because this verification covers perpetuals only.
  [Official public liquidation reference](https://www.gate.com/docs/developers/futures/ws/#public-liquidates-order-api)
  rechecked 2026-10-08; the earlier public-stream absence assumption was wrong.
- The `book_ticker` stream (spot and futures) carries best bid/ask with sizes
  (`b`/`B`/`a`/`A`), doubling as the L1 top-of-book channel.
- Order-book updates can carry `full: true` (announcements 44678/44722,
  mainnet 2026-05-06): the push is the complete book and the client must
  overwrite its local book and re-anchor the sequence at the push's `u`.
  Deltas received before the full push are superseded by it.
- The delivery WebSocket serves `futures.*` channel names, not `delivery.*`
  (verified live 2026-08-07: a `delivery.*` subscription is rejected as an
  unknown channel while `futures.*` channels deliver).

Official references:

- [Spot WebSocket v4](https://www.gate.com/docs/developers/apiv4/ws/en/)
- [Futures WebSocket v4](https://www.gate.com/docs/developers/futures/ws/en/)
- [Delivery WebSocket v4](https://www.gate.com/docs/developers/delivery/ws/en/)
- [Futures REST v4](https://www.gate.com/docs/developers/apiv4/en/futures/)
- [Delivery REST v4](https://www.gate.com/docs/developers/apiv4/en/delivery/)

### SDK and harness consequences

- Accept a record for symbol discovery when its identity fields are valid; do
  not reject that record or the catalog because a non-identity numeric field is
  zero. Identity contradictions remain explicit malformed-data errors instead
  of being guessed around.
- Add a BTC-settled catalog record with `type: inverse`,
  `quanto_multiplier: "0"`, and `in_delisting: false`; prove discovery still
  returns valid USDT-settled instruments and can resolve the target contract.
- Keep product-aware REST bootstrap and WebSocket routing assertions so a spot,
  perpetual, or delivery snapshot cannot be applied to another product.
- When a REST order-book snapshot has no sequence id, wait for at least one
  buffered WebSocket delta before consuming it; the first delta and timestamp
  check provide the required bridge. Never accept an unanchored snapshot.

## L3 order-book scope

The 2026-08-05 scope review found no public order-by-order (L3) depth stream
across the five active exchanges. Their public books expose price levels:
Binance `@depth`, Bybit `orderbook.*`, OKX and Bitget `books*`, and Gate.io
`order_book_update`. L3 remains outside the capability matrix. Reopening it
requires a verified official public L3 source and a new implementation plan.
This preserves the scope decision from the retired implementation plan; it is
not a new live-service verification.

## Manual live-smoke acceptance

After offline gates pass, run an unauthenticated short smoke for spot and one
derivatives product where supported. The smoke should verify discovery,
subscription acknowledgement, at least one expected normalized event for an
active channel, graceful Ctrl-C shutdown, and absence of a retry loop caused by
a successful control frame.

Zero events is not automatically a failure for sparse channels such as
liquidations, funding, or a one-minute candle during a short window. A live
failure is actionable when the exchange returns a protocol error, the parser
rejects a documented message, the connection uses a wrong endpoint, or an
active high-frequency channel remains silent while raw traffic is present.

## Connection/resource verification — 2026-10-10

Current official connection documents were rechecked for native-stream counts,
Bybit's distinct per-request/per-connection args limits, Bitget recommendations,
and OKX subscription length. [The connection guide](connection-planning.md)
records the exact sources and separates service limits from SDK conservative
policies; no undocumented Gate limit is invented. Current wire shapes remain
unchanged; Bybit spot can send multiple ten-arg subscribe frames and Gate queued
requests refresh their time at send. Topic aliases are counted after deduplication.

## Managed confirmation/readiness — 2026-10-10

Managed sessions now bind current official confirmation evidence: Binance
explicit SUBSCRIBE id/result-null on existing combined routed endpoints,
Bybit req_id, Gate id/channel, and Bitget/OKX requested argument identity.
Classic Binance URL subscriptions remain unchanged. [The readiness guide](readiness.md)
records exact primary sources, timeout/correlation rules and local-book criteria.
Synthetic control references and inline assertions were updated together;
manual observations remain separate from protocol fixtures.

## Catalog eligibility review — 2026-10-10

Directory reconciliation uses the current official status fields, rather than
Python-era endpoints or order-permission assumptions. Binance Spot/USD-M
`status` and COIN-M `contractStatus` retain `TRADING`; Bybit queries explicitly
request `status=Trading` and filter pending/non-Trading rows. Bitget v3 retains
`online/limit_open/limit_close` for public data and excludes unavailable/restricted
API states. Gate spot retains `tradable/buyable/sellable`, excluding `untradable`.
Existing OKX `live` and Gate derivative delisting filters are unchanged. Missing
optional status in minimal references retains compatibility; explicit invalid
status types fail. Full market metadata and order eligibility are not implied.
Official sources and synthetic mixed-status references are recorded in
[fixture provenance](../sample_data/SOURCES.md#2026-10-10-catalog-eligibility),
with matching inline markets regressions. This review changes catalog filtering,
not market-data wire normalization or enabled coverage.

## Sparse subscription planning review — 2026-10-10

The active five-exchange adapters now select exact configured channel/symbol
pairs before serializing existing native topics. Current official subscription
shapes and Binance USD-M public/market, OKX public/business and Gate derivative
product routing were rechecked; sources and request references are recorded in
[fixture provenance](../sample_data/SOURCES.md#2026-10-10-sparse-subscription-packing).
This changes SDK packing, not wire formats, market models, sequence rules or
capability cells. L2 REST URL lists and readiness/cache ownership contain only
subscribed L2 symbols; venue-scoped topics still rely on exact dispatch filters.
The separate dated Spot smoke reached Ready on all five exchanges with one
physical connection each; it does not certify global optimal packing.

## Typed market metadata review — 2026-10-10

Existing directory payloads now populate typed metadata beside exact symbol
identity. Current primary field tables were verified: Binance filterType lookup
and separate precision, Bitget v3 multiplier/count applicability, Bybit Spot's
deprecated minimum quantity versus current amount minimum, OKX independent
contract value/currency/multiplier, and Gate precision versus explicit derivative
price rounding. The [metadata guide](market-metadata.md) records source mappings,
optional/zero/error semantics and unit limitations. Sanitized HTTP references and
inline helper/decoder assertions were added together; public event parser/model
semantics and capability cells are unchanged. Metadata does not enable trading.

## Public REST snapshot review — 2026-10-10

Current primary ticker/book endpoints were verified for the five active
exchanges' existing routing profiles. Binance Spot uses the recommended market-
data-only host; futures book ticker remains v1 in current documentation (v2
last-price ticker is a different service). Bitget uses v3; Bybit/OKX v5 and Gate
v4 retain product-qualified routing. [Public REST](public-rest.md) records exact
methods, limits, timestamp fields, native IDs and missing-time behavior.

Gate Spot book `current`/`update` use milliseconds; derivative fields use seconds.
The new parser was corrected from a live-observed unit error with product-based
assertions and sanitized reference updates. No native sequence/bridge rule was
relaxed. Shared HTTP admission/status/Retry-After behavior is SDK resource policy,
not an exchange-weight quota guarantee. Public REST snapshots add no private,
trading, OI-polling or new instrument capability.

## Funding-history review — 2026-10-10

Current public funding-history surfaces were verified before implementation:
Binance fapi/dapi fundingRate uses ascending inclusive millisecond bounds;
Bitget v3 history-fund-rate uses cursor/resultList rather than v2 pageNo/data;
Bybit v5 supports end-time continuation; OKX v5 after is earlier-than and
realizedRate is actual while fundingRate is predicted; Gate v4 uses seconds t/r.
[Funding history](funding-history.md) records normalized range/cursor semantics,
retention/termination limits and sources. Captures and narrow assertions were
added together. No next settlement is inferred, prediction is not relabeled as
actual, and source exhaustion is not proof of complete retention coverage.

## Candle-history increment — 2026-10-10

Current official Binance Spot/UM/CM klines and Bybit v5 kline documentation was
checked for the [candle-history API](candle-history.md). Binance returns 12-element
rows with native close time and trade count; Bybit returns seven-element rows,
reverse-ordered by start time, with native symbol/category in result. Both omit
an explicit completion flag. Bybit inverse volume is quote currency and Binance
COIN-M volume is contracts. CM start/end ranges cannot exceed 200 days. Rust uses
bounded backward windows and a common 100-row cap, not a copied Python API path.
Other venues' candle-history APIs are still pending implementation.

## Remaining candle-history APIs — 2026-10-10

Verified current primary sources linked in [candle history](candle-history.md).
Bitget uses v3 history-candles, maximum 100 rows/90-day request range and seven
fields; its documented extra earlier interval was observed even with aligned
start when the newest bar was unavailable. EndTime at a cycle boundary avoids
shifting one older interval compared with boundary-minus-1ms.

OKX history-candles accepts after (earlier) and before (newer), 300 rows maximum;
nine-field rows carry confirm. Unsuffixed 6H/12H/day/week/month/quarter bars open
in UTC+8. Gate Spot/perpetual range parameters conflict with limit and use seconds.
Observed nonaligned from returned six 1m bars for a five-minute span; the first
legal aligned open returned five. Current Spot rows have base volume and a close
flag (eight fields); the seven-field example lacks base quantity and is rejected.
Perpetual t accepts exact numeric seconds, v is contracts and completion is absent.
Gate documentation calls 30d a calendar month. The full delivery reference also documents `/delivery/{settle}/candlesticks`;
the earlier incomplete inspection missed it. Rust now implements its existing
USDT delivery profile using the documented path and contract-volume row shape.

Gate weekly boundary verification: current Spot `7d` bars open Monday (epoch
remainder four days), while perpetual `7d` bars are epoch-aligned, as documented
for futures. Request rounding uses the product-specific anchor. Public September
2026 probes confirmed both grids and UTC-midnight daily opens. Monthly Spot and
perpetual `30d` probes each returned the September 1 UTC open. These observations
are separate from the ten-product 1m smoke; offline tests assert both week grids.

## Recent public trade APIs — 2026-10-10

Current official sources linked in [recent trades](recent-trades.md) were checked
before implementation. Binance current Spot/UM/CM /trades returns individual
id/price/qty/time/isBuyerMaker rows (1000 cap); aggregated /aggTrades is distinct.
Bitget v3 /market/fills uses execId/price/size/side/ts (100 cap, explicit category),
not older v1/v2 tradeId shapes. Bybit v5 /market/recent-trade returns a category
and list of symbol/execId/price/size/side/time, Spot capped at 60 and contracts 1000.
OKX v5 /market/trades has instId/tradeId/px/sz/side/ts (500 cap).
Gate v4 Spot/futures/delivery trades use current product/settlement routes; Spot
amount/side differs from signed contract size. Spot create_time_ms counts milliseconds, while derivative create_time_ms is
second-valued with fractional precision in current public responses; create_time
is seconds for both. The futures field description specifies precision rather
than an epoch-millisecond unit. Gate Spot maximum
is 1000; derivative parameter tables omit a numeric limit, so Rust's 1000 is a
bounded SDK policy. No private history API or archived download is substituted.

## Historical trade pagination and Gate clocks — 2026-10-10

Current sources linked in [trade history](trade-history.md) confirm Binance
aggregate fromId inclusive and derivative time ranges less than one hour, 48h
retention, and a warning against combining IDs/time parameters. Rust uses time
seeds followed by ID+1. OKX history-trades uses type=2 timestamp after, then type=1
tradeId after (100 rows, last three months); before does not support timestamp
paging. Gate Spot page and perpetual offset were verified with fixed from/to
ranges to return distinct IDs even at the same millisecond. Old last_id is not
used; delivery declares it no longer supported and has no current offset/page.

Public Gate probes returned Spot create_time_ms as millisecond counts with
fractions and perpetual create_time_ms as fractional seconds. The earlier trade
clock assumption was corrected in parser/capture/tests, with no magnitude guess.
The recent manual smoke's added age bound now detects thousand-fold clock errors.
An initial urllib OKX history probe returned HTTP403; a separate curl request
returned the documented public payload. No obsolete host/private fallback added.

Full Gate delivery reference recheck: public candlesticks are documented at
/delivery/{settle}/candlesticks with second bounds, limit/range incompatibility,
contract volume and 7d epoch alignment. Candle capability/reporting and request
planning now include this profile; a narrow offline native-route/normalization
regression replaces the earlier unsupported preflight. Historical delivery
trades have from/to but no current offset/page; range queries stop with explicit
SourceLimit on a full page, rather than guessing retired last_id.

## Raw parser replay source review — 2026-10-11

Existing normalized/parity wire contracts are reused without adapter changes.
Current official Bybit trade/book, Bitget depth/UTA upgrade mapping, Binance stream
and OKX/Gate public guides were checked for replay regressions. Bybit allows up to
1024 trades per message, matching the default output cap. Bitget's first update
bridge includes snapshot seq within [pseq,seq]; a disjoint interval must fail.
The older New-Trades Bitget docs link now redirects to ticker docs; UTA upgrade
still names publicTrade, and this increment's live v3 capture/replay exactly matched
56 current normalized Trade models. No v2/Python protocol fallback was introduced.
See [native raw replay](raw-replay.md) and its bounded/remaining replay scope.

## Consumed HTTP/L2 replay — 2026-10-11

Current official Binance Spot depth and Gate public order-book surfaces remain
unchanged. Raw v2 adds SDK-side consumption/processing markers, not new native
API versions. HTTP JSON/original receipt time feed the existing snapshot parsers;
Gate optional IDs/time anchoring and Binance depth/sequence bridges are not
relaxed. Fresh public Spot capture replay exactly matched native L2 models on both
venues with two consumed snapshots each. See [scope](http-l2-replay.md).

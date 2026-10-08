# Python Baseline vs Rust Parity Checklist

Last updated: 2026-10-08

## Goal

`cryptofeed-rs` aligns core normalized public-market behavior with Python
`cryptofeed` while using the current official protocols for Binance, Bitget,
Bybit, OKX, and Gate.io. Python remains a semantic migration reference; official
exchange documentation and sourced fixtures define protocol correctness.

## Verified Baseline

- [x] Preserve the `FeedHandler` user entrypoint.
- [x] Keep normalized public models in category crates and exchange protocols in
  `crates/runtime`.
- [x] Preserve decimal price and quantity precision and separate exchange and
  receive timestamps.
- [x] Preserve spot, perpetual, and dated-futures product identity.
- [x] Resolve symbols bidirectionally with product-qualified catalogs.
- [x] Reject unknown, ambiguous, mixed-product, and unsupported configurations
  before connecting.
- [x] Process every entry in supported batched market-data messages.
- [x] Implement application heartbeat, idle timeout, clean-close reconnect,
  graceful shutdown, and independent feed failure isolation.
- [x] Reset L2 state on replacement snapshots and preserve product-aware
  snapshot/gap recovery.
- [x] Drive every active exchange through a full-session WebSocket double:
  subscribe framing, control acknowledgement, market-data delivery, injected
  snapshot bootstrap, and clean shutdown.
- [x] Back normalization assertions with sanitized, sourced official protocol
  references.

## Capability Matrix

| Exchange | Products | Ticker | Trades | L2 | L1 | 1m candles | Funding | Liquidations | Open interest | Index | Mark price |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance | spot | yes | yes | yes | yes | yes | no | no | no | no | no |
| Binance | USD-M / coin-margined perpetual and futures | yes | yes | yes | yes | yes | yes (perpetual only) | yes | no | yes | yes |
| Binance | European options | no | no | no | no | no | no | no | no | no | no |
| Bitget v3 | spot, USDT/USDC/coin futures | yes | yes | yes | yes | yes | yes (perpetual only) | yes (derivatives) | yes (derivatives) | yes (derivatives) | yes (derivatives) |
| Bybit v5 | spot, linear, inverse | yes | yes | yes | yes | yes | yes (perpetual only) | yes (derivatives) | yes (derivatives) | yes (derivatives) | yes (derivatives) |
| Bybit v5 | options | no | no | no | no | no | no | no | no | no | no |
| OKX v5 | options | no | no | no | no | no | no | no | no | no | no |
| OKX v5 | spot, swap, futures | yes | yes | yes | yes | yes | yes (swap) | yes (swap/futures) | yes (swap/futures) | yes (spot explicit index; swap/futures mapped index) | yes (swap/futures) |
| OKX v5 | MARGIN (explicit symbols) | no | no | no | no | no | no | no | no | no | no |
| Gate.io v4 | spot, USDT/BTC perpetual, USDT delivery | yes | yes | yes | yes | yes | yes (perpetual only) | yes (perpetual only) | yes (derivatives) | yes (derivatives) | yes (derivatives) |

Bybit funding rides the derivative `tickers.{symbol}` stream (the standalone
`funding.{symbol}` channel was removed by Bybit; verified live 2026-08-06).
Option and MARGIN products are outside the 0.1 release scope and are rejected
by capability preflight. Their protocol parsers, catalog helpers, fixtures, and
tests remain as implementation references for a future explicit plan. Mark
price rides the funding/mark-price streams: Binance `@markPrice`, Bybit
derivative `tickers`, and OKX `mark-price` (swap/futures).

OKX tick-by-tick depth (`books-l2-tbt` 400, `books50-l2-tbt` 50)
is VIP4+-gated (error 64003 otherwise).

Candles are parameterized: the normalized interval (default `1m`) is
validated per exchange during preflight (`1m`..`1M` subsets; Gate.io maps to
`10s`/`24h`/`7d`/`30d`). L2 depth levels are parameterized where the exchange
exposes partial books: Binance `@depth5/10/20@100ms`, Bybit
`orderbook.{50,200,1000}` for spot/linear/inverse,
OKX `books5` (the 50/400 tick-by-tick channels are VIP4+-gated and stay
unsupported), Bitget `books1/5/50`; Gate.io WebSocket depth stays full-depth.
Binance L2 update intervals are parameterized via `l2_book_interval(...)`:
spot `100ms`/`1000ms`, USD-M/COIN-M `100ms`/`250ms`/`500ms` (default `100ms`).

Gate.io derivative funding, open interest, index price, and mark price ride
the `futures.tickers` stream (there is no standalone channel; the funding
normalization carries rate and mark price without an applicable funding
time, mirroring the Python baseline). Gate.io L1 top-of-book is the
`book_ticker` stream (best bid/ask with sizes), which doubles as the BBO
Ticker source — the same dual-event pattern as Binance. Gate.io perpetual
liquidations use the separate public `futures.public_liquidates` channel;
`futures.liquidates` is the authenticated user channel and is never subscribed.
Delivery liquidations stay unsupported pending product-specific evidence. The delivery WebSocket serves
`futures.*` channel names (verified live 2026-08-07; `delivery.*` is
unknown).

The runtime capability preflight is authoritative. A combination absent from
this matrix must fail explicitly rather than creating an empty subscription.

New-channel live smoke on 2026-10-08 received Bitget spot/perpetual L1,
perpetual funding/OI/index/mark price, and OKX SWAP index events. Gate.io
public perpetual liquidation subscription was acknowledged successfully;
no liquidation event occurred in the 45-second window. Dated-product fan-out,
other Bitget derivative categories, and Gate.io liquidation normalization
remain offline-verified in this increment. See
[the smoke report](docs/live-smoke-2026-10-08.md).

## Exchange Protocol Gates

### Binance

- [x] Contract Index reads documented `markPriceUpdate.i` / `E`; shared 1s
  mark-price subscription works for USD-M and COIN-M perpetual/dated symbols,
  including index-only feature builds. Legacy index-stream fixtures remain
  parser references.
- [x] Funding is perpetual-only across all five exchanges; dated requests fail
  preflight instead of opening a transport with no applicable funding data.

- [x] `markPriceUpdate.P` does not populate predicted funding rate; sourced
  settlement-price payloads cover both Funding and MarkPrice.

- [x] Product-specific Spot, USD-M, and coin-margined WebSocket routing.
- [x] Spot book ticker accepts the documented shape without `e`/`E`.
- [ ] European options remain disabled in capability preflight: the legacy
  `nbstream` endpoint returned 404 and no current endpoint has passed live
  verification. Parser fixtures remain as migration references only.
- [x] USD-M book/depth and trade/kline/funding/liquidation split across the
  current public and market WebSocket services.
- [x] Product-specific REST depth snapshots.
- [x] Non-USDT spot and derivative symbols normalize without guessing or panic.
- [x] Spot bridge sequencing and derivative previous-update continuity.
- [x] Bootstrap dispatch contains the snapshot and applied buffered updates.
- [x] Spot partial-depth streams (`@depth5/10/20`) replace the top-N book
  from `lastUpdateId` pushes and bootstrap from a same-width snapshot.
- [x] Snapshot fetches carry an HTTP timeout; buffered deltas are bounded and
  repeated bootstrap failures trigger a bounded in-session resnapshot.
- [x] Binance contract OI remains rejected by capability preflight. On
  2026-10-08 the user explicitly deferred REST polling because multi-symbol
  request volume can exhaust the shared rate budget. No official native
  perpetual/dated-futures OI WS path was found; options OI WS is separate.
  See [the decision record](docs/binance-open-interest-decision.md).

### Bitget v3

- [x] Dated L2 sync and platform-wide liquidation rows retain the configured
  catalog identity through runtime dispatch; unrelated series are filtered.

- [x] Spot/contract L1 `books1` snapshots, independent L2 routing, and shared
  `books1` L1/L2 subscription deduplication.
- [x] Derivative ticker funding/open interest/index/mark-price extraction,
  batch normalization, native-to-configured dated-product binding, and
  independent single-feature parsing without the `ticker` feature.

- [x] V3 instruments endpoint and product categories.
- [x] V3 `topic: kline` plus separate interval and current object payload.
- [x] Batched public trades and product-aware normalization.
- [x] Full-depth snapshot/update distinction and seq/pseq gap handling.
- [x] Current documented v3 intervals parse; normalized hours/days map to
  uppercase `1H/4H/6H/12H/1D`. Longer `3d/1w/1M` inputs fail preflight;
  older raw-parser helpers remain references, not enabled runtime intervals.
- [x] String ping/pong lifecycle.
- [x] Successful subscription acknowledgements bypass market-data parsing while
  error acknowledgements remain visible.
- [x] Derivative `liquidation` (instType-scoped, no symbol in the subscription)
  normalizes per-row symbols with product identity from `instType`,
  `status: Filled`, and no order id; spot liquidations fail preflight.

### Bybit v5

- [x] Per-connection derivative ticker state reconstructs snapshot/delta
  payloads; single-side BBO and USD-value-only OI changes are retained.
  Replacement snapshots and reconnects clear previous values; the cache is
  bounded to configured symbols.

- [x] Spot, linear, and inverse endpoint routing.
- [x] Batched trade and candle normalization.
- [x] JSON heartbeat and subscription-error handling.
- [x] Replacement snapshot and service-restart book reset.
- [x] Spot BBO Ticker uses `orderbook.1`; spot 24-hour ticker statistics are not
  misrepresented as BBO and level-50 remains the L2 source.
- [x] Derivative funding rides the `tickers.{symbol}` stream (the standalone
  `funding.{symbol}` channel was removed by Bybit; verified live 2026-08-06)
  and normalizes rate, mark price, and `next_funding_time`.
- [x] Option parser/catalog references retain `/option` routing,
  base-coin `publicTrade.{base}` row resolution, and level-25 book behavior;
  option feeds themselves are rejected by 0.1 capability preflight.
- [x] Derivative `allLiquidation.{symbol}` normalizes side, size, and price
  with `status: Filled` and no order id; spot funding/liquidation fails
  preflight.
- [x] Derivative open interest is sourced from the `tickers.{symbol}` stream
  (`openInterest`/`openInterestValue`) because Bybit has no standalone channel;
  spot open interest fails preflight.
- [x] `orderbook.1` doubles as the independent L1 channel: `L1Book` events
  carry the best bid/ask with sizes alongside the BBO Ticker mapping.
- [x] The deferred option implementation routes to `publicOption`; discovery
  pages the
  `instruments-info?category=option` catalog (verified live 2026-08-07:
  `tickers?category=option` requires a `baseCoin` and errors otherwise;
  `instruments-info` pages without one) with the `optionsType` field
  cross-checked against the `{C|P}` symbol suffix; settlement derives from
  the documented `-USDT` suffix; trades use the base-coin
  `publicTrade.{BASE}` stream with per-row symbol resolution.

### OKX v5

- [x] Monthly/quarterly candle ends follow UTC+8 calendar boundaries, including
  leap February and 31-day months.

- [x] Spot, swap, and futures identity.
- [x] Public and business WebSocket routing.
- [x] Batched ticker, trade, candle, and book processing.
- [x] Text ping/pong and sequence/reset recovery.
- [x] Full-book checksum validation on the `books` channel: CRC-32 over the
  first 25 levels interleaved per index (`bid1:sz1:ask1:sz1:...`) with
  transmitted string scale preserved; top-N channel checksums are not
  compared to the local full book.
- [x] Candle `end` derives from the wire interval and the interval
  normalizes to the shared lowercase vocabulary; VIP4 tick-by-tick depth
  (50/400) is rejected at preflight until wired.
- [x] SWAP `funding-rate` normalizes rate, next-funding time, and predicted
  rate; futures funding fails preflight because the channel is SWAP-only.
- [x] `liquidation-orders` normalizes side, size, price, and `ordId` as the
  order id with `status: Filled`.
- [x] `open-interest` normalizes `oi`, numeric `oiCcy` into Decimal
  `coin_quantity`, and `oiUsd` for SWAP and
  FUTURES; spot open interest fails preflight.
- [x] `index-tickers` normalizes `idxPx` plus 24h open/high/low; the index
  instId can be supplied explicitly for spot index feeds; SWAP/FUTURES derive
  the index ID from the native base/quote and fan out to matching contracts.
- [x] `bbo-tbt` normalizes as `L1Book` (best bid/ask with sizes) and is kept
  out of the L2 book-sync path.
- [x] Deferred `OPTION` catalog/parser helpers normalize the documented
  `{BASE}-{QUOTE}-{EXPIRY}-{STRIKE}-{C|P}` instId form; 0.1 preflight rejects
  the product.

### Gate.io v4

- [x] Public perpetual liquidation subscription, signed-size normalization,
  row-by-row multi-contract resolution, and spot/delivery negative preflight.

- [x] Fresh subscription timestamps and official public trade object shape.
- [x] Spot, USDT/BTC perpetual, and USDT delivery connection plans.
- [x] Product-aware REST bootstrap, WebSocket depth, and gap resync.
- [x] Delivery expiry and perpetual settlement identity validation.
- [x] Contract discovery does not use `quanto_multiplier` as an identity or
  positive-value gate.
- [x] Bootstrap dispatch contains the snapshot and the applied buffered updates.
- [x] Bounded in-session resnapshot: a failed bootstrap (non-bridging buffered
  deltas) re-fetches the snapshot up to three times per symbol while preserving
  the buffered deltas, then falls back to the session-level retry; the bridge
  rule is never weakened.
- [x] `full: true` order-book pushes replace the local book in-session and
  re-anchor the sequence at the push's `u` instead of erroring the session.

## Verification Baseline

Run from the workspace root:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace
cargo check --workspace --no-default-features
```

The deterministic parity harness is:

```bash
cargo test -p cryptofeed-rs --test public_parity
```

## Deferred Scope

- Binance contract OI, including REST polling: explicit 2026-10-08 decision
  based on multi-symbol rate-limit exposure; current REST availability does
  not enable a capability cell. See [the decision record](docs/binance-open-interest-decision.md).

- Coinbase and Kraken live runtime paths.
- Authenticated feeds and trading.
- Options and MARGIN as public runtime products. Their existing protocol
  helpers and fixtures are retained, but all such feeds fail 0.1 preflight.
- Gate.io live bootstrap capture validation (forensics logging in place; the
  bounded resnapshot path is implemented and deterministically covered).
- L3 order books: no public order-by-order depth stream across the five active
  exchanges (verified 2026-08-05); see
  `docs/plan-advanced-public-channels.md` Section 2.2.
- Deeper full-session WebSocket/REST doubles and remaining checksum coverage.

Funding and liquidations (Bitget v3, Bybit v5, OKX v5, Gate.io perpetual
liquidations), open interest (Bitget, Bybit, OKX, Gate.io), index price
(OKX explicit index/contract mapping, Binance derivative index-price stream,
Bitget/Bybit/Gate.io derivative tickers), and L1 (all five active exchanges) are implemented for spot/contract scope. Options, MARGIN, and L3 are
deferred. See `docs/plan-advanced-public-channels.md`. Further expansion
requires a new explicit plan and current official protocol evidence. See
`docs/current-exchanges-implementation-plan.md` for the detailed acceptance
model and remaining release work.

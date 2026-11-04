# Monitoring venue selection — 2026-11-04

Status: **selection and API-version note.** Adds three public venues chosen for
monitoring/alerting value: Coinbase (USD spot reference), Deribit (derivatives
risk reference) and HTX (Asian spot + USDT-M derivatives). Protocol baselines
and the implementation plan live beside this note.

## Why these three

Monitoring alerts need price discovery across fiat and derivative markets, not
more low-liquidity duplicates:

- **Coinbase** is the largest USD spot reference; its Advanced Trade public
  WebSocket needs no JWT and carries ticker, trades, level 2 and candles. It
  complements the already-supported USDT venues and the KRW venues added in
  PR #16.
- **Deribit** is the options/perpetual pricing centre. One public `ticker`
  subscription carries mark price, index price, open interest and funding, so
  the derivative risk signals (mark/index basis, funding, OI) come from a venue
  that has no substitute among the supported exchanges.
- **HTX** covers Asian-session spot and USDT-margined perpetuals with funding,
  open interest, mark price and liquidation topics.

## API versions to target (verified 2026-11-04)

| Venue | Current public API | Notes |
| --- | --- | --- |
| Coinbase | Advanced Trade WebSocket `wss://advanced-trade-ws.coinbase.com` (public, no JWT) and Advanced Trade REST `https://api.coinbase.com/api/v3/brokerage/market/...` | Public channels: `ticker`, `market_trades`, `level2`, `candles` (5-minute buckets only), `status`, `heartbeats`. REST provides 1m candles and the product catalog. |
| Deribit | JSON-RPC 2.0 WebSocket `wss://www.deribit.com/ws/api/v2` (and testnet); REST same methods over HTTPS | Public methods/subscriptions: `public/get_instruments` (catalog), `book.{instrument}.{interval}` (snapshot plus change-id deltas), `trades.{instrument}.{interval}`, `ticker.{instrument}.{interval}` (mark/index price, open interest, funding). Options are out of scope; perpetual/future instruments only. |
| HTX | Spot: `wss://api.huobi.pro/ws/v2` (and `api-aws.huobi.pro`); USDT-M: `wss://api.hbdm.com/linear-swap-ws` | Spot topics `market.<symbol>.ticker/depth/trade.detail/kline`; USDT-M topics `market.<contract>.ticker/depth/trade.detail/kline`, plus funding and liquidation topics; OI and mark price via documented REST/fields. |

These are the newest documented public APIs at the time of writing. The
protocol baselines must cite the exact page for every endpoint and channel, and
must not copy Python `cryptofeed` adapters, which are unmaintained and target
older endpoints.

## Scope and non-goals

- Public, unauthenticated market data only; no authenticated channels, trading,
  options products, or new normalized categories.
- No delivery, retention, readiness or lossless-semantics change.
- Unsupported products/channels/depths/intervals fail capability preflight.

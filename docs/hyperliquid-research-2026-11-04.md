# Hyperliquid addition — scope and API version note — 2026-11-04

Status: **selection and API-version note.** Adds Hyperliquid as a public
market-data venue: perpetuals and spot, unauthenticated. Protocol baseline and
implementation plan accompany this note.

## Why Hyperliquid

It is a large perpetual venue with a documented public WebSocket/REST API and
no authentication for market data. It extends derivative coverage beyond the
CEX set (it is a DEX, but the public API shape is a normal centralized-style
WS/REST feed, unlike Uniswap's on-chain AMM model).

## Current public API to target (verified 2026-11-04)

- WebSocket: `wss://api.hyperliquid.xyz/ws` (testnet `wss://api.hyperliquid-testnet.xyz/ws`).
- REST info endpoint: `POST https://api.hyperliquid.xyz/info` with a JSON body.
- Catalog: `{"type":"meta"}` for perpetuals (`universe[]` with `name`,
  `szDecimals`, `maxLeverage`, `isDelisted`), `{"type":"spotMeta"}` for spot
  (`tokens[]` with `name`/`index`, `universe[]` with `tokens:[baseIdx,quoteIdx]`
  and `name` = `PURR/USDC` for canonical pairs or `@{index}` otherwise). The WS
  `coin` for spot is the `universe[].name` value; the normalized symbol uses the
  token names.
- Public subscriptions used: `l2Book` (whole-book snapshot per push, 20 levels
  by default, 5 with `fast:true`; optional `nSigFigs`/`mantissa`),
  `trades` (`WsTrade[]`), `bbo` (`WsBbo` with bid/ask `WsLevel`), `candle`
  (intervals `1m,3m,5m,15m,30m,1h,2h,4h,8h,12h,1d,3d,1w,1M`),
  `activeAssetCtx` (perps: `funding`, `openInterest`, `oraclePx`, `markPx`,
  `dayNtlVlm`, `prevDayPx`, `midPx`; spot: `markPx`, `midPx`, ...).
- Control: `subscriptionResponse` acknowledges each subscribe, `error` channel
  reports failures; the docs do not state a ping/pong example, so liveness must
  be verified read-only during implementation and recorded.
- Limits: 10 WebSocket connections per IP, 30 new connections/minute, 1000
  subscriptions, 2000 outbound messages/minute, REST 1200 weight/minute
  (`l2Book`/`allMids` weight 2, most other info requests weight 20).

## Normalized mapping

| Hyperliquid | SDK |
| --- | --- |
| `l2Book` | `L2Book` snapshot only (no sequence/checksum; 20 or 5 levels) |
| `trades` | `Trade` (`side` `B`/`A`, `px`, `sz`, `time`, `tid`; `tid` alone is not globally unique) |
| `bbo` | `Ticker` + `L1Book` (documented bid/ask with sizes) |
| `candle` | `Candle` with documented intervals |
| `activeAssetCtx` perp | `Funding` (`funding`), `OpenInterest` (`openInterest`), `MarkPrice` (`markPx`), `IndexPrice` (`oraclePx`) |
| `activeAssetCtx` spot | `MarkPrice` (`markPx`) only |

Spot and perpetual rows are separate capabilities. No public liquidation
channel exists (liquidations appear only in private user events), so
`Liquidations` is not advertised.

## Scope

Public, unauthenticated data only; no trading, no user/private channels, no new
normalized categories, no delivery/retention/readiness changes. Unsupported
products, intervals, or book depths fail preflight.

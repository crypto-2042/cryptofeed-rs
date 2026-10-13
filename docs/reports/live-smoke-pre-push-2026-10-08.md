# Pre-push smoke — 2026-10-08

The 50-second aggregate run used explicit native mappings on release_smoke,
followed by Ctrl-C and exit code 0. No terminal feed statuses were reported.
It verified then-current decimal/field and identity corrections; it preceded
the later Bybit cache and Binance index/interval/calendar changes. The follow-up
sections distinguish their evidence rather than changing the historical counts.

| Exchange | Symbol | Normalized event counts |
| --- | --- | --- |
| Binance | BTC-USDT | Candles: 25, L2Book: 482, Ticker: 3941, Trade: 376 |
| Binance | BTC-USDT-PERP | Funding: 16, Index: 49, L1Book: 29683, L2Book: 472, MarkPrice: 16, Ticker: 29683, Trade: 498 |
| Bitget | BTC-USDT | Candles: 532, L1Book: 513, L2Book: 769, Ticker: 161, Trade: 169 |
| Bitget | BTC-USDT-PERP | Candles: 533, Funding: 451, Index: 451, L1Book: 1283, L2Book: 987, MarkPrice: 451, OpenInterest: 451, Ticker: 451, Trade: 380 |
| Bybit | BTC-USDT | Candles: 24, L2Book: 1462, Ticker: 423, Trade: 579 |
| Bybit | BTC-USDT-PERP | Candles: 44, Funding: 2, Index: 85, L2Book: 1699, MarkPrice: 33, OpenInterest: 6, Ticker: 128, Trade: 618 |
| Gateio | BTC-USDT | Candles: 22, L2Book: 424, Ticker: 257, Trade: 81 |
| Gateio | BTC-USDT-PERP | Candles: 25, Funding: 38, Index: 38, L1Book: 691, L2Book: 478, MarkPrice: 38, OpenInterest: 38, Ticker: 691, Trade: 288 |
| Okx | BTC-USDT | Candles: 33, L2Book: 454, Ticker: 309, Trade: 125 |
| Okx | BTC-USDT-PERP | Candles: 72, Funding: 1, Index: 163, L2Book: 482, MarkPrice: 241, OpenInterest: 7, Ticker: 309, Trade: 556 |

## Bybit ticker-state follow-up

The post-correction 40-second run received:

- BTC-USDT-PERP: Ticker 316, Funding 316, OpenInterest 316, Index 316, MarkPrice 316.
- BTC-USDT spot: Ticker 404.
- Exit code 0; no terminal feed failures.

The supported ticker row is reconstructed from known snapshot/delta state;
counts therefore represent emitted latest-model snapshots, including unchanged
fields, rather than individual field-change counts.

## Final index and interval follow-up

After the final source changes, a 35-second targeted run received:

- Binance BTC-USDT-PERP: Funding 33, Index 33, MarkPrice 33.
- Binance BTC-USD-PERP (COIN-M): Funding 33, Index 33, MarkPrice 33.
- Bitget BTC-USDT spot with normalized `1h` / wire `1H`: Candles 518,
  including the venue's initial batched/history delivery and subsequent pushes.
- Exit code 0; no terminal feed failures.

This verifies the final shared mark-price index source on both Binance product
families and the current Bitget uppercase hour interval. Calendar-month
boundaries and dated Funding rejection are offline regressions, not live
rollover observations.

## Limits

These are manual service checks, not CI fixtures or a performance benchmark.
The short runs do not prove complete liquidation coverage, all instruments,
calendar rollovers, large-symbol workloads, or behavior in other regions.
The scripts used only public data and no credentials. WebSocket DNS failed
inside the sandbox, so service checks ran outside it. Test/reference captures
remain sanitized and sourced separately.

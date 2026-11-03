# Exchange expansion design and implementation plan — 2026-11-03

Status: **plan plus first implementation slice.** Selected venues and their
protocol baselines: [research](cex-expansion-research-2026-11-03.md),
[KuCoin](kucoin-protocol.md), [Kraken](kraken-protocol.md). KuCoin is phase 1
because one channel set covers spot and futures with nine normalized
categories; Kraken is phase 2 because its spot protocol is small and precise.

## Scope

In scope: public spot and derivative market data for KuCoin (phase 1) and Kraken
Spot (phase 2), mapped onto the existing normalized model and capability
preflight. Out of scope: authenticated streams, trading, options/MARGIN, new
normalized categories, and any change to delivery, retention or readiness
semantics.

## Layer-by-layer changes

| Layer | Change |
| --- | --- |
| `crates/core` | Add `ExchangeId::Kucoin` (the enum is `#[non_exhaustive]`, so this is additive). `ExchangeId::Kraken` already exists. No model changes: KuCoin's `mark-price` payload is split into the existing `MarkPrice`, `IndexPrice` and `OpenInterest` models. |
| `crates/runtime/src/exchange/kucoin/` | New `adapter.rs` (routes, subscription frames, acknowledgement correlation, capability helpers), `parser.rs` (channel payload → normalized models), `book_sync.rs` (sequence `O`/`C` handling and snapshot-only depths). |
| `crates/runtime/src/exchange/kraken/` | Fill the existing stubs: `adapter.rs` (`wss://ws.kraken.com/v2`, `method` subscribe, ISO-8601 timestamps), `parser.rs`, and a CRC32 book checksum helper. |
| `markets.rs` | Capability rows per exchange/product; catalog fetch for `GET /api/ua/v2/market/instrument?tradeType=...` (KuCoin) and `GET /public/AssetPairs` (Kraken); product-qualified discovery, reverse mapping and a shared alias table (`XBT` → `BTC`, futures suffix handling). |
| `planning.rs` / `runtime.rs` / `runtime/connection.rs` | Route planning (spot vs futures host), `planned_url`, heartbeat policy (KuCoin public push liveness; Kraken `{"method":"ping"}`), `consume_*_feed` dispatch arms. |
| validation | Per-exchange channel sets, candle interval sets (KuCoin `1min`..`1month` without `6hour` on futures; Kraken minutes list), L2 depth sets (`obu` 1/5/50/increment; Kraken 10/25/100/500/1000), and unreachable combinations rejected in preflight. |
| docs | `PARITY.md` matrix rows and protocol gates, `README` exchange list, `docs/feature-status.html`, `docs/exchange-protocol-baseline.md`, `sample_data/SOURCES.md` for new fixtures. |

## Book and sequence rules

KuCoin:

- Target `obu` `depth=increment@10ms`: the first push is a snapshot, then deltas
  with `O`/`C`. A delta whose `O` is greater than the anchor + 1 is a gap and
  enters the existing bounded resnapshot path; `C <= anchor` is stale and
  dropped; otherwise the anchor advances to `C`.
- `depth=1/5/50` pushes are snapshots only. They are supported as parameterized
  partial books, not as full-depth recovery sources.
- `dp=increment` (without `@10ms`) is documented as deprecated in July 2026 and
  is not implemented.

Kraken:

- `book` snapshot then updates; the CRC32 over the top ten levels is verified on
  every push. A mismatch discards the local book and requests a resnapshot
  through the same bounded path used by other venues; checksum rules are never
  relaxed to keep a live subscription alive.

## Ordered work

1. Core enum, module skeletons, capability rows, catalog fetch and alias
   mapping; reject every unsupported KuCoin/Kraken product/channel in preflight.
2. KuCoin parsers for `ticker`, `trade`, `kline`, `mark-price`, `funding-fee`,
   `obu`; documented-frame fixtures and unit tests.
3. KuCoin adapter: routes, subscription frames, ack correlation, heartbeat,
   dispatch; book sync; full-session WebSocket double.
4. Kraken parsers (`ticker`, `trade`, `ohlc`) plus book checksum module;
   adapter and full-session double.
5. Capability/docs updates, PARITY rows, feature status, README, sample-data
   provenance.
6. Verification: workspace tests, strict Clippy, rustdoc, isolated features,
   MSRV 1.85, and a dated live smoke per venue (manual; not a CI gate).

## Verification and acceptance

- Deterministic gates must pass for every feature combination touched by the new
  exchange modules; the capability matrix stays authoritative.
- Parser fixtures come from the documented examples in the protocol baselines;
  live captures, if taken, must be sanitized and recorded in
  `docs/reports/live-smoke-YYYY-MM-DD.md`.
- A new venue is "implemented" only when preflight, catalog discovery, parsers,
  book sync, adapter and full-session double exist and its PARITY row is filled.
  Live long-run qualification remains separate, as for the five existing
  exchanges.

## Risks

- KuCoin UTA V2 is newer than the Classic API and its public push handshake,
  acknowledgement shape and subscription limits are only partially documented;
  the open questions in the protocol baseline must be answered before live use.
- KuCoin `ticker` is trade-driven, so it cannot substitute for an L1 channel.
- Kraken's `XBT` alias and quote diversity (USD/EUR/USDT) must not be flattened
  into a USDT assumption.
- Checksum/sequence code is safety-critical: a wrong implementation must fail
  closed (invalidate and resnapshot), never publish a corrupted book.

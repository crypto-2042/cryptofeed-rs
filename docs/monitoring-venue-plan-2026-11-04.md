# Monitoring venue expansion — design and implementation plan (2026-11-04)

Status: **implemented.** This plan covers the three venues selected in
[monitoring-venue-research-2026-11-04.md](monitoring-venue-research-2026-11-04.md):
Coinbase (Advanced Trade public spot), Deribit (JSON-RPC v2 perpetual/dated
futures) and HTX (spot and USDT-M perpetual). Per-venue endpoint facts, field
tables and verification notes live in
[coinbase-protocol.md](coinbase-protocol.md),
[deribit-protocol.md](deribit-protocol.md) and
[htx-protocol.md](htx-protocol.md).

No delivery, retention, readiness or lossless-semantics contract is changed.
Public unauthenticated data only; options, margin products, delivery/quarterly
HTX contracts and authenticated channels are out of scope. No new normalized
categories are introduced.

## Capability rows

| Exchange | Product | Ticker | Trade | L2Book | L1Book | Candles | Funding | OpenInterest | Index | MarkPrice | Liquidations |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Coinbase | Spot | yes | yes | yes | yes | `5m` only | — | — | — | — | — |
| Deribit | Perpetual | yes | yes | yes | — | yes (`chart.trades`) | yes | yes | yes | yes | — |
| Deribit | Futures | yes | yes | yes | — | yes (`chart.trades`) | — | yes | yes | yes | — |
| HTX | Spot | yes | yes | yes | yes | yes | — | — | — | — | — |
| HTX | Perpetual | yes | yes | yes | yes | yes | yes | — | — | — | yes |

Product decisions recorded in the protocol baselines:

- Coinbase `Ticker` and `L1Book` are both projected from the documented ticker
  BBO fields (which include sizes); `Candles` accepts only `5m` because the
  public channel is five-minute-only (the default `1m` fails preflight).
  REST candles exist but the stream runtime has no REST backfill path, so the
  1m gap is documented rather than hidden.
- Deribit `ticker` splits into `Ticker`, `MarkPrice`, `IndexPrice`,
  `OpenInterest` and `Funding`; `book`/`trades` use the public `100ms` interval
  (`raw` is authenticated-only); `chart.trades` maps the documented resolution
  enum to normalized intervals; `L1Book` is not advertised (no standalone L1
  channel; ticker BBO amounts are in the venue's USD unit and are not relabeled
  as an L1 stream). Options are filtered out by the catalog and rejected by
  preflight.
- HTX spot uses `wss://api.huobi.pro/ws` with `sub`/`id` framing (the brief's
  `/ws/v2` is the account socket and rejected a live market subscription);
  USDT-M market topics use `linear-swap-ws` `sub`/`id`; funding and liquidation
  notification topics use `linear-swap-notification` `op`/`topic` and therefore
  a second planned connection. Mark price, index price and open interest have
  no current public WebSocket topic and are not advertised; no REST polling is
  invented. GZIP framing is decoded before control/market handling.

## Layer-by-layer changes

1. **Identity** (`crates/core/src/exchange.rs`): add `ExchangeId::Deribit` and
   `ExchangeId::Htx` (the enum is `#[non_exhaustive]`; additive).
2. **Module skeleton** (`crates/runtime/src/exchange/{deribit,htx}.rs` plus
   `adapter.rs`/`parser.rs`/`book_sync.rs`, and fill in the existing
   `coinbase/` skeleton): builders exported from `prelude.rs`, static planning
   surface only.
3. **Parsers**: pure documented-frame functions returning normalized models or
   `None`; `rust_decimal::Decimal` prices/quantities, RFC 3339/ms/s timestamps
   parsed explicitly, no float conversions for money. Protocol-specific rules:
   Coinbase L2 `new_quantity` absolute updates; Deribit change-id chaining;
   HTX snapshot/version and incremental-version rules; Coinbase maker-side
   inversion; HTX base-coin `quantity` for swap trades.
4. **Book sync**: per-exchange state machines in `book_sync.rs`:
   - Coinbase: snapshot then absolute updates; pre-snapshot update fails closed;
     reconnect rebuilds (no in-session snapshot request contract).
   - Deribit: `change_id`/`prev_change_id`; stale discard, gap → bounded
     unsubscribe/subscribe resnapshot (3 per connection).
   - HTX: spot snapshot replacement with version dedupe; swap incremental
     snapshot + per-connection `version` chain with bounded in-session
     resubscribe (3 per connection).
5. **Catalogs** (`crates/runtime/src/markets.rs`): Coinbase
   `/api/v3/brokerage/market/products?product_type=SPOT` with cursor pagination;
   Deribit `public/get_instruments?currency=any&kind=future&expired=false`,
   options skipped and perpetual/dated classified by `settlement_period`;
   HTX spot `/v1/common/symbols` (`state=online`) and USDT-M
   `swap_contract_info` (`business_type=swap`, `contract_status=1`). Every
   catalog cross-checks native/base/quote identity and fails rather than
   guessing. `MarketInfo::parse` gains Coinbase/Deribit/HTX metadata arms.
6. **Capability preflight**: constants and rows above; candle interval maps;
   L2 depth/interval rejections (Coinbase no depth parameter, Deribit full-book
   only, HTX `step0`/`size_20` fixed); unsupported products/channels fail with
   `UnsupportedCapability` before a connection is planned.
7. **Adapter**: URLs/plans (`coinbase` one public host; `deribit` one JSON-RPC
   host; `htx` spot market host, swap market host and swap notification host
   selected by product/channel), subscription frames (one channel per Coinbase
   message, one JSON-RPC `public/subscribe` with all channels, HTX per-topic
   `sub`/`id` plus `op`/`topic` for notifications), control/ack/error
   classification, heartbeat policy, resnapshot frames.
8. **Runtime**: `consume_*_feed`/`consume_*_session` paths (HTX splits a
   perpetual feed into market and notification connections), message
   processing that classifies control frames before parsing, book-sync
   dispatch, normalized event dispatch and `planned_url`. Coinbase/Deribit are
   single-connection feeds; HTX uses `subscription_urls` + per-URL feed
   projection like Bybit/OKX.
9. **Connection layer** (`runtime/connection.rs`): GZIP decode hook for HTX
   binary frames (pinned `flate2`), heartbeat policy rows for Coinbase (server
   push, no client payload), Deribit (`public/set_heartbeat` + periodic
   `public/test` filtered as control), HTX market (integer `pong`) and HTX
   notification (`op`-`pong`). Readiness/acknowledgement correlation arms for
   the new framing.
10. **Docs and fixtures**: protocol baselines, plan, `PARITY.md` rows and gates,
    README/feature-status/baseline index, sanitized `sample_data` provenance
    and deterministic inline fixtures (no live-network tests).
11. **Verification**: `cargo fmt --all --check`, CI clippy with `-D warnings`,
    full workspace tests, no-default and per-feature lib checks, MSRV
    `cargo +1.85.0 check`, and `RUSTDOCFLAGS="-D warnings" cargo doc`.

## Dependency decision

HTX WebSocket pushes are GZIP binary frames on both hosts and the workspace had
no compression dependency. Add `flate2` at the workspace level with
`default-features = false, features = ["rust_backend"]` (pure-Rust miniz_oxide
backend, no system zlib, no async stack), pinned to the latest compatible
release. `flate2` is dual MIT/Apache-2.0 and supports the workspace MSRV. It is
used only to decompress `ExchangeId::Htx` binary frames; the alternative of
pulling a full async compression transport is explicitly rejected.

## Verification / acceptance

- Deterministic (offline) tests: documented-frame parser fixtures, catalog
  mapping and identity-contradiction rejection, preflight accept/reject
  matrices, book-sync sequence/gap/resnapshot regressions, dispatch tests and a
  full-session WebSocket double per venue.
- Live probes are read-only and sanitized; they are not committed as captures
  and are not required by CI.
- Gates listed above must pass before the venue commits are pushed.

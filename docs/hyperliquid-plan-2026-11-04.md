# Hyperliquid public market data — design and implementation plan (2026-11-04)

Status: **implemented.** This plan adds Hyperliquid perpetuals and spot to the
public, unauthenticated market-data path selected in
[hyperliquid-research-2026-11-04.md](hyperliquid-research-2026-11-04.md).
Endpoint facts, live verification and open questions live in
[hyperliquid-protocol.md](hyperliquid-protocol.md).

No delivery, retention, readiness or lossless-semantics contract is changed.
Public unauthenticated data only; no user/private channels, no trading, no new
normalized categories. Dated futures, options, margin products, HIP-3 perp dexs
and outcome markets are out of scope.

## Capability rows

| Exchange | Product | Ticker | Trade | L2Book | L1Book | Candles | Funding | OpenInterest | Index | MarkPrice | Liquidations |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Hyperliquid | Spot | yes | yes | yes (20/5 snapshot) | yes | yes | — | — | — | yes | — |
| Hyperliquid | Perpetual | yes | yes | yes (20/5 snapshot) | yes | yes | yes | yes | yes | yes | — |

Product decisions recorded in the protocol baseline:

- `l2Book` is a whole-book **snapshot** push (20 levels by default, 5 with the
  documented `fast` flag). The runtime accepts only those two requested depths
  and rejects every other depth at preflight; it never models the snapshot as a
  delta or invents a sequence/checksum.
- `Ticker` and `L1Book` are both projected from `bbo`, which carries best
  bid/ask **with sizes**; a documented null side emits no quote at all.
- The four perpetual context channels share one `activeAssetCtx` subscription;
  spot uses the live `activeSpotAssetCtx` channel and emits `MarkPrice` only.
- `Liquidations` is not advertised: public liquidation data exists only in
  private user events.
- Perpetuals are quoted in USD (`BTC-USD-PERP`); spot normalized symbols come
  from token names with arbitrary quotes preserved (`UBTC-USDH`), while the
  subscription coin stays the native `PURR/USDC` or `@N` value.

## Layer-by-layer changes

1. **Identity** (`crates/core/src/exchange.rs`): add
   `ExchangeId::Hyperliquid` (the enum is `#[non_exhaustive]`; additive).
2. **Module skeleton** (`crates/runtime/src/exchange/hyperliquid.rs` plus
   `adapter.rs`/`parser.rs`/`book_sync.rs`), builder exported from
   `prelude.rs`; `HyperliquidBookSync` map added to `ExchangeFeed` and cleared
   with the other per-exchange book state.
3. **Parsers** (`parser.rs`): pure documented-frame functions returning
   normalized models or `None`. Protocol rules: decimal strings; ms
   timestamps; `side` `B`/`A`; `tid` preserved but not treated as globally
   unique; `bbo` null sides reported and never fabricated; whole-book
   snapshots; candle object-or-array shape; `activeAssetCtx` split fields;
   fail closed on malformed frames.
4. **Book sync** (`book_sync.rs`): snapshot-only state machine; every push is
   an authoritative replacement, no delta/sequence/checksum, reconnect resets
   local state. Empty sides are valid snapshots.
5. **Catalogs** (`crates/runtime/src/markets.rs`): `POST /info` support added
   to the catalog fetch path (the shared cache/recording/replay key is the
   documented URL plus a synthetic `?type=meta`/`?type=spotMeta`
   discriminator, while the request itself goes to the documented base URL).
   - `{"type":"meta"}`: perps, `isDelisted` skipped, quote USD, `szDecimals`
     surfaced as quantity decimal places.
   - `{"type":"spotMeta"}`: token index map, base/quote from token names,
     subscription coin from `universe[].name` (`PURR/USDC` or `@N`), arbitrary
     quotes preserved, `isDelisted` skipped, canonical/`@N`/token-index
     consistency checks fail closed.
   - `SymbolRegistry` preserves the native coin spelling for Hyperliquid
     (case-sensitive `kPEPE`) while reverse lookup stays case-insensitive; the
     explicit `exchange_symbol` builder path does the same for this venue.
6. **Capability preflight** (`markets.rs`): capability rows above; documented
   candle intervals; L2 depth accepts 20/5 only; unsupported products,
   channels, intervals and depths fail with `UnsupportedCapability` before a
   connection is planned.
7. **Adapter** (`adapter.rs`): single host `wss://api.hyperliquid.xyz/ws`;
   per-coin subscribe/unsubscribe framing; shared `bbo`/`activeAssetCtx`
   requests; `subscriptionResponse`/`error`/`pong` classification; readiness
   key `(type, coin, interval)`; `planned_url`; runtime consume/dispatch.
8. **Runtime** (`runtime.rs`): `consume_hyperliquid_feed`/session paths,
   snapshot-only book sync dispatch, normalized event dispatch, planned URL.
   Planning budgets 1000 subscriptions per connection, and the connection
   budget is tightened to the documented 10 connections with a 2 s start
   interval.
9. **Connection layer** (`runtime/connection.rs`): documented client
   `{"method":"ping"}` heartbeat every 30 s, `{"channel":"pong"}` filtered as
   liveness, 60 s idle window.
10. **Recording/HTTP**: the info URL is whitelisted for public HTTP capture so
    the two catalog POST bodies can be recorded/replayed under distinct keys.
11. **Docs and fixtures**: this plan, the protocol baseline, `PARITY.md` rows
    and protocol gates, README/feature-status/baseline index, sanitized
    `sample_data` provenance and deterministic inline fixtures (no
    live-network tests).
12. **Verification**: `cargo fmt --all --check`, CI clippy with `-D warnings`,
    full workspace tests, no-default and per-feature lib checks, the isolated
    feature test combinations from `.github/workflows/ci.yml`, MSRV
    `cargo +1.85.0 check`, and `RUSTDOCFLAGS="-D warnings" cargo doc`.

## Ordered work

1. Protocol baseline + plan + docs index.
2. Identity, module skeleton, parser and book sync with fixtures.
3. Catalog POST path + symbol identity + preflight.
4. Adapter framing + readiness correlation + runtime consume/dispatch.
5. Full-session WebSocket double, dispatch, parser and preflight tests.
6. Documentation/status updates and the full verification matrix.

## Verification / acceptance

- Deterministic (offline) tests: documented-frame parser fixtures; catalog
  mapping including `@N` spot pairs, arbitrary quotes and delisted entries;
  preflight accept/reject for products, channels, intervals and depths; `bbo`
  null-side handling; `activeAssetCtx` splitting; dispatch and a full-session
  WebSocket double with subscribe/ack/market-data/clean shutdown; heartbeat
  ping/pong classification; error-channel failure.
- No live-network tests were added. The only live probes were read-only,
  unauthenticated public requests; sanitized shapes and evidence are recorded
  in [hyperliquid-protocol.md](hyperliquid-protocol.md).
- Feature gates match the existing exchanges: every channel compiles behind
  its own feature and with no default features, and parser tests are gated with
  the same features as the functions they exercise.

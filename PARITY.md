# Python Baseline vs Rust Parity Checklist

Last updated: 2026-04-22

## Goal

Before adding more CEX integrations, `cryptofeed-rs` should align with the Python `cryptofeed` baseline for core public market-data behavior using Binance and Bitget as the first two validation exchanges.

The Rust implementation may use newer official exchange APIs than Python, but the normalized public behavior should remain compatible with Python's public feed model.

## Baseline From Python `cryptofeed`

Relevant Python public market-data concepts:

- `FeedHandler.add_feed(...)`
- exchange-level `channels`
- exchange-level `symbols`
- normalized callbacks
- normalized symbols
- unix timestamp normalization
- decimal price/size precision
- websocket connection lifecycle
- reconnect behavior
- public market channels:
  - `TICKER`
  - `TRADES`
  - `L2_BOOK`
  - `CANDLES`
  - exchange-specific public channels such as `FUNDING`, `OPEN_INTEREST`, `LIQUIDATIONS`

Current Rust public baseline scope for Binance and Bitget:

- `ticker`
- `trade`
- `l2_book`
- `candles`
- Binance-specific `funding`
- Binance-specific `liquidations`
- Binance
- Bitget v3

## Global Parity Requirements

- [x] Rust workspace exists under `rust/cryptofeed-rs`.
- [x] User entrypoint preserves the `FeedHandler` mental model.
- [x] Public data categories are split into category crates.
- [x] Exchange implementations live in `crates/runtime`.
- [x] Public runtime supports multiple feeds concurrently.
- [x] Runtime supports graceful shutdown.
- [x] Runtime supports reconnect/backoff.
- [x] Runtime uses decimal price and amount types.
- [x] Runtime normalizes symbols to `BASE-QUOTE`.
- [x] New exchange implementations must use latest stable official APIs.
- [x] Add public examples showing Binance and Bitget usage.
- [x] Add fixture-based parity tests from captured Python/Rust normalized events.

## Binance Public Parity

Python Binance supports public channels including:

- `TICKER`
- `TRADES`
- `L2_BOOK`
- `CANDLES`
- `FUNDING`
- `LIQUIDATIONS`

Rust Binance current support:

- [x] `ticker`
- [x] `trade`
- [x] `l2_book`
- [x] `candles`
- [x] `funding`
- [x] `liquidations`

Runtime parity:

- [x] websocket URL planning
- [x] combined stream support
- [x] live websocket connection
- [x] text message processing
- [x] normalized ticker parser
- [x] normalized trade parser
- [x] normalized l2 book parser
- [x] normalized candle parser
- [x] handler dispatch
- [x] per-symbol `L2BookState`
- [x] REST snapshot parser
- [x] websocket depth delta sequence parser
- [x] snapshot bootstrap primitives
- [x] stale delta discard
- [x] gap detection
- [x] gap-triggered resync scheduling
- [x] bootstrap snapshot dispatch
- [ ] full live bootstrap integration test with mocked websocket + mocked REST snapshot
- [ ] checksum validation, if applicable
- [ ] documented recovery behavior after repeated snapshot failures
- [x] normalized funding parser and handler
- [x] normalized liquidation parser and handler

## Bitget v3 Public Parity

Python Bitget supports public channels including:

- `TICKER`
- `TRADES`
- `L2_BOOK`
- `CANDLES`

Rust Bitget uses latest stable v3/UTA public API, not legacy Python API shape.

Rust Bitget current support:

- [x] `ticker`
- [x] `trade`
- [x] `l2_book`
- [x] `candles`

Runtime parity:

- [x] official v3 public websocket URL
- [x] subscribe payload generation
- [x] live websocket connection
- [x] subscribe message send
- [x] subscribe ack ignore path
- [x] text message processing
- [x] normalized ticker parser
- [x] normalized trade parser
- [x] normalized l2 book parser
- [x] normalized candle parser
- [x] handler dispatch
- [x] per-symbol `L2BookState`
- [x] books snapshot/update sync state
- [x] seq/pseq gap detection
- [ ] full live subscription integration test with mocked websocket
- [ ] documented recovery behavior after seq/pseq gap
- [ ] checksum validation, if applicable

Unsupported Python public channels in current Bitget parity scope:

- `funding`: not part of current Python Bitget spot public baseline.
- `liquidations`: not part of current Python Bitget spot public baseline.
- `open_interest`: not part of current Python Bitget spot public baseline.
- `index`: not part of current Python Bitget spot public baseline.

## Deferred Public Channels

These are public concepts in Python `cryptofeed`, but are not required before expanding beyond Binance and Bitget spot-public baseline unless explicitly moved into scope:

- `open_interest`
- `index`
- exchange-specific derivatives-only variants

These should be reconsidered when futures/perpetual support is added.

## Verification Baseline

Run from `rust/cryptofeed-rs/`:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace
```

## Expansion Gate

Do not add new CEX runtime implementations until:

- [x] Binance and Bitget examples exist.
- [x] Binance and Bitget fixture tests exist.
- [x] `ticker`, `trade`, `l2_book`, and `candles` parity is documented as complete for both exchanges.
- [x] remaining unsupported Python public channels are explicitly accepted as out of current scope or added to the Rust plan.
- [x] Binance `funding` and `liquidations` are implemented or explicitly deferred.

After this gate, new CEX integrations must use latest stable official APIs.

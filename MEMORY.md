# MEMORY

Project-specific memory for `rust/cryptofeed-rs`.

## Structural Memory

- The Rust workspace lives at `rust/cryptofeed-rs/`.
- Short directory names are intentional:
  - `crates/core`
  - `crates/ticker`
  - `crates/trade`
  - `crates/orderbook`
  - `crates/runtime`
- Package names keep the `cryptofeed-` prefix.
- Exchange implementations are centralized in `crates/runtime`.

## API Policy Memory

- The Rust implementation must follow the latest stable official exchange API, not whatever legacy API exists in Python.
- Bitget is the canonical example: Rust uses official v3.
- Official vendor docs are the source of truth for exchange endpoints and channel naming.

## Runtime Memory

- Binance currently has the most mature public runtime path:
  - websocket URL planning
  - live websocket connect
  - combined-stream unwrap
  - ticker/trade/l2_book/candles/funding/liquidations parsing
  - snapshot parser and delta sequence parsing
  - local book sync primitives
  - bootstrap snapshot dispatch
  - gap -> resync scheduling
  - handler dispatch
  - reconnect/backoff
  - concurrent feed execution
  - graceful shutdown
- Bitget v3 currently has:
  - official public websocket URL
  - live subscription payload generation using `instType/topic/symbol`
  - ticker/trade/l2_book/candles parsing
  - runtime-side dispatch and text-message processing
  - books snapshot/update sync semantics
  - books sequence gap detection
  - live websocket session now sends real subscribe payloads
  - it is now much closer to Binance in public runtime coverage, but Binance still has the more advanced order book bootstrap/resync path
- Bybit V5 currently has:
  - official public spot websocket URL
  - live subscription payload generation
  - ticker/trade/l2_book/candles parsing
  - runtime-side dispatch and text-message processing
  - dedicated order book sync state
- OKX v5 currently has:
  - official public websocket URL
  - live subscription payload generation
  - ticker/trade/l2_book/candles parsing
  - runtime-side dispatch and text-message processing
  - dedicated order book sync state

## Testing Memory

- Some exact-name test filters from the original plan do not match Rust’s fully qualified unit test names.
- When needed, use qualified exact names such as `module::tests::name`.
- Current clean verification baseline for the Rust workspace is:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-features -- -D warnings`
  - `cargo test --workspace`

## Operational Memory

- `cargo` commands in this environment often regenerate `rust/cryptofeed-rs/Cargo.lock` and `rust/cryptofeed-rs/target/`; remove them before commits.
- This repo frequently hits transient `.git/index.lock` races; check whether the lock still exists before trying to clean it up.

## Near-Term Priorities

- Use `PARITY.md` as the source of truth before adding more CEX implementations.
- The scoped Binance/Bitget public parity gate is complete.
- Bybit and OKX live public runtime paths are now present.
- Bybit and OKX order book sync semantics are now present.
- Next maturity work should add deeper mocked websocket integration tests, then continue with Coinbase/Kraken live runtime.

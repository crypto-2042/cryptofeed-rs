# cryptofeed-rs

Public-first Rust workspace for normalized cryptocurrency exchange market data.

## Crates

- `cryptofeed-core`
- `cryptofeed-ticker`
- `cryptofeed-trade`
- `cryptofeed-orderbook`
- `cryptofeed-rs`

## Architecture

- Type crates (`cryptofeed-ticker`, `cryptofeed-trade`, `cryptofeed-orderbook`, `cryptofeed-candles`, `cryptofeed-funding`, `cryptofeed-liquidations`) define normalized public models and handler traits as a stable API surface.
- `cryptofeed-rs` (`crates/runtime`) owns exchange adapters, websocket/http transport, routing, reconnect/shutdown, and parsing/runtime orchestration as the implementation surface.

## Development

```bash
cargo check --workspace
cargo test --workspace
cargo fmt --all
cargo clippy --workspace --all-features -- -D warnings
```

## Examples

Run from `rust/cryptofeed-rs/`:

```bash
cargo run -p cryptofeed-rs --example binance_public
cargo run -p cryptofeed-rs --example bitget_public
```

Example imports use the runtime prelude:

```rust
use async_trait::async_trait;
use cryptofeed_rs::prelude::*;
```

## Exchange Policy

New CEX integrations must use the latest stable official exchange API rather than legacy versions.

- Bitget integrations should target the current official v3 API surface.
- When adding or updating an exchange adapter, verify the current official REST and WebSocket documentation before implementing.
- If an older API version already exists in the Python codebase, treat it as migration reference only, not as the Rust source of truth.

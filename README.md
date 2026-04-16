# cryptofeed-rs

Public-first Rust workspace for normalized cryptocurrency exchange market data.

## Crates

- `cryptofeed-core`
- `cryptofeed-ticker`
- `cryptofeed-trade`
- `cryptofeed-orderbook`
- `cryptofeed-rs`

## Development

```bash
cargo check --workspace
cargo test --workspace
cargo fmt --all
cargo clippy --workspace --all-features -- -D warnings
```

## Exchange Policy

New CEX integrations must use the latest official exchange API rather than legacy versions.

- Bitget integrations should target the current official v3 API surface.
- When adding or updating an exchange adapter, verify the current official REST and WebSocket documentation before implementing.
- If an older API version already exists in the Python codebase, treat it as migration reference only, not as the Rust source of truth.

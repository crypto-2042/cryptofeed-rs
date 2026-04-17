# Progress

Last updated: 2026-04-17

## Current State

`cryptofeed-rs` has moved beyond scaffolding. The workspace, shared abstractions, public model crates, runtime orchestration, Binance public runtime path, Bitget v3 public parser/runtime path, reconnect/backoff, concurrent feed execution, and graceful shutdown support are all in place.

## Completed

- Workspace scaffolding and package layout
- `core` abstractions:
  - `ExchangeId`
  - `Channel`
  - `Symbol`
  - `Subscription`
  - shared `Error` / `Result`
- Public model crates:
  - `Ticker`
  - `Trade`
  - `L2Book`
  - public handler traits
- Runtime shell:
  - `FeedHandler`
  - exchange builders
  - router shell
  - transport shell
  - supervision shell
- Binance public runtime:
  - websocket URL planning
  - live websocket connect
  - combined-stream unwrap
  - `ticker` / `trade` / `l2_book` parsing
  - handler dispatch
  - reconnect/backoff
  - concurrent runtime execution
  - graceful shutdown support
- Bitget v3 public:
  - official v3 public websocket URL
  - live subscription payload generation
  - `ticker` / `trade` / `l2_book` parsing
  - runtime dispatch and text-message processing
  - websocket session sends real subscribe messages

## In Progress

- Bitget v3 still needs the same runtime maturity as Binance for reconnect/ack/channel lifecycle coverage.

## Not Started

- Private/authenticated support
- Coinbase live runtime path
- Kraken live runtime path
- full order book snapshot + delta synchronization semantics

## Verification Baseline

Run from `rust/cryptofeed-rs/`:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace
```

## Recent Milestones

- `4189b5e` `feat: add cryptofeed-rs workspace skeleton`
- `618c6c6` `feat: add cryptofeed core abstractions`
- `d6c0758` `feat: add public category crates`
- `a384bfc` `feat: add feedhandler runtime shell`
- `21d9fd2` `feat: add exchange feed builders`
- `92a0c94` `feat: add runtime transport and parser shells`
- `87eb2e0` `chore: finish rust workspace verification`
- `f9fe4e4` `feat: add binance public runtime planning`
- `57f08b5` `docs: require latest exchange APIs`
- `8659a76` `feat: add binance event dispatch hooks`
- `1efdef8` `feat: connect binance websocket runtime`
- `9b8c749` `feat: add binance l2 book handling`
- `0f48a77` `feat: run feeds concurrently`
- `53d5c6e` `feat: add runtime reconnect backoff`
- `4b1768b` `feat: add bitget v3 public runtime`
- `fac83ae` `feat: add graceful shutdown for runtime`

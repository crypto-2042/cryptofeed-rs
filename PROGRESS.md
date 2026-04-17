# Progress

Last updated: 2026-04-17

## Current State

`cryptofeed-rs` has moved beyond scaffolding. The workspace, shared abstractions, public model crates, runtime orchestration, Binance public runtime path, Bitget v3 public runtime path, reconnect/backoff, concurrent feed execution, graceful shutdown, and order book state synchronization are all in place.

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
  - snapshot parser
  - delta sequence parsing
  - local book sync primitives
  - bootstrap snapshot dispatch
  - gap -> resync scheduling
  - handler dispatch
  - reconnect/backoff
  - concurrent runtime execution
  - graceful shutdown support
- Bitget v3 public:
  - official v3 public websocket URL
  - live subscription payload generation
  - `ticker` / `trade` / `l2_book` parsing
  - runtime dispatch and text-message processing
  - books snapshot/update sync semantics
  - books sequence gap detection
  - websocket session sends real subscribe messages

## In Progress

- Binance and Bitget are now the primary “baseline parity” exchanges for public market data validation.
- The next remaining maturity work is deeper order book synchronization semantics and then parity validation against Python behavior.

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
- `50610ad` `feat: add binance book sync primitives`
- `c3e595c` `feat: integrate binance book sync state`
- `a0317e7` `feat: dispatch binance bootstrap snapshots`
- `2e545c7` `feat: track l2 book runtime state`
- `321d7ce` `feat: resync binance book gaps`

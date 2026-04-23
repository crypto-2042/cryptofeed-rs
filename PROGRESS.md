# Progress

Last updated: 2026-04-22

## Current State

`cryptofeed-rs` has moved beyond scaffolding. The workspace, shared abstractions, public model crates, runtime orchestration, Binance public runtime path, Bitget v3 public runtime path, Bybit V5 public runtime path, OKX v5 public runtime path, reconnect/backoff, concurrent feed execution, graceful shutdown, order book state synchronization, and scoped baseline public parity are all in place.

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
  - `Candle`
  - `Funding`
  - `Liquidation`
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
  - `ticker` / `trade` / `l2_book` / `candles` / `funding` / `liquidations` parsing
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
  - `ticker` / `trade` / `l2_book` / `candles` parsing
  - runtime dispatch and text-message processing
  - books snapshot/update sync semantics
  - books sequence gap detection
  - websocket session sends real subscribe messages
- Bybit V5 public:
  - official V5 public spot websocket URL
  - live subscription payload generation
  - `ticker` / `trade` / `l2_book` / `candles` parsing
  - runtime dispatch and text-message processing
  - websocket session sends real subscribe messages
  - order book sync semantics
- OKX v5 public:
  - official v5 public websocket URL
  - live subscription payload generation
  - `ticker` / `trade` / `l2_book` / `candles` parsing
  - runtime dispatch and text-message processing
  - websocket session sends real subscribe messages
  - order book sync semantics
- Gate.io API v4 public:
  - official v4 public websocket URL
  - live subscription payload generation
  - `ticker` / `trade` / `l2_book` / `candles` parsing
  - runtime dispatch and text-message processing
  - websocket session sends real subscribe messages

## In Progress

- Binance and Bitget scoped public baseline parity is complete for the currently selected channels.
- `PARITY.md` is the source of truth for expansion gates before adding more CEX implementations.
- Bybit and OKX now have live public runtime scaffolding, parity fixtures, and order book sync semantics.

## Not Started

- Private/authenticated support
- Coinbase live runtime path
- Kraken live runtime path
- deeper mocked websocket integration tests
- Gate.io order book sync semantics
- futures/perpetual-only public channels such as `open_interest` and `index`

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
- `be87f2e` `feat: add bybit okx public scaffolding`
- `13f146e` `feat: wire bybit public runtime`
- `b4a88e5` `feat: wire okx public runtime`
- `0a40c13` `feat: add bybit order book sync`
- `eae1fd4` `feat: add okx order book sync`
- `449ff49` `feat: add candles public parity`
- `0f53a06` `feat: add binance funding liquidation parity`

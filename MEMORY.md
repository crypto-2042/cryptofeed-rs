# MEMORY

Project-specific memory for `rust/cryptofeed-rs`.

## Structural Memory

- The Rust workspace lives at the repository root.
- Short directory names are intentional:
  - `crates/core`
  - `crates/ticker`, `crates/trade`, `crates/orderbook`, `crates/candles`,
    `crates/funding`, `crates/liquidations`, `crates/openinterest`,
    `crates/index`, `crates/markprice`
  - `crates/runtime`
- Package names keep the `cryptofeed-` prefix.
- Exchange implementations are centralized in `crates/runtime`.
- The category crates are intentionally thin API crates (normalized models + handler traits), while runtime owns transport/protocol execution.
- Prefer `prelude` exports (`cryptofeed_rs::prelude::*` and crate-local preludes) to keep examples and user code imports stable.

## API Policy Memory

- The Rust implementation must follow the latest stable official exchange API, not whatever legacy API exists in Python.
- Bitget is the canonical example: Rust uses official v3 instruments, topics,
  and payload shapes rather than relabeled v2 fixtures.
- Official vendor docs are the source of truth for exchange endpoints and channel naming.
- Binance spot `bookTicker` has no `e`/`E`; USD-M book/depth and
  trade/kline/mark-price/liquidation currently require public/market WebSocket
  connection splitting.
- Bitget v3 successful `event: subscribe` objects have no market `data` and must
  be classified before topic dispatch.
- Bybit v5 spot `tickers` is a 24-hour statistics feed without BBO. The project
  Ticker contract uses `orderbook.1` for Bybit spot; derivatives keep their
  native ticker channel.
- Gate `quanto_multiplier` is not product identity and may be zero in a live,
  non-delisted inverse contract record.

## Runtime Memory

- A feed is product-homogeneous. Spot, perpetual, and dated futures must not be
  mixed in one `ExchangeFeed`.
- Symbol discovery and reverse parsing are product-qualified. Do not reintroduce
  separator stripping or quote-currency guessing as a fallback.
- Capability validation is authoritative: only combinations in the verified
  matrix may open a connection.
- Normal remote WebSocket closure is reconnectable; explicit shutdown is clean.
- Exchange application heartbeat and idle detection belong to the shared
  session lifecycle.
- Terminal failure of one feed must remain observable without cancelling healthy
  feeds.
- Never relax an L2 bridge/gap rule solely because a live bootstrap is
  intermittent. Capture the failing snapshot ID and buffered sequence, then add
  a bounded resnapshot regression.

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
  - ticker/trade/l1_book/l2_book/candles parsing; derivative ticker funding/OI/index/mark-price events (2026-10-08)
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
- Gate.io API v4 currently has:
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
  - `cargo clippy --workspace --all-features --all-targets -- -D warnings`
  - `cargo test --workspace`
- Manual live-smoke outcomes belong in dated `docs/reports/live-smoke-*.md` reports and
  remain outside CI.
- Model contract tests (including serde roundtrip checks) should live in the category crates first, with runtime tests focused on exchange parsing/routing behavior.
- Full-session doubles drive the generic `consume_*_session_with` helpers over
  `WebSocketStream<DuplexStream>` pairs; extend those helpers rather than
  spawning real connections in tests.
- OKX checksum validation is scoped to the full-depth `books` channel. The
  string reconstruction relies on `Decimal` preserving transmitted scale;
  `books5`/`bbo-tbt` checksums only cover transmitted top levels and must not
  be compared to the local book.
- Gate.io bootstrap failures trigger a bounded in-session resnapshot
  (`GATEIO_MAX_RESNAPSHOTS` per symbol) that preserves the buffered deltas;
  only the limit-exceeded case escalates to the session-level retry. Never
  relax the bridge rule to make a live bootstrap pass.
- Gate.io bootstrap dispatch returns the snapshot plus the applied buffered
  delta events; a bootstrap that silently drops buffered deltas from the
  handler event stream is a defect (Binance already followed this pattern).
- Gate.io `full: true` pushes replace the local book and re-anchor the
  sequence at the push's `u` (announcements 44678/44722, mainnet
  2026-05-06); deltas before the push are superseded by it.
- Gate.io reconnects must regenerate subscription `time`; subscribe responses
  are successful only when `error` is null. An id-less REST book snapshot is
  not consumed until a WebSocket delta is buffered to provide an anchor.
- Binance spot partial-depth pushes (`@depth5/10/20`) carry only
  `{lastUpdateId,bids,asks}` — no `e`/`s`/`U`/`u`; the instrument lives in
  the combined-stream `stream` name and each push replaces the top-N book.
  Partial-depth streams (all products) must be bootstrapped from a snapshot
  of the same width.
- OKX checksum strings interleave per index (`bid1:sz1:ask1:sz1:...`), not
  all bids then all asks; OKX VIP4 tbt depth (50/400) stays rejected at
  preflight until a live capture allows wiring.
- Binance idle detection needs > 3 minutes (server ping cadence): the 90s
  default killed quiet feeds; Binance uses a 240s idle timeout.
- L2 snapshot REST fetches carry a 15s HTTP timeout and pending-delta
  buffers are bounded; a dropped buffered delta surfaces as a non-bridging
  bootstrap and routes into the bounded resnapshot path.

## Operational Memory

- `cargo` commands in this environment often regenerate `rust/cryptofeed-rs/Cargo.lock` and `rust/cryptofeed-rs/target/`; remove them before commits.
- This repo frequently hits transient `.git/index.lock` races; check whether the lock still exists before trying to clean it up.
- The Binance developer docs platform (developers.binance.com, Zudoku) is fully
  client-rendered and returns no content to plain HTTP fetches; bybit-
  exchange.github.io (Bybit v5) and okx.com/docs-v5 remain fetchable. When
  verifying Binance protocol facts, prefer the repo baseline
  (`docs/exchange-protocol-baseline.md`, verified 2026-08-04) and sanitized
  captures, and record the verification gap instead of inventing payloads.
- Bybit v5 exposes no standalone index-price channel (verified 2026-08-06):
  `indexPrice`/`markPrice` ride the derivative `tickers.{symbol}` stream.
- Candle interval and L2 depth levels are validated per exchange during feed
  preflight; the normalized interval vocabulary is Binance-style
  (`1m`..`1M`), and each adapter maps it to its own wire form. Never add a
  wire form to the normalized vocabulary without adding the exchange mapping.
- OKX MARGIN instIds are identical to spot (`BTC-USDT`); the product kind is
  carried by `InstrumentKind::Margin`/`Symbol::margin` (explicit `.instrument`
  only) and events are rebound to the feed instrument in
  `OkxAdapter::parse_messages_for_feed`. Payload parsers alone can never
  recover the margin identity.
- OKX order-book channels: `books`/`books5`/`bbo-tbt` plus VIP4+-gated
  `books-l2-tbt`/`books50-l2-tbt`; `books50-l2`/`books400-l2` do not exist.
  Bybit options depth is `orderbook.{25,100}` only — options default to
  level 25.
- Exchange protocols drift: Bybit removed `funding.{symbol}` (funding now on
  the tickers stream), OKX moved `liquidation-orders` to instType scope and
  transmits `checksum: 0` on books, Gate.io dropped the order_book `id`
  field, and Binance eapi optionSymbols carries `underlying` instead of
  `baseAsset`. Live smoke runs are the only way to catch these; keep
  baseline fixtures updated with the measured facts and dates.
- Catalog hydration uses a 24h in-process cache because Binance exchangeInfo
  (~17.5 MB) downloads take 6–60+ seconds in this environment. HTTP status and
  exchange error envelopes are validated before caching, and responses are
  capped at 32 MiB. Live WS connections to
  binance.com/ws.okx.com exhibit transient slow startups — a smoke window
  shorter than ~40s may show zero events through no fault of the runtime.

## Pre-push review lessons (2026-10-08)

- Official numeric field semantics override legacy Python mappings: Binance
  `P` is a settlement price; OKX `oiCcy` is a coin quantity. Neither is a
  rate/currency identifier. Correct fixtures and public model contracts together.
- JSON numbers must retain decimal precision before Decimal parsing.
- Funding is perpetual/swap-only; sharing a price transport does not enable
  Funding for dated futures. All five dated Funding requests fail preflight.
- Binance Index uses documented `markPriceUpdate.i` and event time `E`, sharing
  the 1s mark-price topic when requested. `T` is next funding time, not Index time.
- Bybit derivative tickers require per-session snapshot/delta reconstruction;
  absent delta fields retain prior known values and reconnect clears the cache.
- Bitget v3 normalized hours/days map to uppercase `1H/4H/6H/12H/1D`; reject
  undocumented longer intervals. OKX `1M/3M` use UTC+8 calendar arithmetic.
- Bitget book sync and market-wide liquidation dispatch bind to hydrated
  native-symbol mappings to preserve dated-series identity.
- Resolver 3 honors Rust 1.85 for fresh lockfile-free resolution. Audit the
  resolved dependencies; never hide a vulnerability to pass a release gate.
- Confirmed GitHub repository: https://github.com/crypto-2042/cryptofeed-rs.
  Rust authorship is unset rather than attributed to the Python author;
  GitHub private vulnerability reporting was enabled and verified on
  2026-10-08. License/provenance remains pending for a formal release;
  source submission to GitHub was explicitly authorized by the maintainer.

## Near-Term Priorities

- Preserve the five-exchange spot/perpetual/futures matrix with sourced offline
  fixtures before expanding it.
- Use `PARITY.md` as the source of truth for verified capabilities.
- Add deeper deterministic session doubles and remaining checksum coverage.
- Complete release/license metadata before publishing.
- Coinbase/Kraken and additional public channel families require a new explicit
  plan before implementation.

## Scope Memory

- The 0.1 runtime supports only spot, perpetual/swap, and dated futures.
  Options and MARGIN protocol helpers, fixtures, and parser tests stay on
  mainline as future references, but every such feed is rejected by the
  authoritative capability matrix until a new explicit plan reopens it.
- Git branches are for experimental work that has not completed the harness
  loop (fixture + parity assertion + capability-matrix cell + docs). A branch
  merges only after its cells are green on mainline rules.

## Public channel completion (2026-10-08)

- No standalone topic does not mean no public data: Bitget v3 derivative
  `ticker` includes funding, OI, index, and mark price. Preserve absent fields
  and native units instead of fabricating rates, denominations, or USD values.
- Bitget L1 uses `books1` on spot/contracts and must not alter a full L2 book.
- Gate.io `futures.public_liquidates` is public; `futures.liquidates` is private.
  Only perpetual capability is enabled; delivery requires separate evidence.
- OKX native SWAP/FUTURES IDs map to base/quote index IDs. One shared index
  push fans out to all matching configured contract symbols.
- Binance contract OI is explicitly deferred by the user (2026-10-08), not
  merely waiting for a parser implementation. No verified official native
  perpetual/dated-futures OI WS path was found; current OI REST is available
  via weight-1 single-symbol requests. Multi-symbol polling/retries can
  exhaust the shared IP budget, so do not add a polling fallback or guessed
  WS topic. Options OI WS is separate. Reopening requires an explicit
  user-approved plan; see `docs/binance-open-interest-decision.md`.

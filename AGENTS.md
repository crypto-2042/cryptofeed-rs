# cryptofeed-rs

Guidance for AI coding agents working in this repository. `AGENTS.md` and
`CLAUDE.md` are intentionally identical; when one changes, update the other.

## Mission

- Build `cryptofeed-rs` as a pure Rust SDK for normalized cryptocurrency
  exchange market data — a ground-up counterpart to the Python `cryptofeed`,
  targeting current stable exchange APIs.
- The Python codebase is a semantic migration reference, not the protocol
  source of truth.
- Keep the workspace split by data category, not by per-exchange crates.
- Keep exchange protocol and transport logic centralized in `crates/runtime`.
- Active 0.1 delivery scope is public spot, perpetual/swap, and dated-futures
  market data. Funding, liquidations, open interest, and index channels are
  implemented where listed in PARITY.md; mark price is also a gated category.
  Binance contract OI is explicitly deferred (see docs/binance-open-interest-decision.md). Options and MARGIN helpers remain as
  implementation references but fail capability preflight; authenticated feeds
  and trading are also out of scope. Further expansion requires an explicit plan.

## Commands

Run from the workspace root. `make check/test/fmt/clippy` are aliases.

```bash
cargo check --workspace
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo check --workspace --no-default-features   # also compile with each individual feature when changing feature boundaries
```

Parity harness (deterministic, offline — no network required):

```bash
cargo test -p cryptofeed-rs --test public_parity
cargo test -p cryptofeed-rs --test public_parity <test_name> -- --exact   # single assertion while iterating
```

Live exchange examples under `crates/runtime/examples/` are manual smoke tests
that require network access; they are not CI gates.

## Workspace Layout

Split by data category, not by exchange:

- `crates/core` (`cryptofeed-core`) — shared abstractions: `ExchangeId`,
  `Channel`, `Symbol`/`InstrumentKind`, `Side`, and `error::Error/Result`.
  No exchange-specific logic.
- `crates/{ticker,trade,orderbook,candles,funding,liquidations,openinterest,index,markprice}`
  — thin API crates: normalized public models + `*Handler` async trait.
  Purely declarative; no transport or protocol code.
- `crates/runtime` (`cryptofeed-rs`) — everything else: exchange adapters,
  websocket/HTTP transport, routing, reconnect/shutdown, parsing, runtime
  orchestration.

The runtime crate gates each data category behind a Cargo feature (`ticker`,
`trade`, `orderbook`, `candles`, `funding`, `liquidations`, `openinterest`,
`index`, `markprice`; all default). Exchange-specific modules use
`#[cfg(feature = ...)]` on model imports and enum variants; keep this intact.

## Exchange API Rules

- New exchange implementations must use the latest stable official API.
- Do not copy a legacy Python exchange version into Rust just because it
  already exists.
- The Python codebase is a semantic migration reference, not the Rust source
  of truth.
- Bitget must target official v3 API surfaces.
- Before implementing or updating an exchange adapter, verify the current
  official REST and WebSocket documentation.

## Runtime Architecture

The user-facing path is `FeedHandler` → `add_feed(exchange_builder.build())`
→ `run()` (see `crates/runtime/examples/binance_public.rs`).

`runtime::run()` (`crates/runtime/src/runtime.rs`) is the orchestrator:

1. **Symbol hydration** — `markets::resolve_feed_symbols` validates the feed
   against the capability matrix and resolves normalized symbols (`BTC-USDT`,
   `BTC-USDT-PERP`, `BTC-USD-240628`) to exchange-native names, either via
   explicit `exchange_symbol(...)` pairs or by fetching the exchange's
   instrument catalog over HTTP. Unknown/ambiguous/mixed-product/unsupported
   combinations fail before connecting; the runtime never guesses quote
   currencies. Explicit native pairs are caller-asserted and bypass catalog
   existence checks. Per-channel subscriptions resolve their normalized union
   once, then compile channels with identical symbol sets into concrete feeds;
   explicit native mapping ambiguity is validated before this partition.
2. **Per-feed task fan-out** — `run_feeds_until_shutdown` spawns one task per
   concrete subscription group (one group for a legacy shared-symbol feed) with a shared `watch::channel` shutdown signal (Ctrl-C sets it).
   Terminal failure of one feed is recorded and reported without cancelling
   healthy feeds.
3. **Per-exchange consumers** (`consume_*_feed`) — build connection plans
   (URL per product when an exchange splits by product, e.g. Binance
   Spot/USD-M/CoinM, Bybit spot/linear/inverse), then run each plan through
   `supervisor::retry_with_backoff_until_shutdown` (unbounded transient
   retries, jittered 1→8s capped backoff, shutdown-aware).
4. **Sessions** — `connection::WsConnection`/`Session`
   (`crates/runtime/src/runtime/connection.rs`) owns heartbeat (per-exchange
   `HeartbeatPolicy` — payloads, intervals, idle timeouts), idle detection,
   ping/pong handling, and clean-close on shutdown. Normal remote close is
   reconnectable; explicit shutdown is clean.
5. **Message processing** — each `process_*_text_message` classifies control
   frames (subscribe acks, errors, pongs), routes orderbook messages to book
   sync when L2 is subscribed, then parses market data via the adapter and
   dispatches to handler traits (`dispatch_*_event`). Bybit derivative tickers
   reconstruct per-connection snapshot/delta state before pure normalization;
   snapshots and reconnects must clear stale fields.

### Per-exchange module pattern (`crates/runtime/src/exchange/<name>/`)

- `adapter.rs` — static planning surface: websocket URLs,
  `connection_plans`/`subscription_urls`, subscription messages,
  `parse_message(s)` / `parse_*_for_instrument` entry points, event enums,
  heartbeat strings. No transport code.
- `parser.rs` — pure functions `serde_json::Value -> Option<NormalizedModel>`
  (or parse errors). Product-aware symbol resolution, `rust_decimal::Decimal`
  for price/amount (never float conversion), separate `exchange_ts` and
  `received_ts` f64 timestamps.
- `book_sync.rs` — per-exchange L2 snapshot/delta sequence bridging: bootstrap
  from REST snapshot + buffered deltas, replacement-snapshot reset, stale
  update discard, gap detection → resync. The `runtime.rs` session loop polls
  snapshot fetches via `oneshot` receivers and buffers deltas while a snapshot
  is in flight. Never relax a bridge/gap rule to make a live bootstrap pass;
  add a bounded resnapshot regression instead.
- `<name>.rs` — re-exports the `ExchangeFeedBuilder` wrapper (e.g.
  `Binance::new()`).

`markets.rs` holds the authoritative capability matrix (exchange × product ×
channel), `SymbolRegistry` (bidirectional, product-qualified symbol maps), and
the HTTP catalog fetchers per exchange.

## Key Invariants

- A single logical `ExchangeFeed` is product-homogeneous: spot, perpetual, and
  dated futures must not mix, including across per-channel subscriptions.
- Per-channel subscription mode and shared channels/symbols are exclusive.
  The runtime compiles `connection_feeds` before adapter/session planning;
  direct low-level adapter callers must do the same. Different symbol sets
  can use extra connections; capacity-based sharding is separate work.
- Only capability-matrix combinations may open a connection — absent
  combinations must fail explicitly, never silently subscribe to nothing.
- Explicit normalized parsing over opaque dynamic conversion; batch messages
  normalize every entry.
- Decimal precision preserved; exchange and receive timestamps kept distinct.
- Coinbase/Kraken builders exist but are not live runtimes and are rejected by
  capability validation.
- Preserve the `FeedHandler` user-facing entrypoint.
- Keep runtime behavior changes covered by focused tests.
- If the Rust implementation moves beyond the original design/plan, update the
  docs instead of silently drifting.

## Parity Harness & Fixtures

- Follow `docs/harness.md` for fixture, replay, and public parity work.
- `crates/runtime/tests/public_parity.rs` holds executable inline JSON payloads
  + assertions for all five active exchanges (Binance, Bitget v3, Bybit v5,
  OKX v5, Gate.io v4).
- Keep the default harness deterministic and offline; live exchange examples
  are manual smoke tests.
- `sample_data/<exchange>.<transport>.<api-version>` files (e.g.
  `binance.http.v3`) are sanitized official protocol references — **not**
  automatically loaded by tests. A capture is not a passing test until its
  payload and expected values are asserted inline. Treat `sample_data/` as
  captured reference data.
- Before changing an adapter, fixture, or expected value: check
  `docs/exchange-protocol-baseline.md` (current official protocol facts),
  verify the current official exchange docs, then update fixture + inline
  assertion together. Do not update an expected value merely to make a test
  green.
- When parser or normalization behavior changes, update the narrow parity test
  and its corresponding sanitized capture together.
- Never commit API keys, signatures, cookies, or private payloads;
  `sample_data/SOURCES.md` records provenance.

## Docs That Must Stay in Sync

- `README.md` — user-facing capabilities and examples.
- `PARITY.md` — exchange/channel support and parity gates (read before
  changing exchange coverage).
- `PROGRESS.md` — milestones.
- `MEMORY.md` — durable project constraints/lessons.
- `docs/README.md` — public documentation index.
- `docs/harness.md` — fixture formats and parity behavior.
- `docs/reports/` — dated, sanitized live-validation evidence.
- `docs/ai-coding.md` — the AI-assisted workflow agreement.
- `docs/exchange-protocol-baseline.md` — current official protocol facts.
- `AGENTS.md` / `CLAUDE.md` — identical repository rules; when one changes,
  update the other.
- `docs/feature-status.html` — visual feature-status report (self-contained,
  Chinese UI). After any meaningful milestone or status change (new
  exchange/channel support, live smoke run, parity/progress changes),
  regenerate this report from the current README / PARITY.md / PROGRESS.md /
  docs/*.md state and keep it accurate. It must reflect current facts, never
  invented ones.

## Repository Hygiene

- Keep internal review notes, retired implementation plans, and presentation
  drafts in ignored `.local/docs/`, not in the public `docs/` tree.

- Workspace dependency resolver 3 honors the declared Rust 1.85 MSRV. Keep
  the no-lockfile policy and run the MSRV gate against fresh dependency resolution.
- Preserve JSON numeric precision (`serde_json/arbitrary_precision`) before
  converting wire prices and quantities to Decimal.
- Python authorship or licensing must not be copied into Rust release metadata
  without verified provenance. The Rust repository is
  https://github.com/crypto-2042/cryptofeed-rs; author/security/license decisions
  remain maintainer-owned.
- Do not commit `target/`.
- Do not commit `Cargo.lock` for this workspace unless policy intentionally
  changes.
- This repository often shows transient `.git/index.lock` collisions; verify
  the lock still exists before acting.
- Preserve unrelated user changes outside the intended scope of a change.

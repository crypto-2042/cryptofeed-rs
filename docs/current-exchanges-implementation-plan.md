# Current Exchanges Completion Plan

Status: five-exchange baseline implemented on 2026-08-04; deferred scope and
release work remain tracked in Sections 5, 7, and 8

## 1. Objective

Bring the five exchanges that already have active runtime paths—Binance,
Bitget, Bybit, OKX, and Gate.io—to a trustworthy public-market-data baseline.

Completion means that documented exchange, product, and channel combinations:

- use the current stable official API;
- resolve symbols without guessing product identity;
- subscribe to the correct product endpoint;
- normalize every event in a batch;
- maintain long-lived connections and recover from disconnects;
- maintain a correct L2 order book across snapshots, updates, gaps, and reconnects;
- fail explicitly for unsupported combinations or rejected subscriptions; and
- are covered by deterministic offline harness evidence.

This plan corrects the current documentation baseline. Existing parity checkboxes
or passing tests do not count as evidence when their fixtures do not match the
current official protocol.

## 2. Scope and Assumptions

### In scope

- Public market data only.
- Binance, Bitget, Bybit, OKX, and Gate.io.
- Spot, perpetual, and dated futures products when the exchange exposes them
  through a stable public API.
- Ticker, trades, L2 book, and candles for supported products.
- Funding and liquidation data where an official public channel exists and the
  current normalized model can represent it without ambiguity.
- Symbol discovery, WebSocket routing, REST order-book bootstrap, normalization,
  heartbeat, reconnect, subscription validation, sequence recovery, harness
  fixtures, examples, and capability documentation.

### Out of scope

- Coinbase and Kraken runtime implementation.
- Authenticated channels, order entry, balances, positions, and account data.
- L3 books, Options, MARGIN, and backend storage. Existing option/MARGIN
  parser fixtures are implementation references, not 0.1 runtime capabilities.
- Compatibility with an obsolete exchange API solely because the Python project
  still uses it.
- Live exchange availability as a CI requirement.

### Source-of-truth order

1. Current stable official exchange documentation.
2. Sanitized current-protocol captures in `sample_data/`.
3. The normalized behavioral contract defined by this project.
4. Python `cryptofeed` as a semantic migration reference.

When these sources disagree, the discrepancy must be documented and resolved;
test expectations must not be changed merely to make the harness pass.

## 3. Target Public Contract

### Product identity

Every configured and received symbol must retain its product kind. Spot,
perpetual, and dated futures instruments that share a native exchange symbol
must remain distinguishable throughout discovery, routing, parsing, callbacks,
and order-book state.

Symbol discovery must produce a reviewed bidirectional mapping between the
normalized instrument and the exchange-native instrument. Unknown or ambiguous
symbols must fail before subscription. Parser-side quote guessing is not an
accepted fallback.

### Capability validation

Each exchange must publish one explicit product-by-channel support matrix.
Configuration must reject unsupported combinations before opening a connection.
An empty subscription, unavailable runtime, or ignored channel must never be
reported as a successful feed.

### Event normalization

- Prices and quantities preserve decimal precision.
- Exchange and receive timestamps retain their distinct meanings.
- Every entry in a batched exchange message produces an event in exchange order.
- Candle interval and closed-state semantics are explicit and consistent.
- Snapshot and delta are distinct L2 event types.
- Malformed market data, protocol errors, and subscription failures are visible
  errors rather than silent `None` results.

### Connection lifecycle

Each connection must implement the official heartbeat format and cadence,
recognize heartbeat responses, detect silent connections, and reconnect after
both abnormal and normal remote closure. Retry exhaustion for one feed must not
terminate unrelated healthy feeds. Shutdown must remain graceful and bounded.

### L2 order-book lifecycle

For each exchange and product:

1. establish the official initial snapshot state;
2. apply buffered updates in the required order;
3. deliver an externally reproducible synchronized state/event sequence;
4. discard stale updates;
5. detect sequence discontinuity or checksum failure;
6. clear stale state before a replacement snapshot; and
7. resubscribe or resnapshot according to the exchange protocol.

Internal state and events delivered to handlers must represent the same logical
book at the same point in the sequence.

## 4. Target Exchange Matrix

The matrix below is the implementation target, not a statement of current
support. A cell is accepted only after its official endpoint, subscription,
payload, heartbeat, and recovery behavior are represented in the harness.

| Exchange | Products | Baseline channels | Additional public channels |
| --- | --- | --- | --- |
| Binance | spot, USD-M perpetual/futures, coin-margined perpetual/futures | ticker, trades, L2 book, candles | funding and liquidations for supported derivatives |
| Bitget v3 | spot, USDT futures, USDC futures, coin futures | ticker, public trades, L2 book, kline | liquidation where officially supported |
| Bybit v5 | spot, linear, inverse | ticker, public trades, L2 book, kline | funding and liquidation where officially supported |
| OKX v5 | spot, swap, futures | ticker, trades, L2 book, candles | funding and liquidation where officially supported |
| Gate.io v4 | spot and documented futures products | ticker, trades, L2 book, candles | funding and liquidation only when the normalized contract is unambiguous |

If official documentation shows that a target cell is unavailable or cannot be
normalized faithfully, it must be marked unsupported rather than approximated.

## 5. Delivery Phases

### Phase A — Correct the evidence baseline

- Inventory current official endpoints, product categories, channels, heartbeat
  requirements, batch shapes, and order-book recovery rules for all five
  exchanges.
- Replace legacy or fabricated captures with sanitized versioned captures.
- Add negative fixtures for subscription rejection, malformed payloads, unknown
  symbols, and sequence gaps.
- Record product identity in every symbol and event fixture.

Gate: each planned capability has an official-document reference and at least
one deterministic fixture before it is marked supported.

### Phase B — Establish shared runtime invariants

- Make product identity and bidirectional symbol resolution common runtime
  invariants.
- Introduce the authoritative capability matrix and preflight validation.
- Standardize protocol-error, subscription-error, malformed-data, and
  unsupported-capability outcomes.
- Ensure feature combinations compile independently and do not retain hidden
  order-book dependencies.

Gate: invalid configuration and ambiguous symbols fail deterministically before
network connection; supported configurations produce a product-specific
connection plan.

### Phase C — Correct connection supervision

- Implement exchange-specific application heartbeats and silent-connection
  detection.
- Treat clean remote closure as reconnectable unless shutdown was requested.
- Keep feed failures isolated while making terminal failures observable.
- Preserve graceful shutdown during connection, heartbeat, snapshot, and retry
  activity.

Gate: deterministic session tests cover heartbeat, pong, clean close, transport
error, retry exhaustion, independent feed survival, and shutdown.

### Phase D — Complete exchange protocol paths

Implement each exchange against its target matrix, using separate product
routing where required. For every supported cell, complete discovery,
subscription, acknowledgment validation, batch normalization, timestamp and
decimal handling, and explicit protocol errors.

Recommended completion order:

1. Gate.io spot and Bitget v3, because current payloads are known to be invalid.
2. Binance product routing, because derivative channels currently use spot
   infrastructure.
3. Bybit product routing and batch processing.
4. OKX product preservation and batch processing.
5. Remaining derivatives cells for Bitget and Gate.io.

Gate: every supported matrix cell has focused parser, adapter, and session
coverage using current-protocol fixtures.

### Phase E — Make order books externally correct

- Distinguish snapshots from deltas for every exchange.
- Clear state on replacement snapshots.
- Validate exchange-specific sequence and checksum rules.
- Align buffered bootstrap state with the events delivered to handlers.
- Exercise reconnect and resnapshot with pre-existing local state.

Gate: replaying handler events produces the same final book and sequence as the
runtime's internal state for snapshot, steady updates, gaps, and recovery.

### Phase F — Align public documentation and release gates

- Update README capabilities and limitations from the verified matrix.
- Reset and then re-check `PARITY.md` entries using harness evidence.
- Update `PROGRESS.md` and `MEMORY.md` with durable protocol and operational
  lessons.
- Keep examples limited to combinations that pass the release gate.
- Add project origin, rewrite rationale, upstream attribution, maturity level,
  and repository license material before public release.

Gate: documentation contains no capability claim that lacks a corresponding
deterministic test and fixture.

## 6. Exchange Acceptance Criteria

### Binance

- Spot, USD-M, and coin-margined products select their official endpoints and
  snapshot services.
- Funding and liquidation subscriptions are available only for products that
  officially expose them.
- Non-USDT spot symbols and dated futures normalize without panic or product
  loss.
- Snapshot/bootstrap sequencing produces an externally reproducible L2 book.

### Bitget v3

- All REST discovery and WebSocket subscriptions use official v3 surfaces.
- Kline uses the v3 topic, interval field, and current push schema.
- Product category is derived from the resolved instrument rather than fixed to
  spot.
- String ping/pong, sequence linkage, full-depth updates, and snapshot-only
  depth variants follow v3 rules.

### Bybit v5

- Spot, linear, and inverse instruments select distinct official public paths.
- Every trade and candle in a batch is normalized.
- Application heartbeat is sent at the documented cadence.
- Any replacement snapshot, including service-restart cases, clears the local
  book before applying new state.

### OKX v5

- Spot, swap, and futures identity is preserved in normalized symbols.
- Every entry in batched ticker, trade, candle, and book messages is processed.
- Text ping/pong and idle timeout behavior follow the official lifecycle.
- Snapshot replacement and sequence/checksum validation cannot retain stale
  levels.

### Gate.io v4

- Subscription timestamps satisfy the official freshness requirement.
- Public trade objects use the current official shape.
- Spot and futures endpoints, channel names, snapshots, and sequence rules are
  never mixed.
- Subscription errors are surfaced and order-book resync remains product-aware.

## 7. Harness Requirements

For each supported exchange/product/channel cell, the offline harness must cover:

- a valid subscription request and successful acknowledgment;
- at least one current official payload;
- all entries from a multi-entry message where batching is supported;
- symbol and product normalization;
- decimal and timestamp normalization;
- an explicit subscription-error or malformed-message case;
- heartbeat request and response behavior; and
- L2 snapshot, update, stale update, gap, and recovery where applicable.

Captured files remain sanitized reference artifacts. Executable parity tests
must represent their relevant fields and expected normalized events. Session
behavior must use local deterministic WebSocket/HTTP doubles; CI must not depend
on live exchange services.

## 8. Completion Verification

The project is complete for this plan only when all of the following pass from
the workspace root:

```text
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace
cargo check --workspace --no-default-features
```

In addition:

- supported single-feature combinations compile;
- the public parity harness passes offline;
- deterministic session and L2 recovery tests pass;
- no documented capability is represented by a silent no-op;
- no current-protocol capture is labeled with a legacy API version; and
- unrelated user changes remain untouched.

Live examples are a final manual smoke test only and are not part of the
deterministic completion gate.

## 9. Implementation Workstreams

Work may proceed in parallel only when ownership boundaries prevent conflicting
edits:

1. Shared product identity, symbol resolution, capability validation, and
   feature compilation.
2. Connection supervision, heartbeat, reconnect, failure isolation, and session
   tests.
3. Gate.io and Bitget v3 protocol correction plus fixtures.
4. Binance product routing and L2 bootstrap correction plus fixtures.
5. Bybit and OKX product routing, batches, heartbeat, and L2 snapshot correction.
6. Cross-exchange harness consolidation, documentation truthfulness, and final
   verification.

Each workstream must begin with a focused failing test or fixture discrepancy,
make only changes traceable to its acceptance criteria, and return its changed
file list and verification results for integration review.

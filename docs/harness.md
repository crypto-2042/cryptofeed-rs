# Public Parity Harness

The current harness is a deterministic, offline validation workflow for
exchange parsing and normalized public market-data behavior. It combines
sanitized official protocol references in `sample_data/` with Rust integration
assertions in `crates/runtime/tests/public_parity.rs`.

The current official-protocol baseline, including product endpoint splits,
control-message handling, and known live-wire differences, is maintained in
[`exchange-protocol-baseline.md`](exchange-protocol-baseline.md). Review that
baseline before changing an adapter or fixture.

Dated manual public-service evidence is indexed in
[the documentation guide](README.md#live-validation-reports). Reports are
diagnostic evidence, not offline test inputs or CI gates.

The 2026-10-08 additions cover Bitget spot/contract L1 and derivative ticker
fields, Gate.io public perpetual liquidation batches, and OKX shared-index
contract fan-out. Single-feature tests in `feature_boundaries.rs` verify that
these parsers do not depend on unrelated ticker/trade features.

## Current Scope

The harness covers public payload normalization for the exchanges represented in
the parity test, currently Binance, Bitget, Bybit, OKX, and Gate.io. Assertions
cover supported combinations of ticker, trade, L2 book, candle, funding,
liquidation, open-interest, and index-price events. The per-exchange channel
scope follows the authoritative capability matrix in `crates/runtime/src/
markets.rs` and `PARITY.md`; a cell absent from the matrix must fail explicitly
rather than being tested as a silent no-op.

The two harness inputs have distinct roles:

- `sample_data/` contains sanitized official HTTP and WebSocket examples used as
  reference material for endpoints, subscriptions, timestamps, payload shapes,
  and market configurations. `sample_data/SOURCES.md` records their provenance
  and distinguishes them from live captures.
- `crates/runtime/tests/public_parity.rs` contains executable inline JSON payloads
  and assertions for normalized Rust values.

The tests do not currently read `sample_data/` automatically. A capture is not a
passing test until its relevant payload and expected normalized values are
represented by an executable assertion.

## Fixture Layout and Format

Capture files follow `<exchange>.<transport>.<api-version>` naming, for example:

```text
sample_data/binance.http.v3
sample_data/binance.ws.v3
sample_data/okx.http.v5
sample_data/okx.ws.v5
```

HTTP files use these record forms:

```text
<request-url> -> <received-unix-seconds>: <json-response>
configuration: <json-configuration>
```

WebSocket files use these record forms:

```text
<websocket-url> <-> <connected-unix-seconds>
send: <json-subscription>
<received-unix-seconds>: <json-message>
```

Keep one record per line. Preserve numeric strings and timestamp precision when
they affect normalization. Use stable, minimal payloads that exercise the
protocol field being tested.

## Running the Harness

Run the complete parity integration test from the workspace root:

```bash
cargo test -p cryptofeed-rs --test public_parity
```

Run one top-level parity assertion while iterating:

```bash
cargo test -p cryptofeed-rs --test public_parity \
  binance_ticker_matches_python_public_baseline -- --exact
```

After the focused test passes, run the workspace completion checks:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace
```

No network access is required for these commands. Live examples are outside the
deterministic harness.

## Interpreting Results

A successful run exits with status 0 and reports all parity tests as passed. A
failure identifies the test and the mismatched assertion or parser error.

Classify failures before changing expectations:

- Parser returns `None` or an error: inspect routing and the official payload
  shape.
- Symbol mismatch: inspect exchange-native to normalized symbol conversion.
- Decimal mismatch: preserve source precision and avoid floating-point
  conversion for price or amount.
- Timestamp mismatch: confirm milliseconds-versus-seconds conversion and the
  distinction between exchange and receive timestamps.
- L2 book mismatch: inspect snapshot/update action and sequence continuity before
  changing the expected value.

Do not update an expected value merely to make a test green. First confirm that
the new value matches the current official API and the normalized public model.

## Adding or Updating a Case

1. Verify the current stable official API version, endpoint, channel, and payload
   schema, then update `docs/exchange-protocol-baseline.md` with the verification
   date and any changed protocol fact.
2. Select the smallest representative official example or approved public
   capture, and record its provenance in `sample_data/SOURCES.md`.
3. Remove secrets, authentication material, private identifiers, and unrelated
   fields.
4. Add or update the versioned file in `sample_data/` using the existing line
   format.
5. Add or update the corresponding inline payload and normalized assertions in
   `crates/runtime/tests/public_parity.rs`.
6. Run the focused parity test, then the full completion checks.
7. Update `PARITY.md` when supported scope or a parity gate changes.

When an API version changes, prefer a new versioned capture name and update the
adapter/tests together. Do not relabel legacy traffic as the new API shape.

There is no snapshot-update command or automatic baseline regeneration. Fixture
and assertion updates are intentionally reviewed as source changes.

## Safety Limits

- Keep the default harness offline and deterministic.
- Use only public market-data payloads in committed captures.
- Never commit API keys, signatures, cookies, account identifiers, private
  channels, or authenticated responses.
- Treat live example runs as explicit manual smoke tests. They contact real
  exchange services and may be rate-limited or fail because of remote state.
- Do not make CI depend on live exchange availability.
- Do not replace a captured baseline solely because a live response differs;
  first verify the official API and determine whether code, fixture, or both must
  change.

## Live Smoke Procedure

Live exchange runs are manual smoke tests only: they are the final acceptance
step after the offline gates pass, they never become CI dependencies, and they
produce diagnostic evidence rather than deterministic expectations. The
acceptance criteria are defined in `docs/exchange-protocol-baseline.md`
(Manual live-smoke acceptance).

### New-channel smoke

`cargo run -p cryptofeed-rs --example channel_completion_smoke` checks the
2026-10-08 additions with explicit symbols and no L2 bootstrap. It stops
automatically after 45 seconds and prints normalized event counts and terminal
feed failures. Zero liquidation events do not prove a subscription failed;
separately verify its acknowledgement when the stream is sparse.

### When to run

- After a new exchange adapter, protocol correction, channel, or capability
  matrix change, once all offline gates are green.
- Before a release gate that claims live-verified support.

### Steps

1. **Offline gates first.** `cargo fmt --all --check`, `cargo clippy
   --workspace --all-features --all-targets -- -D warnings`, `cargo test
   --workspace`, and the parity harness must pass before any live run.
2. **Select targets.** Use the matching `crates/runtime/examples/<name>_
   public.rs` example. Run spot plus at least one derivatives product where
   the exchange supports it (Binance spot and USD-M, Bitget spot and USDT
   futures, Bybit spot and linear, OKX spot and swap, Gate.io spot and USDT
   perpetual).
3. **Run a short smoke.** Execute the example with network access, let it run
   for a short window (the 2026-08-04 smoke used roughly 18–25 seconds), then
   interrupt with Ctrl-C. When forensics may be needed (e.g. a Gate.io
   bootstrap observation), enable tracing with `RUST_LOG=warn` so the
   structured `gateio bootstrap failed` records capture the failing snapshot
   id and buffered `U/u` sequence (see "Capturing a Failing Gate.io
   Bootstrap").
4. **Verify the acceptance points.** Discovery completed; subscription
   acknowledgement received; at least one normalized event per active
   high-frequency channel; Ctrl-C shutdown returns `Ok(())` without a retry
   loop caused by a control frame. Zero events is not a failure for sparse
   channels (funding, liquidations, one-minute candles in a short window).
5. **Record.** Write a dated report `docs/reports/live-smoke-<yyyy-mm-dd>.md`
   following the format of `docs/reports/live-smoke-2026-08-04.md`: a table of
   exchange/product/symbol, per-channel event counts, result per configuration,
   observations, and regression conclusions. Never include credentials,
   signatures, cookies, or private payloads.
6. **Act on the result.**
   - Pass: no code change is required. If the run exposed a wire shape not yet
     represented offline, add a sanitized fixture with provenance in
     `sample_data/SOURCES.md` plus an inline parity assertion before declaring
     the shape handled.
   - Fail (protocol error, parser rejection, wrong endpoint, or a silent
     active channel): compare against the current official docs and
     `docs/exchange-protocol-baseline.md` to decide whether code, fixture, or
     both must change. Never update an expected value merely because a live
     response differs; never replace a captured baseline on live availability
     alone.
   - Forensics case: reproduce the captured sequence as an offline regression
     before declaring the issue resolved.

## Capturing a Failing Gate.io Bootstrap

The bounded resnapshot path emits structured forensics at `warn` level. Run
the Gate.io spot live smoke with tracing enabled (`RUST_LOG=warn` on the
example) and look for `gateio bootstrap failed` entries: each includes the
failing snapshot id and the complete buffered `U/u` sequence. That evidence is
the acceptance input for validating the resnapshot path against a real
failure — reproduce the captured sequence in an offline regression before
declaring the live case resolved.

## Known Gaps

- Capture files are reference artifacts rather than direct test inputs.
- Full-session WebSocket doubles cover all five active exchanges (subscribe
  framing, control acknowledgements, market-data delivery, and clean shutdown;
  Binance additionally exercises an injected snapshot bootstrap). REST snapshot
  doubles for the other four exchanges remain direct parser-level tests.
- Gate.io bootstrap failure and recovery are deterministic, including a
  bounded in-session resnapshot path (max three attempts per symbol, buffered
  deltas preserved, session-level retry as backstop). Confirming it against a
  captured failing snapshot ID and complete buffered `U/u` sequence remains a
  live-only validation step.
- Deferred options remain represented as parser/catalog reference fixtures,
  while capability tests prove that Options and MARGIN fail 0.1 preflight.
  Independent L1 channels remain active; L3 has no public stream across the
  active exchanges and is deferred with a documented analysis.
- Gate.io `full: true` pushes and Binance spot partial-depth pushes are
  asserted inline from the documented wire shapes; live captures remain a
  manual smoke item. OKX VIP4 tick-by-tick depth (`books50-l2-tbt` 50 /
  `books-l2-tbt` 400) is documented but rejected at preflight until a live
  capture allows wiring the snapshot-replacement path.
- A live smoke test is manual evidence only. When it exposes a wire shape not
  represented by the offline harness, add a sanitized fixture and a focused
  regression assertion before declaring the issue fixed.

Keep this section aligned with `PARITY.md` as those capabilities are added.

## Per-channel subscription regression target

`cargo test -p cryptofeed-rs --test subscriptions` checks canonical groups,
configuration rejection, typed/native identity, and concrete subscription
planning across the five active exchanges. Runtime unit tests cover exact
callback/broadcast/counter/book-state filtering after hydration. The
`feature_boundaries` target verifies mapped channel preflight under each feature.
These tests reuse current adapter protocol paths; grouping is not a new wire
format or a promise of optimal connection packing.

## Connection and request budgets

Runtime planning tests verify generated topic/byte boundaries and aligned
symbol/native partitions, including aliases that share a topic. The Bybit
public parity assertion and its sanitized send reference verify ten-arg spot
request batching. Session doubles exercise paced queues while market reads and
shutdown continue. Snapshot/connection admission tests use local futures to
verify capacity, start spacing, failure and cancellation; they do not simulate
external IP traffic or certify live throughput. See
[the budget guide](connection-planning.md) for the official sources and SDK
policy distinctions.

## Managed runtime regression targets

`cargo test -p cryptofeed-rs --test runtime_control` verifies the public control
surface without live feeds. `cargo test -p cryptofeed-rs runtime::control::tests`
drives private preparation/consumption boundaries to verify replacement,
cancellation, busy/invalid commands, source generations, state reset, independent
startup, admission and child drop before acknowledgement. Live evidence belongs
in dated reports; mock sessions are not relabeled as service observations.

## Readiness targets

`runtime::readiness::tests` verifies protocol confirmation and epoch/state
invariants. Session tests cover response timeout and a complete Bybit
confirmation-to-book initialization flow; managed actor tests cover authoritative
queries after lifecycle lag and initial failure repair. Public parity covers
Binance explicit endpoint/topic planning. See [readiness](readiness.md) for
criteria and separate live evidence; no passing test equates handshake or any
market event with complete subscription confirmation.

## Directory reconciliation targets

`cargo test -p cryptofeed-rs discovery::tests` drives the real managed command
actor with scripted directory snapshots and private consumer doubles. It covers
additions/removals/native aliases, unchanged-cycle stability, empty selections,
HTTP failure/backoff/recovery, stop/drop, accepted replacement completion and
manual-owner protection. Catalog unit tests assert mixed active/inactive rows
alongside sanitized HTTP references. No default test requires network access.
`discovery_public` separately observes a real periodic refresh and cleanup; it
cannot prove a new listing occurred during its observation window.


## Candle delivery policy

`cargo test -p cryptofeed-rs candle_completion_policy` checks final, unfinished and
unknown flags against every delivery surface and verifies that receipt after
bar end does not infer completion. This changes SDK filtering, not wire parsing;
existing sourced parser fixtures remain unchanged.


## Handler fan-out targets

The runtime `multiple_handlers`, `handler_panic_and_timeout` and
`shutdown_cancels_slow_handler` regressions exercise primary/append ordering,
add-only registration, model isolation, one publication, per-callback timeout,
panic continuation, subsequent delivery and bounded cancellation. The timeout
regression uses the actual five-second production deadline. Exchange wire
fixtures are unchanged because this increment changes SDK callback execution.


## L2 recovery targets

`cargo test -p cryptofeed-rs books::tests` covers revision/deletion, lag recovery,
resync, connection/generation fencing, retirement and concurrent acquisition.
`l2_recovery_runtime_hooks` verifies cache integration with actual dispatcher,
monitor, attempt drop and stopping. Orderbook-only lib tests verify feature
isolation. Seven older tests now declare their actual feature prerequisites;
their assertions still execute in the full-feature suite. Public `book_recovery_public` provides separate manual OKX evidence;
mock lag and reconnect are not relabeled as real service observations. No
exchange parser or wire fixture changed.


## Runtime option targets

`options::tests`, `configured_handshake_deadline`,
`configured_callback_deadline` and `successful_subscription_resets_budget`
exercise validated budgets, retry reset/permanent failure, stalled establishment,
callback cancellation and planning preservation. The paced-session double also
asserts initialization only after all subscribe writes. Default tests stay offline;
no new exchange wire fixture or live-service claim is introduced.


## Startup and idle timing targets

`startup_delay_is_cancellable` and `false_shutdown_notifications` verify no work
before a cancelled delay and a stable initial timer despite watch notifications.
Duplex `custom_idle_deadline` and `disabling_idle_watchdog` verify configured
expiry and continued heartbeat/shutdown beyond the original disabled deadline.
These are offline timing-policy tests; exchange heartbeat fixtures are unchanged.


## Explicit proxy transport targets

`transport::tests` uses in-process duplex streams for CONNECT success/auth/IPv6,
interim responses, tunnel byte preservation, rejection/malformed/incomplete/size
limits, safe diagnostics and cache/client isolation. Planning tests retain the
same route across channel groups. Default tests open no proxy listener or external
socket; the separately dated `proxy_public` observation uses an authenticated
local forwarding proxy against real Binance catalog/WS/REST bootstrap services.


## Sparse native packing targets

Public parity asserts exact native topic/arg unions on all five exchanges plus
required Binance USD-M routes, Gate settlement products and OKX business/public
separation. Request references are appended with provenance. Subscription tests
verify capacity slices, rule trimming, pair uniqueness and mappings; runtime
tests verify handler/broadcast/counter/book filtering and no trade-only depth
sync/REST work. Separate `packed_public` evidence verifies real confirmations,
L2 readiness and BTC-only recovery; no global optimum/load guarantee is claimed.


## Typed market metadata targets

`market_info::tests` asserts explicit increments versus precision, current field
applicability/deprecation, contract fields, legal zeros and exact JSON/scientific
Decimal handling. Catalog helper tests cover five exchanges' Spot/Perpetual rows,
identity/metadata retention, feature-aware channels and duplicate conflicts.
`metadata_only_refresh` verifies unchanged subscriptions. Matching sanitized HTTP
references have provenance; the separate public example records live catalogs.
No-default metadata tests ensure this surface works without data-channel features.


## Public REST and HTTP backoff targets

`rest::adapter::tests` validates native requests, products/depth, reply identity,
precision, sequence/time and malformed input. REST client tests cover feature-
aware capabilities and preflight before HTTP. Paused-clock HTTP tests prove
queued cooldown extension, venue isolation, cancellation and slot release;
HTTP-response/byte doubles verify status/date parsing, no body echo and bounds.
Default tests use no listening socket. The dated `rest_public` run separately
verifies live Spot/Perpetual snapshots and the corrected Gate Spot unit.

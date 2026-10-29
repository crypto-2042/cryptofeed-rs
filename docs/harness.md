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
   bootstrap observation), use a consumer with an installed tracing subscriber.
   RUST_LOG alone does not install one. Structured Gateio warnings contain the
   failing snapshot ID and a bounded sequence sample; enable raw capture for
   full context (see "Capturing a Failing Gate.io Bootstrap").
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

The bounded resnapshot path emits structured forensics at WARN level. The SDK
never installs a global subscriber. A consumer must install one; `RUST_LOG=warn`
only has an effect if that consumer supports environment filtering. The soak
0.1.2 worker installs its own safe WARN/ERROR evidence subscriber.

Current warnings retain the failing snapshot ID/times, buffered count and the
first 16 U/u ranges. These samples are not the complete buffered sequence or a
replay. Enable bounded raw capture of public book frames and consumed HTTP
snapshots/errors to retain full native context, and mark overflow/incompleteness.
The soak Gateio evidence profiles do this explicitly; their selected JSONL is
forensic evidence, not the SDK's complete native-recording file format.

With issue #4, v1 overlap coverage is accepted, missing IDs fail closed, and
initial plus three additional snapshot operations are cumulative per symbol/
physical session even after successful bootstrap. Live pending recovery has a
60s deadline checked during safe idle polling; the external 120s Ready threshold
is unchanged. See [Gateio recovery](gateio-book-recovery.md).

Reproduce a captured failure deterministically before declaring its unique live
cause resolved. Synthetic tests or warning samples alone cannot establish it.

## Known Gaps

- Capture files are reference artifacts rather than direct test inputs.
- Full-session WebSocket doubles cover all five active exchanges (subscribe
  framing, control acknowledgements, market-data delivery, and clean shutdown;
  Binance additionally exercises an injected snapshot bootstrap). REST snapshot
  wire-level HTTP doubles are incomplete for other exchanges. Gateio additionally
  has injected HTTP-result/WS recovery and idle-socket bootstrap regressions.
- Gate.io bootstrap failure and recovery are deterministic, including a
  bounded in-session resnapshot path (initial plus three additional operations
  per symbol/session across success/gap cycles, preserved buffered deltas and a
  live pending deadline; session retry remains the backstop). Confirming it against a
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


## Funding-history targets

`rest::history::tests` covers current five-venue actual rates/time/cursor shapes,
limits/half-open ranges, scope-bound JSON restore before HTTP, duplicates/conflicts,
missing actual rates, oversized/no-progress pages, source cap/empty-page semantics,
exact negative numeric rates and cancellation during a later fetch. Captures have
provenance. Funding-only builds run these tests independently; default tests use
no external socket. `funding_history_public` provides separate live first/resume
evidence without claiming completeness when the budget is reached.

Candle history regression cases live in `rest::candles::tests`. Matching Binance
and Bybit HTTP captures document substituted field-table values; tests are inline
and offline. They cover cursor scope, empty time-window advancement, leap-month
boundaries and cancellation, without asserting historical source completeness.

Five-venue candle-history tests also assert Bitget's single-earlier-interval
continuation, Gate integer/decimal second units and range rounding, native
completion flags, base-vs-contract volume and OKX UTC+8 quarters. Sanitized
references match those inline assertions; bad/duplicate/out-of-range probes
remain unit-only cases, not supposed real exchange captures.

Recent-trade inline regressions live in `rest::trades::tests`. Their matching
five-venue HTTP references assert taker side, native IDs/units and explicit clock
scales. Same-time distinct IDs remain present; exact Decimal sorting precedes
f64 normalization. Malformed/mismatched/over-budget probes are SDK-only tests.
Recent executions must not be relabeled as paginated complete trade history.

Trade-history inline tests (`rest::trade_history`) cover JSON scope/position,
multiple native pages at one timestamp, empty seeds, offsets, boundary filtering,
non-progress and cancellation. The sanitized aggregate payload is distinct from
recent individual executions. Gate contract trade clocks use second input;
Spot uses milliseconds. Near-epoch same-value regression guards explicit units.

## Normalized recordings (optional)

The `recording` feature's user format is separate from exchange protocol capture
references. `sample_data/recording.normalized.v1.jsonl` is synthetic normalized
public data; matching inline `recording::tests` assertions run offline without
packaging external files into the runtime crate. Do not replace raw parity fixtures
with normalized recordings: the latter do not exercise native parser/book sync.
Tests cover every enabled model category and record bounds/order/failure behavior.
See [recording](recording.md) for the version-1 contract and remaining raw replay.

Public raw WS observation tests (`recording::raw` and connection `raw_tests`)
are transport/sanitization probes, not new native protocol fixture expectations.
They verify long JSON-number preservation, public context, credential redaction,
sticky bounds/failure, reconnect ordering and observation before pong filtering.
The narrow live model-clock assertion is separate from exchange event time.
No persisted raw replay or HTTP bootstrap parity is implied by these observations.

Raw segment tests (`recording::raw::file`) exercise the separate raw-ws file
schema and state validator. Test data is SDK-generated sanitized transport data,
not a new exchange protocol assumption. Numeric JSON literals, metadata/lifecycle,
privacy, truncation/limits and partial I/O are checked offline; file iteration is
not evidence that native parsers or HTTP/L2 reconstruction were replayed.

Native raw replay regressions (`recording::raw::replay`) consume file-format inputs
through existing Rust runtime parser/dispatch code, not normalized model fixtures.
They assert all five trade schemas, product-qualified identity, candle/sparse rules,
Bybit ticker/reset and three WS-native book synchronizers with strict bad sequence/
CRC rejection. Supplied nonzero OKX CRC constants are SDK integrity probes computed
from interleaved level strings, not newly captured server values. Missing HTTP
bootstrap remains an explicit unsupported path, never a live test dependency.

Raw v2 HTTP replay tests drive the actual native bootstrap polls with controlled
snapshot receivers and compare exact models, clocks and source observation links.
The v2 Processing marker disambiguates an observed-but-not-yet-processed WS packet.
Tests assert Gate ID anchoring/timestamp fidelity, safe HTTP429, valid overlap and true-gap resync, missing
pending snapshots and schema/version failures; no live HTTP is a deterministic
test dependency. Legacy v1 references remain readable with their original scope.


Physical scope regressions in `runtime/scope_tests.rs` cover Bybit product and
OKX endpoint sparse projections, normalized/native name alignment, raw session
context file validation, and route-owned reset behavior. The Bybit test preserves
an initialized inverse book through a linear reset and applies its next delta.
Binance market/public and Gate settlement guards use existing adapter plans.
These synthetic SDK-state tests do not change protocol fixture expectations.


`src/sink/tests.rs` exercises sequential storage delivery, identity/generation
preservation, exact decimals, pre-existing and slow-write broadcast lag,
write/finalize failures, panics, deadlines, shutdown/watch closure and runner drop.
Recording-enabled tests validate bounded JSONL accepted counts and strict reader
roundtrips; lag must leave no valid footer. No backend server is needed.


`src/aggregate/ohlcv/tests.rs` covers arrival-clock OHLCV values, Decimal division,
exact boundary placement, timer-only closure, partial tails, empty-window jumps,
feed/generation/exchange/symbol isolation, contract amount preservation, late
exchange clocks, backwards consumer clocks, series bounds and atomic arithmetic
failures. Recording-enabled replay compares every bar field to direct processing.
The tests use SDK models, not modified wire fixtures or network data.


`src/aggregate/throttle.rs` tests strict leading-edge boundaries, subsecond and
large elapsed clocks, independent instances and atomic backwards-clock errors.
`src/aggregate/renko/tests.rs` covers Python price-rule golden vectors, exact
threshold/down/up/reversal/gap behavior, source scopes, retirement/capacity,
invalid input, Decimal increments and recording-replay brick/decision equality.
No-default tests run throttle alone; trade and recording/trade add all helpers.


`src/nbbo/tests.rs` covers highest/lowest selection and native sizes, stable ties,
clock-only suppression, inclusive expiry/fallback/final withdrawal, queue-age
rejection, suspend/resume, generation pinning/replacement, exact symbol/product
and unit labels, crossed venues and invalid local quotes/clocks/configuration.
Recording/orderbook-only tests compare all emitted fields for identical explicit
source-observation/consumer clocks. No transport lifecycle replay is inferred.


`src/aggregate/custom/tests.rs` exercises no-data initialization/empty timers,
pre-callback clock/cap/boundary validation, initializer/reducer error and panic
poisoning, rollover failure, explicit prior-window delivery, subsecond windows,
source-preserving custom state and partial tails. Recording/trade compares every
custom state/window field for deterministic identical input and elapsed clocks.
The example separately bounds application groups and verifies recorded buy/sell
counts; the generic SDK cap does not bound arbitrary callback allocations.


`src/recording/raw/file/series/tests.rs` verifies ordered physical composition,
exact observation/causal context retention, missing/truncated/extra/reordered and
same-length corrupt files, manifest scope/budget/summary mismatches, hard part
ceilings, global prefix limits, create_new and poisoned capture/replay. Native
trade and Binance HTTP/L2 replay compare full output fields/summary against a
single logical file, retaining parser/buffer/bridge state across parts.


`src/recording/http/tests.rs` covers request start IDs/reordered completion/drop
cancellation, exact numeric JSON/redaction, pending-request footer rules, malformed
versions/counts/private URLs/payloads, queue/line bounds, clone cache namespaces,
proxy auth retention and sleeping receiver failure notification. Raw WS has a
matching private-frame wakeup regression. These are deterministic HTTP-result
ledger assertions. The separate offline response-injection/catalog-query
assertions are listed below.


`src/recording/http/replay/tests.rs` asserts invocation-order matching despite
reordered completion, no skipping unfinished requests, failure/retry/cancellation
boundaries, exact source/URL/body budgets, fresh/full file validation, isolated
caches, HTTP/WS guard behavior and existing SDK catalog/ticker/book/trade use.
The synthetic sample_data/recording.public-http.v1.jsonl matches the executable
inline SDK fixture values; its CLI is separate verification, not implicit test loading.

The catalog waiter regression and v2 re-recording case prove cancellation cannot
be shared/reclassified into automatic retry or steal a following caller's result.
Version-1 fixtures remain readable; v2-only metadata is rejected under a v1 header.


`src/markets/cache/tests.rs` uses local bounded caches for LRU/entry/key-byte
accounting, oversized replacement, global expiry/clock rollback, live Arc reader
lifetime, exact JSON numeric roundtrip and 10,000 namespace insertions. HTTP replay
has a scoped cache-eviction integration test proving retained catalog usability,
no implicit future-response consumption and explicit refresh chronology. Existing
failed-refresh/coalescing/cancellation suites remain required.


Resource-policy tests plus scoped HTTP/WS paused-clock suites cover validation,
clone/proxy retention, composite start clocks under global delay, scope exhaustion,
active/pacing cancellation and both permit releases. Scoped planning and managed
admission tests cover rejected excess, same-scope max reservations, scope switching
and healthy-feed identity. Offline replay guards are checked before resource use.


Native quota tests assert official code/type/scope forms against synthetic
`public-rest.native-quota.jsonl` references. Mock HTTP responses/paused clocks
cover shared delay, Retry-After/reset precedence, malformed hints, unchanged JSON,
other-exchange/slot isolation and replay not mutating live cooldowns. The HTTP
archive catalog test asserts numeric quota responses never become cached successes.
No intentionally rate-limited live service is part of CI.

Binance weighted-admission tests use `public-rest.weights.jsonl` as a synthetic
fee reference and assert costs inline against current official documentation.
Paused clocks cover rolling expiry, composite grants, cancellations and free
network slots while waiting. Replay bypasses weights and directory-limit learning.
No network saturation or live ban is a test requirement.

Public WS control tests assert official frequency/service codes against synthetic
`public-ws.control-errors.jsonl`, preserving code types and private-scope exclusions.
Paused clocks test readiness withdrawal, retry floors, false watch notifications,
shutdown and queued admission/cancellation. Raw replay returns failures without
changing live clocks; no live throttling is part of the default harness.

WS lifecycle regressions use `public-ws.lifecycle.jsonl` synthetic status/close
references. Real local loopback servers reject upgrades and CONNECT, then test
connection-slot reuse; no exchange service is called. These tests require local
socket-bind permission. Paused clocks verify retry floors and typed close policy;
in-memory WS pairs verify readiness withdrawal and clean shutdown precedence.

Independent raw-merge tests reuse the native replay engine and existing public
payload shapes. `raw-merge.timeline.json` records synthetic common-axis ordering,
not clock synchronization inferred from captures. Tests cover five native families,
colliding session IDs, local L2 state and consumed Binance/Gate HTTP causal links,
rotated manifest certification, total resource budgets and terminal cancellation.
No exchange/network bootstrap is created by merged replay.

Route-first planning tests reproduce four-to-three socket reductions without
opening connections. `connection-planning.routes.json` is a synthetic allocation
reference, using generated caller-asserted names rather than live instruments.
Exact channel/symbol pair coverage, native alignment, product scope, snapshot
ownership, scoped connection admission and overflow rejection are asserted.

Minimum-packing tests compare iterative exact assignment and safe additive bounds
with an independent exhaustive small-weight oracle. Native fixtures cover all
five budget families and a Bitget case where both contiguous and first-fit need
three sockets but exact search proves two. `connection-planning.minimum.json` is
synthetic SDK allocation evidence, not a wire capture. Worker cancellation/admission
and rejected replacement preservation are exercised without live exchange load.

`replay_audit` regressions test operation-cohort order, leader/waiter cancellation,
32 concurrent HTTP SDK calls on a shared archive, and current REST receipt clocks
versus virtual delivery time and SystemTime catalog retention. The reference
`replay-clock-contracts.json` is synthetic policy evidence. Existing native/model
clock, cache-eviction, no-network and partial-I/O/cancellation suites remain the
authority for the [full contract table](replay-contract-audit.md).

## Binance used-weight header evidence

`sample_data/binance.http.used-weight.jsonl` is a synthetic reference for live
HTTP response metadata, not a replay capture or market payload. Assertions in
`runtime/weights/tests.rs` and `runtime/snapshot.rs::used_weight_feedback_tests`
cover strict header handling, independent floors/local ledgers, cancellation and
response errors. HTTP payload replay continues to bypass live header admission;
no historical header scheduling is inferred from payload archives.

## WS send quota evidence

`sample_data/public-ws.send-budgets.jsonl` records synthetic SDK admission
examples. `runtime/send_budget.rs::tests` and duplex WebSocket session tests
assert rolling windows, hourly controls, cancellation and automatic-Pong
behavior. Native payload replay does not simulate this live session scheduler.

## Crossed-book policy and timestamp fidelity

`sample_data/public-l2.cross-check.jsonl` is a synthetic semantic/native-envelope
reference asserted by the orderbook model, runtime and native-replay tests.
Crossed-book checking is opt-in and does not change default public parity values.
Fixed Linux-CI receipt timestamps now assert exact f64 bit roundtrip through
HTTP recording. Workspace serde_json enables float_roundtrip alongside
arbitrary_precision; do not weaken exact assertions to a tolerance.

## Delivered-depth evidence

`sample_data/public-l2.delivery-depth.jsonl` records synthetic top-N transitions
and a current-schema Bybit replay example. Model/runtime/replay tests assert
them and compare sequential projected updates against an independently rebuilt
full-book top-N oracle. Default public parity values and native integrity rules
remain unchanged. CI also tests minimal orderbook and recording/orderbook builds.

## Anchored consumer contract (#10)

Run `cargo test -p cryptofeed-rs --all-features anchored` for deterministic
producer-anchor, multi-connection, replace/remove, loss/accounting and normalized
v2 replay evidence. The normalized v2 reference is synthetic SDK output, not an
exchange capture. Matching inline assertions run in
`recording::anchored_tests` without depending on files outside the crate; no protocol expectation or exchange fixture
changes are involved. Existing L2 recovery and public parity remain separate
gates. See [readiness](readiness.md#connection-anchored-market-events) and
[recording v2](recording.md#connection-anchors-version-2).

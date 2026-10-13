# 0.1 compatibility policy

Effective for the first published 0.1 release. This policy records commitments
for implemented behavior; it does not claim publication or production qualification.
The current implementation is the baseline, with supported combinations in
[PARITY.md](../PARITY.md) and detailed contracts in the linked guides.

## Versioning and API

- All 11 workspace crates release together with matching versions.
- Within 0.1.x, preserve documented public APIs: `FeedHandler`, exchange builders,
  typed configuration, category models/handler traits, streams, managed control,
  catalogs, public REST/history, L2 recovery, sinks, aggregation and replay.
  Preserve existing default features and documented defaults.
- Breaking source or semantic changes require 0.2.0, migration notes and updated
  fixtures. Removing/renaming APIs or features, changing public field types,
  adding required fields to constructible structs, and adding exhaustive enum
  variants are breaking changes. New functionality in a patch must not break
  existing callers; use opt-in APIs when changing behavior.
- Correctness/security fixes may change previously erroneous data or reject an
  invalid input. Document the trigger, affected combinations and new behavior in
  the changelog and focused regressions; do not silently redefine normalized units.
- `#[doc(hidden)]` internals and undocumented low-level adapter/session interfaces
  are not compatibility entrypoints. Public visibility alone does not make
  exchange protocol helpers stable. No Rust binary ABI compatibility is promised.
- Python method signatures, synchronous execution, configuration-file syntax and
  every Python backend are not compatibility commitments. This is a typed async
  Rust SDK with the documented public workflow.

## Supported scope and data

- Public spot, perpetual/swap and dated futures on Binance, Bitget v3, Bybit v5,
  OKX v5 and Gate.io v4, only for combinations admitted by the capability matrix.
  Preserve explicit preflight failure for unsupported combinations and mixed
  products. Coinbase/Kraken builders do not imply live runtime support.
- Options, MARGIN, private/account channels, trading, L3 and Binance contract OI
  are outside 0.1. Cross-process/multi-machine quota coordination is deferred;
  budgets are process-local. A shared egress IP still needs deployment-level limits.
- Preserve exact Decimal prices/quantities, exchange-native documented quantity
  units, distinct exchange/receive timestamps, optional/unknown fields and
  product-qualified symbol identity. Do not synthesize missing values as zero.
- Shared-symbol and per-channel subscriptions, catalog-backed `*`/`?` selection,
  and managed replacement remain supported. Replacement is a generation change
  with a possible collection gap, not an atomic exchange-side subscription edit.
- Reconnects invalidate connection-owned readiness/book evidence. `Started` means
  launch; `Ready` requires subscription confirmation and requested L2 initialization.
  Transient retries are unbounded by default; healthy feeds survive another feed's
  terminal failure. See [readiness](readiness.md) and [control](runtime-control.md).
- Broadcast streams are bounded and may lag; delivery is not exactly-once,
  lossless or globally ordered across feeds. Consumers must handle lag, reconnects
  and generation changes, including buffered events from an old/removed generation.
  L2 consumers use the documented recovery anchors.
  Callback handlers run serially with a default five-second deadline.
- Preserve full native L2 synchronization/checksum state, opt-in crossed-book
  checks (default off), and separate delivered top-N depth (default unlimited).
  See [depth](book-delivery-depth.md), [checks](book-cross-check.md) and
  [recovery](l2-recovery.md).
- REST/history retain documented endpoint, pagination, retention and unit limits;
  support does not promise arbitrary historical coverage or exchange availability.

## Recording and configuration

- `recording` stays opt-in. Known format versions and missing optional metadata
  use the documented tested defaults; unknown versions and malformed/truncated
  files fail explicitly. A format change requires a versioned schema and an
  explicit reader compatibility decision, not an implicit reinterpretation.
- Normalized recording is not automatically a recoverable L2 stream. Native/HTTP
  replay only reproduces the documented captured boundaries, not historical
  network, cache or admission timing. Feed IDs/generations in files are local labels.
- Environment proxy variables are not automatically adopted. TLS certificate and
  hostname verification remain enabled. Configuration credentials must not enter
  public recording/report artifacts. See [transport](transport.md).

## Compiler, dependencies and platforms

- Retain Rust 1.85 MSRV and edition 2024 throughout 0.1 patch releases. Check fresh
  dependency resolution on 1.85 in CI. A required MSRV increase needs a minor
  release and migration note. Using a newer compiler does not raise this minimum.
- Keep the repository's no-lockfile policy for this library workspace. Downstream
  applications should retain their own lockfile. Security-sensitive lower bounds
  must require fixed versions; a successful audit is dated evidence, not a future
  guarantee. Run dependency and license review for each release.
- Linux is the continuous CI baseline; macOS arm64 has local validation. Windows
  and other targets have no current acceptance gate and are not declared qualified.
  Runtime requires Tokio and network access to the supported official endpoints.
- Exchange-side API changes, listings/delistings, regional restrictions and outages
  are outside SDK control. Adaptations must retain official-source fixtures and
  normalization tests; an exchange-imposed incompatibility must be documented.

Before a release, review public API/feature/default changes against the last tag,
run the [release gates](release-validation.md), and describe any permitted bug fix
or upstream behavior change in the changelog. No 1.0 compatibility claim is made.

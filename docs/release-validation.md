# Release verification and sustained-run plan

This is the release procedure for the current public-market-data 0.1 scope.
The implementation is a release candidate; a plan is not a passing live test.
See [compatibility](compatibility.md) and [PARITY.md](../PARITY.md).
Do not publish to a registry as part of validation.

## Prerelease preparation

`0.1.0-rc.1` is an evaluation candidate, not stable-release approval. Follow the
engineering, package, dependency and extracted-consumer gates below on its final
clean commit. All eleven workspace versions and internal dependency requirements
must select the same RC. Consumers explicitly opt into `=0.1.0-rc.1` only after
registry publication. Disclose the [RC limitations](release-candidate.md) and
leave incomplete sustained-run gates marked FAIL or NOT RUN. Preparing an RC does
not reduce the stable qualification criteria or close unresolved issues.

## Packaging and independent-consumer gates

Use the already-installed development compiler (currently Rust 1.93.0); retain
Rust 1.85 for fresh-resolution compatibility. No toolchain upgrade is required.

1. Run format, all-feature workspace tests, strict all-target Clippy/rustdoc,
   independent feature gates, and fresh Rust 1.85 all-target compilation.
2. Run `cargo publish --workspace --dry-run --all-features`. Keep Cargo's build
   verification enabled. Workspace staging allows unpublished sibling crates to
   be checked together; a single-crate dry-run cannot replace this check.
3. Inspect all 11 `.crate` archives: MIT `LICENSE`, exact third-party notices,
   README, normalized manifests and source must be present. No `.local`, secrets,
   source-workspace paths or build artifacts may be published. Cargo-generated
   archive lockfiles are allowed; the repository still does not track Cargo.lock.
4. Run `bash scripts/verify-packages.sh` after the workspace dry-run. It extracts
   archives outside the checkout and tests all packaged crates, then runs separate
   consumers depending on the exact workspace version (currently
   `cryptofeed-rs = "=0.1.0-rc.1"`), with registry patches pointing only at extracted crates.
   Check default features, no defaults, and all features. Exercise the documented
   FeedHandler/builder, handler, Decimal model and optional recording surfaces.
   Test the packaged core/data/runtime crates from extracted sources as well.
   Patches simulate unpublished siblings; they do not establish registry ownership.
   CI runs this gate after package build verification; the script never uploads.
5. Audit fresh dependency resolution with current RustSec data and inspect the
   SPDX license metadata of all resolved packages. Keep per-dependency notices;
   MIT for this project does not relicense dependencies.
6. Check registry name/version availability read-only. Server ownership/token
   permission and actual uploads remain separate from dry-run. When authorized
   to publish, release core, the nine data crates, then runtime with matching
   versions and verify registry downloads/docs before announcing a release.

`--allow-dirty` is only for reviewing a local candidate; rerun on the final clean
commit. Record commit, compiler/target, resolved versions, command results,
archive SHA-256 hashes and limitations in a dated sanitized report.

## Sustained-run qualification: proposed procedure

### Environment and observability

- Use a dedicated Linux host with a stable clock, persistent process-level RSS/CPU
  sampling every minute, disk-space alarms, and no other collectors sharing its
  egress quota. Record host/OS, commit, compiler, symbols, channels and policies.
- Start with BTC/ETH spot and USDT perpetual on all five active exchanges.
  Include ticker/trade/L2/candles where admitted; derivative funding/OI/index/mark
  and liquidations only where listed in PARITY. Add one live dated-futures symbol
  per supported exchange/product after catalog discovery; do not hard-code expiry.
- Run each product as a homogeneous feed. Include one per-channel unequal-symbol
  set and one discovery-managed feed. Preserve process-local connection/HTTP
  budgets and subscribe conservatively; never induce live rate-limit bans.
- Use identified streams and retained state snapshots. Collect per feed/generation/
  connection: event counts, last receive time, state/readiness, reconnects, errors,
  broadcast lag, L2 gaps/resyncs and shutdown duration. A supervised test consumer
  must turn missing expected data or lag into a failed check, not just print logs.
- Sample L2 state through the recovery API; replay delivered updates into an
  independent local book and compare against an authoritative anchored snapshot.
  Compare price order, deletions, promoted top-N levels and exact quantities.
  Do not compare unaligned network REST snapshots as if they shared a sequence.
- The existing `release_smoke` prints counters at exit and does not implement the
  periodic metrics/independent-book assertions above. Before the sustained run,
  prepare a dedicated observer using the identified streams, state queries and
  recovery API; validate that observer with deterministic doubles first. A probe
  command is `cargo run -p cryptofeed-rs --example release_smoke --all-features`;
  stop with Ctrl-C. Running this command alone does not satisfy the stages below.

- Use bounded, rotated recording around failures and retain only sanitized public
  payloads. Do not record credentials, proxy URLs, raw headers or private traffic.

### Stages

| Stage | Duration | Work and expected evidence |
| --- | --- | --- |
| Preflight | Offline + 30 minutes live | All engineering/package gates; confirm each expected active subscription, data and L2 initialization. Reuse `release_smoke` only as a connectivity probe. |
| Baseline | 24 hours | Observe all selected products without injected failures; capture memory, event freshness, readiness and resync trends. |
| Lifecycle | 4 hours | At least 10 managed add/replace/remove cycles; verify IDs/generations, completed worker shutdown and registry removal. Filter buffered old-generation events; include valid and rejected replacements. |
| Recovery | 4 hours | At least 10 controlled 5–30 second disconnects using a local test proxy/isolated network path; confirm reconnect epoch changes, readiness withdrawal and fresh L2 initialization. No exchange-side flood. |
| Soak | Extend total to 72 hours | Repeat a representative high-volume subset; verify stable resource use and no unrecovered stale books. |
| Shutdown | End of every stage | Caller shutdown/Ctrl-C finishes and closes owned workers/sinks; retain timing and completion results. |

Exercise slow handlers, intentionally lagged consumers, malformed frames,
server silence and quota rejection using deterministic local doubles, not hostile
traffic against exchanges. Run the existing focused connection/quota/recovery
regressions and record their results alongside live observations.

### Acceptance criteria

These are proposed release-test thresholds, not SDK latency/availability SLAs.
Freeze the configured matrix and thresholds before starting; report exceptions.

- Zero unhandled panic, deadlock, silent task loss, incorrect symbol/product routing,
  Decimal precision loss or publication of a known invalid L2 state.
- Zero unhandled consumer lag in the normal-capacity baseline. An intentionally
  lagged consumer must detect loss and recover/restart under its documented contract.
- Active BTC/ETH trade/ticker feeds: flag a 60-second receipt silence and fail if
  unexplained for five minutes. Event-driven sparse channels (funding updates,
  liquidations, quiet dated contracts) require confirmation and an open healthy
  session, not a fabricated minimum event count. Zero liquidation events is not
  proof of liquidation normalization; deterministic fixtures remain mandatory.
- After a controlled disconnect ends, normal subscriptions and L2 readiness should
  recover within 120 seconds. Exclude external exchange outages only with evidence
  and a rerun; never silently relax sequencing/checksum rules to pass.
- For each managed operation, verify the documented acknowledged state and failure
  behavior. Successful replacement can have a collection gap; do not require an
  impossible gap-free handover. Already-buffered old-generation events may arrive
  after acknowledgement: filter by ID/generation and verify worker termination
  separately. No retained worker/socket growth after removals.
- After two hours of warm-up, compare hourly RSS medians: last-six-hour median must
  stay within 20% or 50 MiB (whichever is larger) of the warm baseline, with no
  sustained monotonic growth over six hours. Investigate bounded caches separately.
- Shutdown completes within a 30-second test deadline in the selected setup;
  any timeout fails the run and must be reproduced with a local regression.
- Every observed data-integrity/recovery failure gets a minimized deterministic
  regression and a fresh affected-stage run before release approval.

## Evidence and release decision

Store a dated report in `docs/reports/` with the exact matrix, observation window,
counts, silence/lag errors, reconnect/resync outcomes, RSS/CPU summaries, shutdown
results, package checks and unresolved exceptions. Raw bulky logs stay in ignored
`.local/`. Mark each check PASS/FAIL/NOT RUN; never equate subscription confirmation,
short smoke observations or package dry-run with 72-hour acceptance.

A 0.1 release requires passing the packaged-consumer and baseline/lifecycle/recovery
stages, resolved license metadata and an authorized publisher. The 72-hour extension
is recommended before promoting the build for unattended production use.

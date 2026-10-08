# Pre-push engineering review — 2026-10-08

## Conclusion

The engineering gates pass after the corrections below. The current source is
an unreleased 0.1 candidate. Public distribution still requires maintainer
confirmation of the license/provenance and a working private security reporting
channel. No commit, push, release tag, or crates.io publication was performed
by this review.

The confirmed repository is https://github.com/crypto-2042/cryptofeed-rs.
It was public and empty when inspected. Local `origin` now points to that
repository; the existing local `master` branch was preserved.

## Design and maintainability

| Area | Assessment | Release treatment |
| --- | --- | --- |
| Workspace boundaries | Core identifiers/errors, nine declarative model/handler categories, and a centralized runtime remain appropriate for five exchanges | Preserve the 11-crate split; no per-exchange crates or speculative plugin framework |
| Public entrypoint | FeedHandler plus builders is coherent; preflight separates configuration errors from transport failures | Preserve the entrypoint and capability matrix; distinguish caller-asserted native mappings from catalog validation |
| Protocol correctness | Wire semantics must be authoritative; earlier synthetic fixtures hid several errors | Correct source-backed fixtures and normalization together; see regression table |
| State ownership | Book synchronization belongs to the runtime; partial ticker state belongs to each connection | Hide mutable book-sync internals, reconstruct Bybit deltas, and clear state on snapshot/reconnect |
| API evolution | Consumer matches need to tolerate additional event categories | FeedEvent is non-exhaustive; pre-release OI currency-code field was replaced with a numeric coin quantity |
| Consumption and diagnostics | Callbacks are sequential; bounded broadcast delivery may lag | Expose a retained counter handle, log retry errors/delays, document handler latency and L2 recovery limits |
| Extensibility | Each category/exchange addition has several explicit integration points | Require capability, feature, subscription, parser, dispatch, fixture, parity, and documentation changes together |
| Repository instructions | The previous instructions omitted markprice and referred to a removed subscription abstraction | AGENTS.md and CLAUDE.md are synchronized and equality is enforced by CI |

Large orchestration and inline-test modules are a maintenance cost, not a reason
for a risky pre-push rewrite. Follow-up changes should move exchange session
code and test groups into private modules in small steps while preserving the
facade and behavior. Do not introduce generic dynamic conversion in place of
explicit exchange parsing.

## Corrections made during this review

| Finding | Final behavior / verification |
| --- | --- |
| Binance P treated as predicted funding rate | P is an estimated settlement price; Funding and MarkPrice leave predicted_rate unset |
| Binance COIN-M index planning assumed a contract-specific index topic | Index reads documented markPriceUpdate.i/E; one 1s mark-price topic is shared when Index is requested; default cadence is unchanged otherwise |
| OKX oiCcy treated as a currency code | OpenInterest.coin_quantity is an optional Decimal; original native contracts and USD value are preserved |
| Bitget dated book/liquidation identity lost during parsing | Bind native identifiers with product context before synchronization/dispatch; filter unrequested series and other product groups |
| Bybit ticker deltas omitted unchanged fields | Reconstruct latest supported fields per connection; single-side BBO and USD-value-only OI updates remain visible; snapshots/reconnects clear stale state |
| JSON numeric prices lost precision before Decimal parsing | Enable serde_json arbitrary_precision; raw numeric-wire regressions preserve decimal digits |
| Shared derivative transport incorrectly enabled dated Funding | All five dated-futures Funding requests fail preflight; only perpetual/swap funding is enabled |
| Bitget interval vocabulary did not match current v3 docs | Map normalized hours/days to 1H/4H/6H/12H/1D; reject undocumented 3d/1w/1M runtime intervals |
| OKX monthly/quarterly ends used fixed 30/90 days | Use calendar arithmetic at the documented UTC+8 boundary; leap February and 31-day cases pass |
| Declared Rust 1.85 failed fresh dependency resolution and one let-chain expression | Resolver 3 selects compatible dependencies; the newer syntax was replaced; actual Rust 1.85 compilation passes |
| Producer counters could not be retained when run(self) consumed the handler | FeedHandler::event_counters() returns the existing shared counter handle, readable while running and after shutdown |
| Missing handler warnings described valid event-stream usage as dropped data | Diagnostics describe both consumer paths; transient reconnect errors/delays are observable via tracing |
| Release metadata copied Python authorship and claimed licensing parity | Repository metadata is confirmed, Rust authorship is unset pending input, and the unsupported licensing-parity claim is removed |

The four initial data/identity/precision defects were reproduced with failing
regressions before correction. Additional boundary tests cover snapshot
replacement, connection-state isolation, shared-topic deduplication, product
qualification, unsupported combinations, and retained counter ownership.

Protocol sources are linked from [the baseline](exchange-protocol-baseline.md)
and [fixture provenance](../sample_data/SOURCES.md). Parser references for
unsupported products/channels are not declarations of runtime support.

## Automated gates

| Check | Result |
| --- | --- |
| cargo test --workspace --all-features | 383 tests passed: 263 runtime unit tests, 82 public parity tests, 9 feature-boundary tests, and 29 model/core tests |
| cargo clippy --workspace --all-features --all-targets -- -D warnings | Passed |
| cargo fmt --all --check | Passed |
| RUSTDOCFLAGS=-D warnings cargo doc --workspace --all-features --no-deps | Passed |
| cargo +1.85.0 check --workspace --all-features | Passed on the declared minimum compiler |
| No defaults and all nine individual runtime features | Compilation and feature-boundary targets passed; existing isolated-build unused-code warnings are not suppressed by the all-feature lint gate |
| No-default test/example compilation | Passed |
| Package content | README and LICENSE checked for all 11 crates; repository and docs.rs metadata are consistent |
| cargo audit | Zero vulnerabilities and warnings on the final resolved lockfile |
| Credential-pattern scans | No private-key/GitHub-token/AWS-key pattern matches in current text files or 403 historical Git blobs |
| Repository hygiene | Cargo.lock and target are not tracked; instruction copies match; whitespace and local decision/report links checked |

The initial local lockfile had advisories for rustls 0.23.39,
quinn-proto 0.11.14, and optional rkyv 0.7.46. Fresh compatible resolution uses
rustls 0.23.45, quinn-proto 0.11.19, and rust_decimal 1.43.0, which removes the
old rkyv dependency. No advisory ignore rule was introduced. Cargo.lock remains
local and ignored under the library workspace policy. CI generates and audits
its own fresh resolution; this is not a permanent guarantee about future
consumer lockfiles.

Package listing does not constitute a complete registry publication dry run:
the category crates must be published in dependency order before the runtime.
The installation guide therefore uses a local path/Git dependency for this
unreleased source rather than claiming that this project's 0.1 is on crates.io.

## Manual service evidence

A 50-second release_smoke run exercised spot and perpetual feeds on all five
active exchanges. Every configuration produced normalized high-frequency
market data, no terminal feed failure was reported, and Ctrl-C returned exit
code 0. Detailed counts are in [the pre-push smoke report](live-smoke-pre-push-2026-10-08.md).

After the Bybit state correction, a separate 40-second run received 316 events
for each of Ticker/Funding/OpenInterest/Index/MarkPrice on BTC-USDT-PERP and
404 spot Ticker events, with clean exit. This confirms the live session path;
the offline regressions verify the particular partial-field values and resets.

The final Binance index source, calendar boundaries, and interval preflight
changes also have deterministic regressions. Their narrowly targeted live
status is recorded in the smoke report; historical runs are not relabeled as
verification of later code. No live liquidation payload or calendar-month
rollover was observed in these short windows.

## Remaining maintainer decisions

1. Confirm the Rust project's license and source provenance. The workspace
   still declares XFree86-1.1; the current Python upstream license differs.
   Do not infer Rust authorship or licensing from Python's metadata. Keep
   upstream credit separate from Rust author attribution.
2. Establish a private vulnerability reporting channel. GitHub reporting was
   verified as disabled; no private mailbox was provided. Enable that feature
   or provide a verified private address and update SECURITY.md before public
   distribution.

Authorship is intentionally unset rather than invented. A personal email is
not required merely to configure the repository URL. crates.io owner identity
and package publication remain a separate, future release step.

## Boundaries for the first candidate

- Binance contract OI remains explicitly deferred, including REST polling;
  the multi-symbol rate-budget decision is unchanged.
- Options, MARGIN, authenticated feeds/trading, L3, and Gate.io delivery
  liquidations remain outside enabled capabilities. Specialized equity/rtoken
  instrument exceptions are not claimed as validated cryptocurrency coverage.
- The local checks ran on macOS; the configured Linux CI has not run remotely
  because no push was performed. No Windows or throughput certification is
  claimed.
- Multi-symbol REST bootstrap bursts, subscription/connection limits, and
  large-scale throughput need workload tests before a large deployment.
- Broadcast consumers may lose events. In particular, a lagged L2 consumer
  must wait for a replacement snapshot/restart before treating its own book
  as complete. A read-only snapshot/revision recovery API is follow-up work,
  not an implied guarantee of the current event stream.

## Submission follow-up — 2026-10-08

The maintainer explicitly authorized committing and pushing this unreleased
source candidate to GitHub. Final format, 383 tests, strict all-target Clippy,
rustdoc, no-default and nine isolated feature checks, and Rust 1.85 checks
were rerun successfully. GitHub private vulnerability reporting was enabled
and verified; SECURITY.md now links to the private reporting form. The security
channel decision in the original review is resolved. The license is unchanged;
license/provenance and authorship remain maintainer decisions before a formal
release. This source submission includes no release tag or crates.io publication.

Fresh lockfile-free dependency resolution also passed Rust 1.85 compilation
and a refreshed RustSec audit with zero vulnerabilities or warnings. All 11
crate listings contain README and LICENSE. Credential-pattern scans of current
files and 438 historical Git blobs found no matches; local Markdown links,
instruction-copy equality, and whitespace checks passed. Cargo.lock and target
remain ignored and excluded from the source submission.

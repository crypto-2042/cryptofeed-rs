# Open-issue audit and Bitget failure diagnostics — 2026-10-16

Reviewed the current bodies and comments of GitHub issues #3, #4 and #6.
All three remain open; none currently meets its stated closure criteria.

| Issue | Verified implementation/evidence | Remaining closure evidence |
| --- | --- | --- |
| [#3](https://github.com/crypto-2042/cryptofeed-rs/issues/3) | Bounded-stream diagnostics, observer lock reduction, final drain/accounting; later runs reported zero final receiver lag | Original lag attribution and complete unchanged six-stage Linux acceptance |
| [#4](https://github.com/crypto-2042/cryptofeed-rs/issues/4) | ID/overlap fixes and bounded recovery regressions; #6 reports Gateio evidence/recovery prerequisites PASS | Original live trigger/RSS investigation and full Linux acceptance; prerequisites do not replace the required 72 hours |
| [#6](https://github.com/crypto-2042/cryptofeed-rs/issues/6) | Reproduced synthetic recoverable sequence/crossed-book paths and permanent rejection policy; diagnostic omission repaired | Actual Bitget terminal error and triggering traffic, targeted live diagnosis and full Linux acceptance |

## Issue #6 evidence boundary

The report pins SDK 9a86e83 and soak 0.1.2. Gateio prerequisites and full-matrix
smoke passed, but baseline failed after approximately 2h53m when Bitget perpetual
entered Failed. Later stages were NOT RUN. Final zero lag is not lifecycle
success. No original Bitget frames or actual error cause were retained; this
change cannot retroactively identify that trigger or label it a protocol bug.

Current supervisor policy already retries Parse, MalformedData and transient
Transport errors; rejected subscriptions and non-retryable close/HTTP responses
fail permanently. The default retry limit is unbounded. These policies, cross
checks, subscription matrix and frozen thresholds are unchanged.

## Implemented diagnostic repair

- Public `FailureDiagnostic::from(&Error)` supplies bounded static category,
  reason, permanent-policy flag and optional numeric HTTP/close codes. Arbitrary
  exchange/transport text is never copied into this diagnostic.
- `ConnectionInfo.last_failure` retains the latest failed attempt's classification;
  a successful connect clears it alongside `last_error`. Existing free-text
  errors remain for compatibility and are not safe to publish indiscriminately.
- Connection failure WARN events include stage, feed ID, generation, connection
  ID and epoch. Connect, subscription control and established sessions use the
  same classification. Numeric rejection codes are captured separately.
- Bitget processing failure events include configured product and whitelisted
  native topic; unknown input topics become `unknown`. Join the identity to the
  frozen feed profile for the configured channel/symbol set. Connection errors
  do not necessarily belong to one channel.
- Terminal events distinguish permanent error, retry exhaustion and task
  panic/cancellation; managed worker exits retain identity. Nested aggregate
  SDK errors may be Transport: use the preceding connection/feed diagnostic for
  the actual cause, not only the final aggregate.
- The sibling soak application records these allowed fields, typed connection
  diagnostics and distinct task/run exit outcomes, while continuing to reject
  arbitrary error/message text, URLs, headers and payload fields. All failure
  assertions remain active. A discovered disk-fault test unlink/mkdir race was
  replaced by an atomic symlink swap; its expected failure remains mandatory.

## Regression and protocol scope

Synthetic Bitget perpetual snapshots followed by a sequence gap or crossed book
must revoke cached books/readiness, retain the correct classification, recover
only after a fresh epoch/snapshot, ignore stale-epoch failure writes, and withdraw
on final disconnect. Separate supervisor assertions verify permanent rejection
fails once and recoverable book failures retry. Privacy assertions verify secret
error text cannot enter `FailureDiagnostic` or the soak diagnostic whitelist.
These inputs are not captured issue #6 traffic.

Rechecked [official Bitget v3 depth documentation](https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel)
on 2026-10-16: snapshot/first-update bridging and subsequent pseq continuity
remain the applicable contract. No sequence rule or parser expectation changed.

## Next Linux run

Use the new fixed SDK revision and matching soak build/lockfile. Preserve the
original baseline profile and failure archives. Begin with the full matrix for
load equivalence; a Bitget-only run is a diagnostic control, not acceptance.
Inspect `sdk_diagnostic`, state connection `failure`, `sdk_task_exit` and
`sdk_run_exit`, correlating feed ID/generation/epoch to the frozen profile.
If Bitget fails again, retain the first reason/code and preceding epoch context;
do not infer the cause from the final aggregate or suppress Failed.

Targeted prerequisites do not count toward a newly started unchanged six-stage,
at-least-72-hour campaign. No Linux host test was performed in this local audit.

## Local verification

- All-feature workspace suite: **853 tests passed**.
- Independent orderbook-only Bitget filter: **10 tests passed**.
- Strict all-target Clippy, warnings-as-errors rustdoc, no-default compilation,
  formatting and fresh-resolution Rust 1.85 all-target check passed.
- Sibling soak: **28 component/unit + 7 process regressions passed**, including
  diagnostic privacy and atomic evidence-write fault injection.
- No toolchain upgrade, workspace lockfile commit or baseline threshold change.

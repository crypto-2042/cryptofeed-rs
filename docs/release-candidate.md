# 0.1.0-rc.1: scope and known limitations

Status: **evaluation candidate, not yet published**. This is not stable-release
approval, a lossless market-data service, or qualification for unattended
production. Engineering gates for the final candidate and actual registry
download/docs.rs verification are separate evidence. Existing FAIL and NOT RUN
reports remain valid; issues are not automatically closed by preparing this RC.

All eleven crates use `0.1.0-rc.1`. No runtime code, retention default, feature
default, protocol check or recovery deadline is changed in this preparation.
The admitted API scope remains public spot, perpetual/swap and dated futures on
the five implemented exchanges, as specified in [PARITY](../PARITY.md).
Implementation support is not the same as long-running live qualification.
Authenticated feeds, trading, options and MARGIN are outside this RC scope.

## Evaluation workloads and failure handling

| Workload | Required consumer behavior | Remaining limitation |
| --- | --- | --- |
| Latest ticker/mark/index view | Observe lifecycle/readiness; invalidate stale values after failure or disconnect and refresh from a supported source | Refreshing a current value does not recover every historical update |
| Local L2 view | Use the [recovery API](l2-recovery.md); on lag, anchor gap, withdrawal or epoch/generation change discard the old receiver/book and recover a new snapshot plus subsequent updates | None is unavailable state, not an empty book; no availability or latency SLA |
| Recording, Trade history or derived statistics | Handle every Lagged explicitly; mark affected output incomplete and terminate or reconcile under an application-owned contract | Broadcast, sink and recording do not guarantee complete history or durable delivery |
| Managed feeds | Retain control snapshots, handle terminal lifecycle states, preserve safe failure diagnostics and stop exposing stale output | Re-subscribing cannot fill an arbitrary data gap or prove a defect fixed |

Keep intake short and avoid synchronous reporting/storage or long-held shared
locks while draining. A dedicated executor does not reserve CPU. Extra queues,
larger rings or callbacks are not unconditional lossless fixes: the first two
only change retention boundaries, while slow callbacks can delay socket reads
and hit deadlines. See [delivery semantics](event-streams.md),
[handlers](handlers.md), [sinks](sinks.md) and [recording](recording.md).
Changes to workload, handler grouping or capacity need their own recorded tests;
they must not be relabeled as a PASS of an older frozen configuration.

The existing `book_recovery_public` example demonstrates anchored L2 rebuilding
and the `managed_public` example demonstrates managed lifecycle operations.
They use public network endpoints and are manual examples, not proof of
sustained-run qualification. Application-owned storage, acknowledgements,
deduplication and supported replay require an independently tested design before
claiming complete or durable event history.

## Unresolved qualification

- [Issue #3](https://github.com/crypto-2042/cryptofeed-rs/issues/3): bounded
  broadcast receiver lag remains a failed zero-loss acceptance gate. It does not
  by itself establish parser loss or breach of a lossless API promise. The default
  shared retention is still 1024; consumers must handle loss visibly.
- [Issue #4](https://github.com/crypto-2042/cryptofeed-rs/issues/4): Gateio sequence
  recovery fixes and focused regressions exist; full sustained-run qualification
  for the final candidate is still required. Do not use stale or invalid books
  or disable sequence checks to hide recovery failures.
- [Issue #6](https://github.com/crypto-2042/cryptofeed-rs/issues/6): safe failure
  diagnostics have been added, but the historical Bitget perpetual terminal
  trigger is unresolved. A targeted run without recurrence is not proof of a fix;
  treat its unattended long-running use as unqualified.
- Zero observed events on a sparse channel do not prove live parsing coverage.
  Complete resource/RSS windows, lifecycle and fault-recovery evidence remain
  necessary before declaring stable qualification.

## Installation and promotion

Before publication, use a local checkout or pinned Git commit. Once the RC has
actually been uploaded and verified, consumers opt in with:

```toml
[dependencies]
cryptofeed-rs = "=0.1.0-rc.1"
```

Preparing packages does not authorize upload or establish registry ownership.
Release core, the nine data crates, and runtime at matching versions, then verify
fresh registry downloads and documentation builds. Follow the complete
[release gates](release-validation.md) and [compatibility policy](compatibility.md)
before promotion; retain all historical failures and document any approved
qualification exception explicitly. RC status does not turn a failed normal
baseline into a passing stable release.

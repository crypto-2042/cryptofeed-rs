# Issue #3 — delivery and consumer remediation — 2026-10-14

Source: [Linux acceptance report](https://github.com/crypto-2042/cryptofeed-rs/issues/3).
Status: deterministic remediation verified locally; Linux root-cause attribution
and the new full 72-hour run remain **NOT RUN / pending**. The issue stays open.

## Preserved evidence

SDK 3851298: 30-minute smoke PASS; baseline stopped after about 5.16 hours with
two identified-stream lag notifications, 1321 and 82 (1403 envelopes total).
Lifecycle, recovery, slow-consumer and extended stages were NOT RUN. Earlier
harness suites also remain FAIL. No historical report or frozen profile was
rewritten, and no acceptance threshold was relaxed.

This establishes receiver loss in bounded broadcast, not exchange parser loss,
feed/channel attribution, exact publication burst rate or a unique Linux trigger.
The reported 2-vCPU/shared host lacks scheduling/lock profiling at the failure.
The receive gap is not queue residence time or a direct throughput measurement.

## Changes

SDK:

- `FeedHandler::with_event_capacity(n)` configures normalized/identified broadcast
  retention; the default stays 1024. Existing receivers and overflow behavior
  remain compatible. Status (64) and book recovery (1024) are unchanged.
- `subscribe_identified_with_diagnostics()` adds receiver-local successful/lagged
  counts, lag notifications, effective capacity, sampled pending high-water,
  current pending and closure state, without an extra queue or forwarding task.
- `BookUpdates::pending()` exposes the shared recovery cursor backlog for
  slow-consumer coverage evidence. It includes unrelated and overwritten entries.
- [Delivery guide](../event-streams.md) defines consumption/overflow/final-drain
  boundaries. No unconditional lossless path or throughput guarantee is claimed.

Sibling application cryptofeed-soak:

- Copy owned metric stats while briefly locked, then perform JSON construction,
  validation scans and output admission outside that lock. Registry and stats
  guards are no longer nested for the identified consumer's full processing.
- Measure registry/stats wait maxima, processing and receive-gap maxima, and
  sampled backlog. These are receiver/consumer observations, not exact producer
  or OS scheduler telemetry.
- Optional `minimal_consumer: true` adds a separate counting-only receiver thread
  with no registry/stats lock. It is a diagnostic control with its own payload
  cloning/scheduling cost, recorded in the frozen config; it does not replace the
  full route/top-N observer or weaken zero-lag acceptance.
- Default `event_capacity` remains 1024. Larger explicit values require a new
  approved/frozen configuration; no old profile was enlarged.
- Quiesce SDK publishing before stopping the identified observer, drain retained
  events, then persist final published/received/lagged/closure/pending evidence.
  Compare totals only after producer completion. The report retains final loss
  independently of the last periodic sample and marks missing accounting incomplete.
- Record actual recovery-ring backlog around the configured slow-consumer pause.
  Do not automatically lengthen a frozen pause or claim coverage without actual
  Lagged plus recovery. A new pause profile/run needs explicit configuration.

## Deterministic verification

- SDK full workspace: 841 tests PASS; strict all-feature/all-target Clippy and
  rustdoc PASS; no-default-feature compilation PASS; fresh Rust 1.85 all-target,
  all-feature compilation PASS. Development compiler/toolchains unchanged.
- Four new receiver tests: synthetic two-burst pause produces Lagged(1321) then
  Lagged(82); retained prefix/tail accounting totals 2427; independent subscribers
  and requested capacity rounding; cancellation-safe receive; invalid capacity.
  This is deliberately synthetic, not captured replay of the failed Linux traffic.
- Harness: 24 unit/component + 7 process tests PASS in local combination testing.
  New tests hold the full observer's stats lock while the minimal receiver keeps
  up, retain the resulting real lag, demonstrate snapshot formatting cannot hold
  that lock, drain a queued prefix after publisher close, and prevent last-periodic
  zero lag from masking final loss. Full Git-pinned dependency verification is
  recorded in the sibling application's remediation document.
- Independent trade and recording/trade receiver tests are included in CI.

## Linux diagnostic rerun and closure

1. Freeze repaired SDK/application revisions, binary/lock/config hashes and host.
   Keep old failed run directories immutable. Use new run IDs.
2. On the same workload and original 1024 retention, run the minimal control plus
   full observer and collect final counts, backlog, lock/processing/gap timings
   and host scheduling/cgroup contention evidence. Repeat on the suggested dedicated
   host. Record observer overhead; do not infer feed attribution from aggregate lag.
3. Distinguish lock contention, consumer work, producer bursts and descheduling
   using the new evidence. Do not state that executor isolation or this lock fix
   alone has established the unique real-run root cause.
4. After root-cause attribution and any further repair, freeze the normal acceptance
   config. Complete all six stages in a single worker, at least 72 hours total,
   with zero normal lag and actual intentional-lag/recovery coverage. All other
   routing/L2/resource assertions remain required.
5. Only those new results can satisfy the issue's closure conditions. Local
   deterministic tests and green CI cannot turn old FAIL/NOT RUN into PASS.

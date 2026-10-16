# Acceptance-material review and consumer contention repair — 2026-10-16

Reviewed the three remediation reports and their newer issue-specific updates in
sibling `cryptofeed-soak/docs`, including the sanitized 2026-10-16 material pack.
This repair changes the acceptance application's consumer, not SDK protocols,
retry policy or broadcast retention. SDK remains pinned to 5513831; application
version becomes 0.1.4. Current-build Linux acceptance is **NOT RUN**.

## Evidence and attribution

| Case | Reported evidence | Current interpretation |
| --- | --- | --- |
| Subscriber lag #3 | SDK 5513831 / app 0.1.3 smoke PASS; baseline FAIL after 2,371,257ms; final published 3,430,501 / received 3,430,069 / lagged 432 | Real subscriber loss; neither parser loss nor channel attribution proven |
| Gateio #4 | SDK 9a86e83 / app 0.1.2 evidence 30m and recovery 4h PASS, 10 fault/recovery cycles | Passed prerequisites on that version/topology; not current full-matrix or 72h acceptance |
| Bitget #6 | Old 9a86e83 terminal cause/frame not retained; new 5513831 baseline stopped on lag | Original trigger still unknown; new lag does not reproduce Bitget termination |

All historical failures and NOT RUN results remain intact. The supplied material
pack is immutable evidence; no original reports, archives or checksums changed.

## Deterministically reproduced defect

The original full observer took registry, then blocked on shared stats for every
received envelope. A stats holder could therefore stop broadcast draining and
retain registry, despite the independent observer thread/executor.

A synthetic test holds stats, publishes up to 64 events through a capacity 16
ring, waiting for both full and minimal receivers after each publication. The
material-pack implementation fails to keep draining and reaches the 2s assertion
limit. The repaired implementation passes that same requirement. This proves a
consumer blocking defect; it is not a replay or unique attribution of the Linux
432-event loss. Host scheduling, registry contention and publication bursts
remain possible contributors.

## Repair and safeguards

The observer validates every received envelope and locally aggregates compact
identity-qualified counts/timestamps. Every 32 events or 10ms it attempts a
nonblocking merge: a busy registry/stats lock postpones the merge, rather than
holding registry while waiting for stats. Route lookup itself still uses
registry; this does not claim complete isolation from all locks or scheduling.
No market payload forwarding queue, retention increase or durable delivery API
was introduced. Local pair state follows configured identities, and retired
generations are purged before new identity allocation and before merging.
After producer shutdown and broadcast draining, the final merge may wait and
must complete before published/received/lagged accounting is reported.

Lag notifications remain immediate failures, even if final totals account for
all published positions. The 1024 event ring, frozen feeds, Ready deadline and
book/routing assertions are unchanged. Statistics are periodically merged and
may temporarily lag; final drain remains authoritative. Deferred retired-generation
pair counts become filtered counts, while received still counts those deliveries;
this is configuration filtering, not broadcast loss.

New `stats_deferred_max_micros` measures longest postponed-statistics wall time,
not blocked intake or queue latency. Processing timing now excludes batch merge;
compare historical timing boundaries carefully. `stats_wait_max_micros` records
lock acquisition costs, including final shutdown merge, rather than deferred
periods.

## Local verification

- Repaired contention test: both receivers drain 64 envelopes with zero lag while
  stats remains held; registry is not retained waiting for stats.
- Synthetic 1456-event burst into the unchanged 1024 ring: received 1024, lagged 432,
  final total 1456, closed/drained; lag remains a failure.
- 1000 configuration generations with deferred stats: one retained pair, final
  received 1000, filtered 999 and current pair count 1; old generations not reinserted.
- Explicit route/top-N rejection and existing shutdown/final-loss/privacy tests
  remain enforced; independent process failure supervision remains active.

The sibling application's full local suite passed 31 unit/component tests and
7 process tests in both debug and release builds on the existing default Rust
1.93 toolchain. Strict Clippy, formatting and installed Rust 1.85 all-target
compilation passed. No toolchain was upgraded. No SDK code or dependency change requires
an exchange-protocol fixture change in this iteration.

## Linux follow-up

Use soak 0.1.4 with the same SDK 5513831 and original baseline SHA256
`f94daa013fd8d6e7f8f491c58202fc39ec12db3e7f3be9714fb3970037aadbe8`.
Run full-matrix smoke and baseline in new directories; use the separately frozen
minimal-consumer diagnostic as a changed-load control when attributing stalls.
Do not merge different configurations into one PASS. Preserve the first safe
Bitget failure reason if it recurs; do not suppress Failed or change retry policy
without that evidence. A new unchanged six-stage, at-least-72h campaign is still
required. This repair does not close #3, #4 or #6.

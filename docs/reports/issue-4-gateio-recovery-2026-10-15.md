# Issue #4 — Gateio L2 recovery remediation — 2026-10-15

Status: code defects reproduced/fixed; deterministic verification PASS. Unique
failed-run trigger, RSS investigation and a new full Linux 72-hour suite remain
pending. [Issue #4](https://github.com/crypto-2042/cryptofeed-rs/issues/4) and
[issue #3](https://github.com/crypto-2042/cryptofeed-rs/issues/3) are not closed.

## Retained Linux evidence

SDK f6d0f6b / soak 0.1.1: smoke PASS; baseline FAIL at 37,095,324ms (10h18m15.324s)
with Gateio perpetual books unavailable beyond the frozen 120-second Ready limit.
Final identified published=received=59,137,296, lagged=0, pending=0, closed=true.
Those are whole-worker counts, not Gateio-only throughput. Later four stages NOT RUN.
The baseline profile SHA256 remains
`f94daa013fd8d6e7f8f491c58202fc39ec12db3e7f3be9714fb3970037aadbe8`.
No historical directory/report or threshold was changed.

There were 119 actual book-withdrawal records in the final Gateio window, not
119 proven independent disconnects. Missing original U/u/full and REST id/body
prevents a unique replay-based root-cause attribution. RSS monotonic growth was
an investigation condition, not a demonstrated SDK leak or magnitude-limit breach.

## Correctness fixes

- Current official Spot/Futures order_book_update permits U <= current+1 <= u;
  overlap advances, covered ranges are ignored, real gaps still fail. This is
  distinct from obu/V2 strict adjacency.
- A with_id REST response without id cannot establish a live anchor. Removed the
  timestamp/first-U guess, unanchored first-delta acceptance and stale absolute
  quantity rollback. A complete full push retains its independently valid u anchor.
- Bootstrap applies into a candidate and commits only after every buffered bridge
  succeeds. Rejected bootstrap clears unusable native state even on budget exit.
- Initial plus three additional snapshot operations share a per-symbol physical-
  session budget across bootstrap rejection AND later gaps; successful bootstrap
  cannot refund an immediate-gap loop. Existing global HTTP admission still applies.
- Live pending episodes have a 60-second check; an internal 100ms session poll
  handles HTTP completions/deadlines without requiring another market message or
  cancelling writes. Replay does not impose that live timeout on host/debug delays.
- Public epoch-qualified book_sync evidence tracks synchronization, continuous
  unsynced time, per-epoch/generation attempts, REST IDs/times, delta and gap IDs,
  transition reasons and static failure codes. Old epochs cannot overwrite new ones.

See [contracts and budgets](../gateio-book-recovery.md). Neither the internal
60-second check nor a reconnect guarantees external Ready; the 120-second
acceptance threshold is unchanged and still must be tested.

## Verification scope

- Full workspace: 850 tests PASS. Strict all-feature/all-target Clippy and rustdoc,
  fresh Rust 1.85 all-target compilation and no-default-feature compilation PASS.
- ID range cases: overlap, stale, adjacent, real gap and malformed/unanchored state.
- Missing-ID snapshots older/equal/newer than deltas publish nothing; transactional
  failure cannot commit a partial book. Full push anchors and cancels stale work.
- Real runtime bootstrap-success -> next-gap cycles exhaust cumulative budget;
  readiness and recoverable cache withdraw before the exhausted session exits.
- Completed HTTP bootstrap on an idle socket publishes without another WS message.
- Pending live deadline requires neither delta arrival nor HTTP completion; a slow
  offline replay still accepts later valid-ID recovery and never applies idless data.
- Readiness diagnostics preserve generation totals/continuous unsynced time across
  reconnect and reject stale epochs.
- Positive HTTP causal replay/independent-merge fixtures now carry required id:100,
  retaining their original exact three-event assertions. Separate negative replay
  verifies no idless publication. These are synthetic fixtures, not failed-run captures.
- Source references/captures are updated together; see sample_data/SOURCES.md.

## Sibling harness and next run

Soak 0.1.2 adds optional bounded Gateio book/HTTP evidence, safe WARN/ERROR field
logging, per-book state diagnostics and capture finalization/incomplete checks.
It records no arbitrary SDK error/message, transport headers or endpoint credentials.
Diagnostic JSONL is selected forensic evidence, not a complete native replay file.
Original full-matrix profiles remain unchanged. Dedicated evidence profiles are
new diagnostic workloads with explicit extra CPU/I/O/memory and disk budgets.

The former offline diagnostic example now asserts corrected overlap acceptance,
true-gap rejection and no idless publication; old FAIL evidence stays in the issue.
Local app tests verify trace privacy, native evidence/sequence integrity, report
incompleteness and supervised cleanup. Final Git-pinned app verification and
execution instructions are retained in the sibling project's issue-4 document.

Deploy the new fixed SDK/app with new run IDs. First collect Gateio BTC/ETH startup
and controlled reconnect evidence on the same and a dedicated host; retain native
ranges/REST bodies and host contention. Then restart the original six-stage suite
for >=72 hours without skipping Gateio or weakening any assertion. Neither code
regression, zero final lag nor short connectivity can substitute for this acceptance.

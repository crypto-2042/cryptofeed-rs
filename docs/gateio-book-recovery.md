# Gateio L2 sequence recovery and evidence

Verified against current official Spot/Futures v4 documentation on 2026-10-15,
following [issue #4](https://github.com/crypto-2042/cryptofeed-rs/issues/4).
This covers `spot.order_book_update` / `futures.order_book_update`, not `obu`/V2.
Supported exchange/product/channel combinations are unchanged.

## Sequence correctness

REST uses `with_id=true`. The snapshot's ID is the only REST sequence anchor;
`current`/`update` timestamps cannot prove which increments it contains. Missing
ID fails bootstrap without publishing or applying buffered absolute quantities.
The former timestamp/first-U heuristic was unsafe and has been removed.
Standalone REST models may still represent missing IDs; that does not make such
a response usable for live L2 initialization.

For an anchored book at id N:

- u <= N: stale range, ignore without rolling back quantities.
- U <= N+1 <= u: valid adjacent or overlapping coverage; apply absolute quantities.
- U > N+1: real missing updates; withdraw the book and resnapshot.
- U > u: malformed range; reject.

Bootstrap is transactional: a buffered gap cannot commit a partial sync or publish
an initial snapshot before failing. An already supported complete `full=true`
push with u replaces the entire book, anchors at u and cancels superseded pending
REST/buffers. An ordinary unanchored delta never becomes a guessed snapshot.
No V2 strict U==N+1 rule is imposed on the current order_book_update channel.

## Bounded recovery

Per symbol and physical session, at most one initial snapshot operation plus
three additional operations may be scheduled. All causes share this budget,
including bootstrap rejection and a gap immediately after a successful bootstrap.
Success/full push does not refund previously spent REST attempts. Exhaustion
returns to the existing transient reconnect/backoff supervisor; it does not
silently resume an invalid book. A later reconnect creates a new epoch/budget.

A continuously pending live bootstrap also has a 60-second per-episode deadline.
The session checks ready snapshots/deadlines on a 100ms idle poll as well as on
received market data. Polling occurs inside the session read select, so the runtime
does not cancel an in-progress heartbeat, subscription or Pong write. A completed
REST response can initialize a book even when no new market message arrives.
Polls do not reset receipt-idle timers, fabricate data clocks or close raw sessions.

The deadline is checked when the runtime is scheduled; it is not a real-time SLA.
The default supervisor may retry transient failures indefinitely. HTTP operations
still use process-local admission/cooldown/cancellation; scheduled operation counts
are not exact wire-request counts if HTTP retries or pre-start cancellation occur.
The external Linux Ready deadline remains 120 seconds, unchanged.

Offline native replay uses ID/quantity/budget rules but does not emulate a live
network timeout with the host's wall-clock delay. A slow/debug replay cannot turn
an otherwise valid ID bridge into a network deadline error. Old captures relying
on invalid timestamp-only anchors are not promised their former erroneous output.

## Public managed evidence

`RuntimeControl::state(id).book_sync` exposes `BookSyncDiagnostic` entries for Gateio
L2 symbols. Other exchanges currently leave this vector empty. Entries contain:

- Current connection/epoch, synchronized flag and first continuous unsynchronized
  time (preserved across reconnects, cleared by valid book initialization).
- Snapshot operation count for this epoch and cumulative total for the logical
  configuration generation. Totals persist across reconnects, not replacements.
- Current transition reason and latest static failure code within this epoch.
  A recovered book may retain a historical last_error; it is not itself a current
  failure flag. New epochs reset error/native context and reject stale updates.
- Last consumed REST snapshot ID and generation/update times, last incoming
  delta U/u, and the anchor/U/u of the latest rejected sequence window in this epoch. A complete
  full push has no delta U and records its u separately as the last update.

These are bounded diagnostics, not an audit log or native checksum. Ready still
requires all subscriptions and all requested synchronized books. Recent Trade,
Ticker or historical comparison counts cannot override an unavailable book.

SDK warning/error events include safe recovery reasons, IDs and bounded buffered
sequence samples (first 16 plus total count). Full payloads need explicit raw
capture; warning samples alone cannot replay a failed run. Existing raw capture
can retain public order_book_update frames and consumed HTTP snapshots/errors.
It is bounded and reports overflow/incompleteness; never label a partial capture
as complete evidence. See [raw capture](raw-capture.md) and
[HTTP/L2 replay](http-l2-replay.md).

The sibling soak application adds optional Gateio evidence profiles, a filtered
WARN/ERROR subscriber, typed state logging and capture finalization. Its diagnostic
JSONL selects book updates and HTTP bodies/errors and is **not** a complete SDK
raw-recording replay file. Original full-matrix baseline profiles remain unchanged.

## Verification and remaining boundary

Synthetic regressions cover overlap/stale/adjacent/real-gap ranges; idless rollback
prevention; transactional bootstrap; complete full-push anchoring; successful
bootstrap followed by repeated gap with cumulative budget; idle-socket bootstrap;
live deadline and offline clock separation; stale epoch diagnostics; and HTTP/
independent-merge replay with required IDs. See
[the dated remediation report](reports/issue-4-gateio-recovery-2026-10-15.md).

The failed Linux run lacked raw U/u/full and REST IDs. These fixes address proven
code defects, not a uniquely reconstructed cause of that run. RSS monotonic growth
is still an investigation signal, not an established memory leak. Historical
FAIL/NOT RUN and both issues remain open until new frozen Linux evidence satisfies
their closure criteria.

Sources: [Spot maintenance rules](https://www.gate.com/docs/developers/apiv4/ws/en/),
[Futures maintenance rules](https://www.gate.com/docs/developers/futures/ws/),
[Futures REST with_id](https://www.gate.com/docs/developers/apiv4/en/futures/).

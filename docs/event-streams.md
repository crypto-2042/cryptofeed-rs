# Event-stream delivery, capacity and diagnostics

`FeedHandler::subscribe()` and `subscribe_identified()` are bounded/lossy Tokio
broadcast subscriptions. The default retention is 1024 messages shared across
all feeds and channel/symbol pairs in a handler, with separate rings for plain
and identified streams. Each receiver has a cursor, not a private 1024-message
queue. Producers never wait for receivers; a full ring overwrites old entries.
This is not an end-to-end lossless or exactly-once market-data service.

## Configuration and supported consumption

`FeedHandler::with_event_capacity(n)?` sets the requested retention of both event
rings before any subscriptions are created. Zero and Tokio's unsupported huge
capacities are rejected. Tokio rounds retention up to a power of two. Existing
`new()` and raw subscription return types/defaults are unchanged. Status retention
(64) and L2 recovery retention (1024) are independent and unchanged.

```rust
use cryptofeed_rs::prelude::*;

let mut handler = FeedHandler::with_event_capacity(4096)?;
let mut events = handler.subscribe_identified_with_diagnostics();
// Register feeds and run the handler concurrently, then consume events continuously.
```

4096 is an illustration, not a capacity recommendation or a remedy for sustained
consumer overload. Choose retention using measured bursts and bounded consumer
stalls, accounting for variable-sized L2 payloads and the additional recovery
cache. Test the chosen budget on the actual host; do not silently change a frozen
acceptance profile. A larger ring cannot fix a consumer slower than its producers.

Drain on a dedicated executor when SDK parsing/book work competes with consumption.
Keep route lookup/counting brief; do not format reports, perform synchronous I/O,
or hold shared registry/stats locks across JSON construction or expensive checks.
If using another queue, bound it and make its overflow fatal/visible too. Consider
separate FeedHandlers for documented workload isolation, while process-local
exchange/resource budgets still apply; this is a changed test configuration.

Typed async callbacks offer another supported processing path, but execute in
session order with deadlines (five seconds by default). Waiting on storage or a
backpressured callback can delay socket reading, trigger timeouts/reconnects and
lose upstream market updates; callbacks are not an unconditional lossless path.
Applications needing durable delivery own persistence, acknowledgements,
deduplication and supported recovery. See [handlers](handlers.md) and
[recording](recording.md) for their actual failure boundaries.

## Receiver-local evidence

`subscribe_identified_with_diagnostics()` returns `IdentifiedSubscription`, a
wrapper of exactly one receiver on the existing ring. It adds no forwarding
queue/task and preserves `recv`/`try_recv` errors and cancellation safety.
`diagnostics()` reports:

- `received`: successful receives by this receiver since its creation.
- `lagged`, `lag_notifications`: sticky losses actually acknowledged by receive
  calls, not a prediction from queue length and not publisher/parser loss counts.
- `pending`: unconsumed cursor positions, including already overwritten entries;
  it can exceed retention. It is not the number of still-readable events.
- `sampled_pending_high_water`: largest backlog sampled by receives/diagnostic
  queries; a lower bound on the true peak, not precise producer burst telemetry.
- `effective_capacity`: the rounded retention.
- `closed`: all senders are gone; retained messages may still be unread.

Handle every `Lagged(n)` explicitly. It advances that receiver to the oldest
retained position; later receives continue. It does not clear earlier loss
counts. There is no feed/channel attribution for overwritten envelopes; do not
infer their distribution from the next retained event. Multiple receivers have
independent delivery/loss counts. Subscribing later excludes the earlier prefix.

After all SDK publishers stop, drain until `Closed`, handling lag along the way,
then compare final counts for the same subscription scope. A last periodic sample
is not a final count; producer counters and receiver observations are not sampled
atomically. Keeping an additional sender alive prevents closure. Bound shutdown
and classify a partial drain as incomplete, never as loss-free acceptance.

L2 consumers discard stale state and use [recovery](l2-recovery.md) after loss;
`BookUpdates::pending()` exposes the underlying shared recovery-ring backlog,
including unrelated feeds and overwritten entries. Ticker/mark values may be
refreshed from later observations, but that does not restore historical events.
Trade/candle consumers cannot assume a later event or a bounded REST history call
fills every lost event. Recording/sinks must reject an incomplete capture rather
than labeling it complete.

## Issue #3 and verification boundary

[Linux report](https://github.com/crypto-2042/cryptofeed-rs/issues/3) observed two
lag notifications totaling 1403 envelopes. It establishes subscriber loss, not
SDK parser loss or a known feed/channel distribution. The consumer's shared
stats lock covered JSON metric construction, a reproducible starvation risk.
The harness now copies stats before formatting, records lock waits/processing/
receive gaps and backlog, optionally runs a lock-independent minimal counter,
and performs final drain/accounting. Default retention remains 1024 and normal
acceptance remains zero lag. A minimal diagnostic subscriber also clones received
payloads and has its own scheduling cost; record its presence in the frozen run.

Deterministic SDK tests reproduce paused-consumer overflow, exact lag/accounting,
independent fast/slow subscribers, configured rounding and cancelled receives.
They are synthetic workloads, not captured exchange replay or Linux qualification.
Harness regressions distinguish shared-lock contention from minimal consumption
and preserve final loss even if the last periodic sample was zero. The actual
Linux trigger (burst, locking, scheduler or their combination) still requires a
new frozen diagnostic run and host evidence. Previous FAIL/NOT RUN results remain.

Sources: [Tokio broadcast delivery](https://docs.rs/tokio/1.53.2/tokio/sync/broadcast/index.html),
[receiver backlog/closure](https://docs.rs/tokio/1.53.2/tokio/sync/broadcast/struct.Receiver.html).

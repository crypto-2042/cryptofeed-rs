# Recoverable L2 consumption

`FeedHandler::l2_book_handle()` opts into retained L2 recovery state and managed
runtime startup. Retain the handle before `run`; it works for initial feeds,
later additions and replacements. It can be requested after initial registration
but before startup. Without this call no recovery cache is allocated. Existing
raw/identified broadcasts and callback APIs are unchanged.

```rust,no_run
use cryptofeed_rs::prelude::*;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let books = handler.l2_book_handle();
    let control = handler.control_handle();
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown));
    let identity = control.add_feed(Okx::new().l2_book().symbol("BTC-USDT").build()).await?;
    let symbol = Symbol::spot("BTC", "USDT");
    let mut recovery = books.recover(identity, &symbol);
    // Copy the complete recovery.snapshot if present. Then consume updates.
    // On Lagged or an anchor gap, discard the local book and replace recovery:
    recovery = books.recover(identity, &symbol);
    drop(recovery);
    control.remove_feed(identity.id).await?;
    control.shutdown().await?;
    running.await??;
    Ok(())
}
```

For a complete consumer loop, run
`cargo run -p cryptofeed-rs --example book_recovery_public`. It checks consecutive
OKX snapshot/delta anchors, reconstructs local state, reads a recovery snapshot,
then removes the feed and verifies that its cached book is no longer available.
The network example is a manual observation, not a CI gate or a claim of real
packet loss during that observation.

## Atomic recovery and continuity

`recover(identity, &symbol)` copies a full `BookSnapshot` and subscribes to
subsequent updates under the same lock used by producers. There is no gap
between the snapshot revision and the new queue. `snapshot == None` means the
requested generation/symbol has no initialized, usable cached book; unknown IDs,
unsubscribed symbols, initialization, resync, disconnect and retired generations
all produce None. Do not interpret None as an empty market book.

`BookUpdates::recv()` returns only the selected configuration identity and symbol.
Every `BookUpdate` has a `BookAnchor` containing identity, physical connection ID,
retry epoch and local revision. The revision is contiguous for that symbol within
one connection epoch. It is an SDK delivery version, not a native exchange
sequence number, checksum or cross-exchange ordering guarantee. Existing native
book-sync checks still decide which data can be dispatched.

To apply a Delta, require a usable snapshot and exactly the same identity,
connection and epoch as the last applied anchor, with revision increased by one.
A Snapshot replaces the local book and establishes its supplied anchor. Never
join deltas across anchors, reconnect epochs or configuration generations.
`book == None` withdraws the book: discard local state and wait for a new snapshot
or call recover again. A missing completion snapshot must not be replaced by
applying deltas to an empty book.

The bounded broadcast ring holds 1,024 updates across all recovery-enabled feeds
in the handler. Filtering is receiver-side; unrelated updates consume the ring
and can cause Lagged. On Lagged, discard the old receiver and local state, call
recover again, and restart from the returned complete snapshot. Reading a
snapshot while keeping the old queue would require discarding its older frames;
this API deliberately acquires a fresh queue atomically. Sustained consumer lag
can require repeated recovery; it does not stop exchange ingestion.

## Cache lifecycle and resource boundaries

The cache has one current book per subscribed feed/symbol. It applies normalized
snapshots/deltas in dispatch order and builds sorted full price-level arrays only
when recovery is requested. Prices, quantities and base/contract units remain
those of the existing normalized exchange models; timestamps on an assembled
snapshot come from its last applied normalized update. It is an instantaneous
local view, not a REST snapshot fetched on demand or a durable history.

Native bootstrap/resync must dispatch a snapshot before deltas become recoverable.
In-session resync invalidates only the affected symbol. Disconnection invalidates
all books owned by that physical connection immediately, including when its
attempt future is dropped. New epochs start unavailable; stale epoch updates or
drops cannot publish or invalidate their successors. Healthy sibling connection
books remain usable. Feed stopping/failure/removal retires its cache and rejects
late publications; candidate cancellation cannot retire the older running
configuration. Manual replacement has a new generation and requires a new
identity-specific recovery subscription.

A retained handle remains queryable after shutdown, but contains no retired
usable books. It also retains the sender, so stream closure alone is not a
shutdown signal while handles remain alive. Use runtime lifecycle/control or
your application shutdown signal to end a waiting consumer. The cache/stream is
process-local and optional; it adds memory for the active local books and bounded
update ring. Holding a returned snapshot retains that independent copy.

## Evidence and remaining work

Deterministic tests cover contiguous updates/deletions, lag and recovery,
resync withdrawal/reinitialization, disconnected and stale epochs, independent
connections, candidate cancellation/replacement retirement, and concurrent
producer versus snapshot/subscription acquisition. Runtime hook tests verify
shared initialization, resync, disconnect/retry and stopping. The separate dated
[public smoke report](reports/live-smoke-book-recovery-2026-10-10.md) records actual
OKX observation without presenting scripted lag as live packet loss.

This completes the planned L2 recovery interface. Needed runtime policies,
full market metadata and the optional REST/history, recording/sinks/aggregation
and NBBO workstreams remain unfinished in the broader alignment plan.

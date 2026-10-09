# Managed runtime control

Implemented core commands and configuration-generation identity on 2026-10-10.
Remote subscription readiness, periodic discovery reconciliation and further
consumer recovery remain in the [alignment plan](python-usage-alignment.md).

## Enabling control

Call `FeedHandler::control_handle()` before running the handler. Keep the
returned cloneable handle and run the handler concurrently. `run()` retains
Ctrl-C support; `run_with_shutdown` uses a caller-owned watch signal. The signal
listener is scoped to `run`, so explicit control shutdown does not leave a
detached Ctrl-C task waiting after the runtime finishes.

```rust
use cryptofeed_rs::prelude::*;

async fn service() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    let control = handler.control_handle();
    let mut events = handler.subscribe_identified();
    let (_stop, shutdown) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(handler.run_with_shutdown(shutdown));

    let first = control.add_feed(
        Okx::new().trade().symbol("BTC-USDT")
            .exchange_symbol("BTC-USDT").build()
    ).await?;
    let event = events.recv().await?;
    assert_eq!(event.identity, first);

    let current = control.replace_feed(first.id,
        Okx::new().trade().symbol("ETH-USDT")
            .exchange_symbol("ETH-USDT").build()
    ).await?;
    // Retain current when consuming further events; old queued events
    // may still be present and must be filtered by id plus generation.
    control.remove_feed(current.id).await?;
    control.shutdown().await?;
    running.await??;
    Ok(())
}
```

The bounded, cleanup-aware manual example is
`cargo run -p cryptofeed-rs --example managed_public`. The snippet illustrates
API ordering; production services should also handle timeouts, broadcast lag,
status events and application error paths. Subscription receivers can now be
created after initial `add_feed` registration as well as before it.

## Identity and command results

`add_feed_with_id` retains an initial logical `FeedId`; existing `add_feed`
still returns `()` and keeps its previous calling shape. Dynamic `add_feed`
returns a `FeedIdentity { id, generation }` once validation, hydration and
admission succeed and session tasks launch. `replace_feed` keeps the ID and
allocates a new generation for each accepted attempt, including failed or
cancelled attempts. Thus generation numbers can have gaps. Reconnects of the
same configuration retain the generation; this is not a physical connection ID.
IDs are unique only within the current process and are not persisted identifiers.

`control.feeds()` lists registered logical workers and their latest committed
identity/exchange. This includes initial preparation or failed workers retained
for replacement; the registry is not a health/readiness snapshot. It also lets
a caller reconcile IDs if a command was committed before its reply was lost.
Removed workers disappear from the registry. Unknown IDs and busy updates
return explicit errors.

Commands have a bounded runtime queue of 32; each feed worker has a four-command
queue. Add/replace acknowledgements mean **tasks launched**, not exchange
subscription accepted, first event received, or an L2 book synchronized. Use the
identified data stream for actual data and lifecycle notifications for failures.
Remote-ready status is a remaining implementation item.

## Replacement and cancellation

The old generation continues while the candidate validates, hydrates and checks
connection admission. Invalid candidates return an error without closing the
old generation. Admission retains the maximum old/new count for a same-exchange
replacement; changing exchange reserves both until the old generation stops.
This prevents concurrent updates from overcommitting the handler's plan budget.

Once the candidate is admitted, the worker stops/drains the old tasks and then
launches fresh SDK runtime state. There is an intentional data gap; no
exchange-atomic subscription switch or uninterrupted book is claimed. A remote
startup failure after that point is reported for the new generation and does
not silently restore the old feed. Existing handlers are reused as supplied;
SDK-owned book/ticker state is reset, not caller-owned handler state.

Cancelling a command while it is preparing cancels that preparation. After the
transition is committed and old shutdown starts, replacement finishes even if
the reply receiver disappears. Global shutdown and remove can cancel pending
preparations without waiting for HTTP completion. Concurrent updates for the
same feed are rejected while an update/removal is in progress.

Remove acknowledges stopped asynchronous child tasks, including forced
cancellation after the grace period. Snapshot receivers are dropped so their
cancelled requests cannot deliver a result into a new generation. Already
published events stay in the bounded queues: compare the whole FeedIdentity,
not only exchange/symbol. Async callbacks must yield and return promptly;
forced cancellation does not roll back caller side effects or interrupt
arbitrary blocking application code.

## Startup, failure, and shutdown

Control mode starts initial logical feeds independently: one invalid or failed
feed does not prevent healthy initial feeds from running. Without a control
handle, legacy startup remains strict and validates/hydrates the complete list
before connecting. Both paths preserve capability preflight and resource budgets.

`subscribe_status` preserves existing terminal notifications and adds scoped
Lifecycle states: Preparing, Started, Stopping, Stopped, Failed, Degraded and
Cancelled. Degraded identifies a failed concrete group while other groups can
continue. Candidate-generation failure/cancellation does not mark an older
running generation as stopped. Status delivery remains bounded/lossy (64
entries); it is not a durable audit log or readiness registry.

Failed logical workers remain available for explicit replacement until removed
or the runtime is shut down. Dropping control handles is not a request to stop
healthy feeds; the external watch signal or Ctrl-C can still stop the runtime.
A managed runtime may remain idle waiting for explicit shutdown/replacement.
`shutdown()` closes admission, signals all workers, and waits for bounded async
cleanup. Run/shutdown results retain aggregated terminal failures, including
previous failures even after replacement. Validation errors returned by a
command do not themselves turn a healthy runtime's final result into an error.

## Evidence and remaining alignment

Offline actor tests use injected private preparation/consumption boundaries,
not live exchanges. They cover add/replace/remove, invalid candidates, cancelled
preparation, busy commands, independent initial startup, scoped panic/recovery,
fresh SDK state, admission, old/new event identity and confirmed child drop
before removal acknowledgement. Existing protocol/session tests remain enabled.
The public [managed smoke report](reports/live-smoke-managed-2026-10-10.md)
records one real OKX replacement and clean removal/shutdown.

Next lifecycle work includes remote readiness/state queries and opt-in listing
reconciliation. Multi-handler delivery, closed-only candles, recoverable L2
consumption, public REST/history and ecosystem tooling remain separate unfinished
alignment work. This control increment is not completion of the overall goal.

# Multiple handlers and callback execution

Each category supports an optional primary `*_handler(...)` and additional
`add_*_handler(...)` registrations. This covers ticker, trade, candles, funding,
liquidations, mark price, open interest, index price and orderbook. Orderbook
registrations receive both subscribed L1 and L2 events through their existing
trait methods.

```rust
use cryptofeed_rs::prelude::*;
use std::sync::Arc;

fn configure(primary: Arc<dyn TradeHandler>, archive: Arc<dyn TradeHandler>) -> ExchangeFeed {
    Binance::new()
        .trade()
        .symbol("BTC-USDT")
        .trade_handler(primary)
        .add_trade_handler(archive)
        .build()
}
```

Existing setters replace only the primary handler. The primary always runs
first, followed by additional handlers in registration order. An add-only
configuration works without a primary. Re-registering the same Arc intentionally
invokes it again; registration is not deduplicated. Calling a setter after adds
changes the primary without removing or reordering the additional registrations.
Configuration grouping/reconnect/replacement preserves registered Arc values;
caller-owned state is not reset by the SDK.

The inspected Python `feed.py::callback` awaits each callback sequentially.
Rust similarly awaits callbacks in order for each event on a session. Each gets
an independent clone of the normalized model. L2 local state is updated and the
event is published/counted once before callback execution; adding handlers does
not multiply broadcast events, metrics or book application. Channel/symbol and
candle-completion filtering occurs before any callback.

## Deadlines, failure and shutdown

Each invocation has its own SDK timeout (default five seconds), starting when that
handler begins. Configure it with
[RuntimeOptions::handler_timeout](runtime-options.md). Timeout drops the callback future, logs a warning and continues
with the next handler; it does not retry the event or declare the exchange feed
failed. Catchable Rust panics are logged and likewise do not skip later callbacks
or subsequent events. Future construction and polling run inside the unwind
boundary. Panic hooks may still print; panic-abort builds cannot recover panics.
Caller-owned state may be partially modified or poisoned after panic, and
callbacks may already have produced external side effects before cancellation.

Existing handler traits return `()`, so they have no returned business-error
channel. Handle application errors within the callback. The SDK reports
callback timeout/panic via tracing; they are distinct from exchange terminal
failures/readiness. No new typed callback-result API is claimed by this increment.

Serial execution has no implicit queue or background workers: a slow handler
holds up later handlers and subsequent session reads. N timed-out callbacks can
consume approximately N times the configured timeout for one event. Different sessions
may invoke the same Arc concurrently; Send + Sync does not imply global event
ordering. Callback futures must yield for Tokio timeouts/cancellation to work;
CPU loops, blocking calls and detached caller tasks are not forcibly preempted.

Runtime stop/removal/replacement uses the existing bounded task drain and abort.
A slow callback can be cancelled before later callbacks run; callback delivery
is not transactional or a promise that every handler sees every event during
shutdown. SDK cancellation drops owned async futures; it cannot undo effects or
stop tasks that caller code detached. Bounded/lossy broadcast remains a separate
fan-out choice for consumers that should not block a session.

Offline tests cover order, replacing primary after appends, add-only registration,
independent models, one event/counter publication, panic/timeout continuation,
subsequent events and shutdown while a callback is pending. All category feature
builds retain their gated handler APIs; exchange protocols and normalized models
are unchanged. Recoverable L2 consumption and optional ecosystem work remain
separate increments in the alignment plan.

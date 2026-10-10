# Runtime budgets

`RuntimeOptions` configures one logical feed's physical connection supervisors
and callback deadlines. Its defaults preserve unbounded transient retries,
a 20-second connection establishment timeout and a five-second timeout for each
callback. Options survive symbol hydration, concrete channel groups, capacity
shards, reconnects and managed replacement templates.

```rust
use cryptofeed_rs::prelude::*;
use std::time::Duration;

fn configure() -> Result<ExchangeFeed, Box<dyn std::error::Error>> {
    let options = RuntimeOptions::default()
        .max_retries(Some(3))
        .connect_timeout(Duration::from_secs(10))?
        .handler_timeout(Duration::from_millis(200))?;
    Ok(Okx::new().trade().symbol("BTC-USDT").runtime_options(options).build())
}
```

Timeout setters reject zero and return a configuration error before a feed is
built. `retry_limit`, `connection_deadline` and `callback_deadline` expose current
settings. `WsConnection::runtime_options` also accepts options for low-level
connection users; it uses only the connection deadline.

## Retries and successful initialization

`max_retries(None)` allows unbounded transient retries. `Some(n)` permits the
initial attempt plus at most n retries until initialization succeeds; `Some(0)`
permits only one attempt. The budget belongs to each physical connection,
not all connections or all feeds pooled together. Exhaustion follows existing
terminal/degraded feed handling, without stopping unrelated logical feeds.

The inspected Python connection handler resets retries and delay after connection,
authentication and subscription writes succeed. Public Rust sessions now similarly
reset retry count and the 1-second initial backoff after all initial subscription
writes complete. Before the full queue is sent, incoming data cannot mark that
attempt initialized. Legacy Binance URL subscriptions have no client subscribe
write; their first non-control text delivery marks initialization. This does not
claim remote confirmation or L2 readiness: use the managed state API for that.

After initialization, a later disconnect can start another retry budget. A finite
retry setting therefore does not cap lifetime reconnect count; repeated sessions
that initialize successfully can keep reconnecting. Backoff remains jittered,
doubles from one to eight seconds and resets on successful initialization.
Unrecoverable configuration/capability/subscription-rejection errors remain
terminal immediately, even if initialization was marked successful. Explicit
shutdown remains clean and interrupts attempts/backoff.

The low-level public `retry_with_backoff_until_shutdown` helper retains its
original generic operation-budget contract; the exchange runtime adds private
subscription-progress tracking. No native subscription/ack contract changed.

## Deadline scope and fixed policies

The connection deadline starts after shared connection-slot/start admission.
It covers DNS/TCP/TLS/WebSocket establishment, not catalog HTTP, book bootstrap,
subscription acknowledgements, idle detection or total time until Ready.
Admission and active connection attempts remain cancellation-aware through the
existing supervisor. Changing this deadline does not bypass process-local limits.

The callback deadline starts separately for each primary/additional invocation
and cancels only that future. Timeout/panic still continues to later callbacks;
callbacks must yield, and partial external side effects are not undone. See
[handler semantics](handlers.md). A larger callback deadline does not enlarge the
runtime's bounded shutdown grace period.

Protocol heartbeats/idle deadlines, the 30-second subscription confirmation limit,
HTTP directory/snapshot budgets and shared admission/send pacing remain fixed.
Python's `timeout` is an idle-message watcher; it must not be translated directly
into this connection deadline. Explicit proxy configuration, start delay and
caller idle policy remain separate unfinished work: HTTP and WebSocket proxy
paths must be handled consistently, while idle policy must respect exchange
heartbeat requirements. This increment does not claim general Python config-file
or proxy parity.

## Verification

Offline tests verify defaults/zero rejection, retry count versus initial attempt,
subscription-success reset and permanent-error behavior, stalled establishment,
result preservation, configured callback cancellation after FeedHandler
registration, and per-channel option preservation. Existing session doubles
verify that partial subscribe queues do not reset the budget and completed writes
do. No external endpoint, fixture or parser was changed; these are SDK policy
tests, not new live protocol evidence.

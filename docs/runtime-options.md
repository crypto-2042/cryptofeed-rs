# Runtime budgets

`RuntimeOptions` configures one logical feed's physical connection supervisors
and callback deadlines. Its defaults preserve unbounded transient retries,
a 20-second connection establishment timeout and a five-second timeout for each
callback, zero startup delay and the exchange-specific receipt watchdog. Options survive symbol hydration, concrete channel groups, capacity
shards, reconnects and managed replacement templates.

```rust
use cryptofeed_rs::prelude::*;
use std::time::Duration;

fn configure() -> Result<ExchangeFeed, Box<dyn std::error::Error>> {
    let options = RuntimeOptions::default()
        .max_retries(Some(3))
        .start_delay(Duration::from_secs(2))
        .idle_policy(IdlePolicy::After(Duration::from_secs(120)))?
        .connect_timeout(Duration::from_secs(10))?
        .handler_timeout(Duration::from_millis(200))?;
    Ok(Okx::new().trade().symbol("BTC-USDT").runtime_options(options).build())
}
```

Timeout setters reject zero and return a configuration error before a feed is
built. `retry_limit`, `connection_deadline` and `callback_deadline` expose current
settings. `startup_delay` and `receipt_policy` expose the new timing policies.
`WsConnection::runtime_options` also accepts options for low-level
connection users; it uses the connection deadline and applies idle policy to the
established session. Retry/start delay/callback settings belong to runtime
supervisors/dispatchers, not the low-level connection itself.

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

## Startup and transport receipt watchdog

`start_delay(duration)` waits once per physical connection supervisor before
shared connection admission, after catalog/capability preparation. Default zero
adds no wait. Retry attempts use backoff without repeating startup delay. A new
replacement generation starts new supervisors and delays again, adding to its
replacement data gap. The delay is cancellation-aware; shutdown/removal can end
it before any socket work. False watch notifications do not restart its timer.
This matches the inspected Python connection handler's initial start delay.

`idle_policy(IdlePolicy::ExchangeDefault)` preserves the verified per-exchange
watchdog. `IdlePolicy::After(duration)` overrides it with a positive receipt idle
timeout; zero is rejected. `IdlePolicy::Disabled` removes the receipt watchdog,
corresponding to Python's disabled timeout option. Unlike Python's periodic
watcher, Rust checks the actual deadline in the session select loop.

Receipt means incoming transport frames, including Ping/Pong and control/data
traffic, not just normalized market events. Outgoing heartbeats do not count as
receipts. Neither changing nor disabling the watchdog changes heartbeat
payloads/cadence, Ping/Pong responses, subscription-ack deadlines, explicit remote
close/reconnect behavior or shutdown. Callers can deliberately set a deadline
shorter than the exchange's quiet heartbeat cadence, which can reconnect healthy
but quiet subscriptions; use ExchangeDefault unless the application needs that
tradeoff. Disabled idle detection may leave a quiet connection open until another
transport/error/shutdown condition occurs.

The 30-second subscription confirmation limit, HTTP directory/snapshot budgets
and shared admission/send pacing remain fixed. Python's `timeout` is the idle
message watcher, not the connection establishment deadline. Explicit HTTP/WS
proxy configuration remains unfinished and must handle both transport paths
consistently. This increment does not claim general Python config-file parity.

## Verification

Offline tests verify defaults/zero rejection, retry count versus initial attempt,
subscription-success reset and permanent-error behavior, stalled establishment,
result preservation, configured callback cancellation after FeedHandler
registration, and per-channel option preservation. Existing session doubles
verify that partial subscribe queues do not reset the budget and completed writes
do. Startup regressions cover cancellation and false watch notifications; duplex
sessions cover custom idle expiry and disabled idle with continued heartbeat
and shutdown. No external endpoint, fixture or parser was changed; these are SDK policy
tests, not new live protocol evidence.

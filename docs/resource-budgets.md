# Shared stricter resource budgets

`ResourcePolicy` and `ResourceBudget` let applications share a stricter local
HTTP/WS admission group across transport configurations, feeds, handlers and public
REST clients. Existing process-wide safeguards and native connection/topic/frame/
heartbeat/subscription rules remain active; a scoped policy cannot weaken them.

```rust,no_run
use cryptofeed_rs::prelude::*;
use std::time::Duration;
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let policy = ResourcePolicy::default()
    .http(1, Duration::from_secs(2))?
    .websocket(10, Duration::from_secs(2))?
    .binance_weight_ceiling(100)?;
let shared = ResourceBudget::new(policy);
let transport = TransportConfig::direct().with_resource_budget(shared.clone());
let feed = Okx::new().trade().symbol("BTC-USDT").transport(transport.clone()).build();
let mut handler = FeedHandler::new();
handler.add_feed(feed);
// Pass the same transport (or another transport with shared.clone()) to REST.
# Ok(()) }
```

Cloning a ResourceBudget shares gates and clocks. Constructing another budget,
even with the same policy, creates an independent group; both groups are still
bounded by the original process-wide admission. Policies are immutable. Replacing
a transport's budget does not mutate other clones. Proxy Basic authentication
rebuilding retains the budget. Budget identity does not change catalog cache
identity; cache hits/coalesced directory results create no extra network request.

| Policy | Accepted capacity | Accepted start interval | Scope |
| --- | --- | --- | --- |
| HTTP | 1..=4 | 1 second..=1 day | All actual HTTP operations in this budget group, across exchanges |
| Binance weight ceiling | 1..=6000 | Per native bucket window | Threshold against total process bucket usage, including other groups |
| WebSocket | 1..=100 | 1.1 seconds..=1 day | Live/queued establishment slots separately for each exchange in this group |

Defaults equal the existing SDK safeguards. The one-day maximum is an SDK clock/
configuration bound, not a venue quota. See [connection planning](connection-planning.md)
for the process limits and their official-vs-SDK provenance.

## Admission and cancellation

Each operation must satisfy both the process and scoped start clocks. Clocks are
checked/advanced together immediately before admitting the network operation;
delayed requests cannot accumulate pre-approved local starts then burst after a
global cooldown. This bounds SDK admission times, not server arrival times under
variable DNS/TLS/network latency. HTTP Retry-After remains a shared exchange
cooldown and is not bypassed by a separate group.

HTTP slots cover network requests and acquisition of global capacity. Pacing and
cooldown waits release both layers of network slots. Requests can await capacity;
this adds no worker queue and does not bound how many futures the application
creates. WebSocket slots cover queued establishment and the established session,
with explicit exhaustion errors instead of indefinite capacity queueing. Both
permits stay with Session and release on failure/cancellation/close/drop.

Admission wait is outside existing network/handshake timeout budgets. Caller
cancellation/shutdown drops pending admission and its permits. Read/write/frame,
callback deadlines and idle/heartbeat policy remain configured separately.

Known physical connection plans are checked against shared scope capacity after
symbol hydration and before WS startup. Managed add/replace reserves all current
handler plans: same-scope replacement uses max(old,new); changes of exchange or
scope retain both reservations until old tasks drain. A rejected candidate does
not stop the healthy current feed. Across independent handlers, actual shared
permits enforce the cap; a changing external scope can still make startup fail
after another handler's preflight. This is not a distributed lease/atomic handoff.

[Offline HTTP replay](http-replay.md) intercepts before either HTTP budget. Its WS
prohibition now fails before either WS reservation. Replay does not consume real
request quota, slots or start-clock progress.

## Verification and remaining policies

Paused-clock tests cover composite pacing after global delay, HTTP slot release
while waiting, active/pacing cancellation, scoped WS exhaustion and release.
Policy/clone/proxy tests cover bounds and shared identities. Planning and managed
actor tests cover excess adds, same-scope replacements and scope changes without
invalidating healthy feeds. The public bounded example also verifies real session
slot reuse and three HTTP queries:

```bash
cargo run -p cryptofeed-rs --example resource_budget_public
```

[Binance endpoint weights](http-weights.md) now supplement the process-local
safeguards. [Per-session WS quotas](ws-send-budgets.md) are implemented. Fairness guarantees and sustained throughput
certification remain separate work. Cross-process/machine shared-IP coordination
is explicitly deferred from the current delivery scope (2026-10-12); the earlier
standalone Rust coordinator choice is withdrawn. Multiple SDK processes can run
independently, but their limits do not aggregate across a shared egress IP.
[Minimum packing](minimum-connections.md) is now an opt-in exact policy under
fixed SDK route/instrument constraints, with a separate work budget.

[Native REST codes](native-rate-limits.md) now augment shared HTTP cooldown;
[Other venue public count limits](public-http-frequency-audit.md) are audited
against current HTTP pacing. No distributed quota service is planned
for the current stage.

[WS quota responses](ws-control-errors.md) now extend the process venue start
clock across groups. Scope pacing/capacity still applies; healthy connections
are not stopped and queued establishment reservations release on cancellation.

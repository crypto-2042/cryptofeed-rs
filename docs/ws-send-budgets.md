# Per-session WebSocket send budgets

Verified 2026-10-12. The runtime previously paced queued subscription messages
250 ms apart, but heartbeats and immediate Pongs were outside that accounting.
A per-session governor now computes admission from actual SDK send starts.

| Surface | Official public rule | SDK policy |
| --- | --- | --- |
| Binance Spot | Five incoming Ping/Pong/JSON control messages per second | Rolling five-frame window; texts leave one immediate Pong slot |
| Binance USD-M / COIN-M | Ten incoming messages per second | Conservatively use Spot's five-frame policy for both futures products |
| Bitget v3 | Ten messages/second; 240 subscription requests/hour/connection | Rolling ten-frame window with one Pong slot; rolling 240/hour for subscribe/unsubscribe/login JSON operations |
| OKX v5 | 480 subscribe/unsubscribe/login requests/hour/connection | Rolling 480/hour for those exact operations |
| Bybit v5 / Gate v4 | Existing verified public topic/connection contracts remain authoritative | SDK 250 ms text spacing; no invented native per-second or hourly quota |

Official sources: [Binance Spot](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#websocket-limits),
[USD-M](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect),
[COIN-M](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect),
[Bitget v3](https://www.bitget.com/docs/uta/quick-start),
[OKX connect](https://www.okx.com/docs-v5/en/#overview-websocket-connect).
Bitget's documentation counts string ping and JSON messages; counting transport
Pongs too and grouping its control operations under the subscription ceiling are
conservative SDK choices. This does not enable authenticated feeds or login.

## Admission and lifecycle

All text sends, including application heartbeat and direct low-level
`Session::send_text`, share at least 250 ms spacing. Subscription sends also
retain their existing 250 ms post-write spacing. Rolling windows have no saved
burst credit. Each admitted send counts before polling the sink; started work
is not refunded on cancellation or error. Waiting cancellation costs nothing.
The ten-second socket-write timeout starts after admission; admission itself is
cancellable by dropping a direct send future.

Queued subscriptions and heartbeat timers use governor deadlines in the normal
session select loop. They do not sleep through the receive/shutdown/idle/ack
branches. Hourly control exhaustion leaves application heartbeats eligible.
Acknowledgement timeouts still begin only when a subscription is actually sent;
Gate timestamps are still regenerated at that send. Verified remote frequency
errors retain the existing shared venue cooldown and retry behavior.

Pongs are immediate and count against the second window. Reserved headroom
covers an ordinary server Ping; extra Pings dynamically delay text. If a server
burst exhausts the whole frame budget, the session returns a retryable transport
error before writing another Pong. The runtime drops that session without
flushing tungstenite's queued automatic Pong. Low-level callers must likewise
drop a session after this error. The duplex regression checks that manual Pong
handling emits exactly one payload-preserving reply per admitted Ping, including
when tungstenite has queued its automatic reply. Pong writes now have the same
ten-second write timeout as text, rather than an unbounded sink wait.

State belongs to one physical connection and resets on reconnect. Frame history
is bounded by five/ten entries; hourly control history by 240/480 entries. No
process/distributed send ledger, wall-clock reset or server-capacity learning is
introduced. “Adaptive” here means deadlines change with actual admitted traffic,
including heartbeat/Pong contention; it does not mean raising native limits or
inferring new limits from unknown errors. SDK start windows cannot guarantee
server arrival timing under variable socket/network delays.

## Evidence

Eight new deterministic offline regressions cover rolling frame headroom and
expiry, bounded ping overload, exact operation classification, connection-local
hourly limits, heartbeat exclusions, no accumulated burst credit, uncharged wait
queries, payload-preserving Pong/market-read coexistence, hourly wait with
heartbeat/read/shutdown, direct-send cancellation, and automatic-Pong drop.
Tests use paused Tokio clocks and in-memory duplex WebSocket streams. The existing
idle-disabled heartbeat test now observes three synthetic heartbeats within its
one-second timeout, accounting for shared text spacing while still proving the
watchdog is disabled and shutdown is prompt.

`sample_data/public-ws.send-budgets.jsonl` is a synthetic asserted reference,
not a capture or a saturation test. Native parser/HTTP replay does not instantiate
this live session governor and does not simulate historical send scheduling.
No live rate limit was intentionally triggered. Cross-process/machine quota
coordination remains explicitly deferred; private channels remain out of scope.

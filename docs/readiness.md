# Readiness and retained feed state

Implemented for managed feeds on 2026-10-10. `control.state(feed_id)` returns a
`FeedSnapshot` for the current committed configuration generation. `feeds()`
keeps its existing small, Copy registry-entry API; the richer state query is
separate and remains usable when bounded lifecycle notifications lag.

## Meaning of the states

- Preparing: the candidate is validating/hydrating; an older generation may
  still be running. Candidate statuses carry their own generation identity.
- Started: validation/admission succeeded and the session tasks are launched.
- Connecting: physical connections or their initial confirmations are pending.
- Subscribed: all planned physical connections are connected and their native
  subscriptions have matching successful confirmations; local books may still
  be initializing or resynchronizing.
- Ready: all planned connections/subscriptions are confirmed and every requested
  L2 book is initialized under the exchange's existing sync rules.
- Reconnecting: a connection is down or retrying; previous confirmations and
  that connection's synchronized-book markers are withdrawn.
- Degraded/Failed: a concrete group or logical worker terminated. Healthy
  groups/feeds can continue, but their activity cannot overwrite this failure
  with Ready. Explicit replacement creates a fresh generation.
- Stopping/Stopped/Cancelled: progress updates cannot restore Ready after a stop
  begins. Publication is serialized with state mutation so concurrent progress
  does not emit a stale Ready after Stopping.

Data observations are separate from readiness. Sparse channels can be Ready
without emitting market events; conversely a connection producing data cannot
make an unconfirmed subscription or an unsynchronized L2 book Ready. Ready is a
point-in-time SDK state, not a guarantee of data freshness, zero loss, market
existence for caller-asserted mappings, or complete market-depth coverage beyond
the configured channel/depth. Consumer book recovery is separate alignment work.

## Evidence and correlation

Managed sessions use these current official confirmation contracts:

| Exchange | Confirmation binding |
| --- | --- |
| Binance | Connect to the planned combined endpoint without URL topics, then send an explicit SUBSCRIBE and match id 1 with result null. Classic un-managed URL subscription remains unchanged. |
| Bybit | Each queued subscribe request gets its own req_id; only the matching successful response confirms that request's topics. Spot batching is retained. |
| Gate.io | Each queued request gets an integer id; match id and channel. Explicit null error or success result confirms it, while a non-null error rejects it. Echoed payload is checked when supplied. |
| Bitget v3 | Match the requested instType/topic/symbol; any interval echoed by the server must match. Extra unrelated acknowledgement metadata is ignored. |
| OKX v5 | Match the requested channel and instId/instType parameters; extra response metadata does not change requested identity. |

Sources rechecked on 2026-10-10:

- [Binance Spot official specification](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md).
- [USD-M explicit subscription](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams).
- [COIN-M explicit subscription](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams).
- [Bybit public requests/responses](https://bybit-exchange.github.io/docs/v5/ws/connect).
- [Gate v4 request ID and response contract](https://www.gate.com/docs/developers/apiv4/ws/).
- [Bitget v3 response envelope](https://www.bitget.com/docs/uta/websocket/public/Tickers-Channel).
- [OKX subscribe response](https://app.okx.com/docs-v5/en/#overview-websocket-subscribe).

Unknown replies, duplicates and replies for unsent requests do not advance
confirmation. Each connection has a stable ID within its configuration generation
and an epoch incremented for each connection attempt. Old-epoch confirmations,
book snapshots and drops cannot update a newer attempt. A sent request without
confirmation reaches a 30-second timeout and withdraws the connection state;
queue waiting time is not counted as response time. Rejected subscriptions
follow the SDK's terminal Subscription error policy.

## Snapshot fields and lifecycle

`FeedSnapshot` includes lifecycle, expected physical connection count, per-
connection epochs/state/errors, expected/synchronized L2 counts, observed
normalized channel/symbol-pair count and event count. Subscription counts refer
to native topics, not normalized categories: a Binance/Bybit batch confirmation
covers all topics in the corresponding request. Aliased categories sharing a
native topic still count once. Counts populate when the subscribe batches are
prepared; a connection failing before that can show 0/0 while still not Ready.

`ConnectionInfo.last_received_at` is the local receipt time of any frame,
including control/heartbeat frames. `FeedSnapshot.last_event_at` is local
normalized-event publication observation time. These are runtime diagnostics;
model exchange_ts and received_ts semantics are unchanged. Observed event/pair
counts accumulate within a configuration generation across reconnects.

On reconnect the SDK clears only the books belonging to the affected connection,
then uses its normal snapshot/sequence recovery. Other connections' books are
preserved. In-session Binance/Gate resync also revokes book readiness until a
new synchronized snapshot is accepted. No bridge/gap rule is relaxed.

Initial configured feeds that fail validation remain queryable and replaceable
under their allocated IDs in managed mode. A rejected dynamic add is not a
committed feed and is removed. The current-generation snapshot is retained
independently of lifecycle broadcast capacity; unknown/removed IDs return an
explicit query error. Failed candidate replacement does not overwrite the old
generation's snapshot. Current errors/history still follow the control guide.

## Verification

`cargo run -p cryptofeed-rs --example readiness_public` observes public spot
Trade and L2 for all five active exchanges, waits for Ready plus both normalized
categories, prints state evidence and always requests cleanup. This manual
example does not replace deterministic gates or certify all products/channels.

Offline tests cover all-connection aggregation, request correlation/weighting,
unknown/duplicate/unsent acknowledgements, rejection and timeout, L2
initialization/resync, reconnect/stale epochs, concurrent stop publication,
connection-local cache reset, status lag recovery, initial-failure repair and a
full Bybit WebSocket double that confirms before synchronizing its book.
See the [dated manual observations](reports/live-smoke-readiness-2026-10-10.md).

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

## Gateio per-symbol recovery diagnostics (2026-10-15)

`FeedSnapshot.book_sync` adds epoch-qualified recovery evidence for Gateio L2:
first continuous unsynchronized time, scheduled snapshot counts, REST IDs/clocks,
last U/u and actual gap context, plus static failure codes. Reconnects preserve
continuous unsynced time and generation totals; stale epoch updates are ignored.
The latest error is historical within that epoch and may remain after recovery.
Other exchanges currently return an empty vector. This evidence does not alter
Ready semantics. See [recovery rules and budgets](gateio-book-recovery.md).

## Safe failure classification (2026-10-16)

`ConnectionInfo.last_failure` is an optional `FailureDiagnostic` containing
static `category`/`reason`, `permanent`, `http_status` and `close_code`.
`FailureDiagnostic::from(&Error)` never copies arbitrary error text. Permanent
means retry policy, not that a transient retry budget has been exhausted.
A failure retains its classification until a successful connect clears it;
stale-epoch results cannot replace current state. Existing `last_error` remains
free text for compatibility and should not be published as sanitized evidence.

SDK connection WARN events add `stage`, `feed_id`, `generation`, `connection`,
`epoch` and the classification. Subscription rejection codes are numeric-only.
Bitget processing errors add configured product and a whitelisted native topic;
join identity to the configured feed for the full channel/symbol set. Terminal
ERROR events identify direct permanent failure or task panic/cancel. Exhaustion
is logged at the configured supervisor retry-budget boundary; other terminal
errors use `feed_failed` without inferring retry exhaustion from their category.
An aggregate run error can wrap the underlying cause as Transport, so retain the
preceding diagnostics. The SDK does not install a tracing subscriber itself.
See [issue audit](reports/issue-audit-2026-10-16.md) for evidence limits.

## Connection-anchored market events

Issue #10 identifies a real consumer-contract gap: `FeedEnvelope.identity`
identifies a configuration, and remains equal across reconnects. Independently
buffered lifecycle and market streams have no shared ordering barrier. Checking
`received_ts`, prioritizing a select branch, or observing Ready cannot label
already queued identified events with a connection attempt.

`FeedHandler::subscribe_anchored()` returns a bounded broadcast receiver of
`AnchoredEnvelope { anchor: EventAnchor { identity, connection, epoch }, event }`.
`subscribe_anchored_with_diagnostics()` returns the same stream with the existing
receiver-local accounting. Both explicitly enable **managed runtime**, including
independent initial-feed startup/failure isolation, just like `control_handle()`.
Call before `run`; retain a control handle for authoritative queries. Existing
static subscriptions, `FeedEnvelope`, callbacks and L2 recovery remain unchanged.
There is no static/untracked fallback with an invented epoch.

- `identity.generation` changes on configuration replacement. Connection IDs are
  scoped to that identity, distinguish every physical route/shard, and match
  `FeedSnapshot.connections[].id`. A reconnect increments that connection's epoch;
  failed attempts can therefore leave gaps between observed event epochs.
- The session captures its epoch at attempt start. Publication reads the
  producing session's captured identity/connection/epoch, never the monitor's
  latest epoch. Buffered events and any delayed retired producer retain their old
  anchor even if a newer connection or configuration is already Ready. This
  contract permits old-anchor publication; it does not require draining every
  old event before replacement/removal acknowledges retirement.
- A consumer serializes reset and event admission. Query `control.state(id)`;
  on a new Ready configuration/connection set, clear old windows **before**
  installing the accepted anchors. Admit only exact identity/connection/epoch
  matches from that set. Unknown or mismatched anchors must not establish a new
  window; query authoritative state again if appropriate. Reject input and reset
  according to application policy while not Ready, stopping or removed. Do not
  promote an anchor merely because it arrives later or has a larger epoch.
- Ready remains a point-in-time observation. This API proves event provenance,
  not atomic revocation at the consumer when a remote connection fails. Status
  and event channels still have no cross-channel ordering promise. Lost lifecycle
  hints can be recovered with `state()`; no timer/queue-drain heuristic is needed
  to identify an old buffered epoch. The application owns when to query again.
- Each opt-in ring has the configured shared event retention (default 1024,
  Tokio power-of-two rounding), no forwarding task or unbounded queue. Successful
  reads increment `received`, including events deliberately rejected by business
  admission. Track those application discards separately. Only `Lagged(n)` adds
  to `lagged`; it does not identify an exchange, category or exchange-side loss.
  After publishers stop, drain to Closed for final accounting.

With the `recording` feature, [normalized v2 recording](recording.md#connection-anchors-version-2)
preserves original anchors and ordered replay. Labels are local to a capture;
independent recordings do not establish globally unique identities. A normalized
v2 tape does not record Ready/reset decisions, repair missing events, reproduce
lifecycle timing, or replace `L2BookHandle::recover()` and its revision anchor.
The existing raw replay interfaces are unchanged and do not gain this new
normalized envelope type implicitly.

Deterministic tests cover prefilled old buffers, a late old producer interleaved
with new Ready events, dropped lifecycle notifications, multiple connections,
replace/remove, bounded ring loss, deliberate discard/final drain and anchored
record/replay. These tests do not attribute or close historical #3/#4/#6 failures
or substitute for live Linux qualification.

# Connection planning and resource budgets

Implemented 2026-10-10. These policies cover the current FeedHandler path and
`ExchangeFeed::connection_feeds`. They are not a guarantee about other clients
sharing an IP, remote service availability, or optimal socket packing.

## Planning order

1. Validate all logical feed configurations and their capability/feature cells.
2. Hydrate each logical normalized-symbol union once, checking explicit native
   mapping ambiguity across all channels.
3. Retain the first-seen union and exact channel/symbol membership. All five
   adapters omit unrequested pairs before serializing native subscriptions.
4. Split this union into the largest contiguous symbol slices that
   fit native subscription budgets. Topic aliases are deduplicated by the
   existing exchange adapters before measuring each physical endpoint.
5. Reject a single instrument that exceeds a budget and reject more than 100
   planned physical connections to one exchange within a handler before WS
   startup. This planning happens after hydration because native names matter.

| Exchange | Planning budget | Basis |
| --- | --- | --- |
| Binance | 1024 generated streams per physical connection | Official Spot, USD-M and COIN-M connection limits. Product/public/market routes are measured separately. |
| Bitget v3 | Fewer than 50 deduplicated native subscriptions | Official stability recommendation. The official hard limit is 1000 subscriptions; using 49 is an SDK conservative choice. |
| Bybit v5 | Full serialized subscribe message at most 21,000 UTF-8 bytes per physical endpoint | Official limit is 21,000 characters for the args array. Counting the full message in bytes is stricter. Spot requests are separately batched into at most 10 args each; derivatives have no copied spot count limit. |
| OKX v5 | Full serialized subscribe message at most 64 KiB for each public/business endpoint | Official total subscription length limit is 64 KB; counting the full message is conservative. |
| Gate.io v4 | At most 1024 subscription messages per physical connection, each at most 64 KiB | SDK resource ceilings, not undocumented Gate service limits. |

Sources, rechecked 2026-10-10:

- [Binance official Spot specification](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md).
- [Binance USD-M connection guide](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect).
- [Binance COIN-M connection guide](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect).
- [Bitget current v3 quick start](https://www.bitget.com/docs/uta/quick-start).
- [Bybit v5 connection and args limits](https://bybit-exchange.github.io/docs/v5/ws/connect).
- [OKX v5 connection and subscription limits](https://app.okx.com/docs-v5/en/#overview-websocket).
- [Gate v4 spot protocol](https://www.gate.com/docs/developers/apiv4/ws/) and
  [futures protocol](https://www.gate.com/docs/developers/futures/ws/).

## Sending, connection attempts, and cancellation

Subscription messages are queued with at least 250 ms between sends on one
session. While the queue is waiting, normal socket reads, application heartbeats,
and shutdown continue. Gate subscription `time` is regenerated at the actual
send, so a long queue does not reuse the connection-start timestamp. Reconnects
rebuild the queue from current concrete configuration.

Each exchange also has a process-local connection budget: at most 100 active
or queued handshake slots, with at least 1.1 seconds between handshake starts.
The slot stays with the live Session and is released after failure, cancellation,
or session drop. Exhaustion across concurrent handlers returns an explicit
configuration error rather than silently leaving subscriptions waiting forever.
The 100-connection cap is the documented Bitget IP ceiling and an SDK ceiling
for the other exchanges. The pacing is an SDK conservative policy relative to
Binance/Bitget's documented 300 attempts/5 minutes, Bybit's 500/5 minutes and
OKX's 3/second. It is not a distributed/shared-IP quota service. No claim about
an official Gate request rate is inferred from another exchange's constants.

## REST book snapshots

Directory, public REST and Binance/Gate bootstrap callers share one process-local
HTTP admission budget; HTTP clients
are reused within the selected [transport configuration](transport.md):

- At most four active network requests at once.
- At least one second between request starts, including directory/REST/bootstrap.
- Gate initial REST requests begin after buffering the first depth delta for that
  symbol; a long outbound subscribe queue cannot fetch before the subscription
  starts producing data. Full book pushes establish state directly without REST.
  Unrequested depth data does not create a snapshot request or pending buffer.
- Existing 15-second HTTP timeout and bounded response body apply after admission.
- Dropping a snapshot receiver cancels queued or in-flight work and releases
  capacity. Shutdown does not need to drain an entire pending snapshot queue.
- Sequence bridging, pending-delta bounds, and bounded resnapshot rules remain
  intact. A delayed snapshot never permits skipping sequence validation.

These bounds are not exchange-weight accounting. Other REST calls/processes can
still consume the shared IP quota. Catalog discovery retains its transport-scoped
client/cache and coalescing, but now shares HTTP admission. Per-exchange HTTP
Retry-After cooldowns recheck queued work without occupying active slots; see
[public REST](public-rest.md).

## Verification and remaining work

Offline tests cover exact/overflow limits, deduplicated topics, aligned native
names after slicing, per-request Bybit batches, single-symbol overflow, planned
connection exhaustion, paced sends with market reads, cancellation, timestamp
refresh and snapshot admission. Existing full-session doubles remain enabled.
No new live load test or all-market throughput certification is claimed.

Unequal channel sets now share connections wherever endpoint and capacity allow.
Slicing restricts each channel rule to the shard and removes empty channels.
Native topic deduplication remains in place; venue-scoped topics can still carry
extra market data, which exact dispatch filters omit. L2 REST URL lists, initial
snapshot jobs, resync indexing, retained readiness and recovery ownership contain
only requested L2 symbols. Unrequested depth does not create sync/bootstrap work.

This is deterministic contiguous-union packing, not a globally minimum bin-packing
or cross-endpoint allocation solver. Endpoint/product boundaries are preserved;
configurable/weighted/distributed resource policies remain in the
[usage alignment plan](python-usage-alignment.md). Retained readiness and L2
recovery are implemented and documented in their dedicated guides. The
[Spot smoke](reports/live-smoke-packing-2026-10-10.md) observed one socket per
exchange for Trade BTC/ETH plus L2 BTC; it is not a throughput certification.

Managed updates now reserve per-handler capacity before committing candidates:
same-exchange replacement reserves the larger old/new plan; provider changes
reserve both until old tasks stop. Core runtime controls and configuration-
generation event identity are described in [the control guide](runtime-control.md).

## Physical connection state isolation — 2026-10-11

Physical routes retain a consistent projection of the logical feed: channels,
normalized symbols, native names and sparse subscription rules refer to the same
requested pairs. Bybit linear/inverse routes and OKX public/business routes now
prune inactive sparse rules as well as their channel/symbol unions. This fixes
invalid raw recording contexts for otherwise valid mixed-route sparse feeds.
The public logical configuration and first-seen native mapping order are retained.

A reconnect clears only the route's requested L2 symbols. Binance market-only
routes retain the existing no-snapshot guard; Gate settlement routes retain their
instrument scope. Bitget, Bybit and OKX resets explicitly filter exact L2 pairs.
Price-only symbols and other product/endpoint books are not reset.

Seven offline regressions cover route projections, aligned native names, raw
context roundtrips, Binance market/public and Gate settlement ownership, and a
Bybit inverse book accepting its next delta after a linear reset. This is focused
state isolation evidence, not arbitrary concurrent replay or new live validation.

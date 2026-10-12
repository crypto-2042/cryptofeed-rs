# Replay clock, cache and concurrency audit

Audited 2026-10-12 against the current SDK and executable offline tests. This is
verification of published replay contracts, not a claim to reproduce arbitrary
original scheduling, network latency or exchange delivery completeness.

## Clock domains

| Surface | Model time | Delivery/cache time | Evidence |
| --- | --- | --- | --- |
| Normalized replay | Original normalized exchange/receive fields | Immediate or one Tokio elapsed origin; callbacks do not reset it | recording::roundtrip_retains_order_source_precision_and_original_clocks; recorded_replay_keeps_absolute_schedule_and_callback_order |
| Native raw replay | Captured WS observed_ts and consumed HTTP received_ts; native exchange fields parsed normally | Immediate or one Tokio elapsed origin; v2 Processing/HTTP causality retained | raw::replay::five_native_trade_families_replay_with_original_clocks_and_lifecycle; runtime::http_replay_tests |
| Independent raw merge | Original model clocks and local identity, capture-scoped | Caller offsets plus captured elapsed time; stable input-order ties | raw::merge::independent_merge_* timing/HTTP/L2 tests |
| HTTP payload injection | Native exchange time where supplied; REST receipt time is current SDK SystemTime | Immediate calls; original HTTP delays are not simulated | replay_audit_http_receipt_clock_is_execution_time_and_virtual_time_does_not_expire_catalog |
| Catalog retention | Response bytes/context retained; caller snapshots independent | Actual SystemTime TTL/rollback policy; Tokio replay-time advancement does not age it | markets::cache tests and new HTTP clock audit |
| Catalog in-flight sharing | Completed response/error for eligible waiting callers | Process operation epochs, independent of clock resolution | markets::replay_audit_cohorts |

A missing native time remains None on RestSnapshot.exchange_ts. The compatibility
model exchange_ts then uses the same current receipt time, not a captured request
observation or a guessed venue timestamp. Current [Binance Spot bookTicker](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#symbol-order-book-ticker)
provides the no-native-time fixture; its public response shape was rechecked for
this audit. Existing OKX native millisecond-time assertions also now verify that
HTTP payload replay preserves native time separately from current receipt time.
No parser shape or time-unit conversion changed.

## Directory request cohorts

Sharing formerly compared std::time::Instant reads with >=. Clock reads can be
equal within one tick, which does not express the order of a completed operation
and a later refresh. Starts/completions now receive distinct process operation
epochs. A retained completed gate can serve callers that began before its
completion marker; a later refresh makes its own fetch even if the gate remains
retained by another waiter. This changes neither cache TTL nor recorded timing.

The completion marker remains after envelope validation/cache publication and
before cloning the completed result for waiters. Configuration/captured-cancellation
errors are not stored as shareable completed work. Cancelling a leader releases
its gate; a remaining caller can perform its own request. Cancelling a waiter
does not remove a result needed by another caller. Existing weak gate retention
and cache limits remain intact.

## Concurrency, cache and failure evidence

| Contract | Current evidence |
| --- | --- |
| HTTP replay clones share exactly-once consumption | New four-worker test: reversed captured completions, one initial call plus 31 barrier-synchronized SDK calls, all 32 payloads consumed once |
| Per-key invocation order; other keys independent | repeated_url_consumes_invocation_order_not_completion_order_and_clones_share_cursor; pending_head_never_borrows_later_success_and_unrelated_keys_remain_usable |
| Replay bypasses network/real admission | New concurrent test preserves a one-slot/day scoped clock and capacity; existing entrypoint/client/WS/proxy-rebuild guards and native cooldown/weight replay tests |
| Cache hits consume no archive entries | Existing catalog/REST tests plus new virtual-two-day/SystemTime audit |
| Eviction/expiry cannot silently advance an archive | cache_eviction_cannot_silently_consume_a_later_catalog_refresh; cache TTL/rollback/LRU tests |
| Terminal cancellation does not borrow the next recorded request | cancelled_catalog_call_cannot_automatically_borrow_the_next_explicit_calls_response; cancelled_replay_work_is_not_shared_as_a_completed_request_with_waiters |
| Partial reads/writes, dropped replay and callback failures require restart | Normalized partial-I/O/drop tests; raw callback/cancellation tests; series dropped_or_failed_replay_cannot_restart_and_never_certifies |
| Native states/identities do not cross sessions/captures | Raw reconnect/ticker/book tests; merge identical-session/HTTP/L2 isolation tests |
| EOF certificate and captured prefix are distinct | Codec/series footer/manifest tests; merge Complete/Stopped/LimitReached summaries; HTTP whole-ledger audit |

Concurrent callers are assigned payloads at the shared consumption cursor; thread
names or original wall-clock races do not define that assignment. verify_complete
is a linearized audit point, not proof that every caller parsed its result or
finished callbacks successfully. Late corruption can follow earlier streaming
callbacks; effects are not rolled back.

## Reproduce and scope

```bash
cargo test -p cryptofeed-rs --all-features --lib replay_audit
cargo test -p cryptofeed-rs --all-features --lib recording::
cargo test -p cryptofeed-rs --all-features --lib markets::cache::
```

Five new tests cover cohort order/cancellation, shared cursor concurrency and
HTTP receipt/cache clocks; the full existing replay/cache suite remains enabled.
The synthetic reference is `sample_data/replay-clock-contracts.json`. No exchange
connection, live load or rate limit is needed for these new assertions.

The published clock/cache/concurrency contracts above have executable evidence.
HTTP replay still does not simulate original wire waits, old cache expiry,
admission/cooldown timers or arbitrary original cancellation/coalescing races.
These previously documented unsupported simulations are not silently inferred
from elapsed observations. New protocol/runtime behavior must retain the audit
and add targeted regression evidence when these contracts change.

# Python alignment requirement audit — 2026-10-12

**Result: incomplete.** This audit checks the existing [alignment plan](python-usage-alignment.md)
against implementation and executable evidence. It does not redefine completion
around already implemented features. The inspected Python sibling is commit
`3a6d3ca`; the last fully tested Rust implementation is `e65cc09` (817 all-feature
workspace tests). Exchange count, instrument types and private channels retain
their previously agreed exclusions. Cross-process/machine quota coordination
was explicitly deferred by the maintainer on 2026-10-12.

## Requirement-to-evidence map

Rust source paths below are relative to `crates/runtime/src/` unless a crate or
workspace prefix is shown; test paths are relative to `crates/runtime/`. Test names identify concrete assertions,
not a claim that the test suite proves every possible behavior. “Implemented”
means the listed contract has inspected source and focused executable evidence;
protocol and delivery limitations remain in the linked public guides.

| Requirement | Inspected implementation / representative evidence | Assessment |
| --- | --- | --- |
| Catalog symbols, metadata and exact native lookup | `catalog.rs`, `market_info.rs`; `selection_sorts_deduplicates_and_normalizes_patterns`, `native_lookup_uses_the_snapshot_mapping_and_periodic_union_allows_removals`, venue metadata tests | Implemented |
| Anchored glob selection, deterministic union and explicit misses | `catalog.rs`; `missing_pattern_never_silently_yields_a_partial_subscription`, `patterns_are_anchored_and_backtrack_over_stars` | Implemented Rust convenience; Python feed uses exact symbol mapping |
| Bulk typed/shared configuration | `exchange/mod.rs`, `tests/catalog.rs`; `bulk_symbols_append_to_existing_symbols`, `bulk_instruments_preserve_product_identity` | Implemented |
| Async embedding, caller shutdown and pre-cancelled hydration | `feed.rs`, `runtime.rs`, `tests/catalog.rs`; `pre_signalled_shutdown_skips_invalid_feed_hydration`, `shutdown_preempts_symbol_hydration` | Implemented |
| Catalog refresh, failed refresh preservation, coalescing and cancellation | `markets.rs`; `catalog_refresh_bypasses_cache_and_failed_refresh_preserves_it`, `overlapping_catalog_requests_share_success_or_failure`, `cancelling_catalog_leader_does_not_block_later_requests` | Implemented; per-response cache, not an atomic paginated catalog transaction |
| Bounded cache retention and replay/cache clocks | `markets/cache/tests.rs`, `markets.rs::replay_audit_cohorts`; LRU/expiry/accounting and distinct operation epoch tests | Implemented within [published replay contracts](replay-contract-audit.md) |
| Per-channel exact pairs, exclusive modes, product and feature preflight | `exchange/mod.rs`, `runtime.rs`, `tests/feature_boundaries.rs`; `channel_subscriptions_hydrate_and_filter_handlers_events_and_books`, `channel_subscription_preflight_honors_each_feature_boundary` | Implemented |
| Native topics, batch/byte/connection budgets, route-first and minimum packing | `runtime/planning.rs`, `runtime/planning/`; `binance_exact_stream_limit_and_overflow`, `bybit_character_budget_preserves_native_pairs`, `oversized_single_symbol_is_rejected_without_connecting`, minimum-packing oracle/cancellation tests | Implemented under fixed SDK routes and atomic instruments; alternate API/global feed pooling not claimed |
| HTTP/WS admission, scoped policies, weights, native errors and live feedback | `resources.rs`, `runtime/{budget,snapshot,weights,quota}.rs`; weighted/scoped/HTTP frequency/used-weight tests | Implemented process-local; [frequency audit](public-http-frequency-audit.md), [weight boundaries](http-weights.md) |
| Shared session text/heartbeat/Pong and hourly control windows | `runtime/send_budget.rs`, `runtime/connection.rs`; eight focused send-window/duplex regressions | Implemented; [SDK admission and native limits](ws-send-budgets.md), no remote-capacity learning |
| Managed add/replace/remove/shutdown, IDs and cancellation | `runtime/control.rs`, `tests/runtime_control.rs`; `add_replace_remove_preserves_identity_and_rejects_invalid_candidate`, `cancelling_preparation_keeps_old_generation_running`, `remove_acknowledgement_follows_registry_removal` | Implemented with documented stop/start gap |
| Independent startup and terminal failure isolation | `runtime/control.rs`; `independent_initial_startup_keeps_healthy_feeds_running`, `initial_validation_failure_remains_queryable_and_replaceable` | Implemented opt-in; strict legacy startup retained |
| Retained readiness, matching confirmations, reconnect epochs and books | `runtime/readiness.rs`; `acknowledgements_and_books_are_distinct_readiness_steps`, `every_connection_must_confirm_and_data_alone_is_not_readiness`, stale-attempt/stop tests | Implemented; task launch is separate from remote readiness |
| Periodic symbol reconciliation and manual ownership | `discovery.rs`; additions/removals/native-change, failed-refresh/backoff, metadata-only and stop-cancellation tests | Implemented opt-in; extends inspected Python core |
| Multi-handler order, deadlines, panic isolation and shutdown | `runtime.rs`; `multiple_handlers_preserve_order_primary_replacement_and_single_publication`, `handler_panic_and_timeout_allow_later_handlers_and_future_events`, `shutdown_cancels_slow_handler_before_remaining_callbacks` | Implemented; handlers own business errors, broadcast remains lossy |
| Candle completion policy on every output surface | `runtime.rs`; `candle_completion_policy_filters_every_delivery_surface_without_guessing` | Implemented; All remains Rust default |
| Recoverable L2 snapshot/delta consumption and invalidation | `books.rs`; `lag_recovery_replaces_queue_and_resumes_after_full_snapshot`, `concurrent_recovery_has_no_snapshot_to_subscription_hole`; runtime resync/disconnect/stop hooks | Implemented; no lossless delivery guarantee |
| Optional Python crossed-book check | Python `feed.py::book_callback/check_bid_ask_overlapping`; Rust `dispatch_l2_book` and `L2BookState` have no corresponding configurable check | **Missing; must implement** |
| Python maximum delivered depth | Python `feed.py` describes `max_depth` as maximum levels returned per side; active adapters pass it to OrderBook. Rust `l2_book_depth` selects native subscription depth, not a common output projection | **Missing; must implement separately from native depth** |
| Existing native sequence/checksum integrity | Five exchange `book_sync.rs` modules; bridge/gap/reset and available checksum tests | Implemented; disabling integrity checks to imitate Python defaults is not required |
| Retry/deadline/idle/start-delay and explicit HTTP/WS proxy | `options.rs`, `runtime/supervisor.rs`, `transport.rs`; finite retry, sparse preservation, idle heartbeat/shutdown and CONNECT boundary tests | Implemented typed async configuration; no Python syntax/config-file compatibility claim |
| Public REST ticker/books, recent trades, funding/candle/trade history | `rest/{adapter,history,candles,trades,trade_history}.rs`; page/cursor scope, same-time IDs, source-limit and official route tests | Implemented within documented native capabilities; Bybit/Bitget recent-only and Gate delivery range-only boundaries remain explicit |
| Normalized/raw recording, segments, native/HTTP/L2 replay and independent merge | `recording/`, `runtime/raw_replay/`, `runtime/http_replay_tests.rs`; consumed-bootstrap/processing barrier, exact HTTP cursor, isolated merge and replay audit tests | Implemented bounded workflows; historical network/cache/admission scheduling explicitly unsupported |
| Sink boundary and bounded JSONL storage | `sink/tests.rs`; ordered owned envelopes, lag terminality, failed/panicking/deadline writes and finalization tests | Implemented core boundary; full Python backend adapter collection is optional phase 5, not promised for 0.1 |
| OHLCV, throttle, Python-rule Renko and custom aggregation | `aggregate/`; clock/source scope, timer/partial-tail, Python reversal thresholds and poisoned reducer tests | Implemented bounded local helpers; exchange-native units preserved |
| NBBO | `nbbo/tests.rs`; Python best-price/size rule, ties, freshness, suspended/stale data and lifecycle tests | Implemented with explicit unit and recovery responsibilities |
| Documentation hygiene, coherent progress and GitHub delivery | README, plan, progress, memory, public index, feature report; CI workflow and actual remote run inspection | Audit continues; stale phase status corrected and invalid workflow YAML repaired; remote rerun still needs verification |

## Remaining implementation sequence

1. Add an opt-in crossed-L2-book policy, default disabled as in Python. Check
   reconstructed full sides after native sync and exact subscription filtering,
   before callback/counter/broadcast/recovery publication. Match Python's
   `best_bid >= best_ask` rule; either empty side does not overlap. Reject the
   affected event explicitly, withdraw usable recovery/readiness and propagate
   a recoverable session failure. Verify snapshots, deltas, deletion/empty sides,
   default compatibility, precision, healthy-feed isolation and fresh reconnects.
2. Add a separate opt-in maximum **delivered** L2 depth. Keep native subscription
   selection and full internal sequence/checksum state intact. Derive correct
   top-N snapshot/delta membership, including removals and promotion of levels
   after a better level disappears; do not merely truncate delta arrays. Define
   one coherent contract for handlers, ordinary/identified broadcast and retained
   recovery. Verify replacements/reconnects, sparse subscriptions, scale-preserving
   Decimals, empty sides and promotion across the cutoff.
3. Re-run the full requirement audit after both changes. Compile isolated
   features, run focused/default/all-feature gates, fresh-resolution MSRV and
   inspect actual remote CI. Keep the goal active if a named requirement lacks
   implementation or evidence. The two gaps are not silently deferred.

## Scope distinctions retained from the plan

- In-place native subscribe/unsubscribe is an optimization after managed
  replacement, not a requirement for hot updates; replacement already has a
  user-visible acknowledgement and generation contract.
- Full database/message-bus backend catalog, Python configuration syntax, sync
  wrapper signatures and remote durability are not claimed by the typed async
  0.1 SDK. The original plan labels ecosystem expansion optional and explicitly
  declines full backend parity; this audit does not newly exclude required work.
- Venue/product/private-channel expansion and Binance contract OI retain their
  existing exclusions/decision. Distributed quota coordination is separately
  deferred by explicit maintainer instruction, not by this audit.
- Source-limit pagination and unsupported historical replay timing remain
  capability boundaries rather than fabricated parity. They must stay visible
  in user documentation and tests.

## Engineering audit evidence

- All ten runtime features were compiled independently with
  `cargo check -p cryptofeed-rs --lib --no-default-features --features FEATURE`
  and tested through `--test feature_boundaries` on the same isolated feature.
  No-default `--no-run` compiled all applicable tests. Existing isolated-build
  unused-code warnings remain; no strict minimal-feature Clippy pass is claimed.
- The new CI entries execute `runtime::send_budget::` (4 tests) and
  `runtime::connection::tests` (20 tests) both without defaults and with only
  recording, plus the 2 minimal `used_weight_feedback_tests`: 50 test executions
  across these runs passed locally. Existing all-feature evidence remains
  817 tests, strict Clippy/rustdoc and fresh Rust 1.85 from the prior milestone.
- Remote [run 38210517035](https://github.com/crypto-2042/cryptofeed-rs/actions/runs/38210517035)
  failed with zero jobs/check runs and no downloadable log. Local parsing of
  that exact committed workflow reproduces invalid YAML at line 32: an unquoted
  run scalar contains a Rust path ending `::` followed by a space. The next line
  has the same issue. Quoting both complete commands repairs YAML parsing; all
  five jobs remain. This is a workflow configuration repair, not a passing
  remote run. The pushed repair must still be observed executing in GitHub.

# Issue #9 and #10 acceptance verification — 2026-11-03

Status: **audit of merged fixes; no behavior change.** The only code change on
this branch is one added regression for the parse-to-freshness path of #9. This
report records which requested acceptance cases are already proven at
`93eee634`, and which remain outside the repository. It does not close either
issue, does not requalify Linux live stages and does not overwrite historical
failures for #3/#4/#6.

## Checked revision and environment

- Implementation revision: `93eee6342e35b30cc73f8713b9f663d2c492b306`
  (`feat(runtime): anchor market events to connection epochs`, master). This
  includes the #9 fix `4fa7451928e3390228cc9fbe91c9261216781e7b`
  (`fix(parsers): keep settlement times out of exchange_ts`).
- Host: macOS 15.8 arm64; rustc/cargo 1.93.0. Gates were run on a clean worktree
  of that revision; the report and the added test are the only local changes.
- CI for `93eee634` on master completed successfully on 2026-10-29.

## Issue #9 — OKX funding times

The issue requested three regressions:

| Requested case | Coverage | Evidence |
| --- | --- | --- |
| `fundingTime` in the future, `ts` near now: `exchange_ts == ts`, `next_funding_time == fundingTime` | Implemented | `crates/runtime/tests/public_parity.rs::okx_funding_rate_matches_public_baseline` uses the captured 2026-10-21 BTC-USDT-SWAP frame and asserts `exchange_ts == 1792560006.264`, `next_funding_time == 1792569600.0`, `exchange_ts <= received_ts` and a settlement-to-event gap above 9 000 s. |
| `ts` missing: fall back to `received_ts`, no panic | Implemented | `okx_funding_rate_without_ts_falls_back_to_received_ts` removes `ts` from the same shape and asserts both clocks equal `received_ts`. |
| End-to-end parse to freshness: settlement time must not enter `exchange_ts` | Added on this branch | `okx_funding_rate_push_frame_passes_downstream_freshness_gates` applies the issue's downstream gates (`max_future_skew_ms = 1000`, `max_event_delay_ms = 3000`) to a periodic push frame and asserts the settlement time stays in `next_funding_time`. |

The fix also audited two latent settlement-as-event paths in the same class:
Binance index no longer falls back to mark-price `T`
(`binance_index_price_matches_public_baseline`), and Bybit funding prefers the
ticker envelope `ts` over `fundingRateTimestamp`
(`bybit_funding_ignores_settlement_timestamp_for_exchange_ts`). Documentation
and capture provenance were updated in `docs/exchange-protocol-baseline.md` and
`sample_data/SOURCES.md`; the previous fixture that asserted
`exchange_ts == fundingTime` was replaced, not relaxed.

Deliberately not done, and consistent with the issue's optional suggestion:

- `nextFundingTime` is discarded instead of being exposed as a new model field.
  Adding `subsequent_funding_time` would change the shared `Funding` model for
  all exchanges; that is a separate contract change.
- No new live capture was taken for this verification. The fixture is the
  issue's own public frame, so the evidence is the captured payload, not a new
  production observation.

Remaining external step: the downstream consumer must re-pin to
`4fa74519` or later. That migration is not verifiable from this repository.

## Issue #10 — connection-anchored ordinary events

The issue asked for either an opt-in anchored subscription or an equivalent
in-stream reconnect/withdrawal barrier, with distinct configuration generation
and connection epoch, provable reset/event boundaries and receiver-local
accounting. `93eee634` implements `FeedHandler::subscribe_anchored()` and
`subscribe_anchored_with_diagnostics()`, returning
`AnchoredEnvelope { anchor: EventAnchor { identity, connection, epoch }, event }`,
plus normalized v2 anchored recording/replay.

| Requested case | Coverage | Evidence |
| --- | --- | --- |
| Prefilled old buffer, epoch switch, new event: old anchors cannot establish the new window | Implemented | `crates/runtime/src/feed/subscription.rs::tests::anchored_buffer_and_late_producer_keep_original_epoch_after_ready_reset` queues an old-epoch event, resets to a new attempt, then asserts the queued event keeps epoch 1 while new events carry epoch 2. |
| Late old producer interleaved with new-connection events; identity/epoch not rewritten to current values | Implemented | Same test publishes from a retained old producer after the new attempt is Ready and asserts the observed anchor sequence `[1, 2, 1, 2]` with an unchanged identity. |
| Fast reconnect, slow consumer, lost lifecycle broadcast | Implemented | The same test drops 80 lifecycle notifications (asserting `Lagged`) yet resolves the accepted anchor from `state()`; `anchored_multi_connection_replacement_retirement_and_drain_accounting` fills the ring and observes `Lagged(3)`. |
| Same feed with multiple connections; configuration replace/remove; reconnect | Implemented | The multi-connection test asserts distinct connection IDs under one identity; `crates/runtime/src/runtime/control.rs::tests::anchored_managed_add_replace_remove_keep_buffered_generations_distinct` covers add/replace/remove with a buffered generation and final drain. |
| Bounded ring `Lagged`, deliberate discard, consistent final drain accounting | Implemented | Multi-connection test asserts `(received, lagged, pending) == (7, 3, 0)`, `received + lagged == published`, and `closed`; `recording.rs::anchored_tests::anchored_stream_rejects_ring_loss_and_certifies_closed_drain` covers ring loss versus a complete drain. |
| Anchored record/replay round-trip; no L2 regression | Implemented | `anchored_v2_reference_matches_inline_codec_assertion`, `anchored_roundtrip_and_repeated_replay_preserve_original_labels_and_clocks`, `anchored_codec_rejects_missing_mismatched_zero_and_v1_anchors`; existing L2 recovery tests are unchanged and still pass. |

Documented limits, restated because they bound what the acceptance cases prove:
there is no atomic revocation at the consumer when a remote connection fails,
status and event channels still have no cross-channel ordering promise, capture
labels are local to a recording, v2 tapes do not record Ready/reset decisions,
and the static/identified APIs and `L2BookHandle::recover()` are unchanged. No
downstream adoption of the anchored API is verified here, and the test suite
does not attribute the historical #3/#4/#6 failures.

## Gates run on this branch

| Check | Result |
| --- | --- |
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | PASS, no diagnostics |
| `cargo test --workspace --all-features` | PASS, 864 passed / 0 failed |
| `cargo test -p cryptofeed-rs --all-features --test public_parity okx_funding` | PASS, 3 passed (the added test included) |

The workspace run is the same all-features gate CI uses. A green local run on
macOS says nothing about the Linux live stages; it only shows the deterministic
suite is consistent with the merged implementation.

## Recommendation

Both issues' requested implementation is present at `93eee634` with the
deterministic coverage above, so they are candidates for closure on
implementation grounds once the human maintainer accepts this audit. Closure
still requires the maintainer's decision; this branch only records evidence.

- #9: close after confirming the downstream consumer re-pins to the fixed
  revision and stops rejecting OKX funding rows.
- #10: close after the downstream consumer adopts the anchored API; the
  contract tests already prove the SDK side.
- #3, #4 and #6 are unaffected: their remaining work is live Linux
  qualification and unique failure attribution, not local code.

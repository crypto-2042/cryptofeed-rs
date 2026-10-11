# Offline native parser/state replay

`RawRecordingReader::replay` now feeds sanitized recorded WS inputs through the
runtime's existing product-qualified native parsers and normalized dispatch.
It uses local per-session state, no catalog fetch, WS connection, retry supervisor,
live handler/feed counter or managed book-store publication. The live FeedHandler
entry point remains unchanged.

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let file = tokio::fs::File::open("saved-raw-ws.jsonl").await?;
let mut reader = RawRecordingReader::new(tokio::io::BufReader::new(file),
    RawRecordingLimits::default());
let (_stop, stop) = tokio::sync::watch::channel(false);
let summary = reader.replay(RawReplayOptions::default(), stop, |item| async move {
    match item {
        RawReplayItem::SessionStarted(source) => println!("start {}", source.session_id),
        RawReplayItem::Market(model) => {
            // Match model.event and call existing normalized handler/business logic.
            println!("input {} -> {:?}", model.observation_sequence, model.event.channel());
        }
        RawReplayItem::SessionEnded { session, clean } =>
            println!("end {} clean={}", session.session_id, clean),
        _ => {}
    }
    Ok(())
}).await?;
println!("{:?}", summary);
# Ok(()) }
```

Enable `recording` plus every recorded data category you want to execute. Raw
file inspection itself needs no model category, but parser replay rejects a
disabled/unsupported subscription at Connected, before any callback for that
session. Mappings come from the file, with no quote-currency guess or fresh
instrument catalog. Model/native symbol conflicts are errors.

## Current coverage

| Path | Replay behavior |
| --- | --- |
| Binance, Bitget v3, Bybit v5, OKX v5, Gate v4 non-L2 public categories | Current native parsers, product-qualified mapping and existing subscription/dispatch policy |
| Bybit derivative ticker | Per-connection snapshot/delta reconstruction; fresh connection cannot reuse old fields |
| Bybit / OKX / Bitget L2 | Existing WS snapshot/delta book sync, sequence/reset and checksum rules, with fresh local state |
| Binance / Gate L2 | Explicitly rejected at session start until captured HTTP bootstrap support exists; no online fallback |

No adapter/parser gap/bridge/checksum rule was relaxed for replay. The Bitget
first update may bridge a snapshot within [pseq,seq]; a genuine disjoint interval
fails. Nonzero OKX CRC mismatches fail using the original validator. Session close
removes local parser/book state; reconnect starts fresh even when logical feed
identity/generation is unchanged.

Sent messages remain validated file inputs but do not trigger sends. Application
pong/control replies are classified without market callbacks. Subscribe errors
remain errors; model-less control frames do not count as market data. This is
native **normalization/state replay**, not a simulation of subscription-readiness
correlation, admission, transport idle/heartbeat timers, binary frame activity or
network close handshakes. It interprets recorded bodies using current Rust
adapters; no Python/old-API fallback or compatibility guarantee for arbitrary
historical wire versions is introduced.

Recorded sparse subscriptions, candle intervals/closed-only policy and native
quantity units stay intact. Market callbacks receive session ID, optional recorded
feed identity, original observation sequence and the normalized FeedEvent.
Original exchange/receipt clocks are preserved. Source labels are file-local,
not control handles for a running runtime. No native event is expanded/reordered
just to make output counts match raw observation counts.

## Delivery and resource behavior

Callbacks receive SessionStarted, Market and SessionEnded in file order. A limited
or stopped prefix can end with open sessions: no peer-close event is invented.
Use the returned summary's ending reason to finish/invalidate consumer state at
segment end. These callbacks are not live retained-readiness or revision-anchored
L2 recovery handles.

Default timing is immediate. Configure
`RawReplayOptions::delivery(ReplayOptions::default().timing(ReplayTiming::Recorded))`
for absolute recorded scheduling, including lifecycle records. Slow callbacks
cause catch-up, not extra accumulated sleeps. Existing callback deadline/panic/
business-error behavior applies; all callbacks are sequential.

Default normalized batch cap is 1024 models (fits Bybit's documented public trade
batch maximum); total model budget is 1,000,000. Configure
`output_limits(batch, total)` with batch 1..65536 and positive total. A frame that
exceeds the output queue/batch or remaining total budget fails **before delivering
models from that frame**, never silently discarding a broadcast lag. Earlier
lifecycle/model callbacks can already have effects; replay is not transactional.
File byte/session/observation/privacy/lifecycle bounds still apply independently.

Replay requires a fresh reader. Shutdown returns None, validated EOF returns
Some(RawReplaySummary), errors propagate. Cancelled/dropped/failed replay cannot
resume after consuming an undelivered input; reopen from the beginning. Generic
callbacks may perform user-chosen actions, but the library replay path itself
opens no network connection or HTTP client request.

The larger objective still includes captured HTTP catalogs/bootstrap, Binance/Gate
L2 data replay, raw segment rotation/merge, sinks/aggregation, NBBO and resource
policy work. WS-native state replay does not certify those remaining paths.

Offline tests: `cargo test -p cryptofeed-rs --features recording --lib recording::raw::replay`.
Pure offline CLI: `cargo run -p cryptofeed-rs --features recording --example raw_parser_replay -- RAW_PATH`.
Public capture/live-vs-offline comparison:
`cargo run -p cryptofeed-rs --features recording --example raw_replay_public`.
See [dated evidence](reports/live-smoke-raw-replay-2026-10-11.md).

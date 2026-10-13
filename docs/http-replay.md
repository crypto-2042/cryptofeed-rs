# Offline HTTP payloads through SDK APIs

`HttpRecordingReader::into_replay()` validates a fresh complete ledger before
publishing `HttpReplay`. Both v1 and v2 ledgers are supported. Attach it using
`TransportConfig::direct().with_http_replay(replay.clone())`, then call the
existing MarketCatalog/PublicRestClient transport-aware APIs. HTTP
archive loading retains its record/byte/line limits. Version 2 preserves
permanent configuration failures when replay calls are explicitly recorded again.

```rust,no_run
use cryptofeed_rs::prelude::*;
use tokio::io::BufReader;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let file = tokio::fs::File::open("public-http.jsonl").await?;
let limits = RecordingLimits::new(100_000, 256 * 1024 * 1024, 64 * 1024 * 1024)?;
let replay = HttpRecordingReader::new(BufReader::new(file), limits).into_replay().await?;
let transport = TransportConfig::direct().with_http_replay(replay.clone());
let client = PublicRestClient::load_with_transport(
    ExchangeId::Okx, InstrumentKind::Spot, transport,
).await?;
let result = client.ticker(&Symbol::spot("BTC", "USDT")).await?;
# let _ = result;
// Call the other recorded APIs before auditing the whole ledger.
let progress = replay.report();
# let _ = progress;
# Ok(()) }
```

The example for the previously captured OKX directory/ticker/book/trades workflow
runs entirely from its saved file, with no network approval required:

```bash
cargo run -p cryptofeed-rs --features recording --example http_replay -- RECORDING_PATH
```

A small synthetic, substituted-value reference is available at
`sample_data/recording.public-http.v1.jsonl`. It matches the executable inline
SDK fixtures and can be passed to this CLI; it is not a real exchange capture.

## Matching and audit

Lookup keys are exact exchange plus complete URL, including query values/order.
Per-key responses follow **request invocation ID order**, not completion order.
Different URL keys can be consumed independently. Clones share one cursor; create
another replay from a fresh reader to restart. No ambiguous URL normalization,
product/currency conversion, partial-symbol matching or automatic live fallback
is performed.

- A recorded JSON result is returned through the same current SDK parsers and
  feature/capability checks as live input.
- Recorded safe failures restore their categories and HTTP status/Retry-After.
  Diagnostics that were redacted cannot be restored.
- Recorded cancellation is an explicit InvalidConfiguration error for that call.
  Directory fetch stops retrying it, so it cannot borrow the next explicit call's
  response. A later explicit API call can consume its own next response.
- Actual recorded transient transport/JSON failures retain normal directory retry
  behavior. Terminal configuration errors are neither retried nor shared as
  completed HTTP work with other directory waiters.
- Missing URL/exhaustion, pending captured starts and body-budget mismatches return
  errors, increment mismatches and consume nothing. A missing head outcome cannot
  be skipped to use a later same-URL success. Unrelated keys remain usable.

`report()` exposes source footer summary, requests/consumed, original unfinished/
cancelled counts and mismatches. `verify_complete()` succeeds only when every
archived start has been consumed and no mismatch was attempted. It audits a point
in time, not producer health, normalized query success or side effects. A known
cancel/failure can count as consumed even though the API returns an error. A
Stopped/LimitReached source remains explicitly a captured prefix. Intentionally
replaying a subset is possible, but does not pass whole-ledger verification.

## Network and cache scope

Replay intercepts the shared HTTP entry before request admission/client access.
The HTTP client getter and WebSocket entry also reject replay mode, including
when a proxy/authenticated configuration is cloned or rebuilt. Recorded 429
metadata does not consume live HTTP quota or impose a cooldown on live clients.
The guarantee covers requests through this replay transport; unrelated clients
and caller code are outside its scope. `FeedHandler::run()` is not a WS tape player;
use [native raw replay](raw-replay.md) for WS/session/book state.

Replay instances have isolated catalog cache keys, so older live data cannot
satisfy a replay load. Clones share the same instance/cache and successful cache
hits consume no additional HTTP records. Refresh still explicitly fetches the
next recorded request. [Directory retention](catalog-cache.md) is bounded. After a catalog has been
served in this replay namespace, a non-refresh miss caused by eviction/expiry
returns a mismatch error rather than consuming the next archived response. Reuse
the retained catalog or explicitly refresh. Proxy rebuilding retains replay mode. Setting replay
removes that clone's existing capture observer; if observation is explicitly
attached afterwards, it observes SDK replay calls, not original network activity.
Configure observation before queries because it also scopes catalog caching.

This is immediate payload injection, **not original scheduling**. It does not
simulate wire delays, original 24-hour cache expiry, arbitrary concurrent failure
coalescing, request admission/cooldown timers or caller cancellation timing.
Drive the recorded effective request sequence; use explicit refresh for later
catalog snapshots. An unsupported sequence is reported rather than guessed.

## Bounds and clocks

All records, privacy and footer are checked before any response can be served;
a partially iterated reader cannot become a replay. Loading owns the recorded
payloads and metadata, bounded by RecordingLimits; JSON heap overhead is additional
to encoded bytes. A canonical JSON byte count is computed without retaining an
extra encoded body. Lookup enforces the caller's current HTTP body budget against
that canonical representation. Original wire sizes/compression/headers are not
available, so this is not wire-byte replay.

Native timestamps remain in the JSON. Local normalized receive timestamps and
missing-native-time compatibility fallbacks are generated by SDK execution now,
not overwritten with the recording clock. Consequently this does not certify
all normalized clocks equal a previous live query. Use
[normalized recording/replay](recording.md) for original normalized models/clocks,
and raw v2 consumed snapshots for [causal WS L2 replay](http-l2-replay.md).

The synthetic SDK fixtures were checked against the current
[OKX v5 guide](https://app.okx.com/docs-v5/en/) for public instruments, ticker,
books and trades. This increment changes no exchange endpoint or wire parser.

[Clock/cache/concurrency audit](replay-contract-audit.md) now records executable
evidence for the published replay behavior and keeps unsupported historical
scheduling distinct from model timestamp preservation.

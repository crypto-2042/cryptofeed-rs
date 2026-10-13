# Public HTTP JSON-result recording

Optional `recording` adds `http_capture_channel`,
`TransportConfig::with_http_capture`, `record_http_stream` and the bounded
`HttpRecordingWriter`/`HttpRecordingReader` codec. The shared HTTP entrypoint now
observes SDK GET attempts used by directory discovery/refresh, public REST/history
and Binance/Gate book bootstrap/resnapshot. No adapter endpoint, normalization
rule, HTTP pacing/backoff or ordinary uncaptured query behavior is changed.

This records **SDK call/result boundaries**, not raw HTTP wire bytes. A Request
is emitted before admission; Finished follows a JSON result or safe error; dropping
an in-flight future emits Cancelled, including while waiting for admission. It
does not record headers, proxy credentials, response error bodies, redirect
chains or successful HTTP status codes. HTTP-status failures preserve status and
optional numeric Retry-After duration, without diagnostics.

## Capture before discovery

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let file = tokio::fs::OpenOptions::new().write(true).create_new(true)
    .open("new-public-http.jsonl").await?;
let (capture, input) = http_capture_channel(8, 64 * 1024 * 1024)?;
let transport = TransportConfig::direct().with_http_capture(capture);
let work = async move {
    let client = PublicRestClient::load_with_transport(
        ExchangeId::Okx, InstrumentKind::Spot, transport,
    ).await?;
    let result = client.ticker(&Symbol::spot("BTC", "USDT")).await?;
    drop(client); // release the capture sender before expecting natural EOF
    Ok::<_, cryptofeed_core::error::Error>(result)
};
let (_stop, stop) = tokio::sync::watch::channel(false);
let limits = RecordingLimits::new(100, 256 * 1024 * 1024, 64 * 1024 * 1024)?;
let (ticker, summary) = tokio::try_join!(work, record_http_stream(file, input, limits, stop))?;
# let _ = (ticker, summary);
# Ok(()) }
```

A capture channel gives the transport an isolated catalog cache namespace. Its
clones still share cache/coalesced requests and HTTP client/proxy routing; new
capture channels have separate namespaces. Attach before discovery so the first
catalog fetch is observable rather than hidden by an older global cache entry.
The shared [directory cache](catalog-cache.md) has entry/byte/LRU/TTL bounds;
retention can end before TTL under pressure. Subsequent cache hits emit no fake network calls. Basic proxy authentication
rebuilding retains the observer. Catalog data supplied externally to a REST
client is not retroactively captured.

Hold the stop sender during capture; true or sender closure requests Stopped.
Natural Complete requires dropping all observing transport/client/guard clones.
A bounded public example captures OKX Spot discovery, ticker, L2 and recent trades,
then validates the saved file without making further requests:

```bash
cargo run -p cryptofeed-rs --features recording --example http_recording_public -- NEW_PATH
```

## Request identities and format

One UTF-8 JSON object per newline, externally tagged:

- `header`: format `cryptofeed-rs.public-http`; new writers use version 2, readers accept 1 and 2.
- `observation.record`: global sequence, elapsed_ns, observed_ts and kind.
- `end`: observation count and Complete/Stopped/LimitReached reason.

Request contains request_id, exchange and public URL. IDs start at one and are
allocated in invocation order. Version 2 adds optional `configuration_error` on a failure. It preserves permanent
SDK configuration errors (including re-recorded replay cancellation) so a future
catalog call cannot reinterpret them as retryable protocol work. Version 1 cannot
claim this field; old ordinary outcomes remain readable. Code constructing Failure
values now supplies `configuration_error: None` unless explicitly classifying it.

Finished references the ID and contains either
sanitized arbitrary-precision JSON or a safe failure category/optional retry
duration (integer seconds/nanoseconds). Cancelled references the same ID. Completion order can differ from
start order; the reader matches IDs instead of guessing from URL equality.

Sequence is contiguous and elapsed emission time is monotonic, sampled before
bounded encoding/enqueue. Wall observed_ts
marks call/result boundaries (result time is sampled before sanitation/encoding),
not wire arrival or normalized model receipt time. It can differ from enqueue
elapsed order under concurrent calculation/clock adjustments. Model receive
clocks are not overridden by capture.

Every finish/cancel must reference one outstanding request exactly once. Complete
requires no outstanding requests. Stopped/LimitReached explicitly allow pending
starts, so a future replayer can reject missing responses instead of borrowing a
later request's outcome. Complete certifies the observation stream, not successful
exchange envelopes, normalized queries or complete market history.

## Privacy and resource behavior

Only registered current SDK public endpoint shapes for the active five exchanges
are accepted. HTTPS, expected host/path and an allowlist of public query keys are
required; userinfo, fragments, duplicate/unknown query keys and URLs over 2048 bytes or decoded values over 1024 bytes fail capture. This is not an HTTP client for arbitrary user/private URLs.
New SDK endpoint/query shapes require updating this gate and its tests.

JSON uses the existing public WS sanitation policy: known credential/diagnostic
fields are redacted and authentication/private channel operations rejected. The
file reader revalidates URLs, JSON privacy, versions, counters and lifecycle; it
never silently repairs unsafe input. This is a known-field/public-route policy,
not generic secret recognition inside arbitrary symbol strings or public cursors.
The original live JSON result is not replaced by its sanitized recording copy.

Capture queues are 1..=64 records, with configured encoded-record limit
256 bytes..64 MiB. Producers never wait for file I/O. Overflow, oversize or unsafe
input becomes sticky failure and wakes a waiting receiver even if no record was
enqueued; no successful footer follows a lost stream. Normal queries continue.
The same waiting-receiver fix applies to the raw WS observation channel.

RecordingLimits bounds total observations, bytes and line length, with footer
reserve and five-second I/O deadlines. Defaults remain 100,000 observations,
256 MiB total, **1 MiB per line**; raise the line limit for large catalogs (the
underlying directory HTTP response ceiling is 32 MiB). Queue limits and file
limits are separate. Encoding, JSON sanitation and parsing use additional bounded
buffers; encoded-byte limits are not a strict bound on in-memory JSON overhead.

Writer/reader failures and cancellation poison further use. Missing/truncated
header/line/footer, extra input, version/count/lifecycle/privacy and limit failures
are errors. Async flush does not promise fsync; earlier bytes or query effects
are not rolled back. File prefixes retain original request IDs and pending starts.

## Offline payload replay

`next_observation()` validates/iterates this ledger. [Offline HTTP replay](http-replay.md)
now validates a fresh file into HttpReplay and injects responses through existing
SDK directory/REST APIs. It never falls back to HTTP/WS, but does not simulate
original cache/timer scheduling or normalized receipt clocks. Existing
[raw v2 L2 replay](http-l2-replay.md) uses session-scoped consumed snapshots instead.
HTTP ledgers, normalized files and raw WS series remain separate formats.

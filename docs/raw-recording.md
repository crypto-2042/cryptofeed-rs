# Raw WS observation files

Optional `recording` now supports `RawRecordingWriter`, `record_raw_stream` and
`RawRecordingReader` for the [sanitized public WS observations](raw-capture.md).
The normalized-event codec is separate; neither reader silently accepts the
other format.

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let (capture, input) = raw_capture_channel(64, 1024 * 1024)?;
let mut handler = FeedHandler::new();
handler.add_feed(Okx::new().trade().symbol("BTC-USDT").raw_capture(capture).build());
let control = handler.control_handle();
let runtime = tokio::spawn(async move { handler.run().await });
let file = tokio::fs::OpenOptions::new().write(true).create_new(true)
    .open("new-raw-ws.jsonl").await?;
let (_stop, stop) = tokio::sync::watch::channel(false);
let limits = RawRecordingLimits::new(
    RecordingLimits::new(24, 4 * 1024 * 1024, 1024 * 1024)?, 16)?;
let capture = record_raw_stream(file, input, limits, stop).await;
control.shutdown().await?;
runtime.await??;
let captured = capture?;
let file = tokio::fs::File::open("new-raw-ws.jsonl").await?;
let mut reader = RawRecordingReader::new(tokio::io::BufReader::new(file), limits);
while let Some(observation) = reader.next_observation().await? {
    println!("{} {:?}", observation.sequence, observation.kind);
}
assert_eq!(reader.summary(), Some(captured));
# Ok(()) }
```

Retain stop senders during capture. True/drop stops; false notifications do not.
Use create_new to protect prior captures. The generic writer chooses no path or
open policy. Applications can own/sync the file explicitly; flush is not fsync.

## Version-1 segment format

One externally tagged UTF-8 JSON object per newline:

- `{"header":{"format":"cryptofeed-rs.raw-ws","version":1}}`
- `{"observation":{"record":{...RawObservation version 1...}}}`
- `{"end":{"events":24,"reason":"limit_reached"}}`

End reasons are complete, stopped, limit_reached. `events` counts **transport
observations**, including lifecycle/subscription/heartbeat text, not normalized
models or market trades. Observation sequence starts at one with no gaps; elapsed
nanoseconds cannot reverse. Original wall-clock receipt times and numeric JSON
literals remain unchanged. Context references are interned by the reader after
validation rather than retained as a fresh metadata copy per message.

A session must begin with Connected, use a positive never-reused session ID, keep
identical public metadata and accept Sent/Received only until one Closed. Complete
requires every connected session to close. Stopped/LimitReached intentionally
allow an open-session prefix. This validates a file prefix, not successful feed
health, peer close acknowledgement or exchange-wide completeness.

The segment starts at the beginning of a fresh capture channel. Mid-stream
resumption, file rotation/merging and implicit session reconstruction are not
implemented. Reopening a reader restarts this segment; it does not resume a
partially consumed native session or restore authoritative L2 state.

## Validation, privacy and bounds

Writer and reader recheck version, sequence, elapsed time, finite wall clock,
public exchange/product/symbol/native-map structure, source labels, sparse
channel scopes, frozen context and lifecycle. Readers reject missing/truncated
header/line/footer, count mismatch, extra trailing records and budget excess.

Credential/diagnostic redaction and private-operation rejection are revalidated
on file input. Known sensitive fields must already contain [REDACTED] with the
redacted marker; unredacted or private input fails without echoing its payload.
The reader does not silently rewrite unsafe records. This is the same current
public-protocol policy as the observation channel, not generic secret detection
for arbitrary strings/custom/private protocols. No transport URLs, headers or
proxy configuration are part of the allowed metadata schema.

RawRecordingLimits combines RecordingLimits (default 100,000 observations,
256 MiB total, 1 MiB per line) with a default 1024-session ceiling. Sessions are
counted for the entire segment, including closed ones; custom session limits are
1..100,000. Header/footer/newlines count toward byte bounds. The writer reserves
footer room and returns false before accepting a row that exceeds a resource
limit. record_raw_stream then writes an explicit LimitReached prefix.

Size-limited encoding and bounded streaming line reads are shared with normalized
recording. I/O operations have a five-second deadline. Failed/cancelled partial
I/O poisons the writer/reader, so a later call cannot skip bytes and guess state.
Capture queue loss/error returns failure with no successful end marker. Earlier
bytes/callback effects are not rolled back, and accepted/flushed bytes do not
prove filesystem durability. There is no cryptographic integrity claim.

The next_observation API persists/validates transport input without executing
parsers. [Native replay](raw-replay.md) is now separately available through replay,
including three WS-native L2 paths. Captured HTTP/bootstrap, Binance/Gate L2 and
transport activity simulation remain unfinished; plain JSON iteration is not
proof of those paths.

Offline: `cargo test -p cryptofeed-rs --features recording --lib recording::raw::file`.
Manual public capture + offline file validation:
`cargo run -p cryptofeed-rs --features recording --example raw_recording_public -- NEW_PATH`.
[Public evidence](reports/live-smoke-raw-recording-2026-10-11.md) records the exact result.

A synthetic segment reference is in sample_data/recording.raw-ws.v1.jsonl with matching executable inline tests; it is not an actual exchange recording.

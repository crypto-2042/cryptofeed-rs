# Normalized event recording and replay

Enable `recording` in addition to the data categories you use. It is opt-in and
not a default feature. `FeedHandler` keeps its existing callbacks/runtime entry
point; attach `subscribe_identified()` before running and pass that receiver to
`record_stream`. Recording does not block the producer or change its policies.

```rust,no_run
use cryptofeed_rs::prelude::*;
use tokio::{io::BufReader, sync::watch};
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut feed = FeedHandler::new();
feed.add_feed(Okx::new().trade().symbol("BTC-USDT").build());
let receiver = feed.subscribe_identified();
let control = feed.control_handle();
let runtime = tokio::spawn(async move { feed.run().await });
let file = tokio::fs::OpenOptions::new().create_new(true).write(true)
    .open("new-events.jsonl").await?;
let (_stop, stop) = watch::channel(false);
let capture = record_stream(file, receiver,
    RecordingLimits::new(20, 4 * 1024 * 1024, 1024 * 1024)?, stop).await;
control.shutdown().await?;
runtime.await??;
let capture = capture?;

let file = tokio::fs::File::open("new-events.jsonl").await?;
let mut replay = RecordingReader::new(BufReader::new(file), RecordingLimits::default());
let (_stop, stop) = watch::channel(false);
let summary = replay.replay(ReplayOptions::default(), stop, |record| async move {
    // Match record.event and invoke your existing model-handler/business logic.
    println!("record {} from {:?}", record.sequence, record.identity);
    Ok(())
}).await?;
println!("captured {}, replay result {:?}", capture.events, summary);
# Ok(()) }
```

Retain the stop sender while recording/replaying. Sending true or dropping all
senders stops work; false notifications do not stop it. An event limit stops
immediately at the accepted count, even if the source becomes quiet. Captures may
also end when the producer closes its stream. Choose `create_new` to avoid
accidentally overwriting a previous recording; the generic writer itself does
not choose paths or file-open policy.

## Format version 1

UTF-8 JSON Lines, one compact externally tagged object per newline:

- `{"header":{"format":"cryptofeed-rs.normalized","version":1}}`
- `{"event":{"record":{"sequence":1,"elapsed_ns":0,"identity":{"id":1,"generation":1},"event":{"trade":{...normalized Trade...}}}}}`
- `{"end":{"events":1,"reason":"complete"}}`

The other ending reasons are `stopped` and `limit_reached`. A footer's count must
match the contiguous one-based event sequence. Elapsed nanoseconds must not
reverse; ties are allowed. No records, blank lines or extra footer may follow
end. Missing/truncated footer or line, unknown version/format/category, invalid
source identity, malformed model or unsupported feature category fails explicitly.
The external tags preserve typed f64 timestamps alongside the workspace's JSON
`arbitrary_precision` setting; the codec never disables numeric precision.
A sanitized reference and corresponding executable inline fixture are in
`sample_data/recording.normalized.v1.jsonl` and `recording::tests` respectively.

Data is the existing normalized public model, including Decimal strings, native
quantity units, original exchange/receipt timestamps and optional/unknown fields.
Sources retain recorded feed ID/generation as **file-local labels**, not handles
to a currently running feed. No URL, transport/auth configuration, headers,
raw/control frames or arbitrary feed-status error strings enter this format.
There is no raw-frame parser replay or lifecycle/recovery-anchor reconstruction
in this increment. L2 records can start at a delta if capture begins late;
consumers must not treat this file as an authoritative recoverable book stream.

## Bounds and failure behavior

Defaults: 100,000 events, 256 MiB total and 1 MiB per JSON line. Custom limits
require a positive event budget, line budget at least 256 bytes and space for
an end marker. Raw line/total bytes include newlines/header/footer. Encoding
uses a size-limited buffer; reading does not allocate an unbounded line. Writers
reserve footer space before accepting each event. Serialization is of the typed
models, without cloning a whole event inside append.

`RecordingWriter::append(envelope, elapsed)` supports caller-supplied deterministic
times and returns false when a configured limit prevents accepting the next event.
Call `finish(reason)` for an intentional complete/stopped/limited prefix. Async
writes/flushes and each reader operation have a five-second I/O deadline.
An append failed/cancelled during I/O poisons its writer; a failed/cancelled read poisons its
reader. They cannot be resumed with guessed state. Flush means writer acceptance,
not filesystem fsync durability; applications can own the file and sync it.

The identified broadcast remains bounded/lossy (capacity 1024). `record_stream`
turns `Lagged` into an explicit error and emits no end marker after the detected
gap. Earlier bytes may remain as an incomplete prefix; they are not a successful
recording. Errors during writing or flushing are returned, even if some bytes
were accepted. Capture failure does not automatically stop healthy feeds;
the example explicitly shuts down its runtime after capture.

## Replay semantics

`RecordingReader::next_event()` permits incremental offline reads. `replay()`
requires a fresh reader and invokes one async callback at a time in file order.
Callbacks may match the event and call existing handler traits. Replay creates
no exchange connection, catalog fetch, live counter publication or book-cache
update. All model timestamps/values stay as recorded.

Default timing is `Immediate`. `ReplayTiming::Recorded` schedules each event at
its saved elapsed time relative to replay start, so slow callbacks cause catch-up,
not accumulating extra sleep. In stream capture, elapsed time is the recorder's
observation time, including queuing/I/O delay, not a substitute for wire/receive
model clocks. Caller-supplied append times define their own monotonic schedule.

Callbacks default to a five-second deadline; configure
`ReplayOptions::callback_deadline(duration)` when needed. Business errors,
catchable construction/poll panics and deadlines stop replay. Shutdown returns
`None`; a validated EOF returns `Some(RecordingSummary)`. A cancelled, dropped or
failed replay cannot be restarted on that reader and silently skip its pending
event. Reopen the recording to start again. Earlier callbacks may already have
side effects before a later corruption/error/cancellation is found: replay is
not transactional, and footer validation is necessary to certify the file ended.
There is no checksum/signature or cryptographic tamper-proof claim.

This is the normalized-event portion of the alignment plan. Sanitized raw protocol
capture/replay, storage sinks, aggregation and NBBO remain subsequent increments.

Offline: `cargo test -p cryptofeed-rs --features recording --lib recording::`.
Public end-to-end smoke (creates a new file, captures 20 OKX trades, stops runtime,
then replays offline):
`cargo run -p cryptofeed-rs --features recording --example recording_public -- NEW_PATH`.

The footer certifies the captured file prefix, not exchange-wide completeness or source health. Capture begins at subscription and reflects existing delivery filters.

[Public capture/offline replay evidence](reports/live-smoke-recording-2026-10-11.md) records the successful 20-event workflow.

Incompatible normalized model/schema changes require a recording format migration or version change; do not silently reinterpret older records.

A separate [public WS observation channel](raw-capture.md) now provides sanitized pre-parser text/context. It is not accepted by this normalized codec; persisted raw replay remains pending.

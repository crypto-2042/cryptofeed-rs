# Sanitized public WS observation

With optional `recording`, attach `raw_capture_channel` to a public exchange
builder before `FeedHandler::add_feed`. It observes actual text before application
heartbeat filtering, readiness/control classification and market parsing.

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let (capture, mut input) = raw_capture_channel(64, 1024 * 1024)?;
let mut handler = FeedHandler::new();
handler.add_feed(Okx::new().trade().symbol("BTC-USDT").raw_capture(capture.clone()).build());
let control = handler.control_handle();
let runtime = tokio::spawn(async move { handler.run().await });
for _ in 0..10 {
    let frame = input.recv().await?.ok_or("capture ended")?;
    println!("{} {:?}", frame.sequence, frame.kind);
}
control.shutdown().await?;
runtime.await??;
drop(capture); // retained handles otherwise keep the observation stream open
while input.recv().await?.is_some() {}
# Ok(()) }
```

The receiver is an observation API, **not yet a raw recording-file codec or offline
protocol replayer**. The existing normalized JSONL reader does not accept these
observations. Persisted raw segments, captured HTTP catalogs/bootstrap responses,
parser/session replay and complete L2 sequence reconstruction remain subsequent
work under the full alignment plan.

## Observation scope

`RawObservation` serializes as schema version 1 and contains an enqueue sequence,
monotonic elapsed nanoseconds, wall-clock observation time and shared session
metadata. Sequences and elapsed time follow bounded-queue enqueue order across
sessions, not a sort by exchange timestamps. A successful connection attempt gets
a new process-local session ID; retries/reconnects do not reuse it.

Kinds are Connected, Sent, Received and Closed. Sent records only successfully
written text. Received records include application ping/pong and subscription
replies that the live runtime consumes before returning market text. Transport
binary frames, TCP/TLS details and WS protocol ping payload bytes are not captured.
Closed with clean=true means the session read loop ended normally; fallback Drop
closure is clean=false, not proof of a peer-confirmed network close handshake.

Metadata carries public normalized/native symbol mappings, channel rules, candle
policy/interval and requested book settings, plus feed ID/generation where present.
It contains no transport/proxy configuration, URL, request/response headers or
handler objects. These labels belong to this capture process, not live handles
that a replay application can control.

## Sanitization and bounds

Inputs are restricted to JSON text and literal `ping`/`pong`. JSON is parsed with
the workspace arbitrary-precision setting, preserving numeric wire price/quantity
literals. JSON whitespace/key order may change: this is a sanitized protocol
observation, not byte-for-byte packet capture.

Known credential fields (case/separator-insensitive aliases for API/access keys,
secrets/passwords/passphrases, signatures, auth/authorization, tokens, cookies and
listen keys) are replaced with `[REDACTED]`. Free diagnostic msg/message/ret_msg/
error strings are redacted; fixed success/pong literals are retained for protocol
control behavior. Numeric error codes/object structure remain. Auth/login actions
and recognized private account/order/execution/wallet topics reject capture
entirely. This policy targets current supported public feeds, not arbitrary
private/custom protocols or secrets deliberately placed in market/config fields.

Queue capacity is explicit, 1..1024 observations, and logical input-text size is
256 bytes..8 MiB. Example defaults are 64/1 MiB. Sanitization depth is at most 32.
The source never awaits a disk writer. Full/closed consumer queues, oversized or
unrecognized text, private operations and malformed/deep input cause a **sticky**
capture error without echoing the rejected payload. The receiver checks failure
both before and after receive; it cannot silently validate a prefix after a gap.
Live feed parsing/health is not changed by capture failure; callers decide when
to stop their runtime. Dropping a pending recv is cancel-safe.

WS normalized events now use the exact same receipt timestamp observed at the
text-read boundary, before JSON parsing. Each exchange's event timestamp remains
separate. HTTP bootstrap snapshots retain their own HTTP receipt clock; they are
not covered by this WS-only observer.

Offline tests cover sanitizer precision/context, credential diagnostics, private
operation rejection, sticky overflow, sender closure, reconnect IDs/global order,
size/depth bounds and a real in-process WS session where pong is observed before
filtering and live data shares the receipt clock.

Manual public smoke:
`cargo run -p cryptofeed-rs --features recording --example raw_capture_public`.
The [dated evidence](reports/live-smoke-raw-capture-2026-10-11.md) verifies actual
capture/normalization boundaries without claiming persisted raw replay.

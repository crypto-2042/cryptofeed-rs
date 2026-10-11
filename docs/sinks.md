# Normalized event sinks

`EventSink` and `run_sink` provide a small storage integration boundary for
`FeedHandler::subscribe_identified()`. They preserve each owned `FeedEnvelope`,
including feed ID, configuration generation, typed model, Decimal values and
original exchange/receive timestamps. No database client is added to the default
SDK. Applications can implement the trait for a database, socket or message bus.

## JSONL adapter

With the optional `recording` feature, `RecordingWriter<W>` implements `EventSink`
for any `AsyncWrite + Unpin + Send` writer. It uses the existing versioned
[normalized recording format](recording.md); the reader/replay APIs are unchanged.
The writer's event, byte and line limits bound the accepted prefix. Its summary
reports the actual event count, bytes and Complete/Stopped/LimitReached reason.

```rust,no_run
use cryptofeed_rs::prelude::*;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let file = tokio::fs::OpenOptions::new()
    .write(true).create_new(true).open("market.jsonl").await?;
let writer = RecordingWriter::new(file, RecordingLimits::default()).await?;
let mut feeds = FeedHandler::new();
feeds.add_feed(Okx::new().trade().symbol("BTC-USDT").build());
let receiver = feeds.subscribe_identified();
let control = feeds.control_handle();
let runtime = tokio::spawn(async move { feeds.run().await });
let (_stop_sender, stop) = tokio::sync::watch::channel(false);
let result = run_sink(writer, receiver, SinkOptions::default(), stop).await;
let shutdown = control.shutdown().await;
let runtime_result = runtime.await?;
shutdown?;
runtime_result?;
let summary = result?;
println!("stored {} events: {:?}", summary.events, summary.end);
# Ok(())
# }
```

The live `recording_public` example uses this adapter, captures 20 OKX public
trades, stops the runtime, then verifies offline replay. Files use `create_new`
to avoid overwriting data. `finish` confirms async flush; filesystem durability
requires retaining a file reference and calling `sync_all` yourself, as in that
example. This adapter does not rotate files or reconnect a remote storage service.

## Custom sinks and delivery contract

Implement `write(&mut self, FeedEnvelope, Duration)` and consuming `finish(self,
SinkEnd)`. Each returns a Send future with the SDK `Result`; no async-trait macro
is needed. `write` returns `Accepted` to continue, `AcceptedAndFull` when the final event
was accepted, or `Full` when the current event was rejected before writing. Both
capacity outcomes finalize with `SinkEnd::LimitReached`; the summary reports the
actual accepted count. `elapsed` is
monotonic consumer time since the runner started, separate from model clocks.

The runner processes one write at a time and adds no worker task, batch, queue,
or automatic retry. Each runner owns one sink. Ordering is the broadcast's
arrival order, not global exchange time across sockets. Feed ID/generation are
local runtime scope, not globally unique deduplication keys across processes.

- Sender closure drains queued events and finalizes with `Complete`.
- A true shutdown flag or closed watch sender between writes finalizes with
  `Stopped`, without draining queued events.
- Sink capacity finalizes with `LimitReached`.
- Broadcast lag is terminal. Slow sinks can lag even though writes are sequential;
  this interface does not make the live broadcast lossless or slow the producer.
- Write errors, operation deadlines, catchable panics and shutdown during a write
  return an error and drop the sink without finalization. Dropping the runner also
  drops the sink. The already consumed event is not retried.
- Finalization errors, panics and deadlines return errors. Once finalization
  starts, shutdown does not interrupt it; the deadline bounds it.

`SinkOptions` defaults to a five-second write/finalization timeout; use
`with_operation_timeout` with a positive duration to change it. A JSONL writer
also retains its own five-second I/O deadline. Futures must be safe to drop.
Deadlines require cooperative async futures; blocking work cannot be preempted.
A remote write may have committed before an error, timeout or cancellation;
exactly-once delivery and remote durability are the adapter/application's
responsibility. Do not write a successful footer after an uncertain partial I/O.
The existing `record_stream` convenience API remains supported.

Python's `backends/backend.py` is the semantic reference for asynchronous
callback-to-storage workflows. This Rust boundary adds explicit bounded-source
and failure behavior; it does not claim Python's full adapter collection, L2
snapshot synthesis, batching or multiprocessing parity. Aggregation and NBBO
remain separate work.

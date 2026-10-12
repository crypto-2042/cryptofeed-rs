# Independent raw capture replay

`replay_raw_merge` composes independent public raw recordings or rotated series
onto a caller-defined elapsed axis. It uses the same native parser/HTTP/L2 state
machine as single-capture replay. This completes the independent-capture workflow;
[rotation](raw-series.md) continues to preserve one capture across physical parts.

The sibling Python checkout (`3a6d3ca`, raw_data_collection.py) accepts multiple
filenames and iterates WS files in supplied order. It does not supply this
cross-origin elapsed alignment contract. Rust's explicit common-axis merge is an
extension, not a claim about Python's global chronological sorting.

```rust,no_run
use cryptofeed_rs::prelude::*;
use std::time::Duration;
use tokio::{fs::File, io::BufReader, sync::watch};
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let inputs = vec![
    RawMergeInput::recording("capture-a", Duration::ZERO,
        BufReader::new(File::open("a.jsonl").await?), RawRecordingLimits::default())?,
    RawMergeInput::series("capture-b", Duration::from_secs(5),
        "captures", "series-b", RawSeriesLimits::default()).await?,
];
let (_stop, shutdown) = watch::channel(false);
let summary = replay_raw_merge(inputs, RawMergeOptions::default(), shutdown, |item| async move {
    println!("{} @{}ns {:?}", item.capture_id, item.elapsed_ns, item.item);
    Ok(())
}).await?;
# Ok(()) }
```

Fresh inputs are owned and consumed by replay. Each capture needs a unique ID
(1..=64 ASCII letters/digits/-/_) and a nonnegative offset representable in u64
nanoseconds. A source record maps to `offset + elapsed_ns`, with checked addition.
Choose the common origin yourself, before all desired starts. The SDK neither
compares wall-clock timestamps nor assumes files share a host/clock/origin.

## Ordering and identity

The merger retains one validated head per input and chooses the lowest aligned
time. Ties follow input vector order; a source's own record order never changes.
All lifecycle, Sent, Received, Processing and consumed HTTP observations participate,
including observations that do not emit a model. No state is reconstructed by
sorting normalized exchange timestamps or prices.

`RawMergeItem` carries capture_id, aligned elapsed_ns and the existing RawReplayItem.
Native session IDs, FeedIdentity/generation, observation sequences and exact
normalized exchange/receive clocks remain unchanged. **Identity is namespaced by
capture_id**; independent captures can have identical local IDs. Consumers must
retain that namespace or deliberately rebind identities before combining outputs
with APIs that expect process-global feed identities. No deduplication is inferred
from overlapping symbols, timestamps or identical native trade IDs.

Each input owns independent parser, ticker, book, pending received/processing and
consumed HTTP bootstrap state. Reconnects reset only their native session state.
The same symbol/session ID at another capture cannot seed or advance its book.
There is no HTTP/WS transport, catalog hydration, implicit snapshot fetching or
live quota mutation. Existing protocol/sequence/checksum validation remains active.

Default replay is Immediate. `RawMergeOptions::replay(RawReplayOptions)` accepts
Recorded timing and callback/output bounds. Recorded timing uses one replay start
plus aligned elapsed time; read/callback overhead does not reset the origin.
False shutdown notifications do not bypass waits; true/closed shutdown interrupts
reads, timing, parser processing or callbacks and returns None.

## Bounds, completion and failure

At most 16 inputs are accepted. Default aggregate limits are 100,000 observations
and 256 MiB logical input bytes, configurable through input_limits with positive
values. Header/footer bytes count; series manifests remain separately bounded by
the series reader. Total normalized model count (default 1,000,000) and per-native
batch size (default 1024) come from RawReplayOptions and apply across all captures.
No model batch is partly emitted to meet the global model ceiling.

Each source also enforces its own existing line/byte/observation/session limits,
privacy and causal validation. Memory retains one bounded head and local parser
state per input, plus existing source read buffers. Aggregate byte accounting is
checked after each bounded source read and before delivering that head; it is not
an exact heap-allocation or operating-system prefetch-byte limit. No full-file load,
sort or unbounded worker queue is used.

A successful RawMergeSummary certifies EOF of every input, including series
manifest/checksum/footer agreement. It reports each input's own RecordingSummary,
plus total observations, models and logical bytes. Complete/Stopped/LimitReached
prefix meanings are preserved; reaching every prefix EOF is not proof that the
original captures all ended Complete or captured all exchange traffic.

Malformed/truncated input, time/budget overflow, native parser failure, callback
error/panic/deadline or I/O failure is terminal and returns an error. Streaming
callbacks can run before later corruption is discovered; their effects are not
rolled back. Errors/shutdown consume the inputs and discard replay state. Reopen
from the beginning for another run; no resume-by-skipping or continuation token is
provided. Existing five-second source read/callback defaults remain in effect.

## Example and evidence

```bash
cargo run -p cryptofeed-rs --features recording --example raw_merge -- a 0 a.jsonl b 5000 b.jsonl
```

Add `--recorded` before the triples to use aligned timing. Default data features
cover the recorded categories; isolated builds must explicitly enable them. The
example prints scoped lifecycle/market identifiers and certifies EOF, with Ctrl-C
cancellation; it never opens an exchange connection.

Twelve new deterministic tests cover stable alignment, five native families,
original clocks/sequences, identical-session L2 isolation, Binance/Gate consumed
HTTP causality, reconnects/prefix ends, rotated-series certification, aggregate
budgets, malformed input, overflow, shutdown and callback failures. The existing
single-capture replay suite runs against the shared engine. The synthetic timeline
reference is `sample_data/raw-merge.timeline.json`; no market wire shape or endpoint
changed. [The replay contract audit](replay-contract-audit.md) now verifies
published clock/cache/concurrency guarantees. Distributed quotas remain separate
work under the full alignment plan.

The offline CLI smoke used the existing sanitized v2 Binance reference twice under
IDs a/b with a five-second offset: 14 observations, 6 L2 models and 7052 logical
bytes, both EOFs certified. This demonstrates namespaced duplicate local IDs and
consumed HTTP replay; it is not evidence of two separate live captures.

[Clock/cache/concurrency audit](replay-contract-audit.md) now records executable
evidence for the published replay behavior and keeps unsupported historical
scheduling distinct from model timestamp preservation.

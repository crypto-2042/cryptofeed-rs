# Raw recording rotation and ordered composition

Optional `recording` adds `RawSeriesWriter`, `record_raw_series` and
`RawSeriesReader`. They preserve **one logical raw v2 stream** across bounded,
record-aligned physical files. Global observation sequence, session metadata,
Processing references and consumed HTTP/L2 causality continue across boundaries.
No observation is reclassified, renumbered or normalized by rotation.

## Capture and replay

```rust,no_run
use cryptofeed_rs::prelude::*;
# async fn example(input: RawCaptureReceiver, stop: tokio::sync::watch::Receiver<bool>) -> Result<(), Box<dyn std::error::Error>> {
tokio::fs::create_dir_all("captures").await?;
// Use a new public ID for each capture; all paths are create_new.
let writer = RawSeriesWriter::create("captures", "new-capture", RawSeriesLimits::default()).await?;
let summary = record_raw_series(writer, input, stop).await?;
let mut reader = RawSeriesReader::open("captures", "new-capture", RawSeriesLimits::default()).await?;
while let Some(observation) = reader.next_observation().await? {
    println!("{}", observation.sequence);
}
assert_eq!(reader.summary(), Some(summary));
# Ok(()) }
```

Reopen a fresh `RawSeriesReader` for `.replay(RawReplayOptions, stop, callback)`.
It delegates to the existing [native raw replay](raw-replay.md), including v2
[consumed HTTP/L2 replay](http-l2-replay.md), and creates no HTTP/WS transport.
Parser/book state survives file boundaries because there is only one logical
reader and replay session map. `part_count()` reports the declared file count;
`summary()` is available only after validated EOF and manifest/footer agreement.

Manual public capture, runtime shutdown and offline native replay:

```bash
cargo run -p cryptofeed-rs --features recording --example raw_series_public -- DIRECTORY NEW_SERIES_ID
```

The example records 40 observations from OKX Spot trades into small physical
parts, stops the runtime, checks rotation and replays only those files. Ordinary
single-file raw APIs and v1/v2 schema compatibility are unchanged.

## Manifest and files

The output directory must exist for `RawSeriesWriter::create`. ID is 1..=64 ASCII
letters/digits/hyphens/underscores, with no path separators. It names:

- `ID.manifest.json`: version 1 `cryptofeed-rs.raw-series` metadata.
- `ID.part-0000.jsonl`, `ID.part-0001.jsonl`, ... in manifest order.

The manifest records ID, ordered per-part byte counts and `fnv1a64` checksums,
logical event/byte count and end reason. Only the first part begins with a raw
header and only the last ends with a raw footer. Middle parts are continuation
records, **not standalone recordings**. Concatenating the referenced files in
order yields one valid existing raw v2 file. The reader does this lazily with one
open data file, not by loading the whole capture or sorting directory contents.
Unreferenced files are not part of a series and are not implicitly scanned.

The checksum is unsigned decimal FNV-1a 64-bit: basis 14695981039346656037,
for each byte xor then multiply by 1099511628211 modulo 2^64, as in
[RFC 9923](https://datatracker.ietf.org/doc/html/rfc9923). It detects accidental
corruption/order mistakes; it is not cryptographic authentication or protection
against an actor rewriting data and the manifest. No new dependency is required.

Privacy, protocol version, global sequence/elapsed time, session lifecycle,
frozen contexts, causal HTTP/Processing links and native parser/bridge rules are
still validated by the original codec. A checksum does not replace those gates.
Independent captures are not merged: different origins, IDs, clocks and sequences
need a separate explicit policy, not implicit file concatenation.

## Bounds and failure

`RawSeriesLimits` contains `RawRecordingLimits`, part bytes and maximum part
count. Defaults: the existing 100,000-observation/256 MiB/1 MiB-line/1024-session
logical limits, 16 MiB physical parts, and at most 1024 parts. Configured part bytes
must be >= logical record size limit and part count must be 1..=1024. Manifest
input/output is bounded to 256 KiB; its bytes are additional to logical raw bytes.
No file is split within a JSONL record. Session and observation budgets apply to
the entire logical series, not independently to every part.

Normal observation/byte/session cutoff uses the existing `LimitReached` prefix.
The physical part ceiling and file-create/write failures are terminal errors,
not certified truncations. Allow enough physical space for records **and footer**.
The helper neither deletes older files nor implements a rolling retention ring.
Only one staged record is kept, in addition to codec encoding/line buffers; the
reader keeps bounded manifest metadata and a streaming file/line buffer.

The manifest is reserved empty at creation. Data files flush before finalization
writes the manifest. Existing manifests/parts are never overwritten. Failure or
cancellation poisons the writer/reader; choose a new ID/reopen from the start
rather than guessing a continuation. Empty/partial manifests, missing/truncated/
extra bytes in a referenced part, wrong order/checksum, global sequence errors,
wrong ID/version/budgets and logical footer/manifest disagreement fail explicitly.
Files remain available for diagnosis; cleanup is application-owned.

Five-second I/O deadlines cover create/read/write/flush operations and existing
logical line reads. Successful finalization confirms async acceptance/flush,
not fsync/directory durability or producer task success after an uncertain final
flush. A complete surviving manifest certifies readable bytes and logical EOF,
not every external action. Streaming can deliver earlier observations/callbacks
before later corruption is found; effects are not rolled back. Replay cancellation,
callback failure and partial I/O cannot restart by skipping pending input.

[Public HTTP result recording](http-recording.md) now covers shared directory/
REST/bootstrap calls separately. Its file validator is not yet offline SDK HTTP
response injection or a replacement for consumed-session L2 causality.

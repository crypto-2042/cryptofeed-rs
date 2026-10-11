# Consumed HTTP snapshots and L2 replay

Optional `recording` now records Binance/Gate L2 bootstrap/resnapshot inputs at
the point the runtime consumes their completed HTTP result. Native raw replay
uses those recorded JSON bodies with the existing buffer/bridge/resnapshot rules;
it does not fetch replacement data online.

## Raw protocol v2

New captures and `RawRecordingWriter` use raw-ws header/observation version 2.
Readers remain compatible with version-1 files and preserve their old WS-only
behavior. A session cannot mix observation versions. Version-1 Binance/Gate L2
still rejects missing HTTP history rather than pretending it can reconstruct it.

Version 2 adds:

- `Processing { received_sequence }`: the exact Received input passed into native
  processing after heartbeat/readiness filtering and snapshot polling. It must
  refer to the latest unprocessed receive in the same session, once only.
- `HttpSnapshot { symbol, depth, payload, received_ts }`: safe JSON body consumed
  by the L2 bootstrap poll, with its original HTTP receipt clock. For Binance,
  depth binds to the snapshot width in the recorded plan.
- `HttpSnapshotError { symbol, depth, failure }`: safe failure category/status,
  without URL/header/body/error-string leakage when no usable body exists.

A raw Received packet is transport observation, not proof that live native code
processed it. Pongs/subscribe replies can be consumed earlier in Session. A
bounded segment can also end between receive and processing. Version-2 replay
normalizes only Processing references, and never invents outputs for an input
that was merely observed. Legacy version-1 non-L2 files retain direct receive
parsing for compatibility.

## Why the boundary matters

The live session records receive N, polls completed snapshots, then processes N.
The poll may bootstrap buffered older deltas or schedule a bounded resnapshot
before N is interpreted. Sorting only by HTTP completion time, or immediately
processing every raw receive, changes which deltas are buffered versus subject
to strict live-next rules. The explicit marker preserves this distinction.

Capture keeps the raw response only for a recording-enabled snapshot receiver.
It is emitted at actual main-loop consumption, even when typed snapshot parsing
fails. Superseded/cancelled HTTP results that never get consumed are not included.
This is consumed L2 response capture, not a generic dump of all HTTP traffic,
requests, catalogs, headers, retries or authentication material.

The existing shared HTTP admission/proxy/timeout/body-size policies are unchanged.
Both successful bodies and source failures keep the public normalized symbol/
product context. JSON privacy/size validation still applies; known sensitive
fields/diagnostics redact, and unsafe/deep/oversized input invalidates capture.
Failures retain only HTTP status or a transport/malformed/other category; no raw
error string or Retry-After header is persisted.

## Offline implementation and coverage

Binance/Gate now use the same native L2 message functions, snapshot parsers,
bootstrap polls, pending-delta buffers, strict next/gap logic, full/partial resets
and bounded resnapshot counters as live runtime. Recorded responses are inserted
as completed receivers at the captured consumption boundary. Replay scheduling
uses placeholders that keep a receiver pending **without spawning HTTP tasks**.
A snapshot with no pending bootstrap, wrong width/symbol or impossible Gate
buffered anchor fails. Recorded HTTP failures propagate safely instead of retrying
a public endpoint or silently substituting a fresh snapshot.

WS-native Bybit/OKX/Bitget book replay remains unchanged. Reconnect creates fresh
local state. Bootstrap output is attributed to the consumed snapshot observation;
ordinary WS output is attributed to the original Received sequence, not merely
its later Processing marker. All original model exchange/receipt clocks and
Decimal/native quantity units are preserved.

A file can intentionally end before a bootstrap is ready, yielding no L2 models
for that prefix. Limited/stopped EOF does not certify remote readiness or a full
book. Replay is sequential input/state execution; it does not reproduce arbitrary
cross-task callback interleavings, transport/readiness timers or a managed live
recovery store. Consumers still use source/session lifecycle and ending reason.

Live coverage in this increment is Binance/Gate Spot full-depth. Existing
product-qualified parsers/plans are used for derivative/dated or partial inputs,
but those have no new live certification here. Catalog/general REST capture,
rotation/merge, broader replay edge audits, sinks/aggregation, NBBO and advanced
resource policies remain under the full alignment objective.

Offline regressions compare exact native-vs-replay snapshot/buffer/delta outputs
and their observation origins, including id-less Gate timestamp anchoring, safe
HTTP429 failure, strict overlap-triggered resnapshot, invalid causal references,
frozen versions, legacy compatibility and unprocessed receive prefixes.

Manual public comparison (captures each runtime, shuts it down, then replays):
`cargo run -p cryptofeed-rs --features recording --example raw_l2_replay_public`.
See [dated evidence](reports/live-smoke-http-l2-replay-2026-10-11.md).

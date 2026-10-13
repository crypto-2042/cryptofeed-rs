# Public raw WS file smoke — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example raw_recording_public -- NEW_PATH`.
An exclusive new file was created in /private/tmp; actual captured contents are
not committed. Unauthenticated current OKX Spot BTC-USDT trades, direct transport.
Exit 0; no first-attempt failures.

Console: `raw segment observations=24, market packets=18, bytes=15672, end=LimitReached; offline validation complete`.

The recording accepted 24 observations (including transport/control activity),
closed the managed runtime, reopened the saved file and validated schema,
sequence, frozen public source context, session lifecycle and explicit footer.
18 saved inputs were identified as trade-channel packets. Writer/reader summaries
matched exactly. A limit-ended prefix may still have an open session in the file;
this was intentional and distinguished from a naturally Complete segment.

The offline pass only opened the file; it did not execute exchange parsers or
bootstrap/synchronize an L2 book. No claim is made about normalized trade counts,
complete source history, all-product live privacy, file rotation or durability.
Numeric precision, unsafe input, lifecycle corruption, truncation, limits and
partial I/O cancellation/deadlines are independent deterministic offline tests.

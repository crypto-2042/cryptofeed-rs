# Normalized recording/replay smoke — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example recording_public -- NEW_PATH`.
Actual output was created with create_new in /private/tmp; it is not committed.
Unauthenticated current OKX Spot BTC-USDT trade feed, existing FeedHandler
identified stream, direct transport. Exit 0.

| Stage | Observed |
| --- | --- |
| Capture | 20 normalized events, 6,738 bytes |
| End reason | LimitReached |
| Runtime | Explicit managed shutdown completed successfully before replay |
| Offline replay | 20 trade callbacks, model exchange/symbol checked, footer/count verified |

Console result: `capture=20 bytes=6738 end=LimitReached; offline replay=20 verified`.
No first-attempt failure occurred. The file contains normalized model fields and
source labels, not WS frames/headers/auth/proxy configuration. Replay opened only
the saved file after runtime completion; no exchange connection was used for
replay. This is one venue/channel workflow, not all-category live, sustained-load,
fsync/durability, raw-parser replay or L2 recovery evidence. Those format categories
and bounded failures are covered separately by deterministic offline assertions.

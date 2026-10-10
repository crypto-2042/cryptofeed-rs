# Public raw observation smoke — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example raw_capture_public`.
Current unauthenticated OKX Spot BTC-USDT trades, direct transport, explicit
managed shutdown. Exit 0, no first-attempt failures.

Console: `raw market packets=20; total observations=24; normalized trades=20; exact receipt clocks verified`.

The example checked contiguous capture sequence, current exchange/source identity,
collected raw market-frame receipt times, drained observation closure after
runtime shutdown, and required each normalized Trade receipt timestamp to match
an observed raw packet exactly. Public feed symbol hydration and actual WS
send/read hooks were used, not a synthetic producer.

This is one venue/channel transport observation, not a persisted raw file, raw
protocol replay, captured HTTP bootstrap, all-product/live-privacy certification
or L2 sequence-reconstruction result. Sanitization/private-topic/overflow/depth/
precision/reconnect failure cases are deterministic unit/session-double evidence.
The actual public payloads were not dumped into the repository.

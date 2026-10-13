# Dependency transport validation — 2026-10-13

Candidates: reqwest 0.13.5 (PR #1) and tokio-tungstenite/tungstenite 0.30.0
(PR #2), with the existing Rust 1.85 MSRV and no committed lockfile.
No exchange endpoint, normalized model, capability cell or native protocol
expectation changed.

- HTTP: one bounded public OKX spot catalog load through the SDK's direct
  transport succeeded with 1162 instruments. This exercised the explicit ring /
  static WebPKI configuration and normal certificate/hostname verification.
- WS: one bounded public Binance spot BTC-USDT Trade feed received 2 events and
  exited cleanly after a caller-owned shutdown signal. It used an explicit
  BTCUSDT native mapping and did not require catalog requests.
- Each observation had a 30-second application limit. Temporary examples were
  deleted after execution; no credentials or private payloads were used.
- These brief observations verify connection/data/shutdown paths, not sustained
  throughput, quota saturation, all venues or the full heartbeat interval.
  Payload-preserving/automatic Pong and overload behavior remain independently
  asserted by offline duplex tests.

Both candidates passed 837 all-feature workspace tests, strict Clippy/rustdoc,
no-default compilation and fresh-resolution Rust 1.85 all-target checks. PR #1
also passed all remote CI checks and was merged before final PR #2 integration.
The PR #2 fresh dependency set matched the tested set; PR #1's newly resolved syn
patch was retested. No test expected values or tolerances were relaxed.

Final remote results are tracked on [PR #1](https://github.com/crypto-2042/cryptofeed-rs/pull/1)
and [PR #2](https://github.com/crypto-2042/cryptofeed-rs/pull/2).

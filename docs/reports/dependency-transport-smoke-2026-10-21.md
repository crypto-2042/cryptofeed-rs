# WebSocket TLS validation — 2026-10-21

Candidate: direct and proxied WebSockets moved off tokio-tungstenite's automatic
connector (`rustls-tls-webpki-roots`, webpki-roots 0.26) onto the SDK's own
`Connector::Rustls` built from the static WebPKI roots (webpki-roots 1.0.10) and
ring provider already used by the HTTP client. No exchange endpoint, normalized
model, capability cell or native protocol expectation changed.

- Fresh dependency resolution (203 → 202 packages) no longer contains
  webpki-roots 0.26, and `cargo audit --deny warnings` (cargo-audit 0.22.2,
  advisory-db `e6f94f3760d5e6c54c1b05f204002263bc025524`, 2026-10-20) reports
  zero findings. Before the change the same resolution failed on
  RUSTSEC-2026-0381 (webpki-roots 0.26.11).
- WS: one bounded public Binance spot BTC-USDT feed (`binance_public` example,
  trade + ticker + L2 + candles) ran for 30 seconds under a caller-owned alarm,
  receiving 495 trades, continuous tickers and order-book updates with no
  transport or handshake errors, then exited on SIGALRM. This exercised the new
  explicit connector, SNI/hostname verification against the static roots, and
  the existing shutdown path.
- The proxied path was not exercised live against a proxy. Existing offline
  duplex/proxy tests cover CONNECT parsing and tunneled-byte preservation; they
  do not establish successful proxied certificate validation.
- This brief observation verifies connection, data and TLS trust paths, not
  sustained throughput, quota saturation, all venues, proxy deployments or the
  full heartbeat interval.

The same dependency set passed 853 all-feature workspace tests, strict Clippy and
rustdoc, `cargo check --workspace --no-default-features` and the `make features`
matrix. Cargo.lock remains ignored; no test expected values or tolerances were
relaxed.

## Patched-minimum review

Official RustSec DB `e6f94f3760d5e6c54c1b05f204002263bc025524` marks all
webpki-roots versions before 1.0.10 as affected. With the original `"1"`
requirement, `cargo update -p webpki-roots --precise 1.0.9` succeeded and
`cargo audit --deny warnings` reproduced RUSTSEC-2026-0381. The runtime now
requires `"1.0.10"`. Existing vulnerable lock selections are updated during
resolution; an explicit downgrade from the corrected graph is rejected:

```text
cargo update -p webpki-roots@1.0.10 --precise 1.0.9
error: failed to select a version for the requirement `webpki-roots = "^1.0.10"`
candidate versions found which didn't match: 1.0.9
```

This version floor is part of the published dependency metadata, not a committed
workspace lockfile. Original smoke observations above remain bounded evidence
for the explicit connector; the minimum-version review adds no live proxy claim.

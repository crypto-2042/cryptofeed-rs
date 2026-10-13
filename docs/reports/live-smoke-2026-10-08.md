# Public channel completion smoke — 2026-10-08

This smoke covers the newly enabled Bitget, OKX, and Gate.io public channels.
It is separate from the historical five-exchange release smoke. It does not
re-verify every existing exchange/product cell.

## Configuration and results

`cargo run -p cryptofeed-rs --example channel_completion_smoke` uses explicit
native symbols, skips L2/REST bootstrap, and requests clean shutdown after
45 seconds. The SDK normalized event stream reported:

| Exchange | Product / symbol | Channel | Events | Result |
| --- | --- | --- | --- | --- |
| Bitget v3 | spot / BTC-USDT | L1Book | 330 | normalized data received |
| Bitget v3 | USDT perpetual / BTC-USDT-PERP | L1Book | 1027 | normalized data received |
| Bitget v3 | USDT perpetual / BTC-USDT-PERP | Funding | 389 | normalized data received |
| Bitget v3 | USDT perpetual / BTC-USDT-PERP | OpenInterest | 389 | normalized data received |
| Bitget v3 | USDT perpetual / BTC-USDT-PERP | Index | 389 | normalized data received |
| Bitget v3 | USDT perpetual / BTC-USDT-PERP | MarkPrice | 389 | normalized data received |
| OKX v5 | SWAP / BTC-USDT-PERP → BTC-USDT index | Index | 119 | normalized data received under contract identity |
| Gate.io v4 | USDT perpetual / BTC-USDT-PERP | Liquidations | 0 | sparse stream; subscription acknowledgement verified separately |

The SDK run exited with status 0, shut down cleanly, and reported no terminal
feed failures. These counts measure received normalized events, not guaranteed
exchange completeness or delivery cadence.

Gate.io's identical unauthenticated `futures.public_liquidates` subscription
was also checked directly against the USDT perpetual endpoint. It returned
`event: subscribe`, `payload: ["BTC_USDT"]`, and `result.status: success`.
The acknowledgement is sanitized in `sample_data/gateio.ws.v4` and asserted
inline in `public_parity.rs`; connection and trace identifiers were removed.
No live liquidation payload was captured, so live liquidation normalization
is not claimed. Signed-size, timestamp, multi-contract filtering, and runtime
dispatch are covered by deterministic offline regressions.

## Offline validation

- `cargo test --workspace`: passed, including 260 runtime unit tests,
  74 public parity tests, and 8 feature-boundary tests.
- `cargo clippy --workspace --all-features --all-targets -- -D warnings`: passed.
- `cargo fmt --all --check`: passed.
- `cargo check --workspace --no-default-features` and every individual runtime
  feature (`make features`): passed. Existing warnings in isolated feature
  builds remain; the all-feature lint gate is clean.
- Each affected feature was separately tested without defaults via
  `cargo test -p cryptofeed-rs --no-default-features --features <feature>
  --test feature_boundaries`: orderbook, funding, openinterest, index,
  markprice, and liquidations passed.

## Limits and scope

- WebSocket DNS resolution failed inside the sandbox. The network smoke and
  direct Gate.io acknowledgement check ran outside the sandbox; HTTP access
  alone did not prove WebSocket connectivity.
- USDC/coin Bitget products and dated-futures identity are covered offline;
  this run exercises Bitget spot and USDT perpetual only.
- OKX shared-index fan-out to multiple dated futures is covered offline;
  this run exercises a single SWAP index mapping.
- Gate.io BTC perpetual liquidations are documented and covered by preflight
  and parser tests; this live acknowledgement uses USDT perpetual only.
- Gate.io delivery liquidations remain rejected pending separate protocol
  evidence. Binance OI is unchanged; no REST polling was added.

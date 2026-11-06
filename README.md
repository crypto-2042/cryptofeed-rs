# cryptofeed-rs

[English](README.md) | [简体中文](README.zh-CN.md)

Normalized, public-market-data feeds for cryptocurrency exchanges, written in
Rust with current official venue protocols.

`cryptofeed-rs` is the Rust counterpart to Python
[`cryptofeed`](https://github.com/bmoscon/cryptofeed): the same normalized event
model, but adapters target the current published exchange APIs. Python is a
semantic reference, not a protocol source of truth.

- **Scope:** public, unauthenticated market data. Private feeds, account data,
  order entry and trading are out of scope.
- **Products:** spot, perpetual/swap and dated futures across 13 venues.
- **Delivery:** bounded broadcast rings, readiness/retry supervision, managed
  runtime controls, recording/replay, L2 recovery and structured sinks.
- **Status:** `0.1.0-rc.1` evaluation candidate. See
  [RC scope and known limitations](docs/release-candidate.md) before relying on
  it for unattended production use.

## Quick start

Rust 1.85 or newer. All public-data features are enabled by default. Use a path
dependency for a local checkout, or pin a Git revision until the RC is
published:

```toml
[dependencies]
cryptofeed-rs = { path = "path/to/cryptofeed-rs/crates/runtime" }
async-trait = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use std::sync::Arc;

use async_trait::async_trait;
use cryptofeed_rs::prelude::*;

struct Print;

#[async_trait]
impl TradeHandler for Print {
    async fn on_trade(&self, trade: Trade) {
        println!("{} {} {}", trade.symbol.as_str(), trade.price, trade.amount);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut handler = FeedHandler::new();
    handler.add_feed(
        Binance::new()
            .trade()
            .symbol("BTC-USDT")
            .exchange_symbol("BTCUSDT")
            .trade_handler(Arc::new(Print))
            .build(),
    );
    handler.run().await?;
    Ok(())
}
```

`run()` installs a Ctrl-C handler. Embed `run_with_shutdown(handler, watch)` (or
the managed control API below) when the process owns its own lifecycle.

### Event stream instead of callbacks

Handlers run inside the feed session and should return quickly. Applications
that need slower processing can consume the bounded, lossy broadcast stream
instead:

```rust
let mut events = handler.subscribe();
handler.add_feed(Binance::new().trade().symbol("BTC-USDT").build());

tokio::spawn(async move {
    loop {
        match events.recv().await {
            Ok(FeedEvent::Trade(trade)) => println!("{} {}", trade.symbol.as_str(), trade.price),
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("consumer lagged; {n} events overwritten");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
});

handler.run().await?;
```

A slow broadcast consumer loses the oldest events by design; it never blocks
the feed. See [event delivery](docs/event-streams.md) for retention and
diagnostics.

### Managed runtime

Use `control_handle()` for add/replace/remove with authoritative readiness
state and connection-anchored events:

```rust
let mut handler = FeedHandler::new();
let control = handler.control_handle();
let mut events = handler.subscribe_anchored_with_diagnostics();
let (stop, shutdown) = tokio::sync::watch::channel(false);
let running = tokio::spawn(handler.run_with_shutdown(shutdown));

let identity = control
    .add_feed(Binance::new().trade().symbol("BTC-USDT").build())
    .await?;
let state = control.state(identity.id).await?; // Ready/Recovering/failed
let _ = (stop, running);
```

See [readiness](docs/readiness.md) and
[connection-anchored events](docs/readiness.md#connection-anchored-market-events).

## Core model

Ten normalized public event types, one crate each:

| Category | Type | Typical venues |
| --- | --- | --- |
| Ticker (BBO) | `Ticker` | Binance, Bitget, Bybit, OKX, Gate, KuCoin, Kraken, Coinbase, HTX, Hyperliquid |
| Trades | `Trade` | all supported venues |
| L2 book | `L2Book` (snapshot/delta) | all supported venues |
| L1 book | `L1Book` | venues with a real BBO-with-size channel |
| Candles | `Candle` | most venues (intervals vary) |
| Funding | `Funding` | derivative venues |
| Liquidations | `Liquidation` | venues with a public liquidation stream |
| Open interest | `OpenInterest` | derivative venues |
| Index price | `IndexPrice` | derivative venues (plus explicit OKX spot index) |
| Mark price | `MarkPrice` | derivative venues; Hyperliquid spot ctx |

- `Symbol` is product-qualified (`BTC-USDT` spot vs `BTC-USDT-PERP`
  perpetual) and carries no venue-specific separators.
- Prices and quantities are `rust_decimal::Decimal`; exchange and receive
  timestamps are separate. Native units are preserved unless the venue
  documents a conversion (for example KuCoin contracts vs base quantity).
- `FeedEvent` is non-exhaustive: downstream matches need a `_` arm.
- Capability preflight rejects unsupported product/channel/interval/depth
  combinations before any connection is opened.

## Supported venues

Capability rows are the same data the runtime validates against
(`markets::capability_matrix()`); a check means the public channel is
implemented and covered by a deterministic full-session double.

### Spot

| Exchange | Ticker | Trades | L2 | L1 | Candles | Index |
| --- | --- | --- | --- | --- | --- | --- |
| Binance | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| Bitget v3 | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| Bybit v5 | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| OKX v5 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ explicit index |
| Gate.io v4 | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| KuCoin UTA WS V2 | ✓ | ✓ | ✓ | — | ✓ | — |
| Kraken Spot WS v2 | ✓ | ✓ | ✓ | — | ✓ | — |
| Upbit quotation WS | — | ✓ | ✓ | — | ✓ | — |
| Bithumb public WS | — | ✓ | ✓ | — | — | — |
| Coinbase Advanced Trade | ✓ | ✓ | ✓ | ✓ | ✓ 5m only | — |
| HTX spot | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| Hyperliquid spot | ✓ | ✓ | ✓ | ✓ | ✓ | — |

### Perpetual / swap

| Exchange | Ticker | Trades | L2 | L1 | Candles | Funding | Liquidations | OI | Index | Mark |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance USD-M / coin-M | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ |
| Bitget v3 (USDT/USDC/coin) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bybit v5 linear / inverse | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| OKX v5 SWAP | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Gate.io v4 (USDT/BTC) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ sampled | ✓ | ✓ | ✓ |
| KuCoin UTA V2 | ✓ | ✓ | ✓ | — | ✓ | ✓ | — | ✓ | ✓ | ✓ |
| Deribit JSON-RPC v2 | ✓ | ✓ | ✓ | — | ✓ | ✓ | — | ✓ | ✓ | ✓ |
| HTX USDT-M | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — |
| Hyperliquid | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | ✓ |

### Dated futures

| Exchange | Ticker | Trades | L2 | L1 | Candles | Liquidations | OI | Index | Mark |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Binance USD-M / coin-M | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ |
| Bitget v3 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bybit v5 linear / inverse | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| OKX v5 FUTURES | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Gate.io v4 USDT delivery | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | ✓ |
| Deribit JSON-RPC v2 | ✓ | ✓ | ✓ | — | ✓ | — | ✓ | ✓ | ✓ |

Options, OKX MARGIN and L3 books are outside the 0.1 scope and fail preflight.
Funding exists only on perpetual/swap rows. A dash means the venue has no
applicable public channel or the SDK has no verified implementation.

Venue-specific notes that change how a cell must be read:

- **Korean venues:** Upbit/Bithumb public `ticker` frames have no best bid/ask,
  so no normalized `Ticker`/`L1Book`. Bithumb has no public candle stream.
- **KuCoin:** no standalone L1 channel; `obu depth=1` is snapshot-only L2.
  Futures `mark-price` splits into mark/index/open interest; contract
  quantities stay contracts unless converted with catalog metadata.
- **Kraken:** spot-only; `book` is CRC32-verified over the top ten levels;
  `ticker` is not an `L1Book` channel.
- **Coinbase:** public candles are 5-minute only; `level2` recovery is
  reconnect-based (no per-product sequence).
- **Deribit:** options are rejected; `book` uses change-id chaining.
- **HTX:** funding/liquidations ride a separate notification host; mark price,
  index and open interest have no current public topic.
- **Hyperliquid:** `l2Book` is a whole-book snapshot (20 levels, or 5 with
  `fast`), never a synthetic delta stream; no public liquidations.
- **Gate.io:** perpetual liquidations are a sampled public stream; delivery
  liquidations are rejected.
- **Binance:** contract open interest has no verified public WS path and is not
  polled behind the scenes.

Each venue has a protocol baseline in [docs/](docs/README.md) recording the
official wire format, acknowledgements, liveness and known gaps.

## Feature guide

### Handlers

Every category has a handler trait. Register one primary handler per category
and any number of additional handlers; callbacks run serially with a
five-second default timeout and catchable-panic isolation. See
[handler semantics](docs/handlers.md).

```rust
let feed = Okx::new()
    .trade()
    .l2_book()
    .symbol("BTC-USDT")
    .trade_handler(Arc::new(Print))
    .add_orderbook_handler(Arc::new(Print))
    .build();
```

### Readiness, retries and shutdown

Managed feeds expose `Preparing → Connecting → Subscribed → Ready`, failure and
recovery states, per-connection subscription confirmation, epochs and
book-recovery diagnostics. Transient errors reconnect with bounded backoff;
terminal configuration/protocol errors are not retried indefinitely. See
[readiness](docs/readiness.md) and [runtime options](docs/runtime-options.md).

### Recording and replay

Opt-in `recording` feature writes sanitized raw or normalized streams with
replay support and HTTP catalog replay. See
[recording](docs/raw-recording.md) ([HTTP captures](docs/http-recording.md))
and [replay](docs/raw-replay.md) ([HTTP replay](docs/http-replay.md)).

### L2 recovery and delivered depth

Opt-in L2 handles provide an atomically copied snapshot plus a bounded update
stream, with explicit invalidation when a gap cannot be bridged. Delivered
top-N depth is consistent across snapshots/deltas. See
[L2 recovery](docs/l2-recovery.md) and
[delivered depth](docs/book-delivery-depth.md).

### Sinks

`run_sink` drives a sequential, fallible `EventSink` from the normalized event
stream. A broadcast lag is terminal for a sink, never a silently shortened
capture. See [sinks](docs/sinks.md).

### Catalogs and REST

Product-qualified catalog discovery, explicit/`*`/`?` symbol selection,
`MarketInfo` constraints and a `PublicRestClient` for ticker/book snapshots are
available without opening a WebSocket. See [discovery](docs/discovery.md),
[market metadata](docs/market-metadata.md) and [public REST](docs/public-rest.md).

### Transport and proxies

`TransportConfig` applies explicit proxy routing consistently to catalogs, REST
snapshots and WebSocket connections, with direct defaults. See
[transport](docs/transport.md).

## Instrument naming and tokenized stocks

Native venue formats differ (`BTCUSDT`, `BTC-USDT`, `BTC_USDT`, `BTC/USD`,
`KRW-BTC`, lowercase `btcusdt`, Hyperliquid `BTC` / `PURR/USDC`). The SDK
normalizes base/quote and product kind, while the catalog keeps the exact
native mapping. See [instrument naming](docs/instrument-naming.md) for the
per-venue reference.

Several venues also list **tokenized stocks / RWA equity products** as spot
pairs, each with its own naming scheme:

| Venue | Pattern | Example |
| --- | --- | --- |
| Binance | `<TICKER>B` | `NVDAB`, `AAPLB` |
| OKX | `X<TICKER>` | `XAAPL` |
| Bybit, KuCoin, Gate | `<TICKER>X` | `AAPLX`, `TSLAX` |
| Bitget | `r<TICKER>` | `rAAPL` → `RAAPL-USDT` |
| Gate (Ondo) | `<TICKER>ON` | `TSLAON` |
| Gate (leveraged) | `<TICKER>3L` / `3S` | `AAPL3L` |
| HTX | unmarked lowercase | `aapl`, `msft` (mostly offline) |

**These products are not normalized together with crypto assets.** They are not
US equity market data: there is no SIP/consolidated tape, no pre/post-market or
auction feed, and no corporate-action stream. The same underlying can exist as
multiple issuer tokens with different prices and liquidity (`TSLAX` vs
`TSLAON`), and leveraged tokens (`3L`/`3S`) are rebalanced products, not share
exposure. Mapping them to a single equity-style symbol would silently invent
semantics the SDK cannot verify, so no such unification, underlying mapping or
special filtering is implemented. If a catalog includes a tokenized-stock pair,
it behaves as a venue-native spot symbol with that venue's hours, spreads and
issuer risk.

## Verification

CI runs, and contributors can reproduce locally:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
cargo +1.85.0 check --workspace --all-features --all-targets   # MSRV
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

The `features` CI job additionally builds and tests isolated feature
combinations. Deterministic coverage includes per-venue parser fixtures,
catalog/native-mapping validation, capability preflight and full-session
WebSocket doubles (subscribe, ack/control, data, recovery, clean shutdown).
Fixture provenance and live verification notes live in
[`sample_data/SOURCES.md`](sample_data/SOURCES.md) and the per-venue protocol
documents. No live-network test runs in CI, and a checked capability row is not
a sustained-run qualification.

## Workspace

| Crate | Purpose |
| --- | --- |
| `cryptofeed-core` | `ExchangeId`, `Channel`, `Symbol`, `Side`, errors |
| `cryptofeed-ticker` … `cryptofeed-markprice` | normalized models + handler traits per category |
| `cryptofeed-rs` | runtime: adapters, transport, routing, readiness, recording, sinks |

## Documentation

- [Documentation index](docs/README.md)
- [Parity checklist](PARITY.md)
- [Exchange protocol baseline](docs/exchange-protocol-baseline.md)
- [Instrument naming](docs/instrument-naming.md)
- [Readiness](docs/readiness.md) · [Event streams](docs/event-streams.md) ·
  [Handlers](docs/handlers.md) · [Runtime options](docs/runtime-options.md)

## License

MIT. See [LICENSE](LICENSE).

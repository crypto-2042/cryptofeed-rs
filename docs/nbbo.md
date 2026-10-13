# Cross-exchange best bid/ask

`Nbbo` (feature `orderbook`) selects the highest bid and lowest ask for one exact
normalized symbol, using bounded, explicitly scoped sources. Python's
`nbbo.py::_update` is the price/selected-size reference. The Rust helper adds
source generation, declared quantity units, freshness and explicit withdrawal.
It is a local comparison, not a regulated consolidated market feed or trading
signal, and makes no exchange connections itself.

## Inputs and source compatibility

Construct `Nbbo::new(symbol, sources, max_age)` with 1..=64 distinct feed IDs,
positive maximum age, and a public Spot/Perpetual/Futures symbol. Each
`NbboSource` declares the exact `FeedIdentity`, exchange and native L1 quantity
unit (`Base`, `Quote` or `Contracts`). The caller verifies price comparability and
quantity labels against its feed/catalog metadata; the helper does not infer
contract multipliers or settlement conventions from symbol spelling.

`update(identity, &L1Book, observed_at, now)` accepts a complete two-sided L1
snapshot with positive prices/sizes, finite model clocks and bid <= ask. Symbol,
product, quote currency, exchange and generation must match the configured
scope. Use separate helpers for BTC-USDT, BTC-USD, perpetual and dated futures;
there is no implicit currency conversion or product merging.

The selected bid and ask each retain their own source, native amount/unit,
original exchange/receive clocks and observation time. Sizes are never added
across sources or used to break price ties. `Contracts` does not imply equal face
values across venues; inspect `MarketInfo` before interpreting them. This permits
price comparison without pretending different contract sizes are interchangeable.
Equal prices select the first source in constructor order, independent of arrival
order. Cross-exchange bid > ask is reported as `crossed=true`. A crossed single-
source snapshot is rejected; an exactly locked single-source quote is accepted.

## Minimal use

```rust,no_run
use cryptofeed_rs::prelude::*;
use std::time::Duration;
# fn example(source: NbboSource, input: &L1Book) -> Result<(), Box<dyn std::error::Error>> {
let mut best = Nbbo::new(input.symbol.clone(), vec![source], Duration::from_secs(5))?;
// Use the same monotonic origin for observation and consumption times.
let observed_at = Duration::from_millis(10);
let now = Duration::from_millis(12);
if let Some(change) = best.update(source.identity, input, observed_at, now)? {
    if let Some(quote) = change.quote {
        println!("{} bid {} / ask {}", change.symbol, quote.bid.level.price, quote.ask.level.price);
    }
}
let withdrawal = best.advance(Duration::from_secs(6))?;
# let _ = withdrawal;
# Ok(())
# }
```

Keep the helper across input. FeedHandler setup remains the existing builder →
add feed → run path; `add_feed_with_id` exposes the initial ID (generation 1),
and managed control exposes later identities. The bounded public example uses
Binance/OKX Spot native L1:

```bash
cargo run -p cryptofeed-rs --example nbbo_public
```

It accounts for local receive-to-consume delay, waits for both sources and twenty
best changes, suspends both sources, then shuts down the runtime. It is a bounded
smoke example, not a general lifecycle controller. Production consumers must
handle status/readiness, lag, replacement, reconnect and clock-domain mapping.

For Python-style L2 input, reconstruct full state first using
[L2 recovery](l2-recovery.md). Derive L1 from the best bid/ask of the synchronized
full book, retaining its original clocks. A raw delta's first entries are not
necessarily best prices. On `BookUpdate.book=None`, anchor discontinuity, lag,
empty side or resync, suspend the source and recover a fresh synchronized state
before resuming. Never refresh observation age merely by rereading an unchanged
cached book. The helper does not reconstruct L2 or verify book anchors itself.

## Freshness and lifecycle

Both `observed_at` and `now` are caller-owned monotonic elapsed times from one
origin. `observed_at` is the source observation, not queue dequeue time. Older
queues must retain their earlier observation time. Passing `now` for both values
would hide queue delay. Model exchange time is never used as a freshness clock.
The public example maps local wall receipt time to elapsed age; local wall-clock
jumps are errors there. Applications may instead capture monotonic ingress time.

- Source age >= max_age is stale; stale or future observations are rejected.
- Consumer time cannot go backwards, and one source's observation time cannot
  decrease. Rejected inputs leave all state and clocks unchanged.
- Call `advance(now)` on timer ticks even without packets or after invalid input.
  Expired sources fall back to fresh peers; expiry of the last source emits an
  explicit `NbboUpdate { quote: None, ... }`.
- `set_available(identity, false, now)` removes the quote and rejects queued input
  until explicit resume. Use it on disconnect, gap, removal, stop, unusable book
  or loss of readiness. Resume clears stale state and requires observations at or
  after resume time. Only supply a fresh complete snapshot after recovery; equal
  timestamps do not prove packet/anchor ordering.
- `replace_source(old_identity, new_source, now)` requires a newer generation of
  the same feed ID. It withdraws the old quote, pins the new exchange/unit/scope,
  and rejects buffered old-generation input. Construct a new helper if the fixed
  source roster or symbol changes.

The helper owns no status stream, timer, callback, queue or network task. It cannot
infer transport failure from an absence of packets sooner than the age limit.
Callers own lifecycle signals and forwarding every returned withdrawal/change.

## Change delivery and replay

A change is emitted when selected source, price or size changes, or all quotes
become unavailable. Clock-only refreshes and nonselected changes are suppressed,
as in Python's economic-change callback. These are not keepalives: per-leg clocks
in an emitted change describe that change, not every later refresh.

For deterministic replay, provide the same source observations, consumer times,
expiry ticks and availability/generation transitions. Normalized recordings alone
do not store every lifecycle signal or a monotonic socket-ingress clock. Using
record elapsed time for both clocks is a consumer-time replay policy, not proof
of original live queue age. Offline tests compare every emitted field when the
explicit original inputs/times are provided identically. No live fallback occurs.

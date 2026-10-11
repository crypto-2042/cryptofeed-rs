# Throttle and trade aggregation

`Ohlcv` aggregates normalized trades into bounded local bars. It is available with
`trade`; it does not require `candles` or a storage feature. The local `TradeBar`
is distinct from an exchange `Candle`: its boundaries are elapsed consumer time,
not Unix timestamps or exchange candle schedules.

Python's `backends/aggregate.py::OHLCV` is the semantic reference: prices follow
arrival order, volume sums trade amounts, and VWAP weights prices by amounts.
The Rust helper uses an explicit monotonic clock and source isolation to support
repeatable live/offline calculations. It does not copy the Python example's
symbol-only state or move the next window's origin on every delayed callback.

## Use with live events or replay

```rust,no_run
use cryptofeed_rs::prelude::*;
use std::time::Duration;

# fn example(envelope: FeedEnvelope) -> Result<(), Box<dyn std::error::Error>> {
let mut bars = Ohlcv::new(Duration::from_secs(60), 256)?;
let origin = tokio::time::Instant::now();
if let FeedEvent::Trade(trade) = envelope.event {
    for closed in bars.push(envelope.identity, &trade, origin.elapsed())? {
        println!("{}: {} trades, VWAP {}", closed.symbol, closed.trades, closed.vwap);
    }
}
// In a long-lived consumer, call advance(origin.elapsed()) on timer ticks too.
let closed = bars.advance(Duration::from_secs(60))?;
let partial = bars.finish();
# let _ = (closed, partial);
# Ok(())
# }
```

Keep one aggregator across events. When consuming a live identified broadcast,
handle lag as a gap: stop/discard the affected aggregate or explicitly label the
output incomplete. The helper does not recover missing trades, reorder events,
deduplicate IDs, receive lifecycle notifications or guarantee complete markets.
It makes no network requests and creates no tasks. Applications own timer ticks,
shutdown and delivery of returned bars to their handler/storage code.

For a normalized trade recording, use `Duration::from_nanos(record.elapsed_ns)`
with each `RecordedEvent`. The same inputs/clocks produce identical bars.

```bash
cargo run -p cryptofeed-rs --features recording --example aggregate_replay -- TRADE_RECORDING 60
```

This offline example checks the strict recording footer, aggregates each trade,
then verifies accepted trade counts and volume totals. It finishes the last window
as partial; EOF alone is not evidence that its time boundary was crossed.

## Clock, identity and limits

- Windows are positive whole seconds, aligned to elapsed zero, with `[start,end)`
  boundaries. A trade exactly at `end` belongs to the next window.
- `push` closes the preceding populated window before inserting the boundary
  trade. `advance(now)` closes it without needing another trade. Large jumps emit
  only populated bars, never empty filler candles.
- Consumer time must not move backwards. Exchange/receive timestamps may arrive
  out of order and are retained separately as first/last arrival clocks; they do
  not choose the window or reorder open/close.
- Each `(feed ID, generation, exchange, normalized symbol)` has independent state.
  A generation change creates a separate series; the caller may finish/discard an
  old aggregator when replacing a feed. Sources are never blended by symbol alone.
- One window holds at most the configured 1..=4096 series. Capacity errors leave
  state unchanged, and closing a window releases its series capacity. Output
  follows first-seen series order. There is no queue of all previous windows.
- `finish` consumes the aggregator and returns remaining bars with `closed=false`.
  `closed=true` means only that the caller clock crossed the boundary, not that
  the source had no gaps. Returned values must be delivered by the caller.

## Arithmetic and quantity units

`open/high/low/close` retain the Decimal trade prices. `volume` sums normalized
amounts; `price_volume` sums price times amount; `vwap` divides that sum by volume.
For derivatives, amount may represent contracts. No base-coin conversion,
contract multiplier application, cross-source weighting or quote-volume claim
is made. `price_volume` is an arithmetic weighted sum, not necessarily turnover.

All arithmetic uses checked `rust_decimal` operations, without float conversion.
Decimal's finite precision/rounding applies to multiplication, addition and
nonterminating division (up to 28 decimal places). A single trade's VWAP retains
its price. Overflow, a positive product rounded to zero, invalid price/amount,
nonfinite model clocks, invalid source generation and unrepresentable boundaries
fail explicitly. Rejected inputs do not mutate state, advance the clock or lose
an older window awaiting emission. No division by zero is permitted.

## Leading-edge throttle

`Throttle` is available with any feature set, including no data features. Pass a
positive `Duration` to `new`, then monotonic elapsed time to `allow`:

```rust
use cryptofeed_rs::Throttle;
use std::time::Duration;
let mut throttle = Throttle::new(Duration::from_secs(1)).unwrap();
assert!(throttle.allow(Duration::ZERO).unwrap());
assert!(!throttle.allow(Duration::from_secs(1)).unwrap());
assert!(throttle.allow(Duration::from_millis(1001)).unwrap());
```

The first input passes; later input must be **strictly more** than the interval
since the last accepted input, as in Python's `Throttle`. Dropped input does not
slide that interval. Backwards time is an error and leaves state unchanged.
Subsecond durations are supported. The helper has constant state and no clock,
queue, trailing update, timer, callback or I/O of its own.

One instance throttles its entire input stream. Use separate instances if sources
need independent rates. The caller chooses whether to deliver an event after
`allow` returns true. This is intentional sampling: apply it after L2
reconstruction, not to the deltas required to maintain a book; do not discard
trades before an OHLCV/Renko calculation that requires every input.

## Threshold-triggered Renko

`RenkoFixed` (feature `trade`) implements the price rules of Python's
`RenkoFixed._agg`. Its size is a positive Decimal price threshold, not a tick
count or guarantee that output brick spans equal the configured size.

```rust,no_run
use cryptofeed_rs::prelude::*;
# fn example(identity: FeedIdentity, trade: &Trade) -> Result<(), Box<dyn std::error::Error>> {
let mut renko = RenkoFixed::new("10".parse()?, 256)?;
// Keep this instance across trades; the first price seeds an anchor.
if let Some(brick) = renko.push(identity, trade)? {
    println!("{}: {} -> {} {:?}", brick.symbol, brick.open, brick.close, brick.direction);
}
# Ok(())
# }
```

Each `(feed ID, generation, exchange, symbol)` has independent anchors, running
extrema and direction. For a size of 10, input 100, 111, 120, 121, 115, 101, 100,
91, 101, 111 produces:

| Open | Close | Direction |
| --- | --- | --- |
| 100 | 111 | Up |
| 111 | 121 | Up |
| 111 | 101 | Down |
| 101 | 91 | Down |
| 101 | 111 | Up |

The first price seeds state and emits nothing. A movement equal to the threshold
triggers. A continuation measures from the previous close and opens there; a
reversal measures from the previous open and retains that open. A gap emits one
brick whose close is the current trade price: 100 -> 135 emits one 100/135 brick,
not three synthetic 10-point bricks. After that gap a reversal requires reaching
90 (previous open 100 minus threshold 10), not merely retracing 20 from 135.
These are the inspected Python price rules, not conventional fixed-step charting.

Rust returns a brick immediately on the triggering input. It does not reproduce
the Python callback wrapper's initial empty dictionary notification or next-input
delivery delay. Pending below-threshold state is not emitted as a partial brick.
Original triggering exchange/receive clocks are retained; late clocks do not
reorder prices. Side/amount/ID do not participate in this price-only calculation.

The configured series cap is 1..=4096. Capacity, invalid source/price/clock and
checked Decimal difference errors leave state unchanged. `remove(identity,
exchange, symbol)` discards one gapped/retired series and frees capacity; its next
trade seeds a new anchor. Old generations are not automatically removed. Recreate
the helper to discard all state. There is no missing-trade recovery, deduplication,
network call, timer or output queue. Consumers own lifecycle and lag handling.

Custom callback aggregation and NBBO remain separate work.

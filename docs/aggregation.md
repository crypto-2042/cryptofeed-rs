# Trade OHLCV aggregation

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

Throttle, Renko, custom callback aggregation and NBBO remain separate work.

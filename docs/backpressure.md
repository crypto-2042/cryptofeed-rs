# Bounded asynchronous handler dispatch

By default each normalized event is delivered to user handlers inline: the
session read loop awaits every registered callback (bounded by
[`RuntimeOptions::handler_timeout`](runtime-options.md), five seconds by
default) before continuing to parse messages, answer heartbeats or publish to
the bounded broadcast rings. A slow handler therefore delays the feed read
loop, can create exchange-side backlog and can lead to disconnects.

`HandlerDispatch::Async { capacity }` is the opt-in alternative. It decouples
handler execution from ingestion with a small bounded queue and explicit
per-channel overflow semantics. `Inline` remains the default and keeps today's
exact behavior, ordering and losslessness.

```rust
use cryptofeed_rs::prelude::*;

let options = RuntimeOptions::default()
    .handler_dispatch(HandlerDispatch::Async { capacity: 1024 })?
    .handler_timeout(std::time::Duration::from_millis(250))?;
let feed = Binance::new()
    .trade()
    .ticker()
    .symbol("BTC-USDT")
    .runtime_options(options)
    .trade_handler(trade_handler)
    .ticker_handler(ticker_handler)
    .build();
```

Capacity is validated by `RuntimeOptions::handler_dispatch`: zero and values
above `HandlerDispatch::MAX_CAPACITY` (one million) are rejected before a feed
is built. `RuntimeOptions::handler_dispatch_policy()` returns the configured
policy.

## Execution model

- One background task per logical feed executes callbacks sequentially. A
  sharded logical feed (multiple physical connections) still uses exactly one
  dispatcher; connection clones share it.
- The dispatcher captures clones of the registered handler `Arc`s when the
  runtime starts. Handlers registered after `run`/`run_with_shutdown` begins
  are not delivered in async mode; register them before adding or running the
  feed.
- Ingestion performs a short synchronous enqueue under a small mutex. It never
  awaits user code in async mode. Broadcast publication, `EventCounters`,
  recording/sink semantics and the per-`FeedHandler` event stream are
  unchanged and still happen in ingestion.
- The task drains coalesced state entries and up to 256 event entries per
  pass. Delivery order is the drain order: state channels first (fixed channel
  order, entries ordered by symbol), then event channels in FIFO order. Each
  callback still has its own timeout and panic isolation and runs inside the
  same bounded shutdown machinery as inline mode.
- The consumer task stops with the feed/connection shutdown signal and its
  `JoinHandle` is held by the runtime. Queued entries are flushed best-effort
  within the shared shutdown grace period; remaining entries are discarded
  when that grace expires. In-flight callbacks can be cancelled on abort.

## Per-channel policy

| Channel | Async policy | Loss semantics |
| --- | --- | --- |
| Ticker, L1 book, Funding, Open interest, Index, Mark price, Candles | Latest value per `(symbol[, candle interval])` | Intermediate values are superseded (`coalesced` counter) |
| L2 book, book store configured | Latest applied full `L2Book::Snapshot` per symbol, read from the retained `BookStore` at delivery time | Intermediate applied states are superseded (`coalesced`) |
| L2 book, no book store | Bounded FIFO of raw events | Oldest dropped on overflow (`dropped_oldest`) |
| Trade, Liquidations | Bounded FIFO of events | Oldest dropped on overflow (`dropped_oldest`) |

The L2 coalescing path requires the opt-in managed book store
(`FeedHandler::l2_book_handle()`, [`L2BookHandle`](l2-recovery.md)). Without a
configured store the dispatcher falls back to a lossy event ring and delivers
the raw normalized event unchanged. L2 state application itself still happens
in ingestion before enqueue; the dispatcher never re-applies book state.

## Guarantees

- **Ingestion never blocks on user code in async mode.** Parsing, book
  application, broadcast publication, counters and heartbeats continue while a
  callback is running.
- **Memory is bounded.** The state maps are bounded by the subscribed
  symbols/channels; each event ring is bounded by `capacity`; no unbounded
  channel or detached task is created.
- **Loss is explicit and observable.** Trade/liquidation delivery can lose
  events under sustained overload (oldest dropped, counted). State channels
  deliver latest values only. L2 requires the book store for coalescing.
- **Inline mode remains lossless but can block ingestion.** The default is
  unchanged: each callback is awaited in session order with its deadline.

There is no exactly-once or end-to-end acknowledgement claim. Trade and
liquidation users that cannot tolerate loss must use a durable queue outside
the SDK. Coalesced state channels are appropriate for latest-value consumers;
they never replay intermediate values.

## Diagnostics

`FeedHandler::handler_dispatch_stats()` returns a `HandlerDispatchStats`
snapshot, and `FeedHandler::handler_dispatch_stats_handle()` returns a
cloneable handle that stays readable while feeds run and after shutdown
(counter values are shared across the feeds of the handler).

- `delivered(channel)`: events handed to at least one handler.
- `coalesced(channel)`: state updates superseded before delivery.
- `dropped_oldest(channel)`: event-ring entries dropped on overflow.
- `queued()` / `peak_queued()`: current and high-water aggregate queue depth.
- `lost()`: coalesced plus oldest-first overflow losses.

The first overflow per dispatcher emits a rate-limited `tracing::warn`
(at most one per five seconds) with the channel and retained drop count; there
is no per-event logging. Inline mode leaves every counter at zero. Counters
are producer-side dispatch telemetry, not exchange delivery evidence.

## Verification

Offline tests cover state coalescing (latest value only), event-ring overflow
with oldest-first drops and counts, L2 coalescing through an applied book-store
snapshot, raw L2 fallback without a store, capacity validation, dispatcher
shutdown ending the consumer task, a full-session double proving ingestion
keeps reading (and broadcast publication keeps up) while a handler sleeps, drop
counters after overflow, and unchanged inline blocking behavior.

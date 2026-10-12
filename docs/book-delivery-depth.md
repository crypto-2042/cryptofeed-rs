# Maximum delivered L2 depth

With the `orderbook` feature, `.l2_book_max_depth(N)` limits the consumer-visible
book to at most N best levels **per side**. Zero disables this policy, as does
omitting it. It is separate from `.l2_book_depth(...)`, which selects a supported
native subscription depth. A delivery limit never requests a different stream or
fetches extra levels beyond the data supplied by that native stream.

```rust,no_run
use cryptofeed_rs::prelude::*;
let feed = Bybit::new().l2_book().symbol("BTC-USDT")
    .l2_book_depth(50)       // native stream, validated by capability preflight
    .l2_book_max_depth(10)   // delivered view; 0 disables trimming
    .build();
let mut handler = FeedHandler::new();
handler.add_feed(feed);
```

The semantic reference is sibling Python commit `3a6d3ca`, `feed.py::max_depth`,
which describes the maximum levels returned per side. Rust keeps the explicit
snapshot/delta API: the consumer reconstructs the limited book from its events.
Native sequence bridging, checksum state and the runtime's full local book remain
untrimmed. This policy limits delivered state, not internal native-book memory.

## Correct snapshot and delta membership

A native Snapshot yields a fresh sorted top-N Snapshot. For each accepted Delta,
the runtime computes the new top-N from the full reconstructed book and compares
it to the last delivered view. It emits zero-quantity removals for prices leaving
the view, current quantities for prices entering/changing, and promotions of
unchanged deeper levels when a better level disappears. Merely slicing native
delta arrays would miss those transitions and is deliberately not used.

Example at depth two:

| Side | Previous delivered levels | Native changes | Delivered delta | Reconstructed levels |
| --- | --- | --- | --- | --- |
| Bids | 100:1, 99:2; full state also contains 98:3 | Delete 100 | 100:0, 98:3 | 99:2, 98:3 |
| Asks | 101:1, 102:2 | Insert 100.50:4 | 100.50:4, 102:0 | 100.50:4, 101:1 |

Bids are ordered descending and asks ascending, including delta operations. A
Delta can contain up to 2N changes per side because it may remove an entire old
view and add a new view; the limit applies to reconstructed state, not delta
array length. A deep-only update can produce an empty Delta. It is still
published with the original event clocks and advances producer counters/recovery
revisions; it is not silently dropped. Equal numeric quantities with a changed
Decimal scale are emitted so the consumer retains the current quantity scale.
No float conversion or unit conversion is performed.

The same projected event reaches callbacks, ordinary/identified broadcasts,
normalized recording and the optional L2 recovery cache. Recovery snapshots are
therefore limited too. The full runtime/native book remains available for
synchronization and [crossed-book checking](book-cross-check.md), which runs
before projection. No L1/ticker/REST behavior changes.

## Lifecycle and resources

Projection state retains at most N levels per side per active subscribed symbol
in addition to existing full books. Unchanged defaults allocate no projection
entries. A new Snapshot always replaces the view. Resync, connection reset,
crossed-book failure and fresh managed-generation state clear the relevant
projection. Clearing one connection/symbol does not clear other active views.

While limited mode has no initialized view, a Delta returns recoverable
MalformedData before publication and clears the affected state. It cannot
manufacture a usable Snapshot out of a delta or revive an invalidated recovery
book. The native runtime normally supplies a new Snapshot first; this guard also
covers malformed/offline sequences. Existing retry/shutdown policies apply.
Snapshot copies use bounded BTreeMap iteration (`to_snapshot_with_depth`) rather
than constructing the full arrays then truncating them. No throughput or total
native-book memory guarantee is inferred from this delivery setting.

## Recording and replay

Raw feed metadata stores an optional positive integer `l2_book_max_depth`.
Missing/null means unlimited; the builder's zero maps to that omitted state.
An explicitly encoded zero is invalid metadata. Native raw replay restores the
recorded policy and runs the same publication projection, including top-level
deletions and promotions. Strict older SDK readers may reject captures carrying
the new field. Default/unlimited captures retain their prior metadata shape.
Normalized replay delivers the already projected recorded events without
reapplying a feed policy or inventing missing levels.

## Evidence

Ten new offline regressions cover defaults/zero, independent native depth and
sparse planning, sorted bounded snapshot copies, delete/promote/displace,
quantity scale, empty sides/deep-only changes, strict initialization, resync/
connection/generation clearing, exact pairs, crossed-book interaction, all output
surfaces/recovery and native Bybit replay metadata. A deterministic delta oracle
applies each projected event to an independent consumer book and compares it to
independently selected full-book top levels after every update for N=1,2,3,8.

`sample_data/public-l2.delivery-depth.jsonl` records representative asserted
cases, not live traffic. The Bybit frame shape follows the same current official
[orderbook contract](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)
verified for the crossed-book increment. No parser, endpoint or native bridge rule
changed, and no live exchange load was generated.

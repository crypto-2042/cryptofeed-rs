# Optional crossed-L2-book checking

With the `orderbook` feature, `.l2_book_cross_check(true)` opts one feed into the
Python `cross_check` rule: reject a reconstructed L2 book when both sides exist
and the best bid is **greater than or equal to** the best ask. The default is
false, preserving existing Rust behavior and Python's disabled default. An empty
side does not overlap. Some venues/products can legitimately display overlapping
prices during auctions/pre-open; enabling the policy is an application choice.

```rust,no_run
use cryptofeed_rs::prelude::*;
let feed = Okx::new().l2_book().symbol("BTC-USDT")
    .l2_book_cross_check(true).build();
let mut handler = FeedHandler::new();
handler.add_feed(feed);
```

## Publication and failure

All five active exchange L2 dispatch paths return a result. After native
snapshot/delta/sequence/checksum processing and exact subscription filtering,
the common dispatcher applies the event to its complete local book, then checks
the two best Decimal prices. It does not compare only the levels changed in a
delta, convert prices to float or trim the state before checking.

A failure returns recoverable `Error::MalformedData` containing the normalized
symbol and `crossed L2 book`. Before returning it clears that symbol's local/native
book-sync state and withdraws its retained recovery book and synchronization
readiness. The rejected event reaches no handler, producer counter, ordinary or
identified broadcast, or usable recovery snapshot. A recovery withdrawal is a
lifecycle notification, not publication of the rejected prices.

The error propagates through parsing/session consumption into existing retry
supervision. Session disconnect then invalidates every book owned by that
physical connection, and a fresh attempt must bootstrap/snapshot again. Other
feeds remain independent. Existing retry limits and shutdown still apply; this
policy does not introduce an unlimited retry override. Before that disconnect,
clearing the affected symbol does not remove another symbol or unrelated feed.

Native sequence and checksum checks remain active regardless of this flag. It
adds a semantic policy; it does not replace integrity checks or relax native gap
rules. It applies to L2, not L1, ticker, REST snapshot fetching or authenticated
feeds. Standalone consumers can call `L2BookState::is_crossed()` on their own
reconstructed state. Native subscription depth is unchanged; [maximum delivered depth](book-delivery-depth.md) is a separate implemented
policy applied after this check.

## Recording and replay

Raw feed metadata carries `l2_book_cross_check` only when true. Deserialization
accepts older records missing it as false and requires a JSON boolean when
present. Native raw replay restores the recorded setting onto its fresh feed and
uses the same dispatcher: a rejected snapshot/delta returns an error before model
emission. Replay does not simulate reconnect timing or invent a successful footer.
Older SDK readers with strict metadata schemas may reject captures containing the
new enabled field. Default-false metadata retains its previous encoded shape.
Normalized recordings contain only events already published by the live feed;
they do not re-run feed policies when delivered to a callback.

## Evidence

Nine focused cross-check tests cover exact precision beyond float resolution,
unsorted sides, equality, empty/deleted sides, default compatibility, sparse
planning preservation, all five exchange event dispatchers, full delta state,
subscription filtering, every publication surface, recovery/readiness withdrawal,
fresh epochs, other-symbol/feed isolation, existing supervisor retries and native
Bybit replay with old/enabled metadata. They are offline synthetic cases, not
claims of an observed exchange failure. No adapter parser or native sequence rule
changed. The Bybit test envelope follows the current official
[orderbook snapshot/delta contract](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook),
verified 2026-10-12; its equality case is intentionally synthetic.

`sample_data/public-l2.cross-check.jsonl` records representative asserted cases.
The semantic reference is sibling Python commit `3a6d3ca`,
`cryptofeed/feed.py::book_callback/check_bid_ask_overlapping`.

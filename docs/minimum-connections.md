# Proven minimum connection planning

`RuntimeOptions::minimum_connections(Some(maximum_evaluations))` enables exact
minimum physical-connection planning for one logical feed. None is the default
and preserves [route-first contiguous planning](connection-planning.md). Zero is
rejected; packing_evaluation_limit exposes the configured work budget.

```rust,no_run
use cryptofeed_rs::prelude::*;
# fn configure() -> Result<FeedHandler, Box<dyn std::error::Error>> {
let options = RuntimeOptions::default().minimum_connections(Some(100_000))?;
let mut handler = FeedHandler::new();
handler.add_feed(Bitget::new().trade().l2_book()
    .symbols(["BTC-USDT", "ETH-USDT"])
    .runtime_options(options).build());
# Ok(handler) }
```

The normal FeedHandler path validates capabilities and hydrates native mappings
before planning. Managed add/replace validates the candidate before stopping the
old feed. Insufficient search work returns a configuration error with “optimality
not proved”; it never silently returns the best unproven layout found so far.
The caller can increase the budget or explicitly select None. Existing native
and process/scoped connection caps remain enforced after compilation.

## Objective and constraints

A successful minimum-policy result proves the fewest physical connections under
the current SDK model:

- One logical ExchangeFeed, with its exact channel/symbol pairs and native names.
- The existing adapters' fixed product/public/business/settlement routes.
- All requested channels of one instrument on the same route stay together.
- Existing native-topic/serialized-message/SDK resource budgets and aliases.

One instrument exceeding a budget is still rejected. Logical product kinds must
remain homogeneous. Different logical feeds, alternate venue subscription APIs,
channel-splitting of an instrument and different transport/budget policies are
not combined into this optimization. These are model boundaries, not new exchange
capability claims. The objective counts native physical plans, not the number of
returned concrete feeds; a feed that already fits can retain multiple endpoint
plans in one concrete feed.

Hydration order and the public logical configuration remain intact. Physical bins
can contain noncontiguous instruments; symbols/native names within a returned bin
keep original order. Exact memberships, handler registrations, identities,
transport scope, book ownership and snapshot parameters are preserved. The SDK
adds no cross-socket market-event ordering guarantee.

## Proof and search

Each fixed route is optimized independently; its optimum counts add because a
physical socket cannot combine these distinct endpoint routes. The planner:

1. Accepts an already fitting configuration at its fixed-route lower bound.
2. Measures generated native demand. Count budgets use streams/args/messages;
   byte budgets remove the fixed JSON envelope and retain the existing metadata
   reserve. Every singleton is checked against the original fits predicate.
3. Uses ceil(native demand / capacity) as a lower bound. When singleton demand
   sums to the whole demand, costs are disjoint/additive; their greatest common
   divisor tightens unusable capacity remainders safely.
4. Constructs contiguous and first-fit-decreasing upper bounds. A feasible upper
   bound matching the lower bound is immediately optimal.
5. Otherwise exhaustively tests all smaller bin counts in ascending order with
   iterative assignment. Only empty bins are interchangeable. Equal scalar loads
   are not treated as equivalent when native aliases can overlap. Proven additive
   demand also permits a remaining-weight/remainder bound.

Search uses the existing adapter budget predicate and checks the materialized
layout. It does not assume first-fit is optimal. The 48-instrument Bitget example
has costs [3,3,2 repeated 46 times], total 98 and capacity 49. Both contiguous and
first-fit-decreasing need three sockets. Exact search finds two bins of 49, matching
the lower bound. No native URL, API version, message schema or limit was changed.

## Work, cancellation and failure

The evaluation budget is shared across routes of one compilation and covers
optimizer assessments, bound checks and placements. Catalog hydration and common
native route projection are separate work. This is an operation budget, not a
wall-clock deadline or a heap-byte limit. Worst-case exact packing is exponential;
a finite budget can reject a valid feed whose optimum has not yet been proved.
Input-sized state and iterative search avoid input-depth recursion.

The async runtime offloads enabled planning to blocking workers. At most two SDK
async planning jobs hold workers process-wide; queued futures wait cancellably.
A dropped future signals its worker, which checks cancellation between planning
steps/native calls. An in-progress synchronous serialization is not preempted.
A started job keeps its admission permit until it exits, including after caller
cancellation. Socket reads and runtime command handling stay on async workers.
Direct synchronous connection_feeds callers own their scheduling.

A failed/cancelled replacement does not commit its candidate. Other feeds and the
old generation remain active. No WebSocket/HTTP admission is acquired by search,
no catalog/network fallback is constructed, and no approximate plan is labelled
optimal. CPU admission is separate from exchange request quotas.

## Evidence

Fourteen new regressions cover independent exhaustive small-instance oracles,
exact solutions missed by greedy packing, shared aliases, count/byte budgets for
all five families, native mappings/pair coverage, uniform optima, price-topic
coalescence, budget/overflow/cancellation behavior, two-worker admission, dropped
future cancellation and healthy replacement isolation.

```bash
cargo run -p cryptofeed-rs --example minimum_connections
```

The offline synthetic example prints `3 -> 2 connections, native args [49, 49];
lower bound 2`. Generated names are caller assertions, not live directory evidence.
The reference is `sample_data/connection-planning.minimum.json`. It exercises no
exchange connection or live rate limit. Adaptive/distributed quotas, other venue
HTTP weights and broader replay edge audits remain separate alignment work.

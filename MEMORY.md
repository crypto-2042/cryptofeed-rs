# MEMORY

Project-specific memory for `rust/cryptofeed-rs`.

## Structural Memory

- The Rust workspace lives at the repository root.
- Short directory names are intentional:
  - `crates/core`
  - `crates/ticker`, `crates/trade`, `crates/orderbook`, `crates/candles`,
    `crates/funding`, `crates/liquidations`, `crates/openinterest`,
    `crates/index`, `crates/markprice`
  - `crates/runtime`
- Package names keep the `cryptofeed-` prefix.
- Exchange implementations are centralized in `crates/runtime`.
- The category crates are intentionally thin API crates (normalized models + handler traits), while runtime owns transport/protocol execution.
- Prefer `prelude` exports (`cryptofeed_rs::prelude::*` and crate-local preludes) to keep examples and user code imports stable.

## API Policy Memory

- The Rust implementation must follow the latest stable official exchange API, not whatever legacy API exists in Python.
- Bitget is the canonical example: Rust uses official v3 instruments, topics,
  and payload shapes rather than relabeled v2 fixtures.
- Official vendor docs are the source of truth for exchange endpoints and channel naming.
- Binance spot `bookTicker` has no `e`/`E`; USD-M book/depth and
  trade/kline/mark-price/liquidation currently require public/market WebSocket
  connection splitting.
- Bitget v3 successful `event: subscribe` objects have no market `data` and must
  be classified before topic dispatch.
- Bybit v5 spot `tickers` is a 24-hour statistics feed without BBO. The project
  Ticker contract uses `orderbook.1` for Bybit spot; derivatives keep their
  native ticker channel.
- Gate `quanto_multiplier` is not product identity and may be zero in a live,
  non-delisted inverse contract record.

## Runtime Memory

- A feed is product-homogeneous. Spot, perpetual, and dated futures must not be
  mixed in one `ExchangeFeed`.
- Symbol discovery and reverse parsing are product-qualified. Do not reintroduce
  separator stripping or quote-currency guessing as a fallback.
- Capability validation is authoritative: only combinations in the verified
  matrix may open a connection.
- Normal remote WebSocket closure is reconnectable; explicit shutdown is clean.
- Exchange application heartbeat and idle detection belong to the shared
  session lifecycle.
- Terminal failure of one feed must remain observable without cancelling healthy
  feeds.
- Never relax an L2 bridge/gap rule solely because a live bootstrap is
  intermittent. Capture the failing snapshot ID and buffered sequence, then add
  a bounded resnapshot regression.

- Binance currently has the most mature public runtime path:
  - websocket URL planning
  - live websocket connect
  - combined-stream unwrap
  - ticker/trade/l2_book/candles/funding/liquidations parsing
  - snapshot parser and delta sequence parsing
  - local book sync primitives
  - bootstrap snapshot dispatch
  - gap -> resync scheduling
  - handler dispatch
  - reconnect/backoff
  - concurrent feed execution
  - graceful shutdown
- Bitget v3 currently has:
  - official public websocket URL
  - live subscription payload generation using `instType/topic/symbol`
  - ticker/trade/l1_book/l2_book/candles parsing; derivative ticker funding/OI/index/mark-price events (2026-10-08)
  - runtime-side dispatch and text-message processing
  - books snapshot/update sync semantics
  - books sequence gap detection
  - live websocket session now sends real subscribe payloads
  - it is now much closer to Binance in public runtime coverage, but Binance still has the more advanced order book bootstrap/resync path
- Bybit V5 currently has:
  - official public spot websocket URL
  - live subscription payload generation
  - ticker/trade/l2_book/candles parsing
  - runtime-side dispatch and text-message processing
  - dedicated order book sync state
- OKX v5 currently has:
  - official public websocket URL
  - live subscription payload generation
  - ticker/trade/l2_book/candles parsing
  - runtime-side dispatch and text-message processing
  - dedicated order book sync state
- Gate.io API v4 currently has:
  - official public websocket URL
  - live subscription payload generation
  - ticker/trade/l2_book/candles parsing
  - runtime-side dispatch and text-message processing
  - dedicated order book sync state

## Testing Memory

- Some exact-name test filters from the original plan do not match Rust’s fully qualified unit test names.
- When needed, use qualified exact names such as `module::tests::name`.
- Current clean verification baseline for the Rust workspace is:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-features --all-targets -- -D warnings`
  - `cargo test --workspace`
- Manual live-smoke outcomes belong in dated `docs/reports/live-smoke-*.md` reports and
  remain outside CI.
- Model contract tests (including serde roundtrip checks) should live in the category crates first, with runtime tests focused on exchange parsing/routing behavior.
- Full-session doubles drive the generic `consume_*_session_with` helpers over
  `WebSocketStream<DuplexStream>` pairs; extend those helpers rather than
  spawning real connections in tests.
- OKX checksum validation is scoped to the full-depth `books` channel. The
  string reconstruction relies on `Decimal` preserving transmitted scale;
  `books5`/`bbo-tbt` checksums only cover transmitted top levels and must not
  be compared to the local book.
- Gate.io bootstrap failures trigger a bounded in-session resnapshot
  (`GATEIO_MAX_RESNAPSHOTS` per symbol) that preserves the buffered deltas;
  only the limit-exceeded case escalates to the session-level retry. Never
  relax the bridge rule to make a live bootstrap pass.
- Gate.io bootstrap dispatch returns the snapshot plus the applied buffered
  delta events; a bootstrap that silently drops buffered deltas from the
  handler event stream is a defect (Binance already followed this pattern).
- Gate.io `full: true` pushes replace the local book and re-anchor the
  sequence at the push's `u` (announcements 44678/44722, mainnet
  2026-05-06); deltas before the push are superseded by it.
- Gate.io reconnects must regenerate subscription `time`; subscribe responses
  are successful only when `error` is null. An id-less REST book snapshot is
  not consumed until a WebSocket delta is buffered to provide an anchor.
- Binance spot partial-depth pushes (`@depth5/10/20`) carry only
  `{lastUpdateId,bids,asks}` — no `e`/`s`/`U`/`u`; the instrument lives in
  the combined-stream `stream` name and each push replaces the top-N book.
  Partial-depth streams (all products) must be bootstrapped from a snapshot
  of the same width.
- OKX checksum strings interleave per index (`bid1:sz1:ask1:sz1:...`), not
  all bids then all asks; OKX VIP4 tbt depth (50/400) stays rejected at
  preflight until a live capture allows wiring.
- Binance idle detection needs > 3 minutes (server ping cadence): the 90s
  default killed quiet feeds; Binance uses a 240s idle timeout.
- L2 snapshot REST fetches carry a 15s HTTP timeout and pending-delta
  buffers are bounded; a dropped buffered delta surfaces as a non-bridging
  bootstrap and routes into the bounded resnapshot path.

## Operational Memory

- `cargo` commands in this environment often regenerate `rust/cryptofeed-rs/Cargo.lock` and `rust/cryptofeed-rs/target/`; remove them before commits.
- This repo frequently hits transient `.git/index.lock` races; check whether the lock still exists before trying to clean it up.
- The Binance developer docs platform (developers.binance.com, Zudoku) is fully
  client-rendered and returns no content to plain HTTP fetches; bybit-
  exchange.github.io (Bybit v5) and okx.com/docs-v5 remain fetchable. When
  verifying Binance protocol facts, prefer the repo baseline
  (`docs/exchange-protocol-baseline.md`, verified 2026-08-04) and sanitized
  captures, and record the verification gap instead of inventing payloads.
- Bybit v5 exposes no standalone index-price channel (verified 2026-08-06):
  `indexPrice`/`markPrice` ride the derivative `tickers.{symbol}` stream.
- Candle interval and L2 depth levels are validated per exchange during feed
  preflight; the normalized interval vocabulary is Binance-style
  (`1m`..`1M`), and each adapter maps it to its own wire form. Never add a
  wire form to the normalized vocabulary without adding the exchange mapping.
- OKX MARGIN instIds are identical to spot (`BTC-USDT`); the product kind is
  carried by `InstrumentKind::Margin`/`Symbol::margin` (explicit `.instrument`
  only) and events are rebound to the feed instrument in
  `OkxAdapter::parse_messages_for_feed`. Payload parsers alone can never
  recover the margin identity.
- OKX order-book channels: `books`/`books5`/`bbo-tbt` plus VIP4+-gated
  `books-l2-tbt`/`books50-l2-tbt`; `books50-l2`/`books400-l2` do not exist.
  Bybit options depth is `orderbook.{25,100}` only — options default to
  level 25.
- Exchange protocols drift: Bybit removed `funding.{symbol}` (funding now on
  the tickers stream), OKX moved `liquidation-orders` to instType scope and
  transmits `checksum: 0` on books, Gate.io dropped the order_book `id`
  field, and Binance eapi optionSymbols carries `underlying` instead of
  `baseAsset`. Live smoke runs are the only way to catch these; keep
  baseline fixtures updated with the measured facts and dates.
- Catalog hydration uses a 24h in-process cache because Binance exchangeInfo
  (~17.5 MB) downloads take 6–60+ seconds in this environment. HTTP status and
  exchange error envelopes are validated before caching, and responses are
  capped at 32 MiB. Live WS connections to
  binance.com/ws.okx.com exhibit transient slow startups — a smoke window
  shorter than ~40s may show zero events through no fault of the runtime.

## Pre-push review lessons (2026-10-08)

- Official numeric field semantics override legacy Python mappings: Binance
  `P` is a settlement price; OKX `oiCcy` is a coin quantity. Neither is a
  rate/currency identifier. Correct fixtures and public model contracts together.
- JSON numbers must retain decimal precision before Decimal parsing.
- Funding is perpetual/swap-only; sharing a price transport does not enable
  Funding for dated futures. All five dated Funding requests fail preflight.
- Binance Index uses documented `markPriceUpdate.i` and event time `E`, sharing
  the 1s mark-price topic when requested. `T` is next funding time, not Index time.
- Bybit derivative tickers require per-session snapshot/delta reconstruction;
  absent delta fields retain prior known values and reconnect clears the cache.
- Bitget v3 normalized hours/days map to uppercase `1H/4H/6H/12H/1D`; reject
  undocumented longer intervals. OKX `1M/3M` use UTC+8 calendar arithmetic.
- Bitget book sync and market-wide liquidation dispatch bind to hydrated
  native-symbol mappings to preserve dated-series identity.
- Resolver 3 honors Rust 1.85 for fresh lockfile-free resolution. Audit the
  resolved dependencies; never hide a vulnerability to pass a release gate.
- Confirmed GitHub repository: https://github.com/crypto-2042/cryptofeed-rs.
  Rust authorship is unset rather than attributed to the Python author;
  GitHub private vulnerability reporting was enabled and verified on
  2026-10-08. License/provenance remains pending for a formal release;
  source submission to GitHub was explicitly authorized by the maintainer.

## Near-Term Priorities

- Preserve the five-exchange spot/perpetual/futures matrix with sourced offline
  fixtures before expanding it.
- Use `PARITY.md` as the source of truth for verified capabilities.
- Add deeper deterministic session doubles and remaining checksum coverage.
- Complete release/license metadata before publishing.
- Coinbase/Kraken and additional public channel families require a new explicit
  plan before implementation.

## Scope Memory

- The 0.1 runtime supports only spot, perpetual/swap, and dated futures.
  Options and MARGIN protocol helpers, fixtures, and parser tests stay on
  mainline as future references, but every such feed is rejected by the
  authoritative capability matrix until a new explicit plan reopens it.
- Git branches are for experimental work that has not completed the harness
  loop (fixture + parity assertion + capability-matrix cell + docs). A branch
  merges only after its cells are green on mainline rules.

## Public channel completion (2026-10-08)

- No standalone topic does not mean no public data: Bitget v3 derivative
  `ticker` includes funding, OI, index, and mark price. Preserve absent fields
  and native units instead of fabricating rates, denominations, or USD values.
- Bitget L1 uses `books1` on spot/contracts and must not alter a full L2 book.
- Gate.io `futures.public_liquidates` is public; `futures.liquidates` is private.
  Only perpetual capability is enabled; delivery requires separate evidence.
- OKX native SWAP/FUTURES IDs map to base/quote index IDs. One shared index
  push fans out to all matching configured contract symbols.
- Binance contract OI is explicitly deferred by the user (2026-10-08), not
  merely waiting for a parser implementation. No verified official native
  perpetual/dated-futures OI WS path was found; current OI REST is available
  via weight-1 single-symbol requests. Multi-symbol polling/retries can
  exhaust the shared IP budget, so do not add a polling fallback or guessed
  WS topic. Options OI WS is separate. Reopening requires an explicit
  user-approved plan; see `docs/binance-open-interest-decision.md`.

## Python usage alignment

- The sibling Python checkout does not provide general automatic glob expansion
  in its feed constructor. Its catalog refresh and runtime `add_feed` are
  distinct from in-place subscription updates and auto-following listings.
- `runtime::run_with_shutdown` already existed; the FeedHandler facade now
  exposes it. Do not claim service-controlled shutdown was previously absent.
- Public `MarketCatalog` uses current catalog fetchers/cache. Selection is an
  explicit, product-qualified snapshot; unmatched initial patterns fail. Native
  capacity sharding and opt-in listing reconciliation are implemented; see
  `docs/connection-planning.md` and `docs/discovery.md`.

## API currency review — 2026-10-09

- OKX announced port 8443 retirement for 2026-10-31; public/business WS use
  default TLS port 443. Global REST now uses recommended openapi.okx.com;
  www.okx.com remains a supported alias, not a deprecated protocol.
- Current OKX SPOT discovery can include preopen rows with empty currencies.
  Filter non-live rows; malformed remaining identity must return an error,
  never reach Symbol constructors with empty components or guess currencies.
- Binance API version numbers differ by product; fapi/dapi v1 are not old
  solely because Spot REST uses v3. Official product documentation wins over
  numeric-version comparisons or Python implementation assumptions.

## Catalog refresh semantics

- `MarketCatalog::load` retains the 24-hour cache default; `refresh` bypasses
  cached pages and returns a new snapshot. Neither changes running feeds.
- Requests coalesce by exact URL and transport scope while overlapping, including
  failed results; later calls retry after failure/cancellation. Distinct routes
  and URLs are independent.
- Preserve prior cache entries after HTTP/JSON/envelope failure. Cache commits
  are per response, not transactional across pages or parsed registry validation.
- Catalog/snapshot clients are reused by transport scope. Snapshot concurrency,
  start pacing and native subscription sizing are implemented; they are SDK
  resource policies, not a distributed/shared-IP rate guarantee.

## Per-channel subscriptions

- `.subscription` / `.subscription_instruments` are exclusive with legacy
  shared channel/symbol configuration; handler and interval/depth settings can
  be reused. Repeated channels merge; symbols deduplicate in first-seen order.
- Hydrate the logical union before partitioning so native mapping ambiguity
  cannot escape across channel groups. Explicit native lists follow that union.
- Exact unequal channel/symbol sets share native connections, with capacity
  shards and required endpoint/product separation. Low-level callers still
  compile `connection_feeds` before endpoint planning; FeedHandler does so.
- Managed state exposes logical identity and physical connection evidence.
  Contiguous packing does not prove globally minimal connection allocation.

## Connection and snapshot budgets

- Connection feed compilation now also shards concrete sets using adapter-
  generated native topics/messages. Deduplicated shared price topics count once.
- Bybit spot's ten-arg constraint is per request, not per connection. Its
  connection character budget is separate; do not replace one with the other.
- Session-owned subscription queues retain reads and heartbeat during pacing.
  Gate time is generated at actual send, not once for an entire long queue.
- Handshake and snapshot budgets are process-local SDK policies, not a promise
  to account for other clients' IP usage. Keep official limits distinct from
  SDK ceilings and recommendations in `docs/connection-planning.md`.
- Snapshot limits do not relax sequence bridges. Cancellation must release
  queued/active slots; HTTP timeout begins after snapshot admission.
- With paced Gate subscription queues, initial REST bootstrap must wait until
  that symbol's first depth delta is buffered. Do not prefetch all Gate books
  at connection start; full pushes need no REST bootstrap.

## Managed runtime control

- Enabling control_handle opts into independent initial startup and a retained
  controller. Existing add_feed returns (), add_feed_with_id retains initial
  identity, and raw event/callback APIs remain compatible.
- Configuration generations distinguish replacement attempts (including failed
  or cancelled attempts); reconnect retains the same configuration generation.
  Use FeedEnvelope identity to reject stale queued events; models are unchanged.
- Validate/hydrate/admit before stopping old tasks. Once committed, replacement
  completes even if its reply receiver disappears. Preparing cancellation must
  leave the old generation running. Remove acknowledges async child termination.
- Fresh SDK book/ticker state must not mutate an older clone. Caller-owned
  handler state is not reset. Per-handler admission reserves max(old,new) for
  same-exchange replacement and both exchanges during a provider change.
- Started means task launch, not remote-ready. Registry entries are not health
  snapshots. Runtime/shutdown aggregate historical terminal failures; command
  validation errors alone do not poison a healthy runtime's final result.
- Readiness and opt-in periodic catalog reconciliation are implemented; see
  docs/runtime-control.md, docs/readiness.md and docs/discovery.md. Optional
  ecosystem/resource-policy alignment remains active.

## Readiness evidence

- FeedInfo remains Copy registry metadata. FeedSnapshot from control.state is
  authoritative even after lifecycle lag. Started is launch; Subscribed requires
  all native confirmations; Ready also requires requested SDK L2 initialization.
- Managed Binance uses explicit SUBSCRIBE/id/result-null, while legacy URL-based
  sessions remain available. Bybit/Gate correlate request IDs; Bitget/OKX match
  requested argument identity. Do not count arbitrary data or handshake as ack.
- Connection IDs are scoped to configuration generation; retries increment epochs
  and withdraw confirmations/books. Stale acknowledgements, snapshots and drops
  cannot update current epochs. Reconnect clears only owned book caches.
- Publish readiness transitions under the state lock to preserve stop ordering;
  logging stays outside that lock. Metrics query is not a durable notification log.
- Snapshot times are local diagnostic receipt/publication times, not replacements
  for model exchange_ts/received_ts. Counts are per configuration generation.
- Initial invalid configured IDs stay available for repair/query in managed mode;
  rejected dynamic adds are not committed. Listing reconciliation, multiple
  handlers and L2 recovery are implemented; ecosystem/resource work remains.


## Automatic directory reconciliation

- DiscoveryFeed is opt-in and owns one managed feed identity. Use a symbol-free
  channel template and product-qualified per-channel patterns; default five
  minutes, SDK minimum one minute. Force refresh each sequential cycle.
- Initial patterns are strict; later disappeared patterns/channels may be omitted.
  Total empty selection and fetch/validation failures preserve the last nonempty
  feed and back off. Native mappings must come from the exact catalog snapshot.
- Replace only through compare-and-replace inside the feed worker. Manual
  replacement/removal terminates discovery ownership; state-query-then-replace
  alone races. Unchanged healthy selections avoid restarts.
- Stop/drop cancels future polling; accepted replacements settle. Explicit stop
  returns the final owned identity and leaves the feed registered. Current means
  directory success; query runtime readiness independently. No OI polling fallback.
- Catalog eligibility is public-data policy, not order permission. Preserve
  Bitget limit_open/limit_close and Gate buyable/sellable; exclude explicit
  unavailable statuses. Typed directory metadata and core consumer policies
  are implemented; optional ecosystem/resource-policy work remains pending.


## Candle completion policy

- CandlePolicy filters at the common runtime dispatcher before handlers, both
  broadcasts, counters and normalized observations. Default All stays compatible;
  ClosedOnly requires Some(true); ClosedOrUnknown keeps None unchanged.
- Never infer completion from receive time, candle end, next bars or reconnect.
  Bitget currently has no finality flag, so strict mode emits no candles there.
- Python's inspected default is closed-only, but Rust preserves its existing
  behavior with explicit opt-in. Multiple handlers and recoverable L2 are
  implemented with separate documented delivery contracts.


## Multiple callback registrations

- Preserve primary *_handler setter replacement; add_*_handler appends after
  the primary in registration order. Add-only works; duplicates intentionally run.
- Every callback gets an independent model clone. Update books and publish/count
  once before fan-out, after channel/symbol/candle filters.
- Each callback owns a five-second timeout; drop on timeout and continue. Catch
  unwind around construction/polling and continue after panic; abort panics and
  non-yielding/blocking code are not cancellable guarantees. Traits return (),
  so application errors remain caller-owned. Trace callback failures separately.
- Serial callbacks block session reads; cross-session shared Arcs can run
  concurrently. Bounded shutdown may cancel before remaining callbacks; no
  transactional/exactly-once delivery or detached-task cleanup is implied.


## L2 recovery continuity

- FeedHandler.l2_book_handle opts into managed startup and a shared optional
  cache. recover(identity,symbol) must acquire full snapshot and new receiver
  atomically under the same lock as all publication/invalidation.
- Anchors are local identity/physical connection/epoch/revision, never native
  sequence guarantees. Require contiguous same-owner deltas after a snapshot;
  on Lagged replace queue/state, never splice old buffered deltas into recovery.
- Cache normalized dispatch order separately from internal bootstrap caches,
  which may already include buffered future events. Assemble full arrays only on
  recovery, preserving latest applied timestamps/units/precision.
- Resync withdraws one book; disconnect/drop withdraws only that owner and blocks
  its late publications. New epochs require new snapshots. Stop/failure/removal
  retires cache; cancelling a candidate must not touch older active identities.
- One bounded 1024-update ring includes unrelated symbols, which can cause lag.
  Retained handles keep its sender alive after shutdown; use lifecycle/shutdown
  signals, not stream closure alone. No cache is allocated by default.


## Runtime budgets and retry reset

- RuntimeOptions defaults: None transient retry limit, 20-second handshake and
  five-second per-callback deadlines. Positive duration setters validate before
  building; options propagate with feed planning/replacement configuration.
- Runtime retry budgets are per physical connection. Successful completion of
  initial subscribe writes resets retries and backoff; partial queues/data before
  their completion do not. Legacy implicit Binance URL initialization is marked
  by the first non-control text. This is initialization, not remote readiness.
- Permanent errors still fail immediately; Some(0) is one attempt. Finite limits
  do not cap lifetime reconnect count when sessions successfully reinitialize.
- Handshake budget excludes admission/catalog/bootstrap/ack/idle time. Shared
  pacing, heartbeat/idle/ack policies and bounded shutdown remain intact. Callback
  budgets do not enlarge shutdown grace or preempt non-yielding caller code.
- Explicit HTTP/WS proxy, idle policy and start delay are implemented separately;
  Python idle timeout must not be mistaken for a handshake deadline.


## Startup and idle policy

- Startup delay defaults zero, runs once per physical supervisor after hydration
  and before shared admission, and does not repeat on retries. Replacement starts
  new supervisors and delays again. Shutdown/removal cancels the waiting future;
  false watch notifications must not restart the timer.
- IdlePolicy ExchangeDefault preserves verified policies; After(positive Duration)
  overrides transport receipt idle deadline; Disabled removes only that watchdog.
  Incoming controls/Ping/Pong/data count; outgoing heartbeat does not.
- Heartbeats/Ping-Pong handling and subscription acknowledgement deadlines remain
  active when idle is disabled. Short overrides may reconnect healthy quiet feeds;
  this is caller-owned policy. Startup is separate from connect/idle/ack deadlines.
- Explicit HTTP/WS proxy, idle and startup policies are implemented; keep idle
  receipt detection distinct from establishment/acknowledgement deadlines.


## Explicit transport routing

- TransportConfig is shared by feed catalog hydration, standalone/periodic
  catalogs, Binance/Gate bootstrap/resnapshot and every physical WS. Clone one
  configuration to reuse HTTP pool/cache identity; never lose it in pagination
  or resync jobs. HTTP snapshots still share one process-wide admission budget.
- Default HTTP and WS are explicitly direct; no hidden environment/system proxy
  inference. Existing HTTP-only inference was inconsistent; migration is explicit.
- HTTP proxy endpoints only, Basic auth via separate method, no userinfo/query/
  fragment/non-root path. Debug hides credentials/endpoint; headers are sensitive;
  CONNECT errors omit reason/body content and 407 is configuration failure.
- CONNECT is bounded, preserves following TLS bytes and original destination
  TLS/SNI verification. No fallback direct on proxy failure. Connection timeout
  covers proxy TCP/CONNECT/TLS/WS; cancellation owns and drops the socket future.
- Proxy configurations have process-local opaque cache IDs, never credential keys.
  Clones share results; separate/direct/auth configurations never coalesce caches.
- HTTPS/SOCKS/PAC/custom CA/mTLS are not claimed. Typed market metadata is
  implemented; optional ecosystem work remains active and feed scope unchanged.


## Exact sparse connection packing

- Preserve exact mapped pairs in adapters and capacity shards; never subscribe
  to the channel/symbol Cartesian product. Shards trim rules and remove empty
  channels while preserving first-seen normalized/native alignment.
- Native deduplication and mandatory product/public/business routes stay intact.
  Packing uses largest contiguous union slices under existing native budgets;
  it does not prove a global minimum across endpoints.
- L2 URLs, initial REST jobs, readiness totals/connection book sets and recovery
  owners must use the L2 subset. Gate resnapshot URL indexes use the filtered
  instrument order, not the full trade/book union. Trade-only depth cannot
  create sync/cache/HTTP bootstrap work.
- Changed send selections are captured alongside exact inline parity assertions.
  Five-exchange Spot unequal-set smoke reached Ready with one socket, three
  confirmations and one book each; ETH trades produced no ETH recovery book.
- Typed market metadata is implemented. Global allocation/resource policies
  and optional ecosystem work remain unfinished under the active alignment goal.


## Typed directory metadata

- MarketCatalog market/markets retains MarketInfo beside native identity, sorted
  consistently. Exchange/product and feature-filtered WS capabilities describe
  the catalog/build, not per-symbol permission or REST-history availability.
- Use explicit increments and separate decimal-place counts. Binance filters are
  keyed by filterType, never array position or pricePrecision. Bitget future
  multipliers/minQty are not Spot constraints; Bybit Spot minQty is deprecated.
- Preserve null/empty/inapplicable as None, zero as zero, and exact JSON/string
  Decimal/scientific values without float rounding. Fail applicable malformed,
  inexact, duplicate-filter or conflicting duplicate metadata rather than guess.
- Keep contract face value, currency, multiplier and settlement independent;
  do not infer absent currency or convert public trade/book units. Native limit
  metadata is not a complete order validator or market-order applicability rule.
- Metadata uses existing directory HTTP/cache/transport scope, adds no per-symbol
  request, and is snapshot-only. Metadata-only discovery refresh does not restart
  sessions. Explicit native mappings still bypass catalog hydration as assertions.
- Ten live Spot/Perpetual catalogs passed after an earlier COIN-M request/TLS
  failure; retain both observations. Optional ecosystem/resource-policy work
  remains active; no private/trading or new product capability was enabled.


## Public REST snapshots and shared HTTP

- PublicRestClient uses catalog-native identity and transport for ticker/L2
  snapshots, with data-category feature gates and separate REST capabilities.
  No subscription, polling, feed-counter or recovery-state mutation occurs.
- RestSnapshot carries optional native time/ID; models preserve the received-time
  fallback when native time is absent. Response-generation time is not quote
  event time. Native IDs are not local recovery revisions or cross-venue order.
- Gate Spot REST book time is milliseconds; derivative time is seconds. Select
  by schema/product, never magnitude; preserve the corrected reference/assertion.
- Directory/REST/bootstrap now share four active HTTP slots/one-second starts.
  Pacing/cooldown waiters release slots and recheck before starting. HTTP 418/429
  or unsuccessful responses with Retry-After defer that venue; missing 418/429
  header uses 60s SDK fallback. Other venues do not inherit cooldown.
- HTTP errors keep status/optional parsed header, omit bodies and clone through
  catalog coalescing. No automatic REST query retry; catalog HTTP errors return
  immediately. Network/JSON/envelope retry remains one, and queued work cancels.
- Fixed admission is not weight/native-code/distributed/shared-IP quota accounting.
  Trade/candle history remains pending; funding continuation is implemented.
  Recording/replay, sinks,
  aggregation, NBBO and advanced resource policy remain unfinished.


## Bounded funding history

- FundingHistoryQuery uses half-open integer-ms UTC ranges, page_size/max_pages
  1..100, and <=10,000 raw scanned rows/call. Budget includes out-of-range rows;
  oversized pages fail. Errors/cancellation do not return hidden partial success.
- Binance pages forward observed max+1ms; Bybit/OKX/Gate backward observed min;
  Gate seconds convert explicitly. Bitget v3 uses numeric cursor 1..100 and
  resultList, not legacy pageNo. Its moving offset pages are not atomic history.
- Version-1 JSON cursor binds exchange, normalized/native identity, original
  range, page size and position; validate before HTTP. Batch page budget may
  change on resume, page size may not. Treat cursor as public state, not a secret.
- Return ascending records within a batch, dedup exact settlements and fail
  conflicting rates/mark prices or repeated full pages. Backward resumed chunks
  need merge/sort across batches. No interval/next rate is invented.
- OKX actual realizedRate is mandatory in the target segment; fundingRate is
  prediction. Binance non-Regular types are unsupported. All next/predicted
  model fields stay None; native signed rates stay exact ratios.
- Stop reason distinguishes range, source exhaustion, budget and native limit;
  never claim full retention/settlement coverage. Live five-venue first/resume
  observed 10+10 records each, all budget-limited with further cursors, exit 0.
- Trade/candle history, recording/replay, sinks/aggregation, NBBO and advanced
  resource policy remain active; dated funding/private/trading/OI scope unchanged.

## Bounded candle history

- Binance and Bybit are the first candle-history venues; test availability with
  REST supported_channels(), distinct from WebSocket catalog capabilities.
- Advance bounded backward windows even for empty pages, cap requests at 200 days
  and 100 rows/pages. Monthly calculations are calendar-based. Scoped JSON cursors
  continue older ranges; RangeBoundary is no retention guarantee.
- REST scheduled candle end is not finality: both venues retain closed=None.
  Native volume units and Binance count/inclusive end are preserved. Bybit end
  is next UTC interval/calendar boundary minus 1ms; exchange_ts is open time.

## Remaining candle-history venues

- Bitget/OKX/Gate now join Binance/Bybit. Gate delivery candles use the current
  documented /delivery/usdt/candlesticks path; earlier rejection came from an
  incomplete document inspection and has been corrected.
- Bitget v3 history has 90-day ranges and aligned end bounds. Exactly one earlier
  overlap is allowed/counts against scan budget, then filtered for next window;
  never relax all-venue boundary/duplicate rules.
- Gate from rounds down to a candle open; send first legal open, omit limit with
  from/to, use seconds. Spot eight-field rows use base volume plus close flag;
  perpetual v uses contracts, no completion flag. 30d is calendar, not fixed days.
- OKX unsuffixed monthly/quarterly boundaries use UTC+8 and native confirm.

Gate weekly boundary verification: current Spot `7d` bars open Monday (epoch
remainder four days), while perpetual `7d` bars are epoch-aligned, as documented
for futures. Request rounding uses the product-specific anchor. Public September
2026 probes confirmed both grids and UTC-midnight daily opens. Monthly Spot and
perpetual `30d` probes each returned the September 1 UTC open. These observations
are separate from the ten-product 1m smoke; offline tests assert both week grids.

## Public recent trades

- PublicRestClient recent_trades is one bounded trade-feature batch on five current
  public surfaces. Keep native limits (Bybit Spot 60, Bitget 100, OKX 500) and the
  SDK Gate derivative cap distinct from documented exchange maxima.
- Native units, exact Decimal prices/amounts and string IDs are preserved. Binance
  isBuyerMaker=true means taker Sell. Gate signed contract size determines side;
  normalize amount to abs. Bitget execId is the execution ID, not execLinkId.
- Stable exact timestamp sorting precedes f64 model conversion; never coalesce
  distinct same-time IDs. Gate Spot create_time_ms counts milliseconds; contract create_time_ms is
  fractional seconds despite its name. create_time is seconds for both.
- Binance recent /trades uses individual IDs, distinct from WS/Python aggTrade IDs.
  Future historical aggregate API must name that granularity. No recent-call
  completeness/cursor guarantee, feed publication or recovery anchor is implied.

## Native trade history

- Explicit granularity is mandatory: Binance aggregate IDs differ from recent
  individual IDs; OKX/Gate history is Individual. Report history capabilities
  separately from recent Trade. Bybit/Bitget remain recent-only; Gate delivery range history is one page,
  SourceLimit if full, with no current native continuation.
- Seed Binance with <=one-hour times, then fromId only; never combine IDs/times
  or advance dense same-time pages by milliseconds. OKX type=2 after time seeds,
  type=1 after ID continues. Gate fixed from/to with page or offset avoids retired
  last_id. Native/SDK offset ceiling 100,000 has explicit SourceLimit.
- Count enclosing-second Gate boundary rows against budget, filter exact Decimal
  times; batches can be empty with continuation. Cursors bind exact mapping,
  range, kind and page size. No retention/atomic-completeness claim.
- Gate contract create_time_ms is fractional seconds, unlike Spot milliseconds.
  Positive/not-future smoke checks were insufficient; recent BTC smoke now also
  requires age <=300s. Preserve the correction and earlier weak evidence.

## Normalized event recording

- recording is optional/non-default and uses only enabled public normalized model
  variants. FeedEvent/identity serde derives are gated; no feed/config/status/error
  serialization. Raw protocol recording/replay remains a separate pending scope.
- Version 1 JSONL uses external tags; internal/adjacent serde buffering with
  arbitrary_precision can misdeserialize f64 as maps. Never disable numeric
  precision to work around this; tests roundtrip all categories and long Decimals.
- Reserve footer bytes and cap lines/total/events. Lag makes capture fail without
  footer. Cancelled/failed partial I/O poisons state; replay drop/cancel/error must
  not permit resuming after a record was read but not delivered.
- Original model clocks survive; saved elapsed_ns is recorder observation timing.
  Replay schedules absolute elapsed deadlines to avoid callback-induced drift.
  Recorded identities are file-local source labels, not live control IDs/epochs.
- No L2 recovery/lifecycle or atomic callback transaction is claimed. Ending
  validates sequence/count/EOF, not retained completeness or cryptographic integrity.

## Public raw WS observation

- Builder raw_capture attaches before Session text heartbeat/readiness/control
  filtering. Capture successful sends, received text and session lifecycle, with
  immutable public mappings/policy context, no transport/URL/header/handler data.
- Keep sanitized WS observation separate from normalized JSONL; persisted raw
  codec/HTTP/bootstrap/session replay remains pending. JSON semantics/precision
  survive, whitespace may change, so this is not a byte-exact packet recorder.
- Auth/private topics reject; known credentials and diagnostic strings redact;
  preserve fixed success/pong controls. Never echo rejected payloads in errors.
- Queue failure is sticky and source processing remains healthy. The receiver
  must not retain a Sender through shared state, or producer closure never arrives.
- WS model and raw observation receipt clocks share the pre-JSON read timestamp.
  Reconnect/session IDs are process-local and distinct from feed generations.

## Raw WS file codec

- raw-ws version-1 JSONL is distinct from normalized recording and single raw DTO
  references. Header/observation/footer schema, sequence/context/lifecycle/privacy
  validate on both sides; no parse/session execution or HTTP fetch during reading.
- Complete requires all sessions closed. Stopped/LimitReached certify deliberate
  prefixes; segment starts at observer sequence one, with no rotation/merging yet.
- Session metadata is frozen and interned after equality validation, with a
  bounded total session count. Reused IDs/data after close or unredacted input fail.
- Encoding/line reads share normalized bounded helpers; cancelled/failed I/O
  poisons state. No snapshot recovery/native protocol completeness is implied.

## Native raw parser replay

- Replay uses a fresh local ExchangeFeed per recorded session, current native
  plans/parsers and dispatch; never call runtime run/transport/hydration. No live
  observer/callback/counter/book-store handles attach. Metadata maps are explicit.
- Bybit ticker and WS-native Bybit/OKX/Bitget book caches reset on close/reconnect.
  Binance/Gate L2 require captured HTTP and currently fail before callbacks.
- Do not mistake Bitget first-update overlap for a gap: snapshot seq must lie in
  [pseq,seq]. Synthetic disjoint [12,13] vs snapshot10 is a real failure probe.
- Parser outputs use a bounded local queue with explicit overflow/batch/total
  errors before delivering that frame. Native body/receipt clocks are preserved.
- Start/end callbacks reflect parser session lifecycle, not remote-ready health.
  Limited EOF does not invent a close; consumers use summary to end prefix state.
- Replay drop/cancel/error makes its reader terminal. Transport timers/readiness,
  HTTP/L2, rotation and broader ecosystem work are not implied by parser replay.

## Consumed snapshot replay v2

- Capture HTTP response at main-loop consumption, not async completion order.
  Record receive first, consumed snapshot(s), then Processing(received_sequence).
  This preserves buffered bridge vs strict live-next distinctions. Original HTTP
  receipt clock is separate from consumption/WS clocks; preserve all of them.
- Raw v2 observes actual processing; pongs/acks or cutoff packets can remain
  unprocessed and must not invent model outputs. V1 stays directly parsed and
  explicitly cannot replay Binance/Gate HTTP-dependent L2. Session versions freeze.
- Snapshot receivers carry optional raw slots only when capture is enabled.
  Offline SnapshotMode returns a receiver with retained sender, no spawned fetch.
  Inject recorded ready responses and reuse native polls/buffers/resnapshot logic.
- Snapshot/failure scope must match an L2 symbol and pending received input.
  Replay rejects missing pending bootstrap/width mismatch, never fetches fresh data.
  Safe failure categories/status omit error text/headers/retry metadata.
- This covers consumed L2 JSON, not all catalogs/HTTP, transport timers or arbitrary
  concurrent callback order. Focused mixed-route projection/cache-reset audit is complete (see below);
  arbitrary concurrent replay equivalence remains unclaimed.

## Physical connection projection and cache ownership

- Filter sparse rules together with channels/symbols/native names when projecting
  logical feeds to Bybit product and OKX public/business connections. Leaving
  stale sparse rules broke raw context validation. Reuse the same projection for
  native message budget measurement; preserve normalized/native positional order.
- Reset only exact requested L2 pairs owned by the physical route. Binance/Gate
  already had snapshot-route/instrument guards; preserve those. Seven offline
  regressions cover projection, recording context and independent book state.
- A focused ownership regression is not proof of arbitrary timer/interleaving
  replay. Broader product replay and generic HTTP capture still need work.

## Storage integration boundary

- EventSink/run_sink own a sequential consumer of identified events. No new queue
  or worker/retry policy: a slow sink may lag broadcast; lag is terminal.
- Drop sinks without finalization after write errors/panics/deadlines or in-flight
  shutdown/cancel. Remote commits may be uncertain; never promise exactly-once.
  Only between-operation shutdown produces Stopped. Finalization is deadline-
  bounded and consuming. RecordingWriter is the first adapter, keeping its own
  format, precision, limits, poisoning and I/O deadlines. Flush is not fsync.
- SinkWrite AcceptedAndFull accepts the final event; Full rejects it before
  writing. Both finalize with SinkEnd::LimitReached and an actual prefix summary. No Python backend collection,
  L2 snapshot synthesis, aggregation or NBBO parity is implied by this interface.

## Arrival-clock OHLCV

- Ohlcv lives in runtime behind trade, without candles/storage dependencies.
  Python's example groups trades by arrival clock, not exchange event time. Rust
  uses explicit monotonic elapsed time and fixed [start,end) windows anchored at
  zero; open/close follow arrival order. Out-of-order model clocks are retained.
- Isolate feed ID/generation/exchange/symbol, cap one-window series (1..=4096),
  emit in first-seen order, skip empty windows, and expose timer advance/partial
  consuming finish. Do not merge contract units, sources or generations.
- Decimal overflow/zero-rounded product and invalid input/clock/capacity/boundary
  failures are atomic. Finite Decimal precision still rounds products/division;
  price_volume is not necessarily quote turnover. No ID de-dup or gap recovery.
- Direct and recording-replay all bar fields match in deterministic tests. EOF
  does not close the last time window. Consumers handle lag/lifecycle/delivery.

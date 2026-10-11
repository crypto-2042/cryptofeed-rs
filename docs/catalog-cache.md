# Directory response retention

Directory HTTP responses now have a bounded process-wide cache. It covers the
current MarketCatalog, feed hydration and discovery paths, across direct/proxy,
HTTP capture and HTTP replay transport namespaces. It is a local optimization,
not a guarantee that every response stays cached for its full TTL.

## Retention policy

- At most 256 response entries.
- At most 128 MiB combined serialized JSON bytes and cache-key bytes.
- At most 32 MiB serialized bytes for one response, matching the existing
  directory HTTP response ceiling.
- 24-hour wall-clock TTL, with expiry at the boundary. A backwards clock makes
  future-dated entries unavailable and removes them conservatively.
- Every read/write sweeps expired entries, including namespaces never revisited.
- Capacity pressure evicts least-recently-used responses. Hits update usage order
  without extending the stored TTL; replacing an entry resets that entry's TTL.

These are SDK retention choices, not exchange request quotas. They are currently
fixed process-wide limits, shared across namespaces. HTTP admission/backoff and
connection/topic limits remain separately defined in
[connection planning](connection-planning.md).

Only serialized byte buffers are retained, with reference-counted immutable keys
and payloads. This provides direct payload/key byte accounting instead of guessing
the heap cost of a large serde_json::Value tree. Metadata/allocator overhead and
in-flight readers/caller snapshots are additional to the stated cache byte limit.
Encoding is bounded and decoding runs outside the global lock. Cache hits reparse
JSON, trading some CPU for bounded retained storage. Arbitrary-precision numeric
literals survive the serialization/parse boundary without float conversion.

Eviction releases cache ownership, not active reader references or existing
MarketCatalog/PublicRestClient/feed symbol snapshots. Those objects remain usable.
If a successful new response cannot fit cache policy, it is still returned to the
caller; any older same-key entry is removed so a later load cannot mistake it for
the refreshed data. Failed HTTP/JSON/envelope validation still leaves an existing
unexpired/non-evicted response untouched. The cache is per response/page, not an
atomic transaction across all instrument pages or later parsed-market validation.

## Replay must not advance implicitly

A live cache miss can refetch through the existing HTTP limiter. An offline replay
cannot infer that eviction/expiry should consume a later archived catalog response
which may belong to an explicit refresh. After a successful catalog fetch in that
replay transport namespace, a subsequent non-refresh cache miss therefore returns
an explicit configuration error and increments its mismatch audit. It consumes
nothing, does not use the next response and never falls back online.

Keep and reuse the existing immutable catalog/client snapshot, or explicitly call
`MarketCatalog::refresh_with_transport` to select the next archived snapshot.
Current in-flight/coalesced waiters can still receive their already completed
response. Markers are scoped to replay plus proxy/capture namespace, stored only
in the caller-owned bounded replay object; no second global payload cache is added.
This makes cache loss visible instead of changing archived directory chronology.
See [HTTP replay](http-replay.md) for scheduling/clock limits and strict audit.

Offline tests use small local caches to exercise entry/byte/LRU bounds, key costs,
replacement/oversize behavior, TTL/clock rollback, reference lifetime, numeric
precision and 10,000 independent namespaces. A scoped SDK integration forces one
replay cache eviction and proves the next catalog is not consumed until refresh.
No exchange protocol/fixture change or new live validation is claimed here.

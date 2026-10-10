# Public REST snapshot smoke — 2026-10-10

`rest_public` loaded each catalog, then queried normalized BTC-USDT Spot and
BTC-USDT-PERP ticker plus 20-level book on all five active exchanges. No account,
API key, WS or trading operation was used. Each product observation has a
60-second caller deadline around loading and both queries.

The final run completed all 20 snapshot queries, with 20 bids/20 asks in every
book, correct normalized identity, active-BTC timestamp-unit/local-clock sanity
and process exit 0. Native time observations were:

| Exchange | Spot ticker/book | Perpetual ticker/book | Native book sequence |
| --- | --- | --- | --- |
| Binance | absent / absent | present / present | present |
| Bitget | present / present | present / present | absent |
| Bybit | present / present | present / present | present |
| OKX | present / present | present / present | present |
| Gate | absent / present | absent / present | present |

Absent native time remained None in the result wrapper; it was not relabeled as
exchange time. Example final sequences included Binance Spot 101264143251,
Bybit/OKX/Gate values printed by their respective responses; they are unrelated
provider IDs, not throughput or cross-venue ordering measures.

The first run also completed the requests, but review found Gate Spot book time
reported as 1791621059661 seconds: the new decoder had applied derivative-second
units to Spot's millisecond schema. Current official Spot/futures field tables
confirmed the difference. The decoder was corrected by product, a near-epoch
non-magnitude regression added, and the existing sanitized Spot bootstrap
reference/assertion corrected to millisecond input. The final run reported Gate
Spot native time 1791621725.854 and perpetual 1791621745.736 seconds, passing the
new active-BTC check. The earlier run is not counted as passing time semantics.

No bridge/gap rule or expected market-data value was relaxed. This observation
covers Spot/Perpetual BTC snapshots, not live dated futures, pagination, rate-
limit events, every instrument or guaranteed quote freshness. SDK resource and
malformed/error paths are separately verified with offline doubles.

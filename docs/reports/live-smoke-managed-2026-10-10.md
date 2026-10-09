# Managed runtime smoke — 2026-10-10

The existing public OKX v5 trade protocol was exercised through the new managed
runtime example (`managed_public`). No credentials/private services were used.
Explicit native mappings avoided catalog hydration so the observation isolates
runtime add/replace/remove and event scope. Each observation waits at most 25
seconds; this is not a throughput or full-matrix test.

| Step | Observed identity | Public data observation |
| --- | --- | --- |
| Add BTC-USDT Trade | feed 1, generation 1 | One printed normalized BTC-USDT Trade |
| Replace with ETH-USDT Trade | feed 1, generation 2 | One printed normalized ETH-USDT Trade |
| Remove | same logical feed ID | Registry reported remaining feeds = 0 |
| Shutdown | managed runtime | Process exited with code 0 |

The printed events are selected observations, not total producer event counts.
The same logical ID and changed configuration generation were observed in real
normalized event-stream delivery. Old buffered events are filtered by identity in
the example. Offline tests separately verify invalid replacement preservation,
cancellation, state reset, admission and stop ordering.

This run does not prove every exchange, remote-ready status, all instruments,
large replacement workloads, lossless event delivery, or automatic listing
updates. It introduces no new protocol fixture or exchange capability claim.

The example was rebuilt and rerun after the final registry-removal acknowledgement
ordering correction. It again printed BTC generation 1, ETH generation 2,
remaining feeds 0, and exited with code 0. These final observations are distinct
from the multi-thread ordering regression and do not replace its assertions.

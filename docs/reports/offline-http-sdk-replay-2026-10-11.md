# Offline HTTP through existing SDK queries — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example http_replay -- RECORDING_PATH`.
Input was the temporary OKX Spot HTTP capture described in
[the recording report](live-smoke-http-recording-2026-10-11.md): 4 requests/8
observations, 1,252,409 bytes. This command used ordinary sandbox permissions and
only that saved file, without requesting live network access. Exit 0.

| Check | Observed |
| --- | --- |
| Catalog | 1,152 normalized symbols recreated by existing SDK code |
| Ticker | Archived quote decoded through PublicRestClient |
| L2 | Five bid levels and five ask levels |
| Recent trades | Five normalized trades |
| Audit | Four archived requests, four consumed, zero unfinished/cancelled/mismatches |

Console: `offline HTTP markets=1152 requests=4 consumed=4 unfinished=0 cancelled=0 mismatches=0 book_bids=5 book_asks=5 trades=5`.
The synthetic v1 format reference also ran successfully: one symbol, four consumed
requests, one bid/ask level and one trade, with no mismatch. SDK entry guards and
paused-clock tests independently assert HTTP-client/WS disablement and admission
bypass. This is payload re-execution, not a new exchange test, full live normalized-
clock equality or reconstruction of original cache expiry/concurrent scheduling.

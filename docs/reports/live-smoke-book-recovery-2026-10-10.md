# L2 recovery smoke — 2026-10-10

The `book_recovery_public` example used public OKX v5 Spot `BTC-USDT` L2 through
managed add/remove and the opt-in recovery handle. No private service or
credentials were used. The observation deadline was 35 seconds.

| Observation | Result |
| --- | --- |
| Stream observations | 3 normalized L2 updates |
| Identity/owner | feed 1, generation 1, connection 1, epoch 1 |
| Last applied local revision | 3 |
| Reconstructed local levels | 400 bids, 400 asks |
| Subsequent full recovery snapshot | same anchor, revision 3 |
| Removal | cached snapshot unavailable (`retired=true`) |
| Shutdown | process exit 0 |

The example required each delta to follow the previous anchor by exactly one
local revision and initialized state from the normalized snapshot. This verifies
actual runtime publication, snapshot retrieval and retirement. It is not a
benchmark, guarantee of full exchange depth, or evidence of packet loss/reconnect
in this short observation. Scripted offline regressions separately create lag,
resync and stale epochs. Existing official exchange protocol baselines remain
unchanged; this increment adds SDK delivery metadata and cache access.

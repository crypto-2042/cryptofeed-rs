# Consumed HTTP / L2 replay evidence — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example raw_l2_replay_public`.
Unauthenticated current public Spot BTC-USDT L2, direct transport, exit 0; no
first-attempt failures. Each capture used 200 raw observations, 32MiB total/
8MiB line/body and 16 sessions. Original models were independently drained into
a bounded ledger; lag/overflow would fail comparison.

| Profile | Observations | Consumed HTTP snapshots | Processing markers | Offline L2 models |
| --- | --- | --- | --- | --- |
| Binance Spot | 200 | 2 | 97 | 90 |
| Gate Spot | 200 | 2 | 97 | 98 |

Every offline L2Book exactly equalled a live model, including Decimal levels,
normalized identity, native exchange timestamp and original receipt timestamp.
Both live runtimes completed managed shutdown before offline parsing. Replay used
only saved bytes and replay receiver placeholders, not a live snapshot fallback.
Actual public bodies were not committed. These were intentional LimitReached
prefixes, not complete captures of all source traffic.

Model counts differ from raw/Processing counts because stale/full/reset/bridge
handling is native state logic; no count was forced or validator relaxed. Two
consumed snapshots per profile exercised bootstrap/resnapshot inputs. Separate
offline tests prove same consumption/origin order, id-less Gate anchoring,
strict live-next overlap resync and safe HTTP failure propagation.

This is two Spot full-depth workflows, not new derivative/dated/partial-depth,
general catalog HTTP capture, transport-timer simulation, arbitrary concurrent
callback-order or all-recovery-state certification. Those scopes remain explicit
in the guide and the full alignment plan.

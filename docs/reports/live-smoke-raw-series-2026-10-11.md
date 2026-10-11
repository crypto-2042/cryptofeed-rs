# Raw rotation and continuous native replay — 2026-10-11

Command: `cargo run -p cryptofeed-rs --features recording --example raw_series_public -- DIRECTORY NEW_SERIES_ID`.
Unauthenticated OKX Spot BTC-USDT trades, existing current adapter/direct transport.
Files/manifest were created with create_new in /private/tmp and are not committed.
The runtime completed managed shutdown before replay. Exit 0.

| Check | Observed |
| --- | --- |
| Capture | 40 raw v2 observations, 23,676 logical raw bytes |
| Rotation | 3 ordered physical JSONL parts, each at most 8,192 bytes |
| End | LimitReached prefix, open-session ending is explicit |
| Offline replay | 17 normalized trade models, OKX/BTC-USDT scope checked |
| Certification | Every referenced part/checksum, raw global sequence/context/lifecycle and manifest/footer summary validated |

Console: `raw series observations=40 parts=3 bytes=23676 offline_models=17 certified=true`.
Observation count includes control/processing records, not just market models.
This is one bounded live writer-to-ordered-files-to-native-replay workflow, not
full normalized-model equality against a separate live copy, all-product/L2 live
validation, crash durability, independent-capture merging or authentication.
Deterministic tests separately compare exact observations and full trade/HTTP-L2
replay outputs against a single logical file, including corruption/limit/error cases.
